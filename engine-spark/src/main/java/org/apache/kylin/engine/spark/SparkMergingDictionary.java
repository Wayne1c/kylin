/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
    merge dictionary
 */
public class SparkMergingDictionary extends AbstractApplication implements Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(SparkMergingDictionary.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segmentId").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create("metaUrl");
    public static final Option OPTION_MERGE_SEGMENT_IDS = OptionBuilder.withArgName("segmentIds").hasArg()
            .isRequired(true).withDescription("Merging Cube Segment Ids").create("segmentIds");
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).withDescription("merged dictionary resource path").create(BatchConstants.ARG_OUTPUT);
    private Options options;

    public SparkMergingDictionary() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_MERGE_SEGMENT_IDS);
        options.addOption(OPTION_OUTPUT_PATH);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        final String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        final String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        final String segmentIds = optionsHelper.getOptionValue(OPTION_MERGE_SEGMENT_IDS);
        final String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);

        System.setProperty("HADOOP_USER_NAME", "root");

        Class[] kryoClassArray = new Class[] { Class.forName("scala.reflect.ClassTag$$anon$1"), Class.forName("scala.collection.mutable.WrappedArray$ofRef") };

        SparkConf conf = new SparkConf().setAppName("Merge dictionary for cube:" + cubeName + ", segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);

        JavaSparkContext sc = new JavaSparkContext(conf);
        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        sc.sc().addSparkListener(jobListener);

        HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(outputPath));

        final SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());
        final KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

        final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
        final CubeDesc cubeDesc = CubeDescManager.getInstance(envConfig).getCubeDesc(cubeInstance.getDescName());
        final List<CubeSegment> mergingSegments = getMergingSegments(cubeInstance, segmentIds.split(","));

        logger.info("Output path: {}", outputPath);

        final TblColRef[] tblColRefs = cubeDesc.getAllColumnsNeedDictionaryBuilt().toArray(new TblColRef[0]);
        int columnLength = tblColRefs.length;

        if (columnLength > 0) {
            List<Integer> indexs = Lists.newArrayListWithCapacity(columnLength);

            for (int i = 0; i < columnLength; i++)
                indexs.add(i);

            JavaRDD<Integer> indexRDD = sc.parallelize(indexs, columnLength);

            JavaPairRDD<Text, Text> colToDictPathRDD = indexRDD
                    .mapToPair(new PairFunction<Integer, Text, Text>() {
                        private volatile transient boolean initialized = false;
                        DictionaryManager dictMgr;

                        @Override
                        public Tuple2<Text, Text> call(Integer index) throws Exception {
                            if (initialized == false) {
                                synchronized (SparkMergingDictionary.class) {
                                    if (initialized == false) {
                                        KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);
                                        dictMgr = DictionaryManager.getInstance(kylinConfig);
                                        initialized = true;
                                    }
                                }
                            }

                            TblColRef col = tblColRefs[index];
                            List<DictionaryInfo> dictInfos = Lists.newArrayList();

                            for (CubeSegment segment : mergingSegments) {
                                if (segment.getDictResPath(col) != null) {
                                    DictionaryInfo dictInfo = dictMgr.getDictionaryInfo(segment.getDictResPath(col));
                                    if (dictInfo != null && !dictInfos.contains(dictInfo)) {
                                        dictInfos.add(dictInfo);
                                    }
                                }
                            }

                            DictionaryInfo mergedDictInfo = dictMgr.mergeDictionary(dictInfos);

                            String tblCol = col.getTable() + ":" + col.getName();
                            String dictInfoPath = mergedDictInfo == null ? "" : mergedDictInfo.getResourcePath();

                            return new Tuple2<>(new Text(tblCol), new Text(dictInfoPath));
                        }
                    });

            colToDictPathRDD.coalesce(1, false).saveAsNewAPIHadoopFile(outputPath, Text.class, Text.class,
                    SequenceFileOutputFormat.class);
        }

        HadoopUtil.deleteHDFSMeta(metaUrl);
    }

    private List<CubeSegment> getMergingSegments(CubeInstance cube, String[] segmentIds) {
        List<CubeSegment> result = Lists.newArrayListWithCapacity(segmentIds.length);
        for (String id : segmentIds) {
            result.add(cube.getSegmentById(id));
        }
        return result;
    }
}
