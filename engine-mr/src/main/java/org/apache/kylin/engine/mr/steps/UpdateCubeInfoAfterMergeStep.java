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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.DimensionRangeInfo;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.exception.SegmentNotFoundException;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateCubeInfoAfterMergeStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(UpdateCubeInfoAfterMergeStep.class);

    public UpdateCubeInfoAfterMergeStep() {
        super();
    }

    @Override
    @SuppressWarnings("deprecation")
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager cubeManager = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = cubeManager.getCube(CubingExecutableUtil.getCubeName(this.getParams()))
                .latestCopyForWrite();
        final String dictInfoPath = this.getParams().get(BatchConstants.ARG_DICT_PATH);

        CubeSegment mergedSegment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));
        if (mergedSegment == null) {
            return ExecuteResult.createFailed(new SegmentNotFoundException(
                    "there is no segment with id:" + CubingExecutableUtil.getSegmentId(this.getParams())));
        }

        CubingJob cubingJob = (CubingJob) getManager().getJob(CubingExecutableUtil.getCubingJobId(this.getParams()));
        long cubeSizeBytes = cubingJob.findCubeSizeBytes();

        // update segment dict info
        SequenceFile.Reader reader = null;
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            Configuration conf = HadoopUtil.getCurrentConfiguration();
            FileStatus[] fileStatuss = fs.listStatus(new Path(dictInfoPath), new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return path.getName().startsWith("part");
                }
            });

            Path filePath;
            for (FileStatus fileStatus : fileStatuss){
                filePath = fileStatus.getPath();

                reader = new SequenceFile.Reader(fs, filePath, conf);
                Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);

                String tblCol;
                String dictInfoResource;
                while (reader.next(key, value)) {
                    tblCol = key.toString();
                    dictInfoResource = value.toString();
                    if (StringUtils.isNotEmpty(dictInfoResource)) {
                        TblColRef tblColRef = cube.getDescriptor().findColumnRef(tblCol.split(":")[0], tblCol.split(":")[1]);
                        mergedSegment.putDictResPath(tblColRef, dictInfoResource);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("fail to update cube after merge", e);
            return ExecuteResult.createError(e);
        } finally {
            IOUtils.closeStream(reader);
        }

        // collect source statistics
        List<String> mergingSegmentIds = CubingExecutableUtil.getMergingSegmentIds(this.getParams());
        if (mergingSegmentIds.isEmpty()) {
            return ExecuteResult.createFailed(new SegmentNotFoundException("there are no merging segments"));
        }

        long sourceCount = 0L;
        long sourceSize = 0L;
        boolean isOffsetCube = mergedSegment.isOffsetCube();
        Long tsStartMin = Long.MAX_VALUE, tsEndMax = 0L;
        for (String id : mergingSegmentIds) {
            CubeSegment segment = cube.getSegmentById(id);
            sourceCount += segment.getInputRecords();
            sourceSize += segment.getInputRecordsSize();
            tsStartMin = Math.min(tsStartMin, segment.getTSRange().start.v);
            tsEndMax = Math.max(tsEndMax, segment.getTSRange().end.v);
        }

        Map<String, DimensionRangeInfo> mergedSegDimRangeMap = null;
        for (String id : mergingSegmentIds) {
            CubeSegment segment = cube.getSegmentById(id);
            Map<String, DimensionRangeInfo> segDimRangeMap = segment.getDimensionRangeInfoMap();
            if (mergedSegDimRangeMap == null) {
                mergedSegDimRangeMap = segDimRangeMap;
            } else {
                mergedSegDimRangeMap = DimensionRangeInfo.mergeRangeMap(cube.getModel(), segDimRangeMap,
                        mergedSegDimRangeMap);
            }
        }

        // update segment info
        mergedSegment.setSizeKB(cubeSizeBytes / 1024);
        mergedSegment.setInputRecords(sourceCount);
        mergedSegment.setInputRecordsSize(sourceSize);
        mergedSegment.setLastBuildJobID(CubingExecutableUtil.getCubingJobId(this.getParams()));
        mergedSegment.setLastBuildTime(System.currentTimeMillis());
        mergedSegment.setDimensionRangeInfoMap(mergedSegDimRangeMap);
        if (isOffsetCube) {
            SegmentRange.TSRange tsRange = new SegmentRange.TSRange(tsStartMin, tsEndMax);
            mergedSegment.setTSRange(tsRange);
        }

        try {
            cubeManager.promoteNewlyBuiltSegments(cube, mergedSegment);
            return new ExecuteResult(ExecuteResult.State.SUCCEED);
        } catch (IOException e) {
            logger.error("fail to update cube after merge", e);
            return ExecuteResult.createError(e);
        }
    }
}
