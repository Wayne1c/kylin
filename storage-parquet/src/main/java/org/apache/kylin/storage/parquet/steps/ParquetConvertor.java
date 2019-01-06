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
package org.apache.kylin.storage.parquet.steps;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.cube.kv.RowKeyDecoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dimension.AbstractDateDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.FixedLenDimEnc;
import org.apache.kylin.dimension.FixedLenHexDimEnc;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.dimension.IntDimEnc;
import org.apache.kylin.dimension.IntegerDimEnc;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.measure.basic.BigDecimalIngester;
import org.apache.kylin.measure.basic.DoubleIngester;
import org.apache.kylin.measure.basic.LongIngester;
import org.apache.kylin.metadata.datatype.BigDecimalSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 */
public class ParquetConvertor {
    private static final Logger logger = LoggerFactory.getLogger(ParquetConvertor.class);

    public static final String FIELD_CUBOID_ID = "cuboidId";
    public static final String DATATYPE_BOOLEAN = "boolean";
    public static final String DATATYPE_INT = "int";
    public static final String DATATYPE_LONG = "long";
    public static final String DATATYPE_FLOAT = "float";
    public static final String DATATYPE_DOUBLE = "double";
    public static final String DATATYPE_DECIMAL = "decimal";
    public static final String DATATYPE_STRING = "string";
    public static final String DATATYPE_BINARY = "binary";

    private RowKeyDecoder decoder;
    private BufferedMeasureCodec measureCodec;
    private Map<String, String> colTypeMap;
    private Map<MeasureDesc, String> meaTypeMap;
    private BigDecimalSerializer serializer;
    private GroupFactory factory;
    private List<MeasureDesc> measureDescs;

    public ParquetConvertor(String cubeName, String segmentId, KylinConfig kConfig, SerializableConfiguration sConf, Map<String, String> colTypeMap, Map<MeasureDesc, String> meaTypeMap){
        KylinConfig.setAndUnsetThreadLocalConfig(kConfig);

        this.colTypeMap = colTypeMap;
        this.meaTypeMap = meaTypeMap;
        serializer = new BigDecimalSerializer(DataType.getType(DATATYPE_DECIMAL));

        CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);

        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
        measureDescs = cubeDesc.getMeasures();
        decoder = new RowKeyDecoder(cubeSegment);
        factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(sConf.get()));
        measureCodec = new BufferedMeasureCodec(cubeDesc.getMeasures());
    }

    protected Group parseValueToGroup(Text rawKey, Text rawValue) throws IOException{
        Group group = factory.newGroup();

        long cuboidId = decoder.decode(rawKey.getBytes());
        List<String> values = decoder.getValues();
        List<TblColRef> columns = decoder.getColumns();

        // for check
        group.append(FIELD_CUBOID_ID, cuboidId);

        for (int i = 0; i < columns.size(); i++) {
            TblColRef column = columns.get(i);
            parseColValue(group, column, values.get(i));
        }

        int[] valueLengths = measureCodec.getCodec().getPeekLength(ByteBuffer.wrap(rawValue.getBytes()));

        int valueOffset = 0;
        for (int i = 0; i < valueLengths.length; ++i) {
            MeasureDesc measureDesc = measureDescs.get(i);
            parseMeasureValue(group, measureDesc, rawValue.getBytes(), valueOffset, valueLengths[i]);
            valueOffset += valueLengths[i];
        }

        return group;
    }

    private void parseColValue(final Group group, final TblColRef colRef, final String value) {
        if (value==null) {
            logger.error("value is null");
            return;
        }
        String colName = getColName(colRef);
        String dataType = colTypeMap.get(colName);

        switch (dataType) {
            case DATATYPE_BOOLEAN:
                group.append(colName, Boolean.valueOf(value));
                break;
            case DATATYPE_INT:
                group.append(colName, Integer.valueOf(value));
                break;
            case DATATYPE_LONG:
                group.append(colName, Long.valueOf(value));
                break;
            case DATATYPE_FLOAT:
                group.append(colName, Float.valueOf(value));
                break;
            case DATATYPE_DOUBLE:
                group.append(colName, Double.valueOf(value));
                break;
            case DATATYPE_DECIMAL:
                BigDecimal decimal = BigDecimal.valueOf(Long.valueOf(value));
                decimal.setScale(colRef.getType().getScale());
                group.append(colName, Binary.fromReusedByteArray(decimal.unscaledValue().toByteArray()));
                break;
            case DATATYPE_STRING:
                group.append(colName, value);
                break;
            case DATATYPE_BINARY:
                group.append(colName, Binary.fromString(value));
                break;
            default:
                group.append(colName, Binary.fromString(value));
        }
    }

    private void parseMeasureValue(final Group group, final MeasureDesc measureDesc, final byte[] value, final int offset, final int length) {
        if (value==null) {
            logger.error("value is null");
            return;
        }
        String meaName = measureDesc.getName();
        String dataType = colTypeMap.get(meaName);
        switch (dataType) {
            case DATATYPE_INT:
                group.append(meaName, BytesUtil.readVInt(ByteBuffer.wrap(value, offset, length)));
                break;
            case DATATYPE_LONG:
                group.append(meaName,  BytesUtil.readVLong(ByteBuffer.wrap(value, offset, length)));
                break;
            case DATATYPE_FLOAT:
                group.append(meaName, ByteBuffer.wrap(value, offset, length).getFloat());
                break;
            case DATATYPE_DOUBLE:
                group.append(meaName, ByteBuffer.wrap(value, offset, length).getDouble());
                break;
            case DATATYPE_DECIMAL:
                BigDecimal decimal = serializer.deserialize(ByteBuffer.wrap(value, offset, length));
                decimal = decimal.setScale(measureDesc.getFunction().getReturnDataType().getScale());
                group.append(meaName, Binary.fromReusedByteArray(decimal.unscaledValue().toByteArray()));
                break;
            case DATATYPE_BINARY:
                group.append(meaName, Binary.fromReusedByteArray(value, offset, length));
                break;
            default:
                group.append(meaName, Binary.fromReusedByteArray(value, offset, length));
        }
    }

    protected static MessageType cuboidToMessageType(Cuboid cuboid, IDimensionEncodingMap dimEncMap, CubeDesc cubeDesc, Map<String, String> colTypeMap) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        CuboidToGridTableMapping mapping = new CuboidToGridTableMapping(cuboid);
        DataType[] dataTypes = mapping.getDataTypes();
        List<TblColRef> colRefs = cuboid.getColumns();

        builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(FIELD_CUBOID_ID);

        for (TblColRef colRef : colRefs) {
            DataType dataType = dataTypes[mapping.getIndexOf(colRef)];
            //DimensionEncoding dimEnc = dimEncMap.get(colRef);
            colToMessageType(dataType, getColName(colRef), builder, colTypeMap);
        }

        MeasureIngester[] aggrIngesters = MeasureIngester.create(cubeDesc.getMeasures());

        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            MeasureDesc measureDesc = cubeDesc.getMeasures().get(i);
            DataType meaDataType = measureDesc.getFunction().getReturnDataType();

            colToMessageType(meaDataType, measureDesc.getName(), builder, colTypeMap);
        }

        return builder.named(String.valueOf(cuboid.getId()));
    }

    protected static void generateTypeMap(Cuboid cuboid, IDimensionEncodingMap dimEncMap, CubeDesc cubeDesc, Map<TblColRef, String> colTypeMap, Map<MeasureDesc, String> meaTypeMap){
        List<TblColRef> colRefs = cuboid.getColumns();

        for (TblColRef colRef : colRefs) {
            DimensionEncoding dimEnc = dimEncMap.get(colRef);
            addColTypeMap(dimEnc, colRef, colTypeMap);
        }

        MeasureIngester[] aggrIngesters = MeasureIngester.create(cubeDesc.getMeasures());

        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            MeasureDesc measureDesc = cubeDesc.getMeasures().get(i);
            MeasureType measureType = measureDesc.getFunction().getMeasureType();
            addMeaColTypeMap(measureType, measureDesc, aggrIngesters[i], meaTypeMap);
        }
    }
    public static String getColName(TblColRef colRef) {
        return colRef.getTableAlias() + "_" + colRef.getName();
    }

    private static void addColTypeMap(DimensionEncoding dimEnc, TblColRef colRef, Map<TblColRef, String> colTypeMap) {
        if (dimEnc instanceof AbstractDateDimEnc) {
            colTypeMap.put(colRef, DATATYPE_LONG);
        } else if (dimEnc instanceof FixedLenDimEnc || dimEnc instanceof FixedLenHexDimEnc) {
            DataType colDataType = colRef.getType();
            if (colDataType.isNumberFamily() || colDataType.isDateTimeFamily()){
                colTypeMap.put(colRef, DATATYPE_LONG);
            } else {
                // stringFamily && default
                colTypeMap.put(colRef, DATATYPE_STRING);
            }
        } else if(dimEnc instanceof IntegerDimEnc || dimEnc instanceof IntDimEnc) {
            colTypeMap.put(colRef, DATATYPE_INT);
        } else {
            colTypeMap.put(colRef, DATATYPE_STRING);
        }
    }

    private static Map<MeasureDesc, String> addMeaColTypeMap(MeasureType measureType, MeasureDesc measureDesc, MeasureIngester aggrIngester, Map<MeasureDesc, String> meaTypeMap) {
        if (measureType instanceof BasicMeasureType) {
            MeasureIngester measureIngester = aggrIngester;
            if (measureIngester instanceof LongIngester) {
                meaTypeMap.put(measureDesc, DATATYPE_LONG);
            } else if (measureIngester instanceof DoubleIngester) {
                meaTypeMap.put(measureDesc, DATATYPE_DOUBLE);
            } else if (measureIngester instanceof BigDecimalIngester) {
                meaTypeMap.put(measureDesc, DATATYPE_DECIMAL);
            } else {
                meaTypeMap.put(measureDesc, DATATYPE_BINARY);
            }
        } else {
            meaTypeMap.put(measureDesc, DATATYPE_BINARY);
        }
        return meaTypeMap;
    }

    private static void colToMessageType(DataType dataType, String name, Types.MessageTypeBuilder builder, Map<String, String> colTypeMap) {
        if (dataType.isBoolean()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);
            colTypeMap.put(name, "boolean");
        } else if (dataType.isInt()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(name);
            colTypeMap.put(name, "int");
        } else if (dataType.isBigInt()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);
            colTypeMap.put(name, "long");
        } else if (dataType.isDate()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);
            colTypeMap.put(name, "long");
        } else if (dataType.isFloat()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(name);
            colTypeMap.put(name, "float");
        } else if (dataType.isDouble()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
            colTypeMap.put(name, "double");
        } else if (dataType.isDecimal()) {
            builder.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.DECIMAL).precision(dataType.getPrecision()).scale(dataType.getScale()).named(name);
            colTypeMap.put(name, "decimal");
        } else if (dataType.isStringFamily()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(name);
            colTypeMap.put(name, "string");
        } else {
            builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
            colTypeMap.put(name, "binary");
        }

//        if (dimEnc instanceof AbstractDateDimEnc) {
//            builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(getColName(colRef));
//        } else if (dimEnc instanceof FixedLenDimEnc || dimEnc instanceof FixedLenHexDimEnc) {
//            DataType colDataType = colRef.getType();
//            if (colDataType.isNumberFamily() || colDataType.isDateTimeFamily()){
//                builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(getColName(colRef));
//            } else {
//                // stringFamily && default
//                builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(getColName(colRef));
//            }
//        } else if (dimEnc instanceof IntegerDimEnc || dimEnc instanceof IntDimEnc) {
//            builder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(getColName(colRef));
//        } else {
//            builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(getColName(colRef));
//        }
    }

    private static void meaColToMessageType(MeasureType measureType, String meaDescName, DataType meaDataType, MeasureIngester aggrIngester, Types.MessageTypeBuilder builder) {
        if (measureType instanceof BasicMeasureType) {
            MeasureIngester measureIngester = aggrIngester;
            if (measureIngester instanceof LongIngester) {
                builder.required(PrimitiveType.PrimitiveTypeName.INT64).named(meaDescName);
            } else if (measureIngester instanceof DoubleIngester) {
                builder.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(meaDescName);
            } else if (measureIngester instanceof BigDecimalIngester) {
                builder.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.DECIMAL).precision(meaDataType.getPrecision()).scale(meaDataType.getScale()).named(meaDescName);
            } else {
                builder.required(PrimitiveType.PrimitiveTypeName.BINARY).named(meaDescName);
            }
        } else {
            builder.required(PrimitiveType.PrimitiveTypeName.BINARY).named(meaDescName);
        }
    }
}
