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

package org.apache.kylin.storage.parquet.cube;

import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
import org.apache.kylin.measure.hllc.HLLCMeasureType;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class ParquetColumnFiller implements ColumnFiller {
    private final List<TblColRef> dimensions;
    private final List<MeasureDesc> measures;

    private final int[] tupleIndex;
    private final MeasureType[] measureTypes;
    private final DataTypeSerializer[] serializers;

    public ParquetColumnFiller(List<TblColRef> dimensions, List<MeasureDesc> measures, TupleInfo tupleInfo) {
        this.dimensions = dimensions;
        this.measures = measures;
        this.tupleIndex = new int[dimensions.size() + measures.size()];
        this.measureTypes = new MeasureType[dimensions.size() + measures.size()];
        this.serializers = new DataTypeSerializer[measures.size()];

        int i = 0;
        for (TblColRef dim : dimensions) {
            tupleIndex[i++] = tupleInfo.getColumnIndex(dim);
        }
        for (MeasureDesc met : measures) {
            FunctionDesc func = met.getFunction();
            MeasureType<?> measureType = func.getMeasureType();
            serializers[i - dimensions.size()] = DataTypeSerializer.create(met.getFunction().getReturnDataType());

            if (func.needRewrite()) {
                String fieldName = func.getRewriteFieldName();
                tupleIndex[i] = tupleInfo.getFieldIndex(fieldName);
            } else {
                TblColRef col = func.getParameter().getColRefs().get(0);
                tupleIndex[i] = tupleInfo.getColumnIndex(col);
            }

            if (!measureType.needAdvancedTupleFilling()) {
                measureTypes[i] = measureType;
            } else if (measureType instanceof ExtendedColumnMeasureType) {
                final TblColRef extended = ExtendedColumnMeasureType.getExtendedColumn(func);
                final int extendedColumnInTupleIdx = tupleInfo.hasColumn(extended) ? tupleInfo.getColumnIndex(extended) : -1;
                tupleIndex[i] = extendedColumnInTupleIdx;
            } else {
                throw new UnsupportedOperationException("Unsupported measure type : " + measureType);
            }

            i++;
        }
    }

    public void fill(Object[] row, Tuple tuple) {
        for (int i = 0; i < dimensions.size(); i++) {
            tuple.setDimensionValue(tupleIndex[i], Objects.toString(row[i], null));
        }

        for (int i = dimensions.size(); i < dimensions.size() + measures.size(); i++) {
            MeasureType measureType = measureTypes[i];
            if (measureType != null) {
                if (measureType instanceof HLLCMeasureType ||
                        measureType instanceof BitmapMeasureType ||
                        measureType instanceof PercentileMeasureType) {
                    tuple.setMeasureValue(tupleIndex[i], serializers[i - dimensions.size()].deserialize(ByteBuffer.wrap((byte[])row[i])));
                } else {
                    measureTypes[i].fillTupleSimply(tuple, tupleIndex[i], row[i]);
                }
            } else {
                //for ExtendedColumn
                tuple.setDimensionValue(tupleIndex[i], (String) row[i]);
            }
        }
    }
}
