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

package org.apache.kylin.storage.parquet;

import java.util.List;

import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.ImmutableList;

public class ParquetSchema implements NameMapping {
    public static final String CUBOID_ID = "CUBOID_ID";

    private final NameMapping mapping;
    private final List<TblColRef> dimensions;
    private final List<MeasureDesc> measures;

    public ParquetSchema(NameMapping mapping, List<TblColRef> dimensions, List<MeasureDesc> measures) {
        this.mapping = mapping;
        this.dimensions = ImmutableList.copyOf(dimensions);
        this.measures = ImmutableList.copyOf(measures);
    }

    @Override
    public String getDimFieldName(TblColRef dim) {
        return mapping.getDimFieldName(dim);
    }

    @Override
    public String getMeasureFieldName(MeasureDesc measure) {
        return mapping.getMeasureFieldName(measure);
    }

    public List<TblColRef> getDimensions() {
        return dimensions;
    }

    public List<MeasureDesc> getMeasures() {
        return measures;
    }

    public int getTotalFieldCount() {
        return dimensions.size() + measures.size();
    }
}
