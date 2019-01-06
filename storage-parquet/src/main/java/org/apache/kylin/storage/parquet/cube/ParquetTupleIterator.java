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

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ParquetTupleIterator implements ITupleIterator {
    private final CubeInstance cubeInstance;
    private final StorageContext context;
    private final Iterator<Row> rowIterator;
    private final List<ColumnFiller> fillers;
    private int scanCount;
    private final Tuple tuple;
    private final int colCount;

    public ParquetTupleIterator(CubeInstance cubeInstance, StorageContext context, LookupTableCache lookupCache, ParquetStorageQuery.StorageRequest request, TupleInfo tupleInfo, Iterator<Row> rowIterator) {
        this.cubeInstance = cubeInstance;
        this.context = context;
        this.rowIterator = rowIterator;
        this.tuple = new Tuple(tupleInfo);
        this.colCount = request.groups.size() + request.measures.size();

        this.fillers = new ArrayList<>();
        fillers.add(new ParquetColumnFiller(request.groups, request.measures, tupleInfo));

        // derived columns fillers
        List<CubeDesc.DeriveInfo> deriveInfos = cubeInstance.getDescriptor().getDeriveInfos(request.groups);

        for (CubeDesc.DeriveInfo info : deriveInfos) {
            DerivedIndexMapping mapping = new DerivedIndexMapping(info, request.groups, tupleInfo);
            if (!mapping.shouldReturnDerived()) {
                continue;
            }
            if (info.type == CubeDesc.DeriveType.LOOKUP) {
                ILookupTable lookupTable = lookupCache.get(info.join);
                fillers.add(new DerivedColumnFiller(mapping, lookupTable));

            } else if (info.type == CubeDesc.DeriveType.PK_FK) {
                fillers.add(new PkColumnFiller(mapping));
            }
        }
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    @Override
    public ITuple next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (scanCount++ >= 100) {
            flushScanCountDelta();
            context.getQueryContext().checkMillisBeforeDeadline();
        }

        Object[] row = rowToObjects(rowIterator.next());
        for (ColumnFiller filler : fillers) {
            filler.fill(row, tuple);
        }
        return tuple;
    }

    private void flushScanCountDelta() {
        context.increaseProcessedRowCount(scanCount);
        scanCount = 0;
    }

    private Object[] rowToObjects(Row row) {
        Object[] objects = new Object[colCount];
        for (int i = 0; i < colCount; i++) {
            objects[i] = row.get(i);
        }
        return objects;
    }
}
