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
package org.apache.kylin.query.udf;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.BitmapCounterFactory;
import org.apache.kylin.measure.bitmap.RetentionPartialResult;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;

import java.util.List;

public class BitmapIntersectCountColUDF {
    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;
    public long eval(@Parameter(name = "bitIndex") List<Integer> bitIndex) {
        BitmapCounter counter  = null;
        for(Integer col : bitIndex){
            if(col == null){
                return 0;
            }
            BitmapCounter counterTmp  = RetentionPartialResult.getBitmapCounter(col);
            if(counterTmp == null){
                return 0;
            }
            if(counter == null){
                counter = factory.newBitmap();
                counter.orWith(counterTmp);
            }else{
                counter.andWith(counterTmp);
            }
        }
        System.out.println("BitmapIntersectValueColUDF:" + Thread.currentThread());
        return counter == null ? 0:counter.getCount();
    }

}
