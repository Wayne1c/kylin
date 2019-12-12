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
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.BitmapCounterFactory;
import org.apache.kylin.measure.bitmap.RetentionPartialResult;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;

import java.util.List;

public class BitmapIntersectValueColUDF {
    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;
    public String eval(@Parameter(name = "bitIndex") List<Integer> bitIndex) {
        String result = "";
        BitmapCounter counter  = null;
        for(Integer col : bitIndex){
            if(col == null){
                return result;
            }
            BitmapCounter counterTmp  = RetentionPartialResult.getBitmapCounter(col);
            if(counterTmp == null){
                return result;
            }
            if(counter == null){
                counter = factory.newBitmap();
                counter.orWith(counterTmp);
            }else{
                counter.andWith(counterTmp);
            }
        }
        System.out.println("BitmapIntersectValueColUDF:" + Thread.currentThread());

        if (counter != null && counter.getCount() > 0) {
            result = "[" + StringUtils.join(counter.iterator(), ",") + "]";
        }
        return result;
    }
}
