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
package org.apache.kylin.measure.bitmap;

import com.google.common.collect.Lists;
import org.apache.kylin.measure.ParamAsMeasureCount;

import java.util.List;

public class BitmapByUuidIntersectCountAggFunc implements ParamAsMeasureCount {

    @Override
    public int getParamAsMeasureCount() {
        return 0;
    }

    static class MyPartialResult {
        protected List<Integer> values = Lists.newLinkedList();
    }

    public MyPartialResult init() {
        return new MyPartialResult();
    }

    public MyPartialResult add(MyPartialResult result, Integer key) {
        result.values.add(key);
        return result;
    }

    public MyPartialResult merge(MyPartialResult result, MyPartialResult another) {
        result.values.addAll(another.values);
        return result;
    }

    public long result(MyPartialResult result) {
        return RetentionPartialResult.countResultBC(result.values);
    }
}
