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

import org.apache.kylin.measure.ParamAsMeasureCount;

import java.util.List;

public class BitmapIntersectSubtractValueAggFunc implements ParamAsMeasureCount {
    @Override
    public int getParamAsMeasureCount() {
        return -3;
    }

    public static RetentionPartialResult init() {
        return new RetentionPartialResult();
    }

    public static RetentionPartialResult add(RetentionPartialResult result, Object value, Object key, List keyList, String FilterType) {
        result.add2(key, keyList, value, FilterType);
        return result;
    }

    public static RetentionPartialResult merge(RetentionPartialResult result, Object value, Object key, List keyList, String FilterType) {
        return add(result, value, key, keyList, FilterType);
    }

    public static String result(RetentionPartialResult result) {
        return result.intersectSubtractValueResult();
    }
}
