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

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.regex.*;

public class RetentionPartialResult {

    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;
    public static final String FILTER_DELIMETER = KylinConfig.getInstanceFromEnv().getIntersectFilterOrSeparator();
    public static ThreadLocal<ArrayList<BitmapCounter>> array =
            new ThreadLocal<ArrayList<BitmapCounter>>().withInitial(() -> Lists.newArrayList());
    Map<Object, BitmapCounter> map;
    Map<Object, Map<Object, BitmapCounter>> maps;
    List keyList;
    List keyLists;
    Map<String, List<String>> childKeyToParentKey;
    Map<Object, Map<String, List<String>>> childKeyToParentKeys;
    String computeType="AND";

    public RetentionPartialResult() {
        map = new LinkedHashMap<>();
        maps = new LinkedHashMap<>();
    }

    public void add(Object key, List keyList, Object value, String FilterType) {
        if (this.keyList == null) {
            this.keyList = keyList;
            if("RAWSTRING".equals(FilterType)) {
                childKeyToParentKey = new HashMap<>(5);
                for (Object aKey : keyList) {
                    String sKey = aKey.toString();
                    String[] elements = StringUtil.splitAndTrim(sKey, FILTER_DELIMETER);
                    for (String s : elements) {
                        if (s != null && s.trim().length() > 0) {
                            List<String> parent = childKeyToParentKey.computeIfAbsent(s.trim(), o -> new ArrayList());
                            parent.add(sKey);
                        }
                    }
                }
            }
        }

        if (this.keyList != null) {
            if("RAWSTRING".equals(FilterType)) {
                if (this.keyList.contains(key.toString())) {
                    BitmapCounter counter = map.computeIfAbsent(key.toString(), o -> factory.newBitmap());
                    counter.orWith((BitmapCounter) value);
                }

                if (childKeyToParentKey.size() > 0) {
                    String sKey = key.toString();
                    if (childKeyToParentKey.containsKey(sKey)) {
                        List<String> parents = childKeyToParentKey.get(sKey);
                        for (String parent : parents) {
                            BitmapCounter counter = map.computeIfAbsent(parent, o -> factory.newBitmap());
                            counter.orWith((BitmapCounter) value);
                        }
                    }
                }
            } else if("REGEXP".equals(FilterType)){
                for(Object aKey : this.keyList){
                    String pattern = aKey.toString();
                    if(Pattern.matches(pattern, key.toString())){
                        BitmapCounter counter = map.computeIfAbsent(aKey.toString(), o -> factory.newBitmap());
                        counter.orWith((BitmapCounter) value);
                    }
                }
            }
        }
    }

    private BitmapCounter result() {
        if (keyList == null || keyList.isEmpty()) {
            return null;
        }
        // if any specified key not in map, the intersection must be 0
        for (Object key : keyList) {
            if (!map.containsKey(key)) {
                return null;
            }
        }

        BitmapCounter counter = null;
        for (Object key : keyList) {
            BitmapCounter c = map.get(key);
            if (counter == null) {
                counter = factory.newBitmap();
                counter.orWith(c);
            } else {
                if("AND".equals(computeType)){
                    counter.andWith(c);
                }// subtraction
                else if("SUB".equals(computeType)){
                    counter.orWith(c);
                    counter.xorWith(c);
                }
            }
        }

        return counter;
    }

    public String valueResult() {
        BitmapCounter counter = result();
        String result = "";
        if (counter != null && counter.getCount() > 0) {
            result = "[" + StringUtils.join(counter.iterator(), ",") + "]";
        }
        return result;
    }

    public long countResult() {
        BitmapCounter counter = result();
        return counter != null ? counter.getCount() : 0;
    }

    public int codeResult() {
        System.out.println("codeResult :" + Thread.currentThread());
        BitmapCounter counter = result();
        array.get().add(counter);
        return array.get().size()-1;
    }

    public static int putBitmapCounter(BitmapCounter counter){
        System.out.println("putBitmapCounter :" + Thread.currentThread());
        array.get().add(counter);
        return array.get().size()-1;
    }

    public static BitmapCounter getBitmapCounter(int index){
        return array.get().get(index);
    }

    @Deprecated
    public static void deleteBitmapCounter(int index){
        array.get().remove(index);
    }

    public static void clearThreadLocalContexts(){
        array.remove();
    }

    public static BitmapCounter resultBC(List<Integer> keyList){
        if (keyList == null || keyList.isEmpty()) {
            return null;
        }
        // if any specified key not in map, the intersection must be 0
        for (Integer key : keyList) {
            if (key > array.get().size() || key < 0) {
                return null;
            }
        }
        BitmapCounter counter = null;
        for (Integer key : keyList) {
            BitmapCounter c = array.get().get(key);
            if (counter == null) {
                counter = factory.newBitmap();
                counter.orWith(c);
            } else {
                counter.andWith(c);
            }
        }
        return counter;
    }

    public static String valueResultBC(List<Integer> keyList) {
        BitmapCounter counter = resultBC(keyList);
        String result = "";
        if (counter != null && counter.getCount() > 0) {
            result = "[" + StringUtils.join(counter.iterator(), ",") + "]";
        }
        return result;
    }

    public static long countResultBC(List<Integer> keyList) {
        BitmapCounter counter = resultBC(keyList);
        return counter != null ? counter.getCount() : 0;
    }

    public long countSubtractResult() {
        computeType="SUB";
        BitmapCounter counter = result();
        return counter != null ? counter.getCount() : 0;
    }

    public String valueSubtractResult() {
        computeType="SUB";
        BitmapCounter counter = result();
        String result = "";
        if (counter != null && counter.getCount() > 0) {
            result = "[" + StringUtils.join(counter.iterator(), ",") + "]";
        }
        return result;
    }


    private void initKeyLists(String FilterType){
        if("RAWSTRING".equals(FilterType)){
            childKeyToParentKeys = new HashMap<>(2);
            for(Object keyArray : keyLists) {
                Map<String, List<String>> newChildKeyToParentKey = new HashMap<>(5);
                // if a List's elements are same to another , they have same key
                childKeyToParentKeys.put(keyArray,newChildKeyToParentKey);
                Map<Object, BitmapCounter> newmap = new LinkedHashMap<>();
                maps.put(keyArray,newmap);
                for (Object aKey : (List)keyArray) {
                    String sKey = aKey.toString();
                    String[] elements = StringUtil.splitAndTrim(sKey, FILTER_DELIMETER);
                    for (String s : elements) {
                        if (s != null && s.trim().length() > 0) {
                            List<String> parent = newChildKeyToParentKey.computeIfAbsent(s.trim(), o -> new ArrayList());
                            parent.add(sKey);
                        }
                    }
                }
            }
        }else if("REGEXP".equals(FilterType)){
            for(Object keyArray : keyLists) {
                Map<Object, BitmapCounter> newmap = new LinkedHashMap<>();
                maps.put(keyArray, newmap);
            }
        }
    }

    private void exactlyMatchAdd(Object key, Object value){
        for(Object keyArray : keyLists) {
            if (((List)keyArray).contains(key.toString())) {
                BitmapCounter counter = maps.get(keyArray).computeIfAbsent(key.toString(), o -> factory.newBitmap());
                counter.orWith((BitmapCounter) value);
            }
            if (childKeyToParentKeys.get(keyArray).size() > 0) {
                String sKey = key.toString();
                if (childKeyToParentKeys.get(keyArray).containsKey(sKey)) {
                    List<String> parents = childKeyToParentKeys.get(keyArray).get(sKey);
                    for (String parent : parents) {
                        BitmapCounter counter = maps.get(keyArray).computeIfAbsent(parent, o -> factory.newBitmap());
                        counter.orWith((BitmapCounter) value);
                    }
                }
            }
        }

    }

    private void regexpMatchAdd(Object key, Object value){
        for(Object keyArray : keyLists) {
            for (Object aKey : (List)keyArray) {
                String pattern = aKey.toString();
                if (Pattern.matches(pattern, key.toString())) {
                    BitmapCounter counter = maps.get(keyArray).computeIfAbsent(aKey.toString(), o -> factory.newBitmap());
                    counter.orWith((BitmapCounter) value);
                }
            }
        }
    }

    public void add2(Object key, List keyList, Object value, String FilterType) {
        if (this.keyLists == null) {
            this.keyLists = keyList;
            initKeyLists(FilterType);
        }
        if (this.keyLists != null) {
            if("RAWSTRING".equals(FilterType)) {
                exactlyMatchAdd(key, value);
            } else if("REGEXP".equals(FilterType)){
                regexpMatchAdd(key, value);
            }
        }
    }

    private BitmapCounter subtractResult() {
        if (keyLists == null || keyLists.isEmpty()) {
            return null;
        }
        //List<BitmapCounter> results= new ArrayList();
        BitmapCounter counter = null;
        for(Object keyArray : keyLists) {
            BitmapCounter counterPart = null;
            for (Object key : (List)keyArray) {
                if (!maps.get(keyArray).containsKey(key)) {
                    continue;
                }
                BitmapCounter c = maps.get(keyArray).get(key);
                if (counterPart == null) {
                    counterPart = factory.newBitmap();
                    counterPart.orWith(c);
                } else {
                    counterPart.andWith(c);
                }
            }
            if (counter == null && counterPart != null) {
                counter = factory.newBitmap();
                counter.orWith(counterPart);
            } else if (counter != null && counterPart != null){
                counter.orWith(counterPart);
                counter.xorWith(counterPart);
            }
        }

        return counter;
    }


    public long intersectSubtractCountResult() {
        BitmapCounter counter = subtractResult();
        return counter != null ? counter.getCount() : 0;
    }

    public String intersectSubtractValueResult() {
        BitmapCounter counter = subtractResult();
        String result = "";
        if (counter != null && counter.getCount() > 0) {
            result = "[" + StringUtils.join(counter.iterator(), ",") + "]";
        }
        return result;
    }


}