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
package org.apache.kylin.job;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.threadlocal.ThreadLocal;
import org.apache.kylin.common.util.ImplementationSwitch;

public class SchedulerFactory {
    // Use thread-local because KylinConfig can be thread-local and implementation might be different among multiple threads.
    private static ThreadLocal<ImplementationSwitch<Scheduler>> schedulers = new ThreadLocal<>();

    public static Scheduler scheduler(int schedulerType) {
        ImplementationSwitch<Scheduler> current = schedulers.get();
        if (current == null) {
            current = new ImplementationSwitch<>(KylinConfig.getInstanceFromEnv().getSchedulers(), Scheduler.class);
            schedulers.set(current);
        }
        return current.get(schedulerType);
    }

}
