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

package org.apache.kylin.job.impl.curator;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetaCleanScheduler {
    protected static final int DEFAULT_INIT_DELAY = 1;
    protected ScheduledExecutorService cleanUpExecutor;
    protected KylinConfig config;

    public MetaCleanScheduler(KylinConfig config) {
        this.config = config;
        this.cleanUpExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("MetadataCleanupJob").build());
    }

    public void scheduleCleanup() {
        // metadata clean up scheduler
        cleanUpExecutor.scheduleAtFixedRate(new MetaCleanRunner() {
            @Override
            void metaClean() throws Exception {
                runCleanJob(config);
            }
        }, DEFAULT_INIT_DELAY, config.getCleanJobScheduleInterval(), TimeUnit.MINUTES);
    }

    abstract static class MetaCleanRunner implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(MetaCleanRunner.class);

        @Override
        public void run() {
            try {
                logger.info("Start to cleanup metadata");

                metaClean();

                logger.info("Metadata clean up done");
            } catch (Exception e) {
                logger.error("Failed to clean up metadata", e);
            }
        }

        abstract void metaClean() throws Exception;
    }

    static void runCleanJob(KylinConfig config) throws Exception {
        Object metadataCleanupJob = Class.forName("org.apache.kylin.rest.job.MetadataCleanupJob").getConstructor(KylinConfig.class).newInstance(config);
        Method cleanupMethod = metadataCleanupJob.getClass().getMethod("cleanup", boolean.class, int.class);
        cleanupMethod.invoke(metadataCleanupJob, config.isDelMetaWhenCleanup(), config.getThresholdForJobToCleanup());
    }
}
