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
import org.apache.curator.framework.CuratorFramework;
import org.apache.kylin.common.KylinConfig;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetaCleanLeaderSelector extends CuratorLeaderSelector {
    private static final Logger logger = LoggerFactory.getLogger(MetaCleanLeaderSelector.class);
    private ScheduledExecutorService cleanUpExecutor;
    private KylinConfig config;
    private String lastPeriodPath;
    private static final String META_LAST_CLEAN_PERIOD_PATH = "/kylin/%s/meta_clean/last";
    private static final int DEFAULT_INIT_DELAY = 1;
    private static final int ONE_MINUTE = 60 * 1000;

    public MetaCleanLeaderSelector(CuratorFramework client, String path, String name, KylinConfig config) {
        super(client, path, name);
        this.config = config;
        this.lastPeriodPath = String.format(Locale.ROOT, META_LAST_CLEAN_PERIOD_PATH, config.getMetadataUrlPrefix());
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        logger.info("{} is the leader for job engine now.", name);

        int initDelay;
        int interval = config.getCleanJobScheduleInterval();

        if (client.checkExists().forPath(lastPeriodPath) == null) {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(lastPeriodPath);
            initDelay = DEFAULT_INIT_DELAY;
        } else {
            byte[] data = client.getData().forPath(lastPeriodPath);
            long lastPeriod = Long.parseLong(new String(data, StandardCharsets.UTF_8));
            long lastInterval = (System.currentTimeMillis() - lastPeriod) / ONE_MINUTE;

            if (lastInterval > interval || lastInterval < 0) {
                initDelay = DEFAULT_INIT_DELAY;
            } else {
                initDelay = (int)(interval - lastInterval);
            }
        }

        try {
            cleanUpExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("MetadataCleanupJob").build());
            // metadata clean up scheduler
            cleanUpExecutor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    logger.info("Start to cleanup metadata");
                    try {
                        Object metadataCleanupJob = Class.forName("org.apache.kylin.rest.job.MetadataCleanupJob").getConstructor(KylinConfig.class).newInstance(config);
                        Method cleanupMethod = metadataCleanupJob.getClass().getMethod("cleanup", boolean.class, int.class);
                        cleanupMethod.invoke(metadataCleanupJob, config.isDelMetaWhenCleanup(), config.getThresholdForJobToCleanup());

                        // update period in zk
                        client.setData().forPath(lastPeriodPath, String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
                    } catch (Exception e) {
                        logger.error("Failed to clean up metadata", e);
                    }
                }
            }, initDelay, interval, TimeUnit.MINUTES);

            while (true) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5L));
            }
        } catch (InterruptedException ie) {
            logger.error(this.name + " was interrupted.", ie);
        } catch (Throwable th) {
            logger.error("Other exception occurred when initialization DefaultScheduler:", th);
        } finally {
            logger.warn(this.name + " relinquishing leadership.");
        }

    }
}
