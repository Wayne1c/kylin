package org.apache.kylin.job.impl.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kylin.common.KylinConfig;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class ZKMetaCleanScheduler extends MetaCleanScheduler {
    private static final Logger logger = LoggerFactory.getLogger(ZKMetaCleanScheduler.class);
    private static final String META_LAST_CLEAN_PERIOD_PATH = "/kylin/%s/meta_clean/last";
    private static final int ONE_MINUTE = 60 * 1000;
    private CuratorFramework client;
    private String lastPeriodPath;

    public ZKMetaCleanScheduler(KylinConfig config, CuratorFramework client) {
        super(config);
        this.client = client;
        this.lastPeriodPath = String.format(Locale.ROOT, META_LAST_CLEAN_PERIOD_PATH, config.getMetadataUrlPrefix());
    }

    @Override
    public void scheduleCleanup() {
        int initDelay;
        int interval = config.getCleanJobScheduleInterval();

        try {
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

            cleanUpExecutor.scheduleAtFixedRate(new MetaCleanRunner() {
                @Override
                void metaClean() throws Exception{
                    runCleanJob(config);
                    // update period in zk
                    client.setData().forPath(lastPeriodPath, String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
                }
            }, initDelay, interval, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("Metadata clean job schedule failed", e);
        }
    }
}
