import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.EnsurePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class DistributedLockFactoryImpl implements DistributedLockFactory {

    private volatile CuratorFramework curatorClient;
    private volatile Cache<String, DistributedLock> distributedLockCache;
    private static boolean init = false;
    private static final String BASE_LOCK_PATH = "/locks";
    private static final int CACHE_SIZE = 500;
    private final Logger LOGGER = LoggerFactory.getLogger(DistributedLockFactoryImpl.class);

    @Override
    public DistributedLock getOrCreateLock(String lockKey) {
        init();
        DistributedLock distributedLock = distributedLockCache.getIfPresent(lockKey);
        if (distributedLock == null) {
            distributedLock = new DistributedLockImpl(curatorClient, lockKey);
            distributedLockCache.put(lockKey, distributedLock);
        }
        return distributedLock;
    }

    private void init() {
        if (init) {
            return;
        }
        final String zkURL = "localhost:2181";
        final int zkSessionTimeout = (int) TimeUnit.MINUTES.toMillis(5);
        final int zkConnectionTimeout = (int) TimeUnit.MINUTES.toMillis(5);
        try {
            // Create client
            curatorClient = CuratorFrameworkFactory.builder().connectString(zkURL)
                    .sessionTimeoutMs(zkSessionTimeout).connectionTimeoutMs(zkConnectionTimeout)
                    .retryPolicy(new ExponentialBackoffRetry(100, 10)).build();
            // Start client
            curatorClient.start();
            final EnsurePath ensurePath = new EnsurePath(BASE_LOCK_PATH);
            ensurePath.ensure(curatorClient.getZookeeperClient());
            init = true;
        } catch (Exception eX) {
            LOGGER.error("Failed to initialize");
        }
        this.distributedLockCache = CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).build();
    }
}
