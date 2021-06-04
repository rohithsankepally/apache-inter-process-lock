import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class DistributedLockUnitTest {

    private DistributedLockFactory distributedLockFactory;
    private Thread zookeeperServerThread;
    private final Logger LOGGER = LoggerFactory.getLogger(DistributedLockUnitTest.class);
    private static final String LOCK_KEY = "/my/lock/key";

    @Before
    public void setUpDistributedLockUnitTest() {
        distributedLockFactory = new DistributedLockFactoryImpl();
        // Start zookeeper
        startZookeeper();
        BasicConfigurator.configure();
    }

    @Test
    public void testGetOrCreateLock() {
        final DistributedLock distributedLock = distributedLockFactory.getOrCreateLock(LOCK_KEY);
        Assert.assertNotNull(distributedLock);
        Assert.assertEquals(LOCK_KEY, distributedLock.getKey());
    }

    @Test
    public void testLock() throws Exception {
        final DistributedLock distributedLock = distributedLockFactory.getOrCreateLock(LOCK_KEY);
        // Wait until the lock is acquired by the main thread
        distributedLock.lock(-1, null);
        final Thread secondThread = new Thread(() -> {
            try {
                // Try to acquire lock. Throws exception if it cannot find lock within 10 seconds
                distributedLock.lock(10, TimeUnit.SECONDS);
                Assert.fail("Thread 2 cannot acquire lock");
            } catch (Exception eX) {
                LOGGER.error("Failed to acquire lock : " + eX.getMessage());
            }
        }, "Thread 2");
        // Start second thread
        secondThread.start();
        // Wait until the second thread is done
        secondThread.join();
    }

    @After
    public void after() {
        zookeeperServerThread.stop();
    }

    private void startZookeeper() {
        String[] args = new String[]{"2181", "/mnt1/zookeeper/data"};
        zookeeperServerThread = new Thread(() -> {
            try {
                ZooKeeperServerMain.main(args);
                LOGGER.error("Zookeeper server launched !!!");
            } catch (Exception ignored) {
                LOGGER.error("Failed to start zookeepr server");
            }
        });
        zookeeperServerThread.start();
    }
}
