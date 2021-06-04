import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.concurrent.TimeUnit;

public class DistributedLockImpl implements DistributedLock {

    private InterProcessMutex mutex;
    private String key;

    public DistributedLockImpl(CuratorFramework client, String lockPath) {
        this.mutex = new InterProcessMutex(client, lockPath);
        this.key = lockPath;
    }

    @Override
    public void lock(long time, TimeUnit timeUnit) {
        boolean isLockAcquired;
        try {
            isLockAcquired = mutex.acquire(time, timeUnit);
            if (!isLockAcquired) {
                throw new RuntimeException("This lock cannot be acquired");
            }
        } catch (Exception eX) {
            throw new RuntimeException(eX);
        }
    }

    @Override
    public void unlock() {
        if (mutex != null) {
            try {
                mutex.release();
            } catch (Exception logAndIgnore) {
                //
            }
        }
    }

    @Override
    public String getKey() {
        return key;
    }
}
