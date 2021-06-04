public interface DistributedLockFactory {

    DistributedLock getOrCreateLock(String lockKey);

}
