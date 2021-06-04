import java.util.concurrent.TimeUnit;

public interface DistributedLock {

    void lock(long time, TimeUnit timeUnit);

    void unlock();

    String getKey();
}
