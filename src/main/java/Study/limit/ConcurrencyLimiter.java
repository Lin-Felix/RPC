package Study.limit;

import java.util.concurrent.Semaphore;

/**
 * @author lzk
 * @date 2026/7/9 17:10
 * @description 并发限流
 */
public class ConcurrencyLimiter implements Limiter {
    private final Semaphore semaphore;

    public ConcurrencyLimiter(int limitNum) {
        this.semaphore = new Semaphore(limitNum);
    }

    @Override
    public boolean tryAcquire() {
        return semaphore.tryAcquire();
    }

    @Override
    public void release(int permits) {
        semaphore.release(permits);
    }
}
