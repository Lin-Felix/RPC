package Study.limit;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lzk
 * @date 2026/7/9 17:13
 * @description 速率限流：令牌桶平滑版本：允许1s只能请求1次 | 13:47开始
 */
public class RateLimiter implements Limiter {
    private static final int MAX_TRY_ACQUIRE = 512; // 最大重试次数

    private static final long MAX_QUEUE_NS = TimeUnit.MILLISECONDS.toNanos(500); // 最大重试（排队）时间

    private final AtomicLong nextTokensNs; // 下一次令牌可用的时间，在该时间前不允许拿token

    private final long intervalNs; // 规定两次请求的间隔时间（Ns，纳秒为单位）

    public RateLimiter(int permitsPerSecond) {
        this.intervalNs = TimeUnit.SECONDS.toNanos(1) / permitsPerSecond;
        this.nextTokensNs = new AtomicLong(0L);
    }

    @Override
    public boolean tryAcquire() {
        long now = System.nanoTime();
        for (int count = 0; count < MAX_TRY_ACQUIRE; ++count) {
            long pre = nextTokensNs.get();
            if (now + MAX_QUEUE_NS < pre) {
                return false;
            }
            if (nextTokensNs.compareAndSet(pre, Math.max(now, pre) + intervalNs)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void release(int permits) {

    }
}
