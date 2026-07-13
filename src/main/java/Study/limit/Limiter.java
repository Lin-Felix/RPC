package Study.limit;

/**
 * @author lzk
 * @date 2026/7/9 17:09
 * @description 限流接口 | 限流分为速率限流和并发限流
 */
public interface Limiter {

    boolean tryAcquire();

    default void release() {
        release(1);
    }

    void release(int permits);
}
