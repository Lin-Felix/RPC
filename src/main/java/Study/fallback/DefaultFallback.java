package Study.fallback;

import Study.metrics.RpcCallMetrics;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lzk
 * @date 2026/7/15 18:46
 * @description 默认降级类
 */
@Slf4j
public class DefaultFallback implements Fallback {
    private final CacheFallback cacheFallback;

    private final MockFallback mockFallback;

    public DefaultFallback(CacheFallback cacheFallback,  MockFallback mockFallback) {
        this.cacheFallback = cacheFallback;
        this.mockFallback = mockFallback;
    }

    @Override
    public void recordMetrics(RpcCallMetrics metrics) {
        this.cacheFallback.recordMetrics(metrics);
        this.mockFallback.recordMetrics(metrics);
    }

    @Override
    public Object fallback(RpcCallMetrics metrics) throws Exception {
        try {
            return cacheFallback.fallback(metrics);
        } catch (Exception e) {
            log.warn("缓存降级失效");
            return mockFallback.fallback(metrics);
        }
    }

}
