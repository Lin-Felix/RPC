package Study.breaker;

import Study.metrics.RpcCallMetrics;

/**
 * @author lzk
 * @date 2026/7/14 17:54
 * @description 熔断器接口
 */
public interface CircuitBreaker {

    boolean allowRequest();

    void recordRpc(RpcCallMetrics metrics);

    enum State {
        CLOSE, HALF_OPEN, OPEN,;
    }
}
