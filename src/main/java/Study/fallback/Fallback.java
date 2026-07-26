package Study.fallback;

import Study.metrics.RpcCallMetrics;

/**
 * @author lzk
 * @date 2026/7/15 18:29
 * @description 降级接口
 */
public interface Fallback {
    // 兜底处理
    Object fallback(RpcCallMetrics metrics) throws Exception;

    // 记录Rpc调用指标，default默认没有，便于D
    default void recordMetrics(RpcCallMetrics metrics) {

    }
}
