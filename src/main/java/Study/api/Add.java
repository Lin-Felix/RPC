package Study.api; // 消费者和提供者共享的接口契约包

import Study.fallback.RpcFallback;

/**
 * @author lzk
 * @date 2026/6/26 16:23
 * @description Consumer端的可被RPC的接口
 */
@RpcFallback(ConsumerAddImpl.class)
public interface Add {
    Integer add(int a, int b);

    Integer minus(int a, int b);
}
