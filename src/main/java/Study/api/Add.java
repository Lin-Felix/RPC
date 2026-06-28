package Study.api; // 消费者和提供者共享的接口契约包

/**
 * @author lzk
 * @date 2026/6/26 16:23
 * @description Consumer端的可被RPC的接口
 */
public interface Add {
    int add(int a, int b);
}
