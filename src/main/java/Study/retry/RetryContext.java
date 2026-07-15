package Study.retry;

import Study.loadbalance.LoadBalancer;
import Study.message.Response;
import Study.register.ServiceMetadata;
import lombok.Data;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * @author lzk
 * @date 2026/7/8 16:07
 * @description
 */
@Data
public class RetryContext {
    private ServiceMetadata failService; // 失败服务的元数据
    private List<ServiceMetadata> serviceMetadataList; // 可用的服务器的元数据集合
    private long requestTimeoutMs; // 单次RPC请求超时时间
    private long methodTimeoutMs; // 方法调用的超时时间（总时间预算） = 首次请求实际耗时 + 重试等待时间 + 重试请求实际耗时
    private LoadBalancer loadBalancer; // 负载均衡器
    private Function<ServiceMetadata, CompletableFuture<Response>> doRpcFunction; // 一个"根据服务元数据发起 RPC 调用并返回异步响应"的函数

    public CompletableFuture<Response> doRpc(ServiceMetadata serviceMetadata) {
        return doRpcFunction.apply(serviceMetadata); // 实现转换
    }
}
