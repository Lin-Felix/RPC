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
    private List<ServiceMetadata> serviceMetadataList;
    private long requestTimeoutMs;
    private long methodTimeoutMs;
    private LoadBalancer loadBalancer;
    private Function<ServiceMetadata, CompletableFuture<Response>> doRpcFunction; // 一个"根据服务元数据发起 RPC 调用并返回异步响应"的函数

    public CompletableFuture<Response> doRpc(ServiceMetadata serviceMetadata) {
        return doRpcFunction.apply(serviceMetadata); // 实现转换
    }
}
