package Study.retry;

import Study.exception.RpcException;
import Study.message.Response;
import Study.register.ServiceMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author lzk
 * @date 2026/7/8 17:32
 * @description 重试策略：故障转移
 */
public class FailoverRetryPolicy implements RetryPolicy {
    @Override
    public Response retry(RetryContext context) throws Exception {
        List<ServiceMetadata> serviceMetadataList = new ArrayList<>(context.getServiceMetadataList());
        serviceMetadataList.remove(context.getFailService());
        if (null == serviceMetadataList) {
            throw new RpcException("没有可以重试的provider");
        }
        ServiceMetadata failoverService = context.getLoadBalancer().select(serviceMetadataList);
        CompletableFuture<Response> future = context.doRpc(failoverService);
        return future.get(Math.min(context.getMethodTimeoutMs(), context.getRequestTimeoutMs()), TimeUnit.MILLISECONDS);
    }
}
