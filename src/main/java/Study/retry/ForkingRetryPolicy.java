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
 * @date 2026/7/8 17:42
 * @description 重试接口的实现类：请求对冲重试
 */
public class ForkingRetryPolicy implements RetryPolicy {
    @Override
    public Response retry(RetryContext context) throws Exception {
        List<ServiceMetadata> serviceMetadataList = new ArrayList<>(context.getServiceMetadataList());
        serviceMetadataList.remove(context.getFailService());
        if (serviceMetadataList.isEmpty()) {
            throw new RpcException("没有可重试的provider");
        }
        CompletableFuture[] allFuture = new CompletableFuture[serviceMetadataList.size()];
        for (int i = 0; i < serviceMetadataList.size(); i++) {
            allFuture[i] = context.doRpc(serviceMetadataList.get(i));
        }
        CompletableFuture<Object> mainFuture = CompletableFuture.anyOf(allFuture);
        return (Response) mainFuture.get(Math.min(context.getMethodTimeoutMs(), context.getRequestTimeoutMs()), TimeUnit.MILLISECONDS);
    }
}
