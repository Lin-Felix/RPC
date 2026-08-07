package Study.retry;

import Study.exception.RpcException;
import Study.message.Response;
import Study.spi.Spi;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author lzk
 * @date 2026/7/8 16:12
 * @description 重试策略：指数退避
 */
@Spi("retrySame")
@Slf4j
public class RetrySame implements RetryPolicy {
    private final int retryMax = 3;

    private final Random random = new Random();

    @Override
    public Response retry(RetryContext context) throws Exception {
        long startTime = System.currentTimeMillis();
        int retryCount = 0;
        while (retryCount < retryMax) {
            long nextDelay = nextDelay(retryCount);
            if (nextDelay >= 1000) {
                nextDelay = 1000;
            }
            long methodTimeoutMs = context.getMethodTimeoutMs() - (System.currentTimeMillis() - startTime);
            if (methodTimeoutMs <= 0 || nextDelay >= methodTimeoutMs) {
                throw new TimeoutException();
            }
            Thread.sleep(nextDelay);
            try {
                log.info("开始重试");
                CompletableFuture<Response> future = context.doRpc(context.getFailService());
                return future.get(Math.min(methodTimeoutMs, context.getRequestTimeoutMs()), TimeUnit.MILLISECONDS); // 发起这次重试请求后，最多等min()来拿response，如果期间拿到 response，就立即返回
            } catch (Exception e) {
                log.error("重试发生错误", e);
            }
            ++retryCount;
        }
        throw new RpcException("重试失败");
    }

    private long nextDelay(int retryCount) {
        return 100L * (1 << retryCount) + random.nextInt(0, 50);
    }
}
