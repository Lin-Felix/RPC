package Study.consumer;

import Study.exception.LimitException;
import Study.limit.ConcurrencyLimiter;
import Study.limit.Limiter;
import Study.limit.RateLimiter;
import Study.message.Request;
import Study.message.Response;
import Study.register.ServiceMetadata;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author lzk
 * @date 2026/7/11 17:15
 * @description
 */
@Slf4j
public class InFlightRequestManager {
    private final ConsumerProperties consumerProperties;

    private final Map<Integer, CompletableFuture<Response>> inFlightRequestTable; // 在途请求表：维护没有得到响应的请求；key为requestId，value为响应

    private final HashedWheelTimer timeoutTimer; // 时间轮：用于处理过期任务

    private final Limiter globalLimiter; // 全局限流器

    private final Map<ServiceMetadata, Limiter> channelLimiterMap; // 为每个Channel分配一个限流器，由Map管理；key为ServiceMetadata, value为Limiter

    public InFlightRequestManager(ConsumerProperties consumerProperties) {
        this.consumerProperties = consumerProperties;
        inFlightRequestTable = new ConcurrentHashMap<>();
        timeoutTimer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS, 256);
        this.globalLimiter = new ConcurrencyLimiter(consumerProperties.getRpcPerSecond());
        this.channelLimiterMap = new ConcurrentHashMap<>();
    }

    // 将request保存至在途请求表中管理
    public CompletableFuture<Response> inFlightRequest(Request request, long timeOutMs, ServiceMetadata metadata) {
        CompletableFuture<Response> responseFuture = new CompletableFuture<>();

        // 第一步：进行全局限流
        if (!globalLimiter.tryAcquire()) {
            responseFuture.completeExceptionally(new LimitException("当前请求被限流"));
            return responseFuture;
        }

        // 第二步：进行局部限流
        Limiter channelLimiter = channelLimiterMap.computeIfAbsent(metadata,
                k -> new RateLimiter(consumerProperties.getRpcPerChannel())); // 备注：k仅仅是为了满足lambda表达式语法的书写
        if (!channelLimiter.tryAcquire()) {
            globalLimiter.release();
            responseFuture.completeExceptionally(new LimitException("channel限流， 当前在途请求达到阈值"));;
            return responseFuture;
        }

        inFlightRequestTable.put(request.getRequestId(), responseFuture);

        // 设置定时任务：若超过请求超时时间，将 responseFuture 标记为异常完成
        Timeout timeout = timeoutTimer.newTimeout(
                (t) -> responseFuture.completeExceptionally(new TimeoutException()),
                timeOutMs,
                TimeUnit.MILLISECONDS);

        // 无论任务正常/异常完成，将request从在途请求表中删除
        responseFuture.whenComplete((r, e) -> {
            inFlightRequestTable.remove(request.getRequestId());
            globalLimiter.release();
            timeout.cancel();
        });

        return responseFuture;
    }

    public void clearChannel(ServiceMetadata metadata) {
        channelLimiterMap.remove(metadata);
    }

    // 从在途请求表中移除正常响应的请求
    public boolean completeRequest(int requestId, Response response) {
        CompletableFuture<Response> future = inFlightRequestTable.remove(requestId);
        if (null == future) {
            log.warn("request_Id:{}，空闲返回", requestId);
            return false;
        }
        return future.complete(response);
    }

    // 从在途请求表中移除异常结束的请求
    public boolean completeRequestExceptionally(int requestId, Exception e) {
        CompletableFuture<Response> future = inFlightRequestTable.remove(requestId);
        if (null == future) {
            log.warn("request_Id:{}，空闲异常", requestId, e);
            return false;
        }
        return future.completeExceptionally(e);
    }
}
