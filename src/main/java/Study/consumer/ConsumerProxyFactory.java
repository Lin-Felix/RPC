package Study.consumer;

import Study.breaker.CircuitBreaker;
import Study.breaker.CircuitBreakerManager;
import Study.codec.RequestEncoder;
import Study.codec.SSDecoder;
import Study.exception.RpcException;
import Study.limit.RateLimiter;
import Study.loadbalance.LoadBalancer;
import Study.loadbalance.RandomLoadBalancer;
import Study.loadbalance.RoundRobinLoadBalancer;
import Study.message.Request;
import Study.message.Response;
import Study.metrics.RpcCallMetrics;
import Study.register.DefaultServiceRegistry;
import Study.register.ServiceMetadata;
import Study.register.ServiceRegistry;
import Study.retry.FailoverRetryPolicy;
import Study.retry.ForkingRetryPolicy;
import Study.retry.RetryContext;
import Study.retry.RetryPolicy;
import Study.retry.RetrySame;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author lzk
 * @date 2026/6/30 10:12
 * @description Consumer代理对象工厂，工厂生成ConuserProxy对象
 */
@Slf4j
public class ConsumerProxyFactory {

    private final ConnectionManager manager;

    private final ServiceRegistry registry;

    private final ConsumerProperties consumerProperties;

    private final InFlightRequestManager inFlightRequestManager;

    private final CircuitBreakerManager circuitBreakerManager;

    public ConsumerProxyFactory(ConsumerProperties consumerProperties) throws Exception {
        this.consumerProperties = consumerProperties;
        this.registry = new DefaultServiceRegistry(); // 通过工厂模式创建注册中心
        this.inFlightRequestManager = new InFlightRequestManager(consumerProperties);
        this.registry.init(consumerProperties.getRegistryConfig());
        this.manager = new ConnectionManager(inFlightRequestManager, consumerProperties);
        this.circuitBreakerManager = new CircuitBreakerManager(consumerProperties);
    }

    /**
     *
     * @param interfaceClass
     * @return Consumer代理对象
     * @param <I>
     */
    @SuppressWarnings("unchecked")
    public <I> I createConsumerProxy(Class<I> interfaceClass) {
        return (I) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                new Class[]{interfaceClass}, // 被代理的接口
                new ConsumerInvocationHandler(interfaceClass, createLoadBalancer(), createRetryPolicy())/** 被增强的逻辑 **/);
    }

    private RetryPolicy createRetryPolicy() {
        switch (consumerProperties.getRetryPolicy()) {
            case "retrySame":
                return new RetrySame();
            case "failover":
                return new FailoverRetryPolicy();
            case "forking":
                return new ForkingRetryPolicy();
        }
        throw new IllegalArgumentException("没有这个重试策略" + consumerProperties.getRetryPolicy());
    }

    private LoadBalancer createLoadBalancer() {
        switch (this.consumerProperties.getLoadBalancePolicy()) {
            case "robin":
                return new RoundRobinLoadBalancer();
            case "random":
                return new RandomLoadBalancer();
            default:
                throw new IllegalArgumentException(this.consumerProperties.getLoadBalancePolicy() + "负载均衡不支持");

        }
    }


    public class ConsumerInvocationHandler implements InvocationHandler  {

        private final Class<?> interfaceClass;

        private final LoadBalancer loadBalancer;

        private final RetryPolicy retryPolicy;

        public ConsumerInvocationHandler(Class<?> interfaceClass, LoadBalancer loadBalancer, RetryPolicy retryPolicy) {
            this.interfaceClass = interfaceClass;
            this.loadBalancer = loadBalancer;
            this.retryPolicy = retryPolicy;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable { // 代理对象，代理对象被调用的函数，被调用函数的参数
            if (method.getDeclaringClass() == Object.class) {
                return invokeObjectMethod(proxy, method, args);
            }
            List<ServiceMetadata> serviceMetadata = new ArrayList<>(registry.fetchServiceList(interfaceClass.getName())); // 从注册中心拿到服务元数据
            ServiceMetadata provider = decideProvider(serviceMetadata);
            Request request = buildRequst(method, args);
            Response response;
            RpcCallMetrics metrics = RpcCallMetrics.createRpcCallMetrics(method, args, provider);
            CircuitBreaker breaker = circuitBreakerManager.createOrGetBreaker(provider);
            try {
                CompletableFuture<Response> requestFuture = callRpcAsync(request, provider); // 备注：callRpcAsync的responseFuture和requestFuture本质是一个东西
                response = requestFuture.get(consumerProperties.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
                metrics.complete(response);
                breaker.recordRpc(metrics);
            } catch (Exception e) {
                metrics.errorComplete(e);
                breaker.recordRpc(metrics);
                response = doRetry(metrics, serviceMetadata);
            }
            return processResponse(response);
        }

        // 选择熔断器关闭的provider
        private ServiceMetadata decideProvider(List<ServiceMetadata> candidate) {
            while (!candidate.isEmpty()) {
                ServiceMetadata select = this.loadBalancer.select(candidate); // 通过负载均衡得到provider
                CircuitBreaker breaker = circuitBreakerManager.createOrGetBreaker(select);
                if (breaker.allowRequest()) {
                    return select;
                }
                candidate.remove(select);
            }
            throw new RpcException("当前没有可提供服务的provider");
        }

        // 重试
        private Response doRetry(RpcCallMetrics metrics, List<ServiceMetadata> serviceMetadata) throws Exception {
            Throwable e = metrics.getThrowable();
            // 若异步任务失败的原因是“不允许重试”的RpcException，就直接抛出该异常
            if (e instanceof ExecutionException ee && ee.getCause() instanceof RpcException rpcException && !rpcException.retry()) {
                throw rpcException;
            }
            Response response;
            // 重试逻辑
            long methodTimeoutMs = consumerProperties.getMethodTimeoutMs() - metrics.getDuration();
            if (methodTimeoutMs <= 0) {
                throw new TimeoutException();
            }
            log.warn("rpc出现异常，并进行重试", e);
            RetryContext retryContext = createRetryContextFromFailMetrics(metrics, serviceMetadata, methodTimeoutMs);
            response = this.retryPolicy.retry(retryContext);
            return response;
        }

        // 构建重试上下文
        private @NonNull RetryContext createRetryContextFromFailMetrics(RpcCallMetrics metrics, List<ServiceMetadata> serviceMetadata, long methodTimeoutMs) {
            RetryContext retryContext = new RetryContext();
            retryContext.setFailService(metrics.getProvider());
            retryContext.setServiceMetadataList(serviceMetadata);
            retryContext.setMethodTimeoutMs(methodTimeoutMs);
            retryContext.setRequestTimeoutMs(consumerProperties.getRequestTimeoutMs());
            retryContext.setLoadBalancer(this.loadBalancer);
            retryContext.setDoRpcFunction(provider -> {
                CircuitBreaker breaker = circuitBreakerManager.createOrGetBreaker(provider);
                if (!breaker.allowRequest()) {
                    CompletableFuture<Response> breakFuture = new CompletableFuture<>();
                    breakFuture.completeExceptionally(new RpcException("provider熔断"));
                    return breakFuture;
                }
                RpcCallMetrics retryMetrics = RpcCallMetrics.createRpcCallMetrics(metrics.getMethod(), metrics.getArgs(), metrics.getProvider());
                CompletableFuture<Response> requestFuture = callRpcAsync(buildRequst(metrics.getMethod(), metrics.getArgs()), provider);
                requestFuture.whenComplete((r, retryE) -> {
                    if (null == retryE) {
                        retryMetrics.complete(r);
                    } else {
                        retryMetrics.errorComplete(retryE);
                    }
                    breaker.recordRpc(retryMetrics);
                });
                return requestFuture;
            });
            return retryContext;
        }

        /**
         * 异步向provider发送request, 对invoke()进行解耦
         * @param request
         * @param provider
         * @return
         */
        private CompletableFuture<Response> callRpcAsync(Request request, ServiceMetadata provider) {
            CompletableFuture<Response> responseFuture = inFlightRequestManager
                    .inFlightRequest(
                        request,
                        consumerProperties.getRequestTimeoutMs(),
                        provider);
            Channel channel = manager.getChannel(provider); // 函数内部同步建立连接
            if (null == channel) {
                responseFuture.completeExceptionally(new RpcException(new String("provider连接失败")));
                return responseFuture;
            }

            channel.writeAndFlush(request).addListener(f -> {
                log.info("发送了request:{}", request.getRequestId());
                if (!f.isSuccess()) {
                    responseFuture.completeExceptionally(f.cause());
                }
            });

            return responseFuture;
        }

        private Object processResponse(Response response) {
            if (200 == response.getCode()) {
                return response.getResult();
            }
            throw new RpcException(response.getErrorMessage());
        }

        private Request buildRequst(Method method, Object[] args) {
            Request request = new Request();
            request.setServiceName(interfaceClass.getName());
            request.setMethodName(method.getName());
            request.setParams(args);
            request.setParamsClass(method.getParameterTypes());
            return request;
        }

        private Object invokeObjectMethod(Object proxy, Method method, Object[] args) {
            if (method.getName().equals("toString")) {
                return "Proxy Consumer" + interfaceClass.getName();
            }
            if (method.getName().equals("equals")) {
                return args[0] == proxy;
            }
            if (method.getName().equals("hashCode")) {
                return System.identityHashCode(proxy);
            }
            throw new UnsupportedOperationException(method.getName());
        }
    }
}
