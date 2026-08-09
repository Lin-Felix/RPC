package Study.consumer;

import Study.breaker.CircuitBreaker;
import Study.breaker.CircuitBreakerManager;
import Study.codec.RequestEncoder;
import Study.codec.SSDecoder;
import Study.exception.RpcException;
import Study.fallback.CacheFallback;
import Study.fallback.DefaultFallback;
import Study.fallback.Fallback;
import Study.fallback.MockFallback;
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
import Study.retry.RetryPolicyManager;
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

    private final ConnectionManager manager; // 连接管理器

    private final ServiceRegistry registry;// 注册中心

    private final ConsumerProperties consumerProperties; // 消费者配置

    private final InFlightRequestManager inFlightRequestManager; // 在途请求管理器

    private final CircuitBreakerManager circuitBreakerManager; // 熔断管理器

    private final RetryPolicyManager retryPolicyManager; // 重试管理器：17-SPI机制增加的方法

    private final Fallback fallback; // 降级接口

    public ConsumerProxyFactory(ConsumerProperties consumerProperties) throws Exception {
        this.consumerProperties = consumerProperties;
        this.registry = new DefaultServiceRegistry(); // 通过工厂模式创建注册中心
        this.inFlightRequestManager = new InFlightRequestManager(consumerProperties);
        this.registry.init(consumerProperties.getRegistryConfig());
        this.manager = new ConnectionManager(inFlightRequestManager, consumerProperties);
        this.circuitBreakerManager = new CircuitBreakerManager(consumerProperties);
        this.retryPolicyManager = new RetryPolicyManager();
        this.fallback = new DefaultFallback(new CacheFallback(), new MockFallback());
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
                new ConsumerInvocationHandler(interfaceClass,
                        createLoadBalancer(),
                        createRetryPolicy(consumerProperties.getRetryPolicy()))/** 被增强的逻辑 **/);
    }

    private RetryPolicy createRetryPolicy(String name) {
        RetryPolicy retryPolicy = retryPolicyManager.getRetryPolicy(name);
        if (null == retryPolicy) {
            throw new IllegalArgumentException("没有这个重试策略" + name);
        }
        return retryPolicy;
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
            // 第一步: 判断是否是泛化调用
            boolean genericInvoke = method.getName().equals("$invoke");
            String serviceName = genericInvoke ? args[0].toString() : interfaceClass.getName(); // 服务名，本质是类名

            // 第二步：从注册中心得到服务元数据
            List<ServiceMetadata> serviceMetadata = new ArrayList<>(registry.fetchServiceList(serviceName));
            ServiceMetadata provider = decideProvider(serviceMetadata);
            RpcCallMetrics metrics = RpcCallMetrics.createRpcCallMetrics(method, args, provider);

            // 第三步：13-降级处理
            if (provider == null) {
                return fallback.fallback(metrics);
            }
            Request request = buildRequest(method, args);

            // 第四步：10和12-重试处理 及 熔断处理
            CircuitBreaker breaker = circuitBreakerManager.createOrGetBreaker(provider);
            try {
                CompletableFuture<Response> requestFuture = callRpcAsync(request, provider); // 备注：callRpcAsync的responseFuture和requestFuture本质是一个东西
                Response response = requestFuture.get(consumerProperties.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
                metrics.complete(response);
                breaker.recordRpc(metrics);
                fallback.recordMetrics(metrics);
                return processResponse(response);
            } catch (Exception e) {
                metrics.errorComplete(e);
                breaker.recordRpc(metrics);
            }
            // 13-降级处理； todo 疑问：执行catch后还能向下执行吗
            try {
                return processResponse(doRetry(metrics, serviceMetadata));
            } catch (Exception e) {
                return fallback.fallback(metrics);
            }
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
            return null; // 根据null值判断是否要进行兜底
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
                RpcCallMetrics retryMetrics = RpcCallMetrics.createRpcCallMetrics(metrics.getMethod(), metrics.getParams(), metrics.getProvider());
                CompletableFuture<Response> requestFuture = callRpcAsync(buildRequest(metrics.getMethod(), metrics.getParams()), provider);
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

        private Request buildRequest(Method method, Object[] args) {
            boolean genericInvoke = method.getName().equals("$invoke");
            Request request = new Request();
            request.setGenericInvoke(genericInvoke);
            if (genericInvoke) {
                request.setServiceName(args[0].toString());
                request.setMethodName(args[1].toString());
                request.setParamsClassStr((String[]) args[2]);
                request.setParams((Object[]) args[3]);
            } else {
                request.setServiceName(interfaceClass.getName());
                request.setMethodName(method.getName());
                request.setParams(args);
                request.setParamsClass(method.getParameterTypes());
            }

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
