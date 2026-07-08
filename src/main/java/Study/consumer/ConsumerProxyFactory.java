package Study.consumer;

import Study.codec.RequestEncoder;
import Study.codec.SSDecoder;
import Study.exception.RpcException;
import Study.loadbalance.LoadBalancer;
import Study.loadbalance.RandomLoadBalancer;
import Study.loadbalance.RoundRobinLoadBalancer;
import Study.message.Request;
import Study.message.Response;
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
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

    // 在途请求
    private final Map<Integer, CompletableFuture<Response>> inFlightRequestTable; // key为requestId，value为响应

    private final ServiceRegistry registry;

    private final ConsumerProperties consumerProperties;

    private final HashedWheelTimer timeoutTimer; // 时间轮：用于处理过期任务

    public ConsumerProxyFactory(ConsumerProperties consumerProperties) throws Exception {
        this.consumerProperties = consumerProperties;
        this.registry = new DefaultServiceRegistry(); // 通过工厂模式创建注册中心
        this.registry.init(consumerProperties.getRegistryConfig());
        this.manager = new ConnectionManager(creatBootStrap(consumerProperties));
        this.inFlightRequestTable = new ConcurrentHashMap<>();
        this.timeoutTimer = new HashedWheelTimer(1, TimeUnit.SECONDS, 64);
    }

    private Bootstrap creatBootStrap(ConsumerProperties consumerProperties) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup(consumerProperties.getWorkThreadNum()))
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, consumerProperties.getConnectTimeoutMs())
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel nioSocketChannel) throws Exception {
                        nioSocketChannel.pipeline()
                                .addLast(new SSDecoder())
                                .addLast(new RequestEncoder())
                                .addLast(new ConsumerHandler());
                    }
                });
        return bootstrap;
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
            long startTime = System.currentTimeMillis();
            List<ServiceMetadata> serviceMetadata = registry.fetchServiceList(interfaceClass.getName()); // 从注册中心拿到服务元数据
            if (serviceMetadata.isEmpty()) {
                throw new RpcException(interfaceClass.getName() + "没有对应的provider");
            }
            ServiceMetadata providerMetadata = loadBalancer.select(serviceMetadata); // 负载均衡
            Request request = buildRequst(method, args);
            Response response;
            try {
                CompletableFuture<Response> requestFuture = callRpcAsync(request, providerMetadata); // 备注：callRpcAsync的responseFuture和requestFuture本质是一个东西
                response = requestFuture.get(consumerProperties.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                // 重试逻辑
                long methodTimeoutMs = consumerProperties.getMethodTimeoutMs() - (System.currentTimeMillis() - startTime);
                if (methodTimeoutMs <= 0) {
                    throw new TimeoutException();
                }
                RetryContext retryContext = new RetryContext();
                retryContext.setFailService(providerMetadata);
                retryContext.setServiceMetadataList(serviceMetadata);
                retryContext.setMethodTimeoutMs(methodTimeoutMs);
                retryContext.setRequestTimeoutMs(consumerProperties.getRequestTimeoutMs());
                retryContext.setLoadBalancer(this.loadBalancer);
                retryContext.setDoRpcFunction(provider -> callRpcAsync(buildRequst(method, args), provider));
                response = this.retryPolicy.retry(retryContext);
            }
            return processResponse(response);
        }

        /**
         * 异步向provider发送request, 对invoke()进行解耦
         * @param request
         * @param provider
         * @return
         */
        private CompletableFuture<Response> callRpcAsync(Request request, ServiceMetadata provider) {
            CompletableFuture<Response> responseFuture = new CompletableFuture<>();
            Channel channel = manager.getChannel(provider.getHost(), provider.getPort()); // 函数内部同步建立连接
            if (null == channel) {
                responseFuture.completeExceptionally(new RpcException(new String("provider连接失败")));
                return responseFuture;
            }

            inFlightRequestTable.put(request.getRequestId(), responseFuture);

            // 设置定时任务：若超过请求超时时间，将 responseFuture 标记为异常完成
            Timeout timeout = timeoutTimer.newTimeout((t) -> responseFuture.completeExceptionally(new TimeoutException()),
                    consumerProperties.getRequestTimeoutMs(),
                    TimeUnit.MILLISECONDS);

            // 无论任务正常/异常完成，将request从在途请求表中删除
            responseFuture.whenComplete((r, e) -> {
                inFlightRequestTable.remove(request.getRequestId());
                timeout.cancel();
            });

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

    private class ConsumerHandler extends SimpleChannelInboundHandler<Response> {
        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Response response) throws Exception {
            CompletableFuture<Response> responseFuture = inFlightRequestTable.remove(response.getRequestId()); // 移除完成响应的在途请求
            if (null == responseFuture) {
                log.warn("request Id {}找不到", response.getRequestId());
                return;
            }
            responseFuture.complete(response);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("地址:{}连接了", ctx.channel().remoteAddress());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("地址:{} 断开了连接", ctx.channel().remoteAddress());
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("发生了异常", cause);
            ctx.channel().close();
        }
    }
}
