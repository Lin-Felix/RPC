package Study.consumer;

import Study.codec.RequestEncoder;
import Study.codec.SSDecoder;
import Study.exception.RpcException;
import Study.message.Request;
import Study.message.Response;
import Study.register.DefaultServiceRegistry;
import Study.register.RegistryConfig;
import Study.register.ServiceMetadata;
import Study.register.ServiceRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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

    public ConsumerProxyFactory(ConsumerProperties consumerProperties) throws Exception {
        this.consumerProperties = consumerProperties;
        this.registry = new DefaultServiceRegistry(); // 通过工厂模式创建注册中心
        this.registry.init(consumerProperties.getRegistryConfig());
        this.manager = new ConnectionManager(creatBootStrap(consumerProperties));
        this.inFlightRequestTable = new ConcurrentHashMap<>();
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
                new ConsumerInvocationHandler(interfaceClass)/** 被增强的逻辑 **/);
    }


    public class ConsumerInvocationHandler implements InvocationHandler  {

        private Class<?> interfaceClass;

        public ConsumerInvocationHandler(Class<?> interfaceClass) {
            this.interfaceClass = interfaceClass;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable { // 代理对象，代理对象被调用的函数，被调用函数的参数
            if (method.getDeclaringClass() == Object.class) {
                return invokeObjectMethod(proxy, method, args);
            }
            CompletableFuture<Response> responseFuture = new CompletableFuture<>();
            try {
                List<ServiceMetadata> serviceMetadata = registry.fetchServiceList(interfaceClass.getName()); // 从注册中心拿到服务
                if (serviceMetadata.isEmpty()) {
                    throw new RpcException(interfaceClass.getName() + "没有对应的provider");
                }
                ServiceMetadata providerMetadata = serviceMetadata.get(0);
                Channel channel = manager.getChannel(providerMetadata.getHost(), providerMetadata.getPort()); // 函数内部同步建立连接
                if (null == channel) {
                    throw new RpcException("provider连接失败");
                }
                Request request = buildRequst(method, args);
                inFlightRequestTable.put(request.getRequestId(), responseFuture);
                channel.writeAndFlush(request).addListener(f->{
                    if (!f.isSuccess()) { // 如果请求发送失败，将在途请求从表中移除
                        inFlightRequestTable.remove(request.getRequestId());
                        responseFuture.completeExceptionally(f.cause());
                    }
                });
                Response response = responseFuture.get(consumerProperties.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);// 超时处理，仅等待3s
                return processResponse(response);

            } catch (RpcException rpcException) {
                throw rpcException;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
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
                log.warn("request Id{}找不到", response.getResult());
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
