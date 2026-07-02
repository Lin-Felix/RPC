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

    private final ConnectionManager manager = new ConnectionManager(creatBootStrap());

    // 在途请求
    private final Map<Integer, CompletableFuture<Response>> inFlightRequestTable = new ConcurrentHashMap<>(); // key为requestId，value为响应

    private final ServiceRegistry registry;

    public ConsumerProxyFactory(RegistryConfig registryConfig) throws Exception {
        this.registry = new DefaultServiceRegistry(); // 通过工厂模式创建注册中心
        this.registry.init(registryConfig);
    }

    private Bootstrap creatBootStrap() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel nioSocketChannel) throws Exception {
                        nioSocketChannel.pipeline()
                                .addLast(new SSDecoder())
                                .addLast(new RequestEncoder())
                                .addLast(new SimpleChannelInboundHandler<Response>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Response response) throws Exception {
                                        CompletableFuture<Response> responseFuture = inFlightRequestTable.remove(response.getRequestId()); // 移除完成响应的在途请求
                                        if (null == responseFuture) {
                                            log.warn("request Id{}找不到", response.getResult());
                                            return;
                                        }
                                        responseFuture.complete(response);
                                    }
                                });
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
    public <I> I createConsumerProxy(Class<I> interfaceClass) {
        return (I) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{interfaceClass}, // 被代理的接口
                new InvocationHandler() { // 被增强的逻辑
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable { // 代理对象，代理对象被调用的函数，被调用函数的参数
                        if (method.getDeclaringClass() == Object.class) {
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
                        try {
                            CompletableFuture<Response> responseFuture = new CompletableFuture<>();
                            List<ServiceMetadata> serviceMetadata = registry.fetchServiceList(interfaceClass.getName()); // 从注册中心拿到服务
                            if (serviceMetadata.isEmpty()) {
                                throw new RpcException(interfaceClass.getName() + "没有对应的provider");
                            }
                            ServiceMetadata providerMetadata = serviceMetadata.get(0);
                            Channel channel = manager.getChannel(providerMetadata.getHost(), providerMetadata.getPort()); // 函数内部同步建立连接
                            if (null == channel) {
                                throw new RpcException("provider连接失败");
                            }
                            Request request = new Request();
                            request.setServiceName(interfaceClass.getName());
                            request.setMethodName(method.getName());
                            request.setParams(args);
                            request.setParamsClass(method.getParameterTypes());
                            inFlightRequestTable.put(request.getRequestId(), responseFuture);
                            channel.writeAndFlush(request).addListener(f->{
                                if (!f.isSuccess()) { // 如果请求发送失败，将在途请求从表中移除
                                    inFlightRequestTable.remove(request.getRequestId());
                                    responseFuture.completeExceptionally(f.cause());
                                }
                            });
                            Response response = responseFuture.get(3, TimeUnit.SECONDS);// 超时处理，仅等待3s
                            if (200 == response.getCode()) {
                                return response.getResult();
                            } else {
                                throw new RpcException(response.getErrorMessage());
                            }
                        } catch (RpcException rpcException) {
                            throw rpcException;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }


}
