package Study.provider;

import Study.message.Request;
import Study.codec.ResponseEncoder;
import Study.codec.SSDecoder;
import Study.message.Response;
import Study.register.DefaultServiceRegistry;
import Study.register.RegistryConfig;
import Study.register.ServiceMetadata;
import Study.register.ServiceRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lzk
 * @date 2026/6/23 14:36
 * @description 提供者
 */
@Slf4j
public class ProviderServer {
    private final String host;

    private final int port;

    private final ProviderRegistry registry; // 注册表

    private final RegistryConfig registryConfig; // 注册中心的配置类

    private final ServiceRegistry serviceRegistry; // 注册中心

    private EventLoopGroup bossEventLoopGroup;

    private EventLoopGroup workerEventLoopGroup;



    public ProviderServer(String host, int port, RegistryConfig registryConfig) {
        this.host = host;
        this.port = port;
        this.registry = new ProviderRegistry();
        this.registryConfig = registryConfig;
        this.serviceRegistry = new DefaultServiceRegistry();
    }

    // 将函数注册至注册表
    public <I> void register(Class<I> interfaceClass, I serviceInstance) {
        registry.register(interfaceClass, serviceInstance);
    }


    // 启动服务器
    public void start() {
        bossEventLoopGroup = new NioEventLoopGroup();
        workerEventLoopGroup = new NioEventLoopGroup(4);
        try {
            this.serviceRegistry.init(registryConfig); // 注册中心初始化
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossEventLoopGroup, workerEventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<NioSocketChannel>() {
                        @Override
                        protected void initChannel(NioSocketChannel nioSocketChannel) throws Exception {
                            nioSocketChannel.pipeline()
                                    .addLast(new SSDecoder())
                                    .addLast(new ResponseEncoder())
                                    .addLast(new ProviderHandler());
                        }
                    });
            serverBootstrap.bind(port).sync();
            // map(this::buildMetadata)等价于map(name -> buildMetadata(name))
            registry.allServiceName().stream().map(this::buildMetadata).forEach(this.serviceRegistry::registerService); // 将注册表的服务注册至注册中心中
        } catch (Exception e) {
            throw new RuntimeException("服务器启动异常", e);
        }
    }

    private ServiceMetadata buildMetadata(String serviceName) {
        ServiceMetadata serviceMetadata = new ServiceMetadata();
        serviceMetadata.setHost(host);
        serviceMetadata.setPort(port);
        serviceMetadata.setServiceName(serviceName);
        return serviceMetadata;
    }

    public class ProviderHandler extends SimpleChannelInboundHandler<Request> {
        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Request request) throws Exception {
            ProviderRegistry.Invocation<?> invocation = registry.findService(request.getServiceName()); //
            // 从注册表获取服务实例（即类实例）
            if (null == invocation) {
                Response failResp = Response.fail(String.format("%s 没有对应的服务", request.getServiceName()), request.getRequestId());
                channelHandlerContext.writeAndFlush(failResp);
                return;
            }
            try {
                Object result = invocation.invoke(request.getMethodName(), request.getParamsClass(),
                        request.getParams());
                log.info("{}，函数被调用了{}，结果是{}", request.getServiceName(), request.getMethodName(), result);
                channelHandlerContext.writeAndFlush(Response.success(result, request.getRequestId()));
            } catch (Exception e) {
                Response failResp = Response.fail(e.getMessage(), request.getRequestId());
                channelHandlerContext.writeAndFlush(failResp);
            }
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

    // 关闭服务器
    public void stop() {
        if (bossEventLoopGroup != null) {
            bossEventLoopGroup.shutdownGracefully();
        }
        if (workerEventLoopGroup != null) {
            workerEventLoopGroup.shutdownGracefully();
        }
    }

}
