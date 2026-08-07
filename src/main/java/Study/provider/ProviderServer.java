package Study.provider;

import Study.codec.SSEncoder;
import Study.compress.Compression;
import Study.compress.CompressionManager;
import Study.handler.HeartbeatHandler;
import Study.handler.TrafficRecordHandler;
import Study.limit.ConcurrencyLimiter;
import Study.limit.Limiter;
import Study.limit.RateLimiter;
import Study.message.Request;
import Study.codec.ResponseEncoder;
import Study.codec.SSDecoder;
import Study.message.Response;
import Study.register.DefaultServiceRegistry;
import Study.register.RegistryConfig;
import Study.register.ServiceMetadata;
import Study.register.ServiceRegistry;
import Study.serialize.Serializer;
import Study.serialize.SerializerManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lzk
 * @date 2026/6/23 14:36
 * @description 服务提供者
 */
@Slf4j
public class ProviderServer {
    private final ProviderProperties properties;

    private final ProviderRegistry registry; // 注册表

    private final ServiceRegistry serviceRegistry; // 注册中心

    private final Limiter globalLimiter; // 全局限流器

    private EventLoopGroup bossEventLoopGroup;

    private EventLoopGroup workerEventLoopGroup;

    private final SerializerManager serializerManager; // 序列化管理器

    private final CompressionManager compressionManager; // 压缩器管理器

    private ThreadPoolExecutor invokeExecutor; // 线程池：提高读写吞吐量


    public ProviderServer(ProviderProperties properties) {
        this.properties = properties;
        this.registry = new ProviderRegistry();
        this.serviceRegistry = new DefaultServiceRegistry();
        this.globalLimiter = new ConcurrencyLimiter(properties.getGlobalMaxRequest());
        this.serializerManager = new SerializerManager();
        this.compressionManager = new CompressionManager();
        this.invokeExecutor = new ThreadPoolExecutor(
                4,
                4,
                10,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new FastFailResponseHandler());
    }

    // 将函数注册至注册表
    public <I> void register(Class<I> interfaceClass, I serviceInstance) {
        registry.register(interfaceClass, serviceInstance);
    }


    // 启动服务器
    public void start() {
        bossEventLoopGroup = new NioEventLoopGroup();
        workerEventLoopGroup = new NioEventLoopGroup(properties.getWorkThreadNum());
        try {
            this.serviceRegistry.init(properties.getRegistryConfig()); // 注册中心初始化
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossEventLoopGroup, workerEventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<NioSocketChannel>() {
                        @Override
                        protected void initChannel(NioSocketChannel nioSocketChannel) throws Exception {
                            nioSocketChannel.pipeline()
                                    .addLast(new TrafficRecordHandler())
                                    .addLast(new SSDecoder())
                                    .addLast(new SSEncoder())
                                    .addLast(new IdleStateHandler(30, 5, 0, TimeUnit.SECONDS)) // 心跳检测处理器：30秒内没有接收到数据，则触发读空闲事件; 5秒内没有发送数据，则触发写空闲事件; 避免在RPC请求频发时发送心跳消息
                                    .addLast(new HeartbeatHandler()) // 心跳处理器
                                    .addLast(new LimiterHandler()) // 限流处理器
                                    .addLast(new ProviderHandler());
                        }
                    });
            serverBootstrap.bind(properties.getHost(), properties.getPort()).sync();
            // map(this::buildMetadata)等价于map(name -> buildMetadata(name))
            registry.allServiceName().stream().map(this::buildMetadata).forEach(this.serviceRegistry::registerService); // 将注册表的服务注册至注册中心中
        } catch (Exception e) {
            throw new RuntimeException("服务器启动异常", e);
        }
    }

    private ServiceMetadata buildMetadata(String serviceName) {
        ServiceMetadata serviceMetadata = new ServiceMetadata();
        serviceMetadata.setHost(properties.getHost());
        serviceMetadata.setPort(properties.getPort());
        serviceMetadata.setServiceName(serviceName);
        return serviceMetadata;
    }

    // 成员内部类：处理限流的处理器
    public class LimiterHandler extends ChannelDuplexHandler {
        private static final AttributeKey<Limiter> CHANNEL_LIMITER_KEY = AttributeKey.valueOf("channel_limiter_key"); // 当前Consumer的局部限流器

        private static final AttributeKey<AtomicInteger> GLOBAL_PERMITS = AttributeKey.valueOf("global_permits"); // 当前Consumer对全局限流的次数

        // 入站处理逻辑
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            Request request = (Request) msg;
            // 第一步：获取全局限流器
            if (!globalLimiter.tryAcquire()) {
                ctx.writeAndFlush(Response.fail("provider 限流", request.getRequestId()));
                return;
            }

            // 第二步：获取局部限流器
            Limiter channelLimiter = ctx.channel().attr(CHANNEL_LIMITER_KEY).get();
            if (!channelLimiter.tryAcquire()) {
                globalLimiter.release();
                ctx.writeAndFlush(Response.fail("provider 限流", request.getRequestId()));
                return;
            }

            ctx.channel().attr(GLOBAL_PERMITS).get().incrementAndGet();
            ctx.fireChannelRead(request);
        }


        // 出战处理逻辑，ChannelPromise:实现了Future接口和Promise接口；表示一次 Channel 异步操作结果的对象
        // write()执行表示完成响应，promise注册监听器执行限流器的释放操作
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            promise.addListener(f -> {
                int remain = ctx.channel().attr(GLOBAL_PERMITS).get().getAndDecrement();
                if (remain > 0) {
                    ctx.channel().attr(CHANNEL_LIMITER_KEY).get().release();
                    globalLimiter.release();
                }
            });
            ctx.write(msg, promise); // 写至Channel上下文
        }


        // 回调函数：连接建立时调用
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            RateLimiter rateLimiter = new RateLimiter(properties.getPerConsumerMaxRequest());
            ctx.channel().attr(CHANNEL_LIMITER_KEY).set(rateLimiter);
            ctx.channel().attr(GLOBAL_PERMITS).set(new AtomicInteger(0));
            ctx.fireChannelActive();
        }


        // 回调函数：连接断开时调用，注意：可能先于write()调用
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            int remain = ctx.channel().attr(GLOBAL_PERMITS).get().getAndSet(0);
            globalLimiter.release(remain);
            ctx.fireChannelInactive();
        }
    }

    public class ProviderHandler extends SimpleChannelInboundHandler<Request> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Request request) throws Exception {
            ProviderRegistry.Invocation<?> invocation = registry.findService(request.getServiceName()); //
            // 从注册表获取服务实例（即类实例）
            if (null == invocation) {
                Response failResp = Response.fail(String.format("%s 没有对应的服务", request.getServiceName()), request.getRequestId());
                ctx.writeAndFlush(failResp);
                return;
            }
            // 复习：execute(Runnable command)中参数表示一个任务，用lambda表达式书写，()->{}
            invokeExecutor.execute(new InvokeTask(request, ctx, invocation));
        }


        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("地址:{}连接了", ctx.channel().remoteAddress());
            ctx.channel().attr(SSEncoder.SERIALIZE_KEY).set(properties.getSerialize());
            ctx.channel().attr(SSEncoder.SERIALIZER_MANAGER_KEY).set(serializerManager);

            ctx.channel().attr(SSEncoder.COMPRESS_KEY).set(properties.getCompress());
            ctx.channel().attr(SSEncoder.COMPRESS_MANAGER_KEY).set(compressionManager);
            ctx.fireChannelActive();
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

    private static class FastFailResponseHandler implements RejectedExecutionHandler {

        // 由于入参为Runnable，便将线程池操作封装为Runnalbe
        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            if (task instanceof InvokeTask invokeTask) {
                Response fastFail = Response.fail("服务器繁忙", invokeTask.request.getRequestId());
                invokeTask.ctx.writeAndFlush(fastFail);
                return;
            }
            throw new RuntimeException("你的task 有问题！");
        }
    }

    private static class InvokeTask implements Runnable {
        private final Request request;
        private final ChannelHandlerContext ctx;
        private final ProviderRegistry.Invocation<?> invocation;

        InvokeTask(Request request, ChannelHandlerContext ctx, ProviderRegistry.Invocation<?> invocation) {
            this.request = request;
            this.ctx = ctx;
            this.invocation = invocation;
        }

        @Override
        public void run() {
            // 使EventLoop仅执行IO操作，线程池的线程执行业务逻辑（即invoke()调用）
            EventLoop eventLoop = ctx.channel().eventLoop();
            try {
                long startTime = System.currentTimeMillis();
                Object result = invocation.invoke(request.getMethodName(), request.getParamsClass(),
                        request.getParams());
                log.info("requestId:{}，{}，函数被调用了{}，结果是{}，耗时是{}ms",
                        request.getRequestId(),
                        request.getServiceName(),
                        request.getMethodName(),
                        result,
                        System.currentTimeMillis() - startTime);
                eventLoop.execute(() -> {
                    ctx.writeAndFlush(Response.success(result, request.getRequestId()));
                });
            } catch (Exception e) {
                eventLoop.execute(() -> {
                    Response failResp = Response.fail(e.getMessage(), request.getRequestId());
                    ctx.writeAndFlush(failResp);
                });
            }
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
