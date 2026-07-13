package Study.consumer;

import Study.codec.RequestEncoder;
import Study.codec.SSDecoder;
import Study.message.Response;
import Study.register.ServiceMetadata;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lzk
 * @date 2026/6/28 19:36
 * @description 连接管理器：保存长连接
 */
@Slf4j
public class ConnectionManager {
    private final Map<String, ChannelWrapper> channelTable = new ConcurrentHashMap<>();

    private final Bootstrap bootstrap;

    private final InFlightRequestManager inFlightRequestManager;

    private final ConsumerProperties properties;

    public ConnectionManager(InFlightRequestManager inFlightRequestManager, ConsumerProperties properties) {
        this.inFlightRequestManager = inFlightRequestManager;
        this.bootstrap = creatBootStrap(properties);
        this.properties = properties;
    }


    public Channel getChannel(ServiceMetadata metadata) {
        String host = metadata.getHost();
        int port = metadata.getPort();
        String key = host + ":" + port;
        ChannelWrapper channelWrapper = channelTable.computeIfAbsent(key, (k) -> {
            try {
                ChannelFuture channelFuture = bootstrap.connect(host, port).sync(); // 同步的方式建立连接
                Channel channel = channelFuture.channel();
                channel.closeFuture().addListener((f) -> {
                    channelTable.remove(key);
                    inFlightRequestManager.clearChannel(metadata);
                }); // 当监听到连接关闭时，移除连接
                return new ChannelWrapper(channel);
            } catch (InterruptedException e) {
                log.error("连接超时{}，{}", host, port, e);
                return new ChannelWrapper(null);
            }
        });
        Channel channel = channelWrapper.channel;
        if (null == channel || !channel.isActive()) { // 删除超时的失败连接和不活跃的连接
            channelTable.remove(key);
            return null;
        }
        return channel;
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

    private class ConsumerHandler extends SimpleChannelInboundHandler<Response> {
        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Response response) throws Exception {
            inFlightRequestManager.completeRequest(response.getRequestId(), response);
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


    private static class ChannelWrapper {
        private final Channel channel;

        public ChannelWrapper(Channel channel) {
            this.channel = channel;
        }
    }
}
