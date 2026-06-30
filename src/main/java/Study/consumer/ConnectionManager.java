package Study.consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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

    public ConnectionManager(Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }


    public Channel getChannel(String host, int port) {
        String key = host + ":" + port;
        ChannelWrapper channelWrapper = channelTable.computeIfAbsent(key, (k) -> {
            try {
                ChannelFuture channelFuture = bootstrap.connect(host, port).sync(); // 同步的方式建立连接
                Channel channel = channelFuture.channel();
                channel.closeFuture().addListener((f) -> channelTable.remove(key)); // 当监听到连接关闭时，移除连接
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

    private static class ChannelWrapper {
        private final Channel channel;

        public ChannelWrapper(Channel channel) {
            this.channel = channel;
        }
    }
}
