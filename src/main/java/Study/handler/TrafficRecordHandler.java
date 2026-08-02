package Study.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lzk
 * @date 2026/8/1 18:45
 * @description 流量统计处理器：计算上行流量和下行流量; 流量指传输的字节数;
 * 从11:19开始听
 */
public class TrafficRecordHandler extends ChannelDuplexHandler {
    public static final AttributeKey<TrafficRecord> RECORD_ATTRIBUTE_KEY = AttributeKey.valueOf("traffic_record");

    private TrafficRecord trafficRecord; // 便于更快查找，避免每次.channel().attr().get(); 类似属性中使用Map作为缓存的效果

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf byteBuf) {
            trafficRecord.download.getAndAdd(byteBuf.readableBytes());// ctx.channel().attr(RECORD_ATTRIBUTE_KEY).get().download.getAndAdd(((ByteBuf) msg).readableBytes());
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf byteBuf) {
            trafficRecord.upload.getAndAdd(byteBuf.readableBytes());
            ctx.write(msg);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        trafficRecord = new TrafficRecord();
        // eventLoop是对线程的抽象
        ctx.channel().eventLoop().scheduleAtFixedRate(() ->
            {System.out.printf("当前上行流量：%d 下行流量 %d %n", trafficRecord.upload.get(), trafficRecord.download.get());},
                5, 5, TimeUnit.SECONDS);
        ctx.channel().attr(RECORD_ATTRIBUTE_KEY).set(trafficRecord);
        super.channelActive(ctx);
    }

    private class TrafficRecord {
        private AtomicLong download = new AtomicLong(); // 因为没有在构造函数中实例化，因为在属性定义时进行new

        private AtomicLong upload = new AtomicLong();
    }
}
