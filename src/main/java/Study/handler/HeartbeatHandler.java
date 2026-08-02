package Study.handler;

import Study.message.HeartbeatRequest;
import Study.message.HeartbeatResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * @author lzk
 * @date 2026/8/1 16:45
 * @description <Object>表示只能处理Object类型的对象（即消息）
 */
public class HeartbeatHandler extends SimpleChannelInboundHandler<Object> {

    // 当Channel接收到Object类型数据时，Netty自动调用此方法
    // 处理心跳请求、心跳响应
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HeartbeatRequest request) {
            ctx.writeAndFlush(new HeartbeatResponse(request.getRequestTime()));
            return;
        }

        if (msg instanceof HeartbeatResponse response) {
            long duration = System.currentTimeMillis() - response.getRequestTime();
            System.out.println("接受到了一个心跳响应，延迟： " + duration + "毫秒");
            return;
        }

        ctx.fireChannelRead(msg); // 传递给pipeline后续的Handler
    }


    /**
     * 用户事件回调函数：当 pipeline 中触发用户事件时被调用
     *
     * 常见的用户事件有：
     * ① IdleStateEvent（空闲状态事件，如读空闲、写空闲、读写空闲）
     * ② SslHandshakeCompletionEvent（SSL握手完成事件）
     * ③ 自定义用户事件（通过 ctx.fireUserEventTriggered() 触发）
     *
     * @param ctx 上下文对象
     * @param evt 事件对象
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idleStateEvent) {
            IdleState state = idleStateEvent.state();
            // 当收到读空闲事件: 关闭连接
            // 读事件：从 Channel 接收数据
            if (state == IdleState.READER_IDLE) {
                ctx.channel().close();
            }
            // 当收到写空闲事件: 发送心跳包，判断对方是否存活
            // 写事件：向 Channel 发送数据
            if (state == IdleState.WRITER_IDLE) {
                ctx.writeAndFlush(new HeartbeatRequest());
            }
        }
        ctx.fireUserEventTriggered(evt);
    }
}
