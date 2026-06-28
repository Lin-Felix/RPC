package Study.consumer;

import Study.api.Add;
import Study.exception.RpcException;
import Study.message.Request;
import Study.codec.RequestEncoder;
import Study.message.Response;
import Study.codec.SSDecoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author lzk
 * @date 2026/6/23 14:36
 * @description 消费者
 */
public class Consumer implements Add  {
    // 在途请求
    private Map<Integer, CompletableFuture<?>> inFlightRequestTable = new ConcurrentHashMap<>(); // key为requestId，value为响应

    private ConnectionManager manager = new ConnectionManager(creatBootStrap());

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
                                        CompletableFuture requestFuture = inFlightRequestTable.remove(response.getRequestId()); // 移除完成响应的在途请求
                                        if (200 == response.getCode()) {
                                            requestFuture.complete(Integer.valueOf(response.getResult().toString()));
                                        } else {
                                            requestFuture.completeExceptionally(new RpcException(response.getErrorMessage()));
                                        }
                                    }
                                });
                    }
                });
        return bootstrap;
    }

    public int add(int a, int b) {
        try {
            CompletableFuture<Integer> addResultFuture = new CompletableFuture<>();
            Channel channel = manager.getChannel("localhost", 7777);
            if (null == channel) {
                throw new RpcException("provider连接失败");
            }
            Request request = new Request();
            request.setServiceName(Add.class.getName());
            request.setMethodName("add");
            request.setParams(new Object[]{a, b});
            request.setParamsClass(new Class[]{int.class, int.class});
            channel.writeAndFlush(request).addListener(f->{
                if (f.isSuccess()) {
                    inFlightRequestTable.put(request.getRequestId(), addResultFuture);
                }
            });
            return addResultFuture.get(3, TimeUnit.SECONDS); // 超时处理，仅等待3s
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
