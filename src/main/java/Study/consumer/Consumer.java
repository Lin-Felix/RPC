package Study.consumer;

import Study.api.Add;
import Study.exception.RpcException;
import Study.message.Request;
import Study.codec.RequestEncoder;
import Study.message.Response;
import Study.codec.SSDecoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author lzk
 * @date 2026/6/23 14:36
 * @description 消费者
 */
public class Consumer implements Add  {

    public int add(int a, int b) {
        try {
            CompletableFuture<Integer> addResultFuture = new CompletableFuture<>();

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
                                            if (200 == response.getCode()) {
                                                addResultFuture.complete(Integer.valueOf(response.getResult().toString()));
                                            } else {
                                                addResultFuture.completeExceptionally(new RpcException(response.getErrorMessage()));
                                            }
                                        }
                                    });
                        }
                    });
            ChannelFuture channelFuture = bootstrap.connect("localhost",7777).sync();
            Request request = new Request();
            request.setServiceName(Add.class.getName());
            request.setMethodName("add");
            request.setParams(new Object[]{a, b});
            request.setParamsClass(new Class[]{int.class, int.class});
            channelFuture.channel().writeAndFlush(request);
            return addResultFuture.get(3, TimeUnit.SECONDS); // 超时处理，仅等待3s
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
