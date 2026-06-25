package Study.consumer;

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

/**
 * @author lzk
 * @date 2026/6/23 14:36
 * @description 消费者
 */
public class Consumer {

    public int add(int a, int b) throws Exception {
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
                                        System.out.println(response);
                                        int result = Integer.valueOf(response.getResult().toString());
                                        addResultFuture.complete(result);
                                    }
                                });
                    }
                });
        ChannelFuture channelFuture = bootstrap.connect("localhost",7777).sync();
        Request request = new Request();
        request.setServiceName("bbbb");
        request.setMethodName("aaa");
        request.setParams(new Object[]{1, 2});
        request.setParamsClass(new String[]{"int", "int"});
        channelFuture.channel().writeAndFlush(request);
        return addResultFuture.get();
    }
}
