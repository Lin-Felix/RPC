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
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author lzk
 * @date 2026/6/23 14:36
 * @description 消费者
 */
@Slf4j
public class Consumer implements Add  {
    // 在途请求
    private Map<Integer, CompletableFuture<Response>> inFlightRequestTable = new ConcurrentHashMap<>(); // key为requestId，value为响应

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

    public int add(int a, int b) {
        try {
            CompletableFuture<Response> responseFuture = new CompletableFuture<>();
            Channel channel = manager.getChannel("localhost", 7777); // 函数内部同步建立连接
            if (null == channel) {
                throw new RpcException("provider连接失败");
            }
            Request request = new Request();
            request.setServiceName(Add.class.getName());
            request.setMethodName("add");
            request.setParams(new Object[]{a, b});
            request.setParamsClass(new Class[]{int.class, int.class});
            channel.writeAndFlush(request).addListener(f->{
                if (f.isSuccess()) { // 如果请求发送成功，则保存至在途请求表中维护
                    inFlightRequestTable.put(request.getRequestId(), responseFuture);
                }
            });
            Response response = responseFuture.get(3, TimeUnit.SECONDS);// 超时处理，仅等待3s
            if (200 == response.getCode()) {
                return (Integer) response.getResult();
            } else {
                throw new RpcException(response.getErrorMessage());
            }
        } catch (RpcException rpcException) {
            throw rpcException;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int minus(int a, int b) {
        try {
            CompletableFuture<Response> responseFuture = new CompletableFuture<>();
            Channel channel = manager.getChannel("localhost", 7777); // 函数内部同步建立连接
            if (null == channel) {
                throw new RpcException("provider连接失败");
            }
            Request request = new Request();
            request.setServiceName(Add.class.getName());
            request.setMethodName("minus");
            request.setParams(new Object[]{a, b});
            request.setParamsClass(new Class[]{int.class, int.class});
            channel.writeAndFlush(request).addListener(f->{
                if (f.isSuccess()) { // 如果请求发送成功，则保存至在途请求表中维护
                    inFlightRequestTable.put(request.getRequestId(), responseFuture);
                }
            });
            Response response = responseFuture.get(3, TimeUnit.SECONDS);// 超时处理，仅等待3s
            if (200 == response.getCode()) {
                return (Integer) response.getResult();
            } else {
                throw new RpcException(response.getErrorMessage());
            }
        } catch (RpcException rpcException) {
            throw rpcException;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
