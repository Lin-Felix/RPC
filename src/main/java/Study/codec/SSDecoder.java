package Study.codec;

import Study.message.Message;
import Study.message.Request;
import Study.message.Response;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author lzk
 * @date 2026/6/25 16:17
 * @description 自定义解码器：用于客户端和服务端
 */
public class SSDecoder extends LengthFieldBasedFrameDecoder {
    public SSDecoder() {
        super(1024 * 1024, 0, Integer.BYTES, 0, Integer.BYTES);
    }

    /**
     * 从上一个入站处理器的上下文中读取的数据进行反序列化操作
     * @param ctx             Channel对应的处理器上下文
     * @param in              入站缓冲区
     * @return
     * @throws Exception
     */
    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in); // ByteBuf指向计算机直接内存，需要手动回收内存
        if (null == frame) {
            return null;
        }
        try {
            Message message = new Message();
            byte[] magic = new byte[Message.MAGIC.length];
            frame.readBytes(magic);
            if (!Arrays.equals(magic, Message.MAGIC)) {
                throw new IllegalArgumentException("魔数不对！协议有问题");
            }
            byte messageType = frame.readByte();
            byte[] body = new byte[frame.readableBytes()];
            frame.readBytes(body);
            if (Objects.equals(Message.MessageType.REQUEST.getCode(), messageType)) {
                return deserializeRequest(body);
            }
            if (Objects.equals(Message.MessageType.RESPONSE.getCode(), messageType)) {
                return deserializeResponse(body);
            }
            throw new IllegalArgumentException("消息类型不支持" + messageType);
        } finally {
            frame.release();
        }
    }

    public Request deserializeRequest(byte[] body) {
        return JSONObject.parseObject(new String(body, StandardCharsets.UTF_8), Request.class, JSONReader.Feature.SupportClassForName);
    }

    public Response deserializeResponse(byte[] body) {
        return JSONObject.parseObject(new String(body, StandardCharsets.UTF_8), Response.class);
    }
}
