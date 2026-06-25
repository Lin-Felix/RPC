package Study.codec;

import Study.message.Message;
import Study.message.Response;
import com.alibaba.fastjson2.JSONObject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

/**
 * @author lzk
 * @date 2026/6/25 16:57
 * @description 响应编码器：用于服务端；注意在实际生产中可以用于客户端和服务端
 */
public class ResponseEncoder extends MessageToByteEncoder<Response> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Response response, ByteBuf out) throws Exception {
        byte[] logic = Message.LOGIC;
        byte messageType = Message.MessageType.RESPONSE.getCode();
        byte[] body = serializeResponse(response);
        int length = logic.length + Byte.BYTES + body.length;
        out.writeInt(length);
        out.writeBytes(logic);
        out.writeByte(messageType);
        out.writeBytes(body);
    }

    private byte[] serializeResponse(Response response) {
         return JSONObject.toJSONString(response).getBytes(StandardCharsets.UTF_8);
    }
}
