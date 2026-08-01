package Study.codec;

import Study.compress.Compression;
import Study.compress.CompressionManager;
import Study.message.Message;
import Study.message.Request;
import Study.message.Response;
import Study.serialize.Serializer;
import Study.serialize.SerializerManager;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import static Study.codec.SSEncoder.COMPRESS_MANAGER_KEY;
import static Study.codec.SSEncoder.SERIALIZER_MANAGER_KEY;

/**
 * @author lzk
 * @date 2026/6/25 16:17
 * @description 自定义解码器：用于客户端和服务端
 */
public class SSDecoder extends LengthFieldBasedFrameDecoder {
    // 由于序列化器和压缩器是编码时规定好的，因此解码器中不包含对应的属性，只添加序列化管理器和压缩管理器即可
    private volatile SerializerManager serializerManager;

    private volatile CompressionManager compressionManager;

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
        initIfNecessart(ctx);

        // ① 让 Netty 根据消息长度处理半包、粘包，得到一条完整消息；LengthFieldBasedFrameDecoder的decode()会利用消息头中的“长度字段”划分完整消息
        // ② ByteBuf指向计算机直接内存，需要手动回收内存
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
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
            short version = frame.readShort();
            byte serializerAndCompress = frame.readByte();
            Compression compression = this.compressionManager.getCompression(serializerAndCompress & 0b00001111);
            if (null == compression) {
                throw new IllegalArgumentException("没有支持的压缩器");
            }
            Serializer serializer = this.serializerManager.getSerializer((serializerAndCompress & 0b11110000) >>> 4);
            if (serializer == null) {
                throw new IllegalArgumentException("没有支持的序列化器");
            }
            byte[] body = new byte[frame.readableBytes()];
            frame.readBytes(body);
            body = compression.decompress(body);
            Message.MessageType type = Message.MessageType.ofCode(messageType);
            if (type == null) {
                throw new IllegalArgumentException("不支持的消息类型" + messageType);
            }
            return serializer.deserialize(body, type.getMessageClass());
        } finally {
            frame.release();
        }
    }

    private void initIfNecessart(ChannelHandlerContext ctx) {
        if (null != serializerManager) {
            return;
        }
        serializerManager = ctx.channel().attr(SERIALIZER_MANAGER_KEY).get();
        compressionManager = ctx.channel().attr(COMPRESS_MANAGER_KEY).get();
    }
}
