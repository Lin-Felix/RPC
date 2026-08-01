package Study.codec;

import Study.compress.Compression;
import Study.compress.CompressionManager;
import Study.message.Message;
import Study.serialize.Serializer;
import Study.serialize.SerializerManager;
import Study.version.Version;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lzk
 * @date 2026/7/30 19:48
 * @description 和最终代码不一样，没有实现 且 没有记录笔记中
 */
@Slf4j
public class SSEncoder extends MessageToByteEncoder {

    public static final AttributeKey<Integer> SERIALIZE_KEY = AttributeKey.valueOf("serializeKey");
    public static final AttributeKey<SerializerManager> SERIALIZER_MANAGER_KEY = AttributeKey.valueOf(
            "serializeManagerKey");

    public static final AttributeKey<Integer> COMPRESS_KEY = AttributeKey.valueOf("compressKey");
    public static final AttributeKey<CompressionManager> COMPRESS_MANAGER_KEY = AttributeKey.valueOf(
            "compressManagerKey");

    private volatile Serializer defaultSerializer;

    private volatile Compression defaultCompression;

    private volatile byte defaultSerializeAndCompress;

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        // pre: 对defaultSerializer、defaultCompression、defaultSerializeAndCompress进行初始化操作，初始化序列化器和压缩器（懒加载，只初始化一次），避免每次编码都从Channel属性中获取（即ctx.channel().attr().get()）
        initIfNecessary(ctx);

        // 第一步: 判断传输的消息的类型是否支持序列化
        Message.MessageType messageType = Message.MessageType.ofMessageClass(msg.getClass());
        if (null == messageType) {
            log.warn("{} 不支持序列化，无法发送", msg.getClass().getName());
            return;
        }

        // 第二步：编码
        // 2.1: 准备协议头的信息：魔数、消息编号、协议版本
        byte[] magic = Message.MAGIC;
        byte messsageCode = messageType.getCode();
        Version current = Version.V1;

        // 2.2:对消息进行序列化
        byte[] body = defaultSerializer.serialize(msg);

        // 2.3:对消息进行压缩，小于256字节的消息不进行压缩
        byte finalSac = defaultSerializeAndCompress;
        if (body.length < 256) {
            finalSac &= (byte)11110000;
        } else {
            body = defaultCompression.compress(body);
        }

        int length = magic.length + Byte.BYTES * 2 + Short.BYTES + body.length;

        // 第三步: 将编码后的消息写入至下一个处理器
        out.writeInt(length);
        out.writeBytes(magic);
        out.writeByte(messsageCode);
        out.writeShort(current.getVersionNum());
        out.writeByte(finalSac);
        out.writeBytes(body);
    }

    private void initIfNecessary(ChannelHandlerContext ctx) {
        if (null != defaultSerializer) {
            return;
        }

        Integer serializeCode = ctx.channel().attr(SERIALIZE_KEY).get();
        SerializerManager serializerManager = ctx.channel().attr(SERIALIZER_MANAGER_KEY).get(); // attr()返回类型是Attribute<T>，get()的返回类型是T
        defaultSerializer = serializerManager.getSerializer(serializeCode);

        Integer compressCode = ctx.channel().attr(COMPRESS_KEY).get();
        CompressionManager compressionManager = ctx.channel().attr(COMPRESS_MANAGER_KEY).get();
        defaultCompression = compressionManager.getCompression(compressCode);

        if (null == defaultSerializer) {
            throw new IllegalArgumentException("不存在默认的序列化器");
        }

        if (null == defaultCompression) {
            throw new IllegalArgumentException("不存在默认的压缩器");
        }

        defaultSerializeAndCompress = (byte) (serializeCode << 4 | compressCode);
    }

    // 由于以下三个函数均要从ctx.channel中得到Attribute值，故抽象为initIfNessary()和defaultSerializer属性
    // 避免每次RPC处理请求时，进行编码都要进行ctx.channel()的get()操作，浪费资源
/*

    private byte getDefaultSerializeAndCompress(ChannelHandlerContext ctx) {
        Integer serializeCode = ctx.channel().attr(SERIALIZE_KEY).get();
        Integer compressCode = ctx.channel().attr(COMPRESS_KEY).get();
        return (byte) (serializeCode << 4 | compressCode);
    }

    private Serializer getDefaultSerializer(ChannelHandlerContext ctx) {
        Integer serializeCode = ctx.channel().attr(SERIALIZE_KEY).get();
        SerializerManager serializerManager = (SerializerManager) ctx.channel().attr(SERIALIZER_MANAGER_KEY);
        return serializerManager.get(serializeCode);
    }

    private Compression getDefaultCompression(ChannelHandlerContext ctx) {
        Integer compressCode = ctx.channel().attr(COMPRESS_KEY).get();
        CompressionManager compressionManager = ctx.channel().attr(COMPRESS_MANAGER_KEY).get();
        return compressionManager.getCompression(compressCode);
    }


*/

}
