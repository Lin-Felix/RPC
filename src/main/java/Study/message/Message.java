package Study.message;

import lombok.Data;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lzk
 * @date 2026/6/23 18:34
 * @description 自定义协议，信息 = 消息字节数 + magic（魔数） + 消息类型 + 消息内容
 */
@Data
public class Message {
    public static final byte[] MAGIC = "生生".getBytes(StandardCharsets.UTF_8);

    private byte[] magic; // 魔数

    private byte messageType; // 消息类型：

    private short version; // 协议的版本

    private byte serializeAndCompress; // 序列化和压缩算法标志位：标记使用的序列化算法 和 压缩算法

    private byte[] body; // 消息体：实际传输的数据



    public enum MessageType { // 避免代码中出现魔法值
        REQUEST(1, Request.class), RESPONSE(2, Response.class), ;

        private final byte code; // 消息类的编码
        private Class<?> messageClass; // 消息类被反序列化后的类的Class对象
        private static final Map<Class<?>, MessageType> CLASS_CACHE = new HashMap<>();

        MessageType(int code, Class<?> messageClass) {
            this.code = (byte) code;
            this.messageClass = messageClass;
        }

        static {
            for (MessageType value : MessageType.values()) {
                if (CLASS_CACHE.put(value.messageClass, value) != null) {
                    throw new IllegalArgumentException(value + "没有唯一对应消息类");
                }
            }

        }

        public byte getCode() {
            return code;
        }

        public Class<?> getMessageClass() {
            return messageClass;
        }

        public MessageType ofMessageClass(Class<?> messageClass) {
            return CLASS_CACHE.get(messageClass);
        }
    }
}
