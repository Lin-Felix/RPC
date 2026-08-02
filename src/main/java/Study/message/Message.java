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
        REQUEST(1, Request.class),
        RESPONSE(2, Response.class),
        HEARTBEAT_REQUEST(3, HeartbeatRequest.class),
        HEARTBEAT_RESPONSE(4,HeartbeatResponse.class),
        ;

        private final byte code; // 消息类的编号
        private Class<?> messageClass; // 消息类被反序列化后的类的Class对象
        private static final Map<Class<?>, MessageType> CLASS_CACHE = new HashMap<>();
        private static final Map<Byte, MessageType> CODE_CACHE = new HashMap<>();

        MessageType(int code, Class<?> messageClass) {
            this.code = (byte) code;
            this.messageClass = messageClass;
        }

        static {
            for (MessageType value : MessageType.values()) {
                if (CLASS_CACHE.put(value.messageClass, value) != null) {
                    throw new IllegalArgumentException(value + "没有唯一对应消息类");
                }
                if (CODE_CACHE.put(value.getCode(), value) != null) {
                    throw new IllegalArgumentException(value + "没有对应的编号");
                }
            }

        }

        public byte getCode() {
            return code;
        }

        public Class<?> getMessageClass() {
            return messageClass;
        }

        public static MessageType ofMessageClass(Class<?> messageClass) {
            return CLASS_CACHE.get(messageClass);
            // 使用Map<Class<?>, MessageType> CLASS_CACHE保存的原因，避免每次调用该函数时，都要执行以下代码（即每次都要遍历）
            /*for (MessageType value : MessageType.values()) {
                if (value.getMessageClass().equals(messageClass)) {
                    return value;
                }
            }
            return null;*/
        }

        public static MessageType ofCode(Byte messageCode) {
            return CODE_CACHE.get(messageCode);
        }
    }
}
