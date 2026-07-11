package Study.message;

import lombok.Data;
import java.nio.charset.StandardCharsets;

/**
 * @author lzk
 * @date 2026/6/23 18:34
 * @description 自定义协议，信息 = 消息字节数 + magic + 消息类型 + 消息内容
 */
@Data
public class Message {
    public static final byte[] MAGIC = "生生".getBytes(StandardCharsets.UTF_8);

    private byte[] magic;

    private byte messageType;

    private byte[] body;

    public enum MessageType { // 避免代码中出现魔法值
        REQUEST(1), RESPONSE(2), ;

        private final byte code;

        MessageType(int code) {
            this.code = (byte) code;
        }

        public byte getCode() {
            return code;
        }
    }
}
