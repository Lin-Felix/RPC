package Study.serialize;

import Study.spi.Extension;

/**
 * @author lzk
 * @date 2026/7/30 19:49
 * @description
 */
public interface Serializer extends Extension {
    // 序列化
    byte[] serialize(Object object);

    // 反序列化
    <T> T deserialize(byte[] bytes, Class<T> objectClass);
}
