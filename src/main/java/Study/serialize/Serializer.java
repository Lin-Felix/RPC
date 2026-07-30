package Study.serialize;

/**
 * @author lzk
 * @date 2026/7/30 19:49
 * @description
 */
public interface Serializer {
    // 序列化
    byte[] serialize(Object object);

    // 反序列化
    <T> T deserialize(byte[] bytes, Class<T> objectClass);


    enum SerializerType {
        JSON(0), HESSIAN(1),;

        private final int typeCode;

        SerializerType(int typeCode) {
            this.typeCode = typeCode;
        }

        public int getTypeCode() {
            return typeCode;
        }
    }
}
