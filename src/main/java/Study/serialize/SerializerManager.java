package Study.serialize;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lzk
 * @date 2026/7/31 16:59
 * @description 序列化管理器，拿到序列化器 | 13:50写完，和最终代码不一致，没有总结至笔记中
 */
public class SerializerManager {
    private final Map<Integer, Serializer> serializerMap = new HashMap<>();

    public SerializerManager() {
        init();
    }

    public Serializer getSerializer(int typeCode) {
        return serializerMap.get(typeCode);
    }

    private void init() {
        serializerMap.put(Serializer.SerializerType.JSON.getTypeCode(), new JsonSerializer());
        serializerMap.put(Serializer.SerializerType.HESSIAN.getTypeCode(), new HessianSerializer());
    }

}
