package Study.serialize;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @author lzk
 * @date 2026/7/31 16:59
 * @description 序列化管理器，拿到序列化器 | 13:50写完，和最终代码不一致，没有总结至笔记中
 */
public class SerializerManager {
    private final Map<String, Serializer> nameMap = new HashMap<>();

    private final Map<Integer, Serializer> codeMap = new HashMap<>();

    public SerializerManager() {
        init();
    }

    public Serializer getSerializer(String name) {
        return nameMap.get(name.toUpperCase(Locale.ROOT));
    }

    public Serializer getSerializer(int code) {
        return codeMap.get(code);
    }


    // 备注：argument的中文是实参，parameter表示形参
    private void init() {
        ServiceLoader<Serializer> loader = ServiceLoader.load(Serializer.class);
        for (Serializer serializer : loader) {
            if (codeMap.put(serializer.code(), serializer) != null) {
                throw new IllegalArgumentException("序列化器的code重复");
            }
            if (serializer.code() >= 16) {
                throw new IllegalArgumentException("序列化器的code不能超过15");
            }
            if (nameMap.put(serializer.getName().toUpperCase(Locale.ROOT), serializer) != null) {
                throw new IllegalArgumentException("序列化器的名字重复");
            }
        }
    }

}
