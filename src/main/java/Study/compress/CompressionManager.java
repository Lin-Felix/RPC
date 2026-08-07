package Study.compress;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @author lzk
 * @date 2026/7/31 17:58
 * @description
 */
public class CompressionManager {
    private final Map<Integer, Compression> codeMap = new HashMap<>();

    private final Map<String, Compression> nameMap = new HashMap<>();

    public CompressionManager() {
        init();
    }

    public Compression getCompression(int code) {
        return codeMap.get(code);
    }

    public Compression getCompression(String name) {
        return nameMap.get(name.toUpperCase(Locale.ROOT));
    }

    public void init() {
        for (Compression compression : ServiceLoader.load(Compression.class)) {
            if (codeMap.put(compression.code(),compression) != null) {
                throw new IllegalArgumentException("压缩器的code重复");
            }
            if (compression.code() >= 16) {
                throw new IllegalArgumentException("压缩器code不能超过15");
            }
            if (nameMap.put(compression.getName().toUpperCase(Locale.ROOT),compression) != null) {
                throw new IllegalArgumentException("压缩器的名字重复");
            }
        }
    }
}
