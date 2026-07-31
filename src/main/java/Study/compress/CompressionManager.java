package Study.compress;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lzk
 * @date 2026/7/31 17:58
 * @description
 */
public class CompressionManager {
    private final Map<Integer, Compression> compressionMap = new HashMap<>();

    public CompressionManager() {
        init();
    }

    public Compression getCompression(int typeCode) {
        return compressionMap.get(typeCode);
    }

    public void init() {
        compressionMap.put(Compression.CompressionType.NONE.getCompressionCode(), new NoneCompression());
        compressionMap.put(Compression.CompressionType.GZIP.getCompressionCode(), new GzipCompression());
    }
}
