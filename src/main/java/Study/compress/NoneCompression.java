package Study.compress;

/**
 * @author lzk
 * @date 2026/7/31 17:56
 * @description
 */
public class NoneCompression implements Compression {
    @Override
    public byte[] compress(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] decompress(byte[] bytes) {
        return bytes;
    }

    @Override
    public String getName() {
        return "none";
    }

    @Override
    public int code() {
        return 0;
    }
}
