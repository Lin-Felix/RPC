package Study.compress;

/**
 * @author lzk
 * @date 2026/7/31 17:55
 * @description 压缩接口
 */
public interface Compression {
    byte[] compress(byte[] bytes);

    byte[] decompress(byte[] bytes);

    enum CompressionType {
        NONE(0), GZIP(1),;

        private final int compressionCode;

        CompressionType(int compressionCode) {
            this.compressionCode = compressionCode;
        }

        public int getCompressionCode() {
            return compressionCode;
        }
    }
}
