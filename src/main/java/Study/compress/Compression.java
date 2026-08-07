package Study.compress;

import Study.spi.Extension;

/**
 * @author lzk
 * @date 2026/7/31 17:55
 * @description 压缩接口
 */
public interface Compression extends Extension {
    byte[] compress(byte[] bytes);

    byte[] decompress(byte[] bytes);
}
