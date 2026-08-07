package Study.spi;

/**
 * @author lzk
 * @date 2026/8/7 15:07
 * @description 实现SPI机制时，根据name 或 code得到对应的实现类
 */
public interface Extension {
    String getName();

    default int code() {
        return -1;
    }
}
