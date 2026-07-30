package Study.provider;

import Study.register.RegistryConfig;
import lombok.Data;

/**
 * @author lzk
 * @date 2026/7/2 20:32
 * @description
 */
@Data
public class ProviderProperties {
    private String host;
    private int port;
    private int workThreadNum = 4;
    private int globalMaxRequest = 10; // 可承受的总并发
    private int perConsumerMaxRequest = 5; // 每个消费者最大的请求
    private String serialize = "json"; // 序列化协议
    private String compress = "none"; // 压缩方法
    private RegistryConfig registryConfig;
}
