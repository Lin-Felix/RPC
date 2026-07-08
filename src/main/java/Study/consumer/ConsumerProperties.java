package Study.consumer;

import Study.register.RegistryConfig;
import lombok.Data;

/**
 * @author lzk
 * @date 2026/7/2 20:23
 * @description
 */
@Data
public class ConsumerProperties {
    private Integer workThreadNum = 4;
    private Integer connectTimeoutMs = 3000;
    private Integer requestTimeoutMs = 3000; // 请求超时时间（一个连接对应多个请求）
    private Integer methodTimeoutMs = 10000; // 函数超时时间（包含请求时间 + 等待重试时间 + 重试请求的时间）
    private String loadBalancePolicy = "robin"; // 负载均衡策略
    private String retryPolicy = "forking"; // 重试策略
    private RegistryConfig registryConfig = new RegistryConfig();
}
