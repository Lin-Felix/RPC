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
    private String serialize = "json"; // 序列化协议
    private String compress = "none"; // 压缩方法
    private int rpcPerSecond = 100; // 每秒调用10个rpc请求
    private int rpcPerChannel = 50; // 每个Channel每秒调用5个rpc请求
    private double slowRequestBreakRatio = 0.5;
    private long slowRequestMs = 1000;
    private RegistryConfig registryConfig = new RegistryConfig();
}
