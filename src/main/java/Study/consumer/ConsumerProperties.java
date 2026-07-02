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
    private Integer requestTimeoutMs = 3000;
    private RegistryConfig registryConfig = new RegistryConfig();
}
