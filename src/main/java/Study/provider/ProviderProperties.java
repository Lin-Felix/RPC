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
    private RegistryConfig registryConfig;
}
