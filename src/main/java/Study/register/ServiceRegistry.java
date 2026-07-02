package Study.register;

import java.util.List;

/**
 * @author lzk
 * @date 2026/7/1 18:34
 * @description
 */
public interface ServiceRegistry {
    void init(RegistryConfig config) throws Exception;

    void registerService(ServiceMetadata metadata);

    List<ServiceMetadata> fetchServiceList(String serviceName) throws Exception;
}
