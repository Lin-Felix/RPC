package Study.register;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lzk
 * @date 2026/7/2 15:33
 * @description
 */
@Slf4j
public class DefaultServiceRegistry implements ServiceRegistry {

    private ServiceRegistry delegate;

    // 缓存:key为ServiceName, value为List<ServiceMetadata>
    private final Map<String, List<ServiceMetadata>> cache = new ConcurrentHashMap<>();

    @Override
    public void init(RegistryConfig config) throws Exception {
        this.delegate = createServiceRegistry(config);
        this.delegate.init(config);
    }

    @Override
    public void registerService(ServiceMetadata metadata) {
        log.info("向{} 注册了一个Service{}", delegate.getClass(), metadata.getServiceName());
        delegate.registerService(metadata);
    }

    @Override
    public List<ServiceMetadata> fetchServiceList(String serviceName) throws Exception {
        try {
            List<ServiceMetadata> serviceMetadata = delegate.fetchServiceList(serviceName);
            cache.put(serviceName, serviceMetadata);
            return serviceMetadata;
        } catch (Exception e) { // 若注册中心掉线,从缓存中拿到服务的元信息
            log.error("{}注册中心查询{}出现异常", delegate.getClass().getSimpleName(), serviceName, e);
            return cache.getOrDefault(serviceName, new ArrayList<>());
        }
    }

    private static ServiceRegistry createServiceRegistry(RegistryConfig config) {
        if (config.getRegisterType().equals("zookeeper")) {
            return new ZookeeperServiceRegistry();
        }
        if (config.getRegisterType().equals("redis")) {
            return new RedisServiceRegistry();
        }
        throw new IllegalArgumentException(config.getRegisterType() + "没有实现");
    }
}
