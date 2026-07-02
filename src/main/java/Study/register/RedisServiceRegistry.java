package Study.register;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author lzk
 * @date 2026/7/1 19:51
 * @description 模拟Redis实现注册中心
 */
@Slf4j
public class RedisServiceRegistry implements ServiceRegistry {

    @Override
    public void init(RegistryConfig config) throws Exception {
        log.info("redis 注册中心还未实现");
    }

    @Override
    public void registerService(ServiceMetadata metadata) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ServiceMetadata> fetchServiceList(String serviceName) {
        throw new UnsupportedOperationException();
    }
}
