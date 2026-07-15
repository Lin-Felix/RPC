package Study.breaker;

import Study.consumer.ConsumerProperties;
import Study.register.ServiceMetadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lzk
 * @date 2026/7/14 18:06
 * @description 熔断器管理类：每一个Provider对应一个CircuitBreaker
 */
public class CircuitBreakerManager {
    private final Map<ServiceMetadata, CircuitBreaker> circuitBreakerMap = new ConcurrentHashMap<>();

    private final ConsumerProperties consumerProperties;

    public CircuitBreakerManager(ConsumerProperties consumerProperties) {
        this.consumerProperties = consumerProperties;
    }

    public CircuitBreaker createOrGetBreaker(ServiceMetadata metadata) {
        return circuitBreakerMap.computeIfAbsent(metadata, this::createBreaker); // this::表示引用当前对象实例的方法
    }

    private CircuitBreaker createBreaker(ServiceMetadata metadata) {
        return new ResponseTimeCircuitBreaker(consumerProperties.getSlowRequestBreakRatio(), consumerProperties.getSlowRequestMs());
    }
}
