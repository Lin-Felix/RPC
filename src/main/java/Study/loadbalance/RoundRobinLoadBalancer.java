package Study.loadbalance;

import Study.register.ServiceMetadata;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lzk
 * @date 2026/7/5 15:39
 * @description 轮询负载均衡器
 */
public class RoundRobinLoadBalancer implements LoadBalancer {
    private final AtomicInteger index = new AtomicInteger();

    @Override
    public ServiceMetadata select(List<ServiceMetadata> metadataList) {
        int metadataIndex = index.incrementAndGet() % metadataList.size();
        return metadataList.get(Math.abs(metadataIndex));
    }
}
