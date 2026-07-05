package Study.loadbalance;

import Study.register.ServiceMetadata;

import java.util.List;
import java.util.Random;

/**
 * @author lzk
 * @date 2026/7/5 16:01
 * @description 随机负载均衡器
 */
public class RandomLoadBalancer implements LoadBalancer {
    private final Random random = new Random();

    @Override
    public ServiceMetadata select(List<ServiceMetadata> metadataList) {
        int metadataIndex = random.nextInt(0, metadataList.size());
        return metadataList.get(Math.abs(metadataIndex));
    }
}
