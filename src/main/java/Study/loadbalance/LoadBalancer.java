package Study.loadbalance;

import Study.register.ServiceMetadata;

import java.util.List;

/**
 * @author lzk
 * @date 2026/7/4 20:43
 * @description 负载均衡接口
 */
public interface LoadBalancer {
    ServiceMetadata select(List<ServiceMetadata> metadataList);
}
