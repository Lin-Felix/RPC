package Study.register;

import lombok.Data;

/**
 * @author lzk
 * @date 2026/7/1 18:37
 * @description 被注册服务的元数据
 */
@Data
public class ServiceMetadata {
    private String host;
    private int port;
    private String serviceName;
}
