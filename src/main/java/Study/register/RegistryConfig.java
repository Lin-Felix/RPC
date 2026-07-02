package Study.register;

import lombok.Data;

/**
 * @author lzk
 * @date 2026/7/1 19:19
 * @description
 */
@Data
public class RegistryConfig {

    private String registerType = "zookeeper";

    private String connectString;

}
