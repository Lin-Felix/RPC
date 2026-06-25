package Study.message;

import lombok.Data;

/**
 * @author lzk
 * @date 2026/6/25 16:55
 * @description
 */
@Data
public class Request {
    private String serviceName;
    private String methodName;
    private String[] paramsClass;
    private Object[] params;
}
