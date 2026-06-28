package Study.message;

import lombok.Data;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lzk
 * @date 2026/6/25 16:55
 * @description
 */
@Data
public class Request {

    private static final AtomicInteger REQUEST_COUNTER = new AtomicInteger(); // 全局计数器

    private int requestId = REQUEST_COUNTER.getAndDecrement();
    private String serviceName; // 服务名，本质是类名
    private String methodName;
    private Class<?>[] paramsClass; // 参数类型
    private Object[] params;
}
