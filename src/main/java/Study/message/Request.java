package Study.message;

import Study.serialize.Serializer;
import lombok.Data;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lzk
 * @date 2026/6/25 16:55
 * @description
 */
@Data
public class Request implements Serializable {

    private static final AtomicInteger REQUEST_COUNTER = new AtomicInteger(); // 全局计数器

    private int requestId = REQUEST_COUNTER.getAndIncrement();
    private boolean genericInvoke; // 标识符：是否是泛化调用
    private String serviceName; // 服务名，本质是类名
    private String methodName;
    private Class<?>[] paramsClass; // 参数类型
    private String[] paramsClassStr; // 泛化调用时的参数类型：因为泛化调用时调用方没有任何JAR包，因此使用String更通用
    private Object[] params;
}
