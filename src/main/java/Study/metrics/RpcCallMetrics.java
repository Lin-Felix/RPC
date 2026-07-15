package Study.metrics;

import Study.message.Response;
import Study.register.ServiceMetadata;
import lombok.Data;

import java.lang.reflect.Method;

/**
 * @author lzk
 * @date 2026/7/14 17:59
 * @description Rpc调用指标
 */
@Data
public class RpcCallMetrics {
    private boolean complete; // RPC调用状态：成功、失败
    private Throwable throwable; // RPC调用产生的异常
    private long startTime; // RPC调用的开始时间
    private long duration; // RPC调用的时间

    // 以下属性为重试操作的参数，将10-Retry的参数移动到该类中
    private ServiceMetadata provider;
    private Method method;
    private Object[] args;

    private Object result;


    private RpcCallMetrics() {

    }

    // 知识点：通过私有构造函数，强制通过工厂模式创建并返回对象
    public static RpcCallMetrics createRpcCallMetrics(Method method, Object[] args, ServiceMetadata provider) {
        RpcCallMetrics metrics = new RpcCallMetrics();
        metrics.startTime = System.currentTimeMillis();
        metrics.method = method;
        metrics.args = args;
        metrics.provider = provider;
        return metrics;
    }

    public void complete(Response response) {
        this.complete = true;
        this.duration = System.currentTimeMillis() - this.startTime;
        this.result = response.getResult();
    }

    public void errorComplete(Throwable throwable) {
        this.duration = System.currentTimeMillis() - this.startTime;
        this.throwable = throwable;
    }


}
