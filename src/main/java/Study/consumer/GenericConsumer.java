package Study.consumer;

/**
 * @author lzk
 * @date 2026/8/8 15:42
 * @description 泛化调用接口
 */
public interface GenericConsumer {
    // $是为了区分业务方法名称，表示泛化调用方法
    Object $invoke(String serviceName, String methodName, String[] paramsType, Object[] args);
}
