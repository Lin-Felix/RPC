package Study.provider;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lzk
 * @date 2026/6/26 16:27
 * @description 服务注册表（不是注册中心），保存可被调用的函数
 */
public class ProviderRegistry {
    private Map<String, Invocation<?>> serviceInstanceMap = new ConcurrentHashMap<>();

    /**
     *
     * @param interfaceClass 为什么是接口
     * @param serviceInstance
     * @param <I>
     */
    public <I> void register(Class<I> interfaceClass, I serviceInstance) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("注册的类型必须是接口");
        }
        if (null != serviceInstanceMap.putIfAbsent(interfaceClass.getName(), new Invocation<>(interfaceClass, serviceInstance))) {
            throw new IllegalArgumentException(interfaceClass.getCanonicalName() + "重复注册了!");
        }
    }

    public Invocation<?> findService(String serviceName) {
        return serviceInstanceMap.get(serviceName);
    }

    public static class Invocation<I> {
        final I serviceInstance;
        final Class<I> interfaceClass; // 该字段确保能够调用方法，若没有该字段，当实现的方法为private或者class为private时，方法无法被调用，而接口则规定为public

        public Invocation(Class<I> interfaceClass, I serviceInstance) {
            this.interfaceClass = interfaceClass;
            this.serviceInstance = serviceInstance;
        }

        public Object invoke(String methodName, Class<?>[] paramsClass, Object[] params) throws Exception {
            Method invokeMethod = interfaceClass.getDeclaredMethod(methodName, paramsClass); // 通过反射调用方法
            return invokeMethod.invoke(serviceInstance, params);
        }
    }

}
