package Study.fallback;

import Study.exception.RpcException;
import Study.metrics.RpcCallMetrics;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lzk
 * @date 2026/7/15 18:46
 * @description
 */
public class MockFallback implements Fallback {
    private final Map<Class<?>, Object> mockObjectCache = new ConcurrentHashMap<>();

    @Override
    public Object fallback(RpcCallMetrics metrics) throws Exception{
        // 第一步：判断方法所在的接口/类是否标注了 @RpcFallback 注解
        Method method = metrics.getMethod();
        RpcFallback annotation = method.getDeclaringClass().getAnnotation(RpcFallback.class);
        if (null == annotation) {
            throw new RpcException("属实是没招了！");
        }

        // 第二步：校验 Mock 类与接口的类型兼容性（Mock 类必须实现该接口）
        Class<?> methodClass = annotation.value();
        if (!method.getDeclaringClass().isAssignableFrom(methodClass)) { // A.isAssignableFrom(B)判断两个类型是否是同类型、父子类、接口/实现类的关系(A和B的关系)
            throw new RpcException(String.format("你调用了%s,但是降级策略是%s", method, methodClass));
        }

        // 第三步：从缓存获取或创建 Mock 对象，并调用对应方法
        Object mockObject = mockObjectCache.computeIfAbsent(methodClass, this::createMockObject);
        return method.invoke(mockObject, metrics.getParams());
    }

    // 为了体现Spring创建对象的思想，既工厂模式创建对象的思想
    private Object createMockObject(Class<?> methodClass) {
        try {
            return methodClass.getConstructor().newInstance();
        } catch (Exception e) {
            throw new RpcException("创建mock对象失败", e);
        }
    }
}
