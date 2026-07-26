package Study.fallback;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author lzk
 * @date 2026/7/15 20:36
 * @description 降级注解：绑定接口，value表示降级时调用的本地实现类
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RpcFallback {
    Class<?> value();
}
