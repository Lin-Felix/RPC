package Study.retry;

import Study.spi.Spi;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @author lzk
 * @date 2026/8/7 16:25
 * @description
 */
@Slf4j
public class RetryPolicyManager {
    private final Map<String, RetryPolicy> nameMap = new HashMap<>();

    public RetryPolicyManager() {
        init();
    }

    public RetryPolicy getRetryPolicy(String name) {
        return nameMap.get(name);
    }


    // 由于是懒加载，因此使用private修饰
    private void init() {
        for (RetryPolicy retryPolicy : ServiceLoader.load(RetryPolicy.class)) {
            Class<? extends RetryPolicy> aClass = retryPolicy.getClass();
            if (!aClass.isAnnotationPresent(Spi.class)) {
                log.warn("这个类{}没有SPI注解,无法被管理", aClass.getName());
                continue;
            }
            Spi spi = aClass.getAnnotation(Spi.class);
            nameMap.put(spi.value(), retryPolicy);
        }
    }
}