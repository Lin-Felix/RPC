package Study.fallback;

import Study.exception.RpcException;
import Study.metrics.RpcCallMetrics;
import lombok.Data;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lzk
 * @date 2026/7/15 18:46
 * @description 降级类：从缓存中得到数据
 * 优化点：通过定时器、或者LRU思想，清理map中冗余的kv键值对
 */
public class CacheFallback implements Fallback {
    private static final Object NULL_OBJECT = new Object();

    private final Map<InvokeKey, Object> rpcResultCache = new ConcurrentHashMap<>();


    @Override
    public void recordMetrics(RpcCallMetrics metrics) {
        InvokeKey invokeKey = new InvokeKey(metrics.getMethod(),metrics.getParams());
        Object result = metrics.getResult();
        if (result == null) {
            result = NULL_OBJECT;
        } else {
            rpcResultCache.put(invokeKey, result);
        }
    }

    @Override
    public Object fallback(RpcCallMetrics metrics) throws Exception {
        InvokeKey invokeKey = new InvokeKey(metrics.getMethod(), metrics.getParams());
        Object cacheResult = rpcResultCache.get(invokeKey);
        if (cacheResult == NULL_OBJECT) {
            return null;
        }
        if (cacheResult == null) {
            throw new RpcException("缓存降级没招啦！");
        }
        return cacheResult;
    }

    @Data
    private class InvokeKey {
        private final Method method;
        private final Object[] args;

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            InvokeKey that = (InvokeKey) o;
            return Objects.equals(method, that.method) && Objects.deepEquals(args, that.args);
        }

        @Override
        public int hashCode() {
            return Objects.hash(method, Arrays.hashCode(args));
        }
    }
}
