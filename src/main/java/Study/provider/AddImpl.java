package Study.provider;

import Study.api.Add;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @author lzk
 * @date 2026/6/26 16:26
 * @description Provider端对Add接口的实现，即可被RPC的函数
 */
public class AddImpl implements Add {
    @Override
    public Integer add(int a, int b) {
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        return a + b;
    }

    @Override
    public Integer minus(int a, int b) {
        return a - b;
    }
}
