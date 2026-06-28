package Study.provider;

import Study.api.Add;

/**
 * @author lzk
 * @date 2026/6/26 16:26
 * @description Provider端对Add接口的实现，即可被RPC的函数
 */
public class AddImpl implements Add {
    @Override
    public int add(int a, int b) {
        return a + b;
    }
}
