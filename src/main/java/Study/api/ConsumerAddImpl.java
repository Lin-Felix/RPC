package Study.api;

/**
 * @author lzk
 * @date 2026/7/15 20:20
 * @description
 */
public class ConsumerAddImpl implements Add {
    @Override
    public Integer add(int a, int b) {
        return 0; // 本地实现接口的类，故意返回0，用于标识在本地服务进行计算
    }

    @Override
    public Integer minus(int a, int b) {
        return 0;
    }
}
