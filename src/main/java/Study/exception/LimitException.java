package Study.exception;

/**
 * @author lzk
 * @date 2026/7/11 17:17
 * @description
 */
public class LimitException extends RpcException {
    public LimitException(String message) {
        super(message);
    }
}
