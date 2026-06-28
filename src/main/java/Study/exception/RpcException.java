package Study.exception;

/**
 * @author lzk
 * @date 2026/6/28 17:40
 * @description Consumer端的异常报错
 */
public class RpcException extends RuntimeException {
    public RpcException(String message) {
        super(message);
    }
}
