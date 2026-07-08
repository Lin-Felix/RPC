package Study.retry;

import Study.message.Response;

/**
 * @author lzk
 * @date 2026/7/7 20:51
 * @description 重试策略接口
 */
public interface RetryPolicy {
    Response retry(RetryContext context) throws Exception;
}
