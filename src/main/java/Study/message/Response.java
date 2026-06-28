package Study.message;

import lombok.Data;

/**
 * @author lzk
 * @date 2026/6/25 16:55
 * @description
 */
@Data
public class Response {
    private Object result;
    private int code;
    private String errorMessage;

    public static Response fail(String errorMessage) {
        Response response = new Response();
        response.setCode(400);
        response.setErrorMessage(errorMessage);
        return response;
    }

    public static Response success(Object result) {
        Response response = new Response();
        response.setResult(result);
        response.setCode(200);
        return response;
    }
}
