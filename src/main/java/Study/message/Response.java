package Study.message;

import lombok.Data;

import java.io.Serializable;

/**
 * @author lzk
 * @date 2026/6/25 16:55
 * @description
 */
@Data
public class Response implements Serializable {
    private Object result;
    private int code;
    private String errorMessage;
    private int requestId;

    public static Response fail(String errorMessage, int requestId) {
        Response response = new Response();
        response.code = 400;
        response.errorMessage = errorMessage;
        response.requestId = requestId;
        return response;
    }

    public static Response success(Object result, int requestId) {
        Response response = new Response();
        response.result = result;
        response.code = 200;
        response.requestId = requestId;
        return response;
    }
}
