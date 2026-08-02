package Study.message;

import lombok.Data;

import java.io.Serializable;

/**
 * @author lzk
 * @date 2026/8/1 16:58
 * @description
 */
@Data
public class HeartbeatResponse implements Serializable {
    private final long requestTime;
}
