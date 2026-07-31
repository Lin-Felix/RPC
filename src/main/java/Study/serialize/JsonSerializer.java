package Study.serialize;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

// 备注：本质是调api，没有跟着实现
public class JsonSerializer implements Serializer {

    @Override
    public byte[] serialize(Object object) {
        return JSONObject.toJSONString(object).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> objectClass) {
        String jsonString = new String(bytes, StandardCharsets.UTF_8);
        return JSONObject.parseObject(jsonString, objectClass, JSONReader.Feature.SupportClassForName); // 第三个参数的作用：将字符串转对象时当对象中有class属性时使用
    }
}
