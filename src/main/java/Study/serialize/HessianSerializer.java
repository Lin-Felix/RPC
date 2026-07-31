package Study.serialize;


import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

@Slf4j
public class HessianSerializer implements Serializer {

    @Override
    public byte[] serialize(Object object) {
        try (ByteArrayOutputStream oos = new ByteArrayOutputStream()) {
            Hessian2Output hessian2Output = new Hessian2Output(oos);
            hessian2Output.writeObject(object);
            hessian2Output.flush();
            return oos.toByteArray();
        } catch (Exception e) {
            log.error("Hessian 序列化失败 {}", object.getClass().getName(), e);
            return new byte[0];
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] bytes, Class<T> objectClass) {
        try (ByteArrayInputStream is = new ByteArrayInputStream(bytes)) {
            Hessian2Input input = new Hessian2Input(is);
            return (T) input.readObject();
        } catch (Exception e) {
            log.error("Hessian 反序列化失败 {}", objectClass.getName(), e);
            return null;
        }
    }

}
