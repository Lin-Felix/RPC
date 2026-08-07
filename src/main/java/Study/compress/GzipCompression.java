package Study.compress;

import Study.exception.RpcException;
import com.alibaba.fastjson2.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author lzk
 * @date 2026/7/31 17:57
 * @description GZip压缩
 */
public class GzipCompression implements Compression {
    @Override
    public byte[] compress(byte[] bytes) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
            gzip.write(bytes);
            gzip.finish();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RpcException("压缩过程失败", e);
        }
    }

    @Override
    public byte[] decompress(byte[] bytes) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
            byte[] buffer = new byte[4096];
            int len;
            while ((len = gzip.read(buffer)) != -1) {
                bos.write(buffer, 0, len);
            }
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RpcException("解压缩过程失败", e);
        }
    }

    @Override
    public String getName() {
        return "gzip";
    }

    @Override
    public int code() {
        return 1;
    }
}
