package Study.consumer;

import Study.api.Add;
import Study.api.User;
import Study.register.RegistryConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @author lzk
 * @date 2026/6/25 17:17
 * @description
 */
public class ConsumerApp {
    public static void main(String[] args) throws Exception {
        RegistryConfig registryConfig = new RegistryConfig();
        registryConfig.setRegisterType("zookeeper");
        registryConfig.setConnectString("127.0.0.1:2181");
        ConsumerProperties consumerProperties = new ConsumerProperties();
        consumerProperties.setRegistryConfig(registryConfig);
        ConsumerProxyFactory consumerProxyFactory = new ConsumerProxyFactory(consumerProperties);
        Add addConsumer = consumerProxyFactory.createConsumerProxy(Add.class);


        System.out.println(addConsumer.add(1, 2));
        GenericConsumer genericConsumer = consumerProxyFactory.createConsumerProxy(GenericConsumer.class);
        // $invoke() 调用被代理对象拦截，实际执行的是 ConsumerInvocationHandler.invoke()
        // $invoke() 的参数作为 invoke() 的 args 参数传递：
        //   - JVM 调用：invoke(proxy, $invokeMethod, [服务名, 方法名, 参数类型, 参数值])
        //   - buildRequest() 从 args 中提取这 4 个元素构建 RPC 请求
        System.out.println(genericConsumer.$invoke(Add.class.getName(), "add", new String[]{"int", "int"},
                new Object[]{4, 5}));

        //  aaa rpc + auth --->  gateway/proxy --> provider
        Map<String,Object> user1 = new HashMap<>();
        user1.put("age",1);
        user1.put("name","张三");
        Map<String,Object> user2 = new HashMap<>();
        user2.put("age",3);
        user2.put("name","李四");

        System.out.println(genericConsumer.$invoke(Add.class.getName(), "mergeAge", new String[]{User.class.getName(), User.class.getName()},
                new Object[]{user1, user2}));

    }
}
