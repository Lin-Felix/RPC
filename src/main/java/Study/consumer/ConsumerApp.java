package Study.consumer;

import Study.api.Add;
import Study.register.RegistryConfig;

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
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                while (true) {
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                    long startTime = System.currentTimeMillis();
                    System.out.println(addConsumer.add(1, 2) + " " + (System.currentTimeMillis() - startTime) + "毫秒");
                }
            }).start();
        }


        new Thread(() -> {
            while (true) {
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                long startTime = System.currentTimeMillis();
                System.out.println(addConsumer.minus(1, 2) + " " + (System.currentTimeMillis() - startTime) + "毫秒");
            }
        }).start();


    }
}
