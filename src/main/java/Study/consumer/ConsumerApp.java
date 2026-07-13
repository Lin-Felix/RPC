package Study.consumer;

import Study.api.Add;
import Study.register.RegistryConfig;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

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
        consumerProperties.setRpcPerSecond(100000);
        consumerProperties.setRpcPerChannel(100000);
        ConsumerProxyFactory consumerProxyFactory = new ConsumerProxyFactory(consumerProperties);
        Add addConsumer = consumerProxyFactory.createConsumerProxy(Add.class);
        CyclicBarrier cyclicBarrier = new CyclicBarrier(10);
        for (int i = 0; i < 10; i++) {
            new Thread(()->{
                try {
                    cyclicBarrier.await(); // 等待10个线程都启动准备好后，再执行下一行代码
                    System.out.println(addConsumer.add(1, 2));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }


    }
}
