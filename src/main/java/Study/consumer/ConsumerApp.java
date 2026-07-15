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
        ConsumerProxyFactory consumerProxyFactory = new ConsumerProxyFactory(consumerProperties);
        Add addConsumer = consumerProxyFactory.createConsumerProxy(Add.class);
        CyclicBarrier cyclicBarrier = new CyclicBarrier(10);
        while (true) {
            Thread.sleep(300);
            addConsumer.add(1, 2);
        }


    }
}
