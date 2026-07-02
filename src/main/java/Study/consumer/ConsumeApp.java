package Study.consumer;

import Study.api.Add;
import Study.register.RegistryConfig;

/**
 * @author lzk
 * @date 2026/6/25 17:17
 * @description
 */
public class ConsumeApp {
    public static void main(String[] args) throws Exception {
        RegistryConfig registryConfig = new RegistryConfig();
        registryConfig.setRegisterType("zookeeper");
        registryConfig.setConnectString("127.0.0.1:2181");
        ConsumerProxyFactory consumerProxyFactory = new ConsumerProxyFactory(registryConfig);
        Add addConsumer = consumerProxyFactory.createConsumerProxy(Add.class);
        while (true) {
            try {
                System.out.println(addConsumer.add(1, 2));
            } catch (Exception e) {
                e.printStackTrace();
            }
            Thread.sleep(1000);
        }

    }
}
