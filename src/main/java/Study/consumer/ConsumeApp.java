package Study.consumer;

import Study.api.Add;

/**
 * @author lzk
 * @date 2026/6/25 17:17
 * @description
 */
public class ConsumeApp {
    public static void main(String[] args) throws Exception {
        ConsumerProxyFactory consumerProxyFactory = new ConsumerProxyFactory();
        for (int i = 0; i < 10; i++) {
            Add addConsumer = consumerProxyFactory.createConsumerProxy(Add.class);
            System.out.println(addConsumer.add(1, 2));
            System.out.println(addConsumer.add(12, 2));
            System.out.println(addConsumer.add(12, 2));
            System.out.println(addConsumer.add(12, 2));
            System.out.println(addConsumer.add(12, 2));
            System.out.println(addConsumer.add(12, 2));
            System.out.println(addConsumer.add(12, 2));
            System.out.println(addConsumer.add(12, 2));
        }

    }
}
