package Study.consumer;

/**
 * @author lzk
 * @date 2026/6/25 17:17
 * @description
 */
public class ConsumeApp {
    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer();
        System.out.println(consumer.add(1, 2));
        System.out.println(consumer.add(11, 2));
    }
}
