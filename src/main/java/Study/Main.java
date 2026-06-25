package Study;

/**
 * @author lzk
 * @date 2026/6/23 14:34
 * @description
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer();
        System.out.println(consumer.add(1, 2));
        System.out.println(consumer.add(12, 2));
    }
}
