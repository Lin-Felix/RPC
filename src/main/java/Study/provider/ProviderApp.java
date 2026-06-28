package Study.provider;

import Study.api.Add;

/**
 * @author lzk
 * @date 2026/6/25 17:18
 * @description
 */
public class ProviderApp {
    public static void main(String[] args) {
        ProviderServer providerServer = new ProviderServer(7777);
        providerServer.register(Add.class, new AddImpl());
        providerServer.start();
    }
}
