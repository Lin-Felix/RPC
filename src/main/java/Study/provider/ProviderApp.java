package Study.provider;

import Study.api.Add;
import Study.register.RegistryConfig;

/**
 * @author lzk
 * @date 2026/6/25 17:18
 * @description
 */
public class ProviderApp {
    public static void main(String[] args) {
        RegistryConfig registryConfig = new RegistryConfig();
        registryConfig.setRegisterType("zookeeper");
        registryConfig.setConnectString("127.0.0.1:2181");
        ProviderServer providerServer = new ProviderServer("127.0.0.1", 7777, registryConfig);
        providerServer.register(Add.class, new AddImpl());
        providerServer.start();
    }
}
