package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.common.NameserverAccessConfig;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;

/** Direct invocation of the pinned public MQClientAPIFactory surface. */
public final class RuntimeApiFactoryFixtures {
    private RuntimeApiFactoryFixtures() { }

    public static void main(String[] args) throws Exception {
        Path fixturePath = Path.of(args[0]);
        Map<String, Object> fixture = observe();
        String json = JSON.toJSONString(fixture) + "\n";
        if (Arrays.asList(args).contains("--verify")) {
            String expected = Files.readString(fixturePath);
            if (!expected.equals(json)) {
                throw new IllegalStateException("MQClientAPIFactory fixture drift\nexpected="
                    + expected + "\nactual=" + json);
            }
            System.out.println("Runtime MQClientAPIFactory invoked cases verified");
        } else {
            Files.createDirectories(fixturePath.getParent());
            Files.writeString(fixturePath, json);
            System.out.println("Wrote " + fixturePath);
        }
    }

    private static Map<String, Object> observe() throws Exception {
        String vipProperty = ClientConfig.SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY;
        String namesrvAddressProperty = "rocketmq.namesrv.addr";
        String domainProperty = "rocketmq.namesrv.domain";
        String subgroupProperty = "rocketmq.namesrv.domain.subgroup";
        Map<String, String> previous = new TreeMap<>();
        for (String key : List.of(vipProperty, namesrvAddressProperty, domainProperty, subgroupProperty)) {
            previous.put(key, System.getProperty(key));
        }

        Map<String, Object> fixture = new TreeMap<>();
        try {
            System.setProperty(vipProperty, "true");
            System.setProperty(domainProperty, "before.example");
            System.setProperty(subgroupProperty, "before-subgroup");
            NameserverAccessConfig access = new NameserverAccessConfig(
                "127.0.0.1:9876;127.0.0.2:9876", "", null);
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            MQClientAPIFactory factory = new MQClientAPIFactory(access, "fixture-", 2, null, null, scheduler);
            try {
                fixture.put("nameserverAccessConfig", Arrays.asList(
                    access.getNamesrvAddr(), access.getNamesrvDomain(), access.getNamesrvDomainSubgroup()));
                fixture.put("vipPropertyAfterInit", System.getProperty(vipProperty));
                fixture.put("namesrvAddressPropertyAfterInit", System.getProperty(namesrvAddressProperty));
                fixture.put("clientsNullBeforeStart", factory.getClients() == null);
                String beforeStartError;
                try {
                    factory.getClient();
                    beforeStartError = "NONE";
                } catch (Throwable error) {
                    beforeStartError = error.getClass().getSimpleName();
                }
                fixture.put("getClientBeforeStartError", beforeStartError);
                String nullAddressBeforeStartError;
                try {
                    factory.onNameServerAddressChange(null);
                    nullAddressBeforeStartError = "NONE";
                } catch (Throwable error) {
                    nullAddressBeforeStartError = error.getClass().getSimpleName();
                }
                fixture.put("nullAddressBeforeStartError", nullAddressBeforeStartError);

                factory.start();
                MQClientAPIExt[] clients = factory.getClients();
                fixture.put("clientCountAfterStart", clients.length);
                fixture.put("clientInstanceNames", Arrays.stream(clients).map(RuntimeApiFactoryFixtures::instanceName).collect(Collectors.toList()));
                fixture.put("clientAddressesAfterStart", Arrays.stream(clients)
                    .map(client -> sorted(client.getNameServerAddressList())).collect(Collectors.toList()));
                MQClientAPIExt selected = factory.getClient();
                fixture.put("selectedClientBelongsToPool", Arrays.stream(clients).anyMatch(client -> client == selected));
                MQClientAPIExt first = clients[0];
                clients[0] = clients[1];
                fixture.put("getClientsReturnsSameArray", factory.getClients() == clients);
                fixture.put("arrayMutationVisible", factory.getClients()[0] == clients[1]);
                clients[0] = first;

                factory.onNameServerAddressChange("127.0.0.3:9876");
                fixture.put("clientAddressesAfterChange", Arrays.stream(factory.getClients())
                    .map(client -> sorted(client.getNameServerAddressList())).collect(Collectors.toList()));
                factory.onNameServerAddressChange(null);
                fixture.put("clientAddressesAfterNullChange", Arrays.stream(factory.getClients())
                    .map(client -> sorted(client.getNameServerAddressList())).collect(Collectors.toList()));
                factory.shutdown();
                fixture.put("clientArrayRetainedAfterShutdown", factory.getClients() == clients);
            } finally {
                try {
                    factory.shutdown();
                } catch (Throwable ignored) {
                    // The active path has already shut down; cleanup is best effort.
                }
                scheduler.shutdownNow();
            }

            System.setProperty(domainProperty, "factory-domain.example");
            System.setProperty(subgroupProperty, "factory-subgroup");
            ScheduledExecutorService domainScheduler = Executors.newSingleThreadScheduledExecutor();
            try {
                new MQClientAPIFactory(new NameserverAccessConfig(null, "factory-domain.example",
                    "factory-subgroup"), "domain-", 0, null, null, domainScheduler);
                fixture.put("domainPropertyAfterInit", System.getProperty(domainProperty));
                fixture.put("domainSubgroupPropertyAfterInit", System.getProperty(subgroupProperty));
            } finally {
                domainScheduler.shutdownNow();
            }

            List<String> constructors = new ArrayList<>();
            for (Constructor<?> constructor : MQClientAPIFactory.class.getConstructors()) {
                constructors.add(constructor.toGenericString());
            }
            constructors.sort(String::compareTo);
            fixture.put("publicConstructors", constructors);
            List<String> methods = new ArrayList<>();
            for (Method method : MQClientAPIFactory.class.getMethods()) {
                if (method.getDeclaringClass() != Object.class) {
                    methods.add(method.toGenericString());
                }
            }
            methods.sort(String::compareTo);
            fixture.put("publicMethods", methods);
            return fixture;
        } finally {
            for (Map.Entry<String, String> entry : previous.entrySet()) {
                if (entry.getValue() == null) System.clearProperty(entry.getKey());
                else System.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }

    private static String instanceName(MQClientAPIExt client) {
        try {
            var field = MQClientAPIExt.class.getDeclaredField("clientConfig");
            field.setAccessible(true);
            return ((ClientConfig) field.get(client)).getInstanceName();
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException(error);
        }
    }

    private static List<String> sorted(List<String> addresses) {
        List<String> result = new ArrayList<>(addresses);
        result.sort(String::compareTo);
        return result;
    }
}
