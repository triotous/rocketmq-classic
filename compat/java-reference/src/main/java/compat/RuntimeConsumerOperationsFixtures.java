package compat;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;

/** Direct public runtime operations, without starting transport or workers. */
public final class RuntimeConsumerOperationsFixtures {
    private RuntimeConsumerOperationsFixtures() { }

    public static void main(String[] args) throws Exception {
        List<Object> runningInfo = new ArrayList<>();
        for (String name : new String[] {"active-no-addresses", "passive-empty-addresses",
            "active-address-list", "missing-group", "null-group", "null-snapshot",
            "null-properties", "null-consume-type", "snapshot-throws"}) {
            runningInfo.add(runningInfo(name));
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("serverBaselineContacted", false);
        fixture.put("runningInfoEntrypoint", "MQClientInstance.consumerRunningInfo(String)");
        fixture.put("runningInfo", runningInfo);
        String json = JSON.toJSONString(fixture, JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path path = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(path))) throw new IllegalStateException("fixture drift: " + path);
        } else {
            Files.createDirectories(path.getParent()); Files.writeString(path, json);
        }
        System.out.println("Direct runtime consumer operations verified: " + runningInfo.size() + " Running Info cases");
    }

    private static Map<String, Object> runningInfo(String name) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("runtime-consumer-operations"); config.setNamesrvAddr(null);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        List<String> calls = new ArrayList<>();
        ConsumerRunningInfo snapshot = "null-snapshot".equals(name) ? null : new ConsumerRunningInfo();
        Properties properties = null;
        if (snapshot != null) {
            if ("null-properties".equals(name)) {
                snapshot.setProperties(null);
            } else {
                properties = snapshot.getProperties();
                properties.setProperty("owner", "retained");
                properties.setProperty(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, "owner-ns");
                properties.setProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE, "owner-type");
                properties.setProperty(ConsumerRunningInfo.PROP_CLIENT_VERSION, "owner-version");
            }
        }
        ConsumeType consumeType = "null-consume-type".equals(name) ? null
            : name.startsWith("passive") ? ConsumeType.CONSUME_PASSIVELY : ConsumeType.CONSUME_ACTIVELY;
        MQConsumerInner consumer = (MQConsumerInner) Proxy.newProxyInstance(
            RuntimeConsumerOperationsFixtures.class.getClassLoader(), new Class<?>[] {MQConsumerInner.class},
            (proxy, method, arguments) -> {
                if ("consumerRunningInfo".equals(method.getName())) {
                    calls.add("info");
                    if ("snapshot-throws".equals(name)) throw new IllegalStateException("snapshot failed");
                    return snapshot;
                }
                if ("consumeType".equals(method.getName())) { calls.add("type"); return consumeType; }
                throw new IllegalStateException("unexpected consumer method: " + method.getName());
            });
        List<String> addresses = "active-address-list".equals(name)
            ? List.of("ns-b:9876", "ns-a:9876", "ns-b:9876")
            : "passive-empty-addresses".equals(name) ? List.of() : null;
        Map<String, Object> item = new TreeMap<>();
        try {
            if (addresses != null) {
                var remoting = runtime.getMQClientAPIImpl().getRemotingClient();
                remoting.updateNameServerAddressList(new ArrayList<>(List.of("seed:9876")));
                // The public getter is a live list. Pin its order after the
                // updater's documented shuffle; no ordering is normalized away.
                remoting.getNameServerAddressList().clear();
                remoting.getNameServerAddressList().addAll(addresses);
            }
            if (!"missing-group".equals(name)) runtime.getConsumerTable().put("selected", consumer);
            String group = "null-group".equals(name) ? null : "selected";
            item.put("name", name); item.put("addresses", addresses); item.put("group", group);
            item.put("consumeType", consumeType == null ? null : consumeType.name());
            try {
                ConsumerRunningInfo result = runtime.consumerRunningInfo(group);
                item.put("resultPresent", result != null);
                item.put("sameSnapshot", result != null && result == snapshot);
                item.put("sameProperties", result != null && result.getProperties() == properties);
                item.put("error", null);
            } catch (Exception error) {
                item.put("error", error.getClass().getSimpleName());
            }
            item.put("calls", calls);
            item.put("propertiesAfter", properties == null ? null : new TreeMap<>(properties));
            return item;
        } finally {
            runtime.getConsumerTable().clear();
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
    }
}
