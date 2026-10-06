package compat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;

/** Executes pinned ClientConfig methods without starting a transport. */
public final class RuntimeConfigFixtures {
    private RuntimeConfigFixtures() { }

    public static void main(String[] args) throws Exception {
        List<Object> identities = new ArrayList<>();
        String[] units = {null, "", " \t\n", "unit-a", " unit-a ", "\u2003", "\u00a0", "\u0085", "\u202f"};
        for (String ip : new String[] {"192.0.2.4", "[2001:db8::4]", "", null}) {
            for (String unit : units) {
                for (boolean stream : new boolean[] {false, true}) {
                    ClientConfig config = new ClientConfig();
                    config.setClientIP(ip);
                    config.setInstanceName("instance");
                    config.setUnitName(unit);
                    config.setNamespace("ignored-namespace");
                    config.setEnableStreamRequestType(stream);
                    Map<String, Object> item = new TreeMap<>();
                    item.put("ip", ip);
                    item.put("instance", "instance");
                    item.put("unit", unit);
                    item.put("stream", stream);
                    item.put("id", config.buildMQClientId());
                    identities.add(item);
                }
            }
        }
        ClientConfig config = new ClientConfig();
        config.setClientIP("192.0.2.4");
        config.setInstanceName(null);
        Map<String, Object> nullInstance = new TreeMap<>();
        nullInstance.put("ip", "192.0.2.4");
        nullInstance.put("instance", null);
        nullInstance.put("unit", null);
        nullInstance.put("stream", false);
        nullInstance.put("id", config.buildMQClientId());
        identities.add(nullInstance);

        Map<String, Object> defaults = new TreeMap<>();
        config = new ClientConfig();
        defaults.put("pollNameServerInterval", config.getPollNameServerInterval());
        defaults.put("heartbeatBrokerInterval", config.getHeartbeatBrokerInterval());
        defaults.put("persistConsumerOffsetInterval", config.getPersistConsumerOffsetInterval());
        defaults.put("pullTimeDelayMillsWhenException", config.getPullTimeDelayMillsWhenException());
        defaults.put("mqClientApiTimeout", config.getMqClientApiTimeout());
        defaults.put("detectTimeout", config.getDetectTimeout());
        defaults.put("detectInterval", config.getDetectInterval());
        defaults.put("traceMsgBatchNum", config.getTraceMsgBatchNum());
        defaults.put("maxPageSizeInGetMetadata", config.getMaxPageSizeInGetMetadata());
        defaults.put("accessChannel", config.getAccessChannel().toString());
        defaults.put("enableConcurrentHeartbeat", config.isEnableConcurrentHeartbeat());
        defaults.put("enableHeartbeatChannelEventListener", config.isEnableHeartbeatChannelEventListener());

        config.setTraceMsgBatchNum(99);
        config.setMaxPageSizeInGetMetadata(77);
        config.setAccessChannel(org.apache.rocketmq.client.AccessChannel.CLOUD);
        config.setNamespaceV2("v2");
        config.setEnableConcurrentHeartbeat(true);
        config.setConcurrentHeartbeatThreadPoolSize(7);
        ClientConfig copy = config.cloneClientConfig();
        Map<String, Object> clone = new TreeMap<>();
        clone.put("traceMsgBatchNum", copy.getTraceMsgBatchNum());
        clone.put("maxPageSizeInGetMetadata", copy.getMaxPageSizeInGetMetadata());
        clone.put("accessChannel", copy.getAccessChannel().toString());
        clone.put("namespaceV2", copy.getNamespaceV2());
        clone.put("enableConcurrentHeartbeat", copy.isEnableConcurrentHeartbeat());
        clone.put("concurrentHeartbeatThreadPoolSize", copy.getConcurrentHeartbeatThreadPoolSize());
        config.setClientIP("manager-copy");
        config.setNamesrvAddr("MQ_INST_a_b.example:9876");
        config.setNamespace(null); // Caller initialized; manager clone must reset its flag.
        org.apache.rocketmq.client.impl.MQClientManager manager = org.apache.rocketmq.client.impl.MQClientManager.getInstance();
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime = manager.getOrCreateMQClientInstance(config);
        Map<String, Object> managed = new TreeMap<>();
        try {
            ClientConfig actual = runtime.getClientConfig();
            managed.put("traceMsgBatchNum", actual.getTraceMsgBatchNum());
            managed.put("maxPageSizeInGetMetadata", actual.getMaxPageSizeInGetMetadata());
            managed.put("accessChannel", actual.getAccessChannel().toString());
            managed.put("namespaceV2", actual.getNamespaceV2());
            managed.put("namespace", actual.getNamespace());
            managed.put("inputNamespaceStillNull", config.getNamespace() == null);
            managed.put("enableConcurrentHeartbeat", actual.isEnableConcurrentHeartbeat());
            managed.put("concurrentHeartbeatThreadPoolSize", actual.getConcurrentHeartbeatThreadPoolSize());
            managed.put("sharedIdentity", runtime == manager.getOrCreateMQClientInstance(config));
        } finally {
            try {
                java.lang.reflect.Method cleanup = runtime.getClass().getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("manager fixture teardown"));
            } finally {
                manager.removeClientFactory(runtime.getClientId(), runtime);
            }
        }
        List<Object> endpoints = new ArrayList<>();
        for (String endpoint : new String[] {null, "", "broker:9876", "http://broker:9876", " http://broker:9876 ",
            "https://MQ_INST_a_b.example:80", "MQ_INST_a_b.example:80", "http://MQ_INST_a_b.example:80",
            "http://MQ_INST_a_b.example/path", "http://MQ_INST_a_b.example\n", "http://MQ_INST_a_b.example\u2028", "MQ_INST_é_b.example"}) {
            ClientConfig c = new ClientConfig();
            c.setNamesrvAddr(endpoint);
            Map<String, Object> item = new TreeMap<>();
            item.put("endpoint", endpoint); item.put("address", c.getNamesrvAddr());
            try { item.put("namespace", c.getNamespace()); item.put("outcome", "OK"); }
            catch (RuntimeException error) { item.put("outcome", error.getClass().getSimpleName()); }
            endpoints.add(item);
        }
        List<Object> resources = new ArrayList<>();
        for (String namespace : new String[] {null, "", "ns"}) {
            for (String resource : new String[] {null, "", "topic", "ns%topic", "other%topic", "%topic", "%RETRY%group", "%DLQ%group",
                "%RETRY%ns%group", "%DLQ%other%group", "TBW102", "SCHEDULE_TOPIC_XXXX", "BenchmarkTest", "rmq_sys_custom", "CID_RMQ_SYS_custom", "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC", " ", "ns%"}) {
                ClientConfig c = new ClientConfig(); c.setNamespace(namespace);
                Map<String, Object> item = new TreeMap<>(); item.put("namespace", namespace); item.put("resource", resource);
                item.put("with", c.withNamespace(resource)); item.put("without", c.withoutNamespace(resource));
                resources.add(item);
            }
        }
        Map<String, Object> pid = new TreeMap<>();
        for (String instance : new String[] {"DEFAULT", "custom", "default", ""}) {
            ClientConfig c = new ClientConfig(); c.setInstanceName(instance); c.changeInstanceNameToPID();
            String changed = c.getInstanceName(); c.changeInstanceNameToPID();
            pid.put(instance, instance.equals("DEFAULT") ? changed.matches(org.apache.rocketmq.common.UtilAll.getPid() + "#-?\\d+") && changed.equals(c.getInstanceName()) : instance.equals(changed));
        }
        Map<String, Object> fixture = new TreeMap<>();
        List<Object> rebalanceLongs = new ArrayList<>();
        for (String input : new String[] {"٢٠٠٠٠", "+１２３４", "-١", "1٢3", "०", "9223372036854775807",
            "-9223372036854775808", "９２２３３７２０３６８５４７７５８０７", "-９２２３３７２０３６８５４７７５８０８",
            "9223372036854775808", "-9223372036854775809", "９２２３３７２０３６８５４７７５８０８",
            "", "+", "-", " 1", "1 ", "²", "𝟚", "NaN"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", input);
            try { item.put("value", Long.parseLong(input)); item.put("outcome", "OK"); }
            catch (NumberFormatException error) { item.put("outcome", "NumberFormatException"); }
            rebalanceLongs.add(item);
        }
        fixture.put("rebalanceLongs", rebalanceLongs);
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "ClientConfig.buildMQClientId/cloneClientConfig/getNamesrvAddr/getNamespace/withNamespace/withoutNamespace/changeInstanceNameToPID; MQClientManager.getOrCreateMQClientInstance; MQClientInstance.getClientConfig; Long.parseLong (RebalanceService property parser)");
        fixture.put("serverBaselineContacted", false);
        fixture.put("identities", identities);
        fixture.put("defaults", defaults);
        fixture.put("clone", clone);
        fixture.put("managed", managed);
        fixture.put("endpoints", endpoints);
        fixture.put("resources", resources);
        fixture.put("pid", pid);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("ClientConfig identities/defaults/clone verified: " + identities.size());
    }
}
