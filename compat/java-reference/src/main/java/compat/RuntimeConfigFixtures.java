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
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "ClientConfig.buildMQClientId/cloneClientConfig");
        fixture.put("serverBaselineContacted", false);
        fixture.put("identities", identities);
        fixture.put("defaults", defaults);
        fixture.put("clone", clone);
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
