package compat;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;

/** Invoked explicit-namespace offset conversion; no transport starts. */
public final class RuntimeOffsetFixtures {
    private RuntimeOffsetFixtures() { }
    public static void main(String[] args) throws Exception {
        ClientConfig config = new ClientConfig(); config.setClientIP("offset-client"); config.setNamespace("ignored-config-namespace");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        List<Object> cases = new ArrayList<>();
        try {
            for (String namespace : new String[] {null, "", "ns", "other", " "}) {
                Map<MessageQueue, Long> input = new TreeMap<>();
                String[] topics = {"ns%topic", "other%topic", "%RETRY%ns%group", "%DLQ%ns%group", "TBW102", "rmq_sys_custom", "", "plain", "ns%collision", "collision"};
                for (int i = 0; i < topics.length; i++) input.put(new MessageQueue(topics[i], "broker", i < 8 ? i - 1 : 0),
                    i < 8 ? (i == 0 ? Long.MIN_VALUE : i == 1 ? Long.MAX_VALUE : (long) -i) : (i == 8 ? 99L : 7L));
                record(cases, runtime, namespace, input);
                record(cases, runtime, namespace, new TreeMap<>());
            }
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true); cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1"); fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "MQClientInstance.parseOffsetTableFromBroker/NamespaceUtil.withoutNamespace");
        fixture.put("inputMapType", "TreeMap; MessageQueue ordering; unequal collision follows input iteration");
        fixture.put("serverBaselineContacted", false); fixture.put("cases", cases);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture, com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else { Files.createDirectories(file.getParent()); Files.writeString(file, json); }
        System.out.println("Runtime offset conversion verified: " + cases.size());
    }
    private static void record(List<Object> cases, MQClientInstance runtime, String namespace, Map<MessageQueue, Long> input) {
        Map<String, Object> item = new TreeMap<>(); item.put("namespace", namespace); item.put("input", rows(input));
        Map<MessageQueue, Long> output = runtime.parseOffsetTableFromBroker(input, namespace);
        item.put("output", rows(output)); item.put("mutatedInput", rows(input)); cases.add(item);
    }
    private static List<Object> rows(Map<MessageQueue, Long> table) {
        List<Map.Entry<MessageQueue, Long>> entries = new ArrayList<>(table.entrySet());
        entries.sort(Comparator.comparing((Map.Entry<MessageQueue, Long> entry) -> entry.getKey().getTopic())
            .thenComparing(entry -> entry.getKey().getBrokerName()).thenComparingInt(entry -> entry.getKey().getQueueId()));
        List<Object> rows = new ArrayList<>();
        for (Map.Entry<MessageQueue, Long> entry : entries) {
            Map<String, Object> row = new TreeMap<>(); row.put("topic", entry.getKey().getTopic()); row.put("brokerName", entry.getKey().getBrokerName());
            row.put("queueId", entry.getKey().getQueueId()); row.put("offset", entry.getValue()); rows.add(row);
        }
        return rows;
    }
}
