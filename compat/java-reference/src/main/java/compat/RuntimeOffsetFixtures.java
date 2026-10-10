package compat;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
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
        List<Object> nullableCases = new ArrayList<>();
        try {
            for (String namespace : new String[] {null, "", "ns", "other", " "}) {
                Map<MessageQueue, Long> input = new TreeMap<>();
                String[] topics = {"ns%topic", "other%topic", "%RETRY%ns%group", "%DLQ%ns%group", "TBW102", "rmq_sys_custom", "", "plain", "ns%collision", "collision"};
                for (int i = 0; i < topics.length; i++) input.put(new MessageQueue(topics[i], "broker", i < 8 ? i - 1 : 0),
                    i < 8 ? (i == 0 ? Long.MIN_VALUE : i == 1 ? Long.MAX_VALUE : (long) -i) : (i == 8 ? 99L : 7L));
                record(cases, runtime, namespace, input);
                record(cases, runtime, namespace, new TreeMap<>());
            }
            Map<MessageQueue, Long> nullableWithoutNamespace = new LinkedHashMap<>();
            nullableWithoutNamespace.put(null, null);
            nullableWithoutNamespace.put(new MessageQueue("ns%topic", "broker", 7), null);
            recordNullable(nullableCases, runtime, "null-namespace-null-key-value", null,
                nullableWithoutNamespace);

            Map<MessageQueue, Long> nullableEmptyNamespace = new LinkedHashMap<>();
            nullableEmptyNamespace.put(null, null);
            nullableEmptyNamespace.put(new MessageQueue("topic", "broker", 8), 9L);
            recordNullable(nullableCases, runtime, "empty-namespace-null-key-value", "",
                nullableEmptyNamespace);

            Map<MessageQueue, Long> nullableQueueFields = new LinkedHashMap<>();
            nullableQueueFields.put(new MessageQueue(null, null, 4), null);
            recordNullable(nullableCases, runtime, "namespace-null-queue-fields-null-offset", "ns",
                nullableQueueFields);

            Map<MessageQueue, Long> nullKeyAfterValidQueue = new LinkedHashMap<>();
            nullKeyAfterValidQueue.put(new MessageQueue("ns%first", "broker", 1), 10L);
            nullKeyAfterValidQueue.put(null, 11L);
            recordNullable(nullableCases, runtime, "namespace-null-key-after-valid-queue", "ns",
                nullKeyAfterValidQueue);

            recordNullable(nullableCases, runtime, "null-input-map", null, null);
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true); cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1"); fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "MQClientInstance.parseOffsetTableFromBroker/NamespaceUtil.withoutNamespace");
        fixture.put("inputMapType", "TreeMap; MessageQueue ordering; unequal collision follows input iteration");
        fixture.put("serverBaselineContacted", false); fixture.put("cases", cases);
        fixture.put("nullableCases", nullableCases);
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
    private static void recordNullable(List<Object> cases, MQClientInstance runtime, String name,
        String namespace, Map<MessageQueue, Long> input) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name); item.put("namespace", namespace);
        item.put("input", nullableRows(input));
        try {
            Map<MessageQueue, Long> output = runtime.parseOffsetTableFromBroker(input, namespace);
            item.put("error", null); item.put("output", nullableRows(output));
        } catch (Exception error) {
            item.put("error", error.getClass().getSimpleName()); item.put("output", null);
        }
        item.put("mutatedInput", nullableRows(input));
        cases.add(item);
    }
    private static List<Object> nullableRows(Map<MessageQueue, Long> table) {
        if (table == null) return null;
        List<Map.Entry<MessageQueue, Long>> entries = new ArrayList<>(table.entrySet());
        entries.sort(Comparator.comparing((Map.Entry<MessageQueue, Long> entry) -> entry.getKey() == null ? null : entry.getKey().getTopic(),
                Comparator.nullsFirst(String::compareTo))
            .thenComparing(entry -> entry.getKey() == null ? null : entry.getKey().getBrokerName(),
                Comparator.nullsFirst(String::compareTo))
            .thenComparingInt(entry -> entry.getKey() == null ? Integer.MIN_VALUE : entry.getKey().getQueueId()));
        List<Object> rows = new ArrayList<>();
        for (Map.Entry<MessageQueue, Long> entry : entries) {
            Map<String, Object> row = new TreeMap<>();
            MessageQueue queue = entry.getKey();
            if (queue == null) {
                row.put("queue", null);
            } else {
                Map<String, Object> queueRow = new TreeMap<>();
                queueRow.put("topic", queue.getTopic()); queueRow.put("brokerName", queue.getBrokerName());
                queueRow.put("queueId", queue.getQueueId()); row.put("queue", queueRow);
            }
            row.put("offset", entry.getValue()); rows.add(row);
        }
        return rows;
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
