package compat;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;

/** Invokes actual runtime registration and private heartbeat preparation.
 * The transport is never started; finally releases all constructed resources. */
public final class RuntimeHeartbeatFixtures {
    private RuntimeHeartbeatFixtures() { }

    private static List<Object> subscriptionFieldCases() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{}",
            "{\"topic\":null,\"subString\":null,\"tagsSet\":null,\"codeSet\":null,\"expressionType\":null,\"subVersion\":0}",
            "{\"topic\":\"T\",\"subString\":\"tag\",\"tagsSet\":[\"tag\"],\"codeSet\":[7],\"expressionType\":\"SQL92\",\"subVersion\":42}"
        }) {
            long before = System.currentTimeMillis();
            SubscriptionData value = com.alibaba.fastjson2.JSON.parseObject(input, SubscriptionData.class);
            long after = System.currentTimeMillis();
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("rawInput", input);
            item.put("topicNull", value.getTopic() == null);
            item.put("subStringNull", value.getSubString() == null);
            item.put("tagsNull", value.getTagsSet() == null);
            item.put("codeNull", value.getCodeSet() == null);
            item.put("expressionNull", value.getExpressionType() == null);
            item.put("tagsEmpty", value.getTagsSet() != null && value.getTagsSet().isEmpty());
            item.put("codeEmpty", value.getCodeSet() != null && value.getCodeSet().isEmpty());
            item.put("versionCurrent", value.getSubVersion() >= before && value.getSubVersion() <= after);
            item.put("versionZero", value.getSubVersion() == 0L);
            item.put("versionFortyTwo", value.getSubVersion() == 42L);
            item.put("encodedHasTopic", encoded.containsKey("topic"));
            item.put("encodedHasSubString", encoded.containsKey("subString"));
            item.put("encodedHasTags", encoded.containsKey("tagsSet"));
            item.put("encodedHasCode", encoded.containsKey("codeSet"));
            item.put("encodedHasExpression", encoded.containsKey("expressionType"));
            cases.add(item);
        }
        return cases;
    }

    private static void record(List<Object> cases, MQClientInstance runtime, String name,
        boolean withoutSub) throws Exception {
        Method prepare = MQClientInstance.class.getDeclaredMethod("prepareHeartbeatData", boolean.class);
        prepare.setAccessible(true);
        Map<String, Object> item = new TreeMap<>();
        item.put("case", name);
        HeartbeatData data = (HeartbeatData) prepare.invoke(runtime, withoutSub);
        item.put("body", com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(data)));
        item.put("fingerprint", data.computeHeartbeatFingerprint());
        cases.add(item);
    }

    public static void main(String[] args) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("192.0.2.6");
        config.setInstanceName("DEFAULT");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        List<Object> cases = new ArrayList<>();
        List<Object> multiplicityCases = new ArrayList<>();
        try {
            record(cases, runtime, "empty", false);
            DefaultMQProducer producer = new DefaultMQProducer("producer");
            if (!runtime.registerProducer("producer", producer.getDefaultMQProducerImpl()))
                throw new IllegalStateException("initial producer registration");
            if (runtime.registerProducer("producer", producer.getDefaultMQProducerImpl()))
                throw new IllegalStateException("duplicate producer accepted");
            record(cases, runtime, "producer-only", false);
            SubscriptionData[] subscription = {new SubscriptionData()};
            subscription[0].setTopic("topic");
            subscription[0].setSubString("*");
            subscription[0].setSubVersion(12L);
            MQConsumerInner consumer = (MQConsumerInner) Proxy.newProxyInstance(
                MQConsumerInner.class.getClassLoader(), new Class<?>[] {MQConsumerInner.class},
                (proxy, method, values) -> {
                    switch (method.getName()) {
                        case "groupName": return "consumer";
                        case "consumeType": return ConsumeType.CONSUME_ACTIVELY;
                        case "messageModel": return MessageModel.CLUSTERING;
                        case "consumeFromWhere": return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
                        case "isUnitMode": return false;
                        case "subscriptions": return Collections.singleton(subscription[0]);
                        default: return method.getReturnType() == boolean.class ? false : null;
                    }
                });
            if (!runtime.registerConsumer("consumer", consumer)) throw new IllegalStateException("consumer registration");
            if (runtime.registerConsumer("consumer", consumer)) throw new IllegalStateException("duplicate consumer accepted");
            record(cases, runtime, "both", false);
            subscription[0] = new SubscriptionData();
            subscription[0].setTopic("other");
            subscription[0].setSubString("tag");
            subscription[0].setSubVersion(13L);
            record(cases, runtime, "subscription-change", false);
            record(cases, runtime, "without-sub", true);
            runtime.unregisterProducer("producer"); // Empty Broker map; no network.
            record(cases, runtime, "consumer-only", false);
            runtime.unregisterConsumer("consumer");
            record(cases, runtime, "after-unregister", false);
            // ConsumerData inherits Object equality: distinct registration keys
            // must remain distinct even when their returned metadata is equal.
            if (!runtime.registerConsumer("registered-a", consumer)
                || !runtime.registerConsumer("registered-b", consumer))
                throw new IllegalStateException("distinct keys rejected");
            recordMultiplicity(multiplicityCases, runtime, "equal-full", false);
            recordMultiplicity(multiplicityCases, runtime, "equal-simple", true);
            runtime.unregisterConsumer("registered-b");
            MQConsumerInner otherConsumer = (MQConsumerInner) Proxy.newProxyInstance(
                MQConsumerInner.class.getClassLoader(), new Class<?>[] {MQConsumerInner.class},
                (proxy, method, values) -> {
                    if ("subscriptions".equals(method.getName())) {
                        SubscriptionData other = new SubscriptionData();
                        other.setTopic("second");
                        other.setSubString("*");
                        other.setSubVersion(14L);
                        return Collections.singleton(other);
                    }
                    return method.invoke(consumer, values);
                });
            if (!runtime.registerConsumer("registered-b", otherConsumer))
                throw new IllegalStateException("replacement registration rejected");
            recordMultiplicity(multiplicityCases, runtime, "different-full", false);
            recordMultiplicity(multiplicityCases, runtime, "different-simple", true);
            runtime.unregisterConsumer("registered-a");
            recordMultiplicity(multiplicityCases, runtime, "one-after-unregister", false);
            runtime.unregisterConsumer("registered-b");
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "MQClientInstance.registerProducer/registerConsumer/prepareHeartbeatData/unregisterProducer/unregisterConsumer; HeartbeatData.computeHeartbeatFingerprint");
        fixture.put("serverBaselineContacted", false);
        fixture.put("cases", cases);
        fixture.put("multiplicityCases", multiplicityCases);
        fixture.put("subscriptionFieldCases", subscriptionFieldCases());
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("Runtime heartbeat invoked cases verified: " + cases.size()
            + "; multiplicity cases: " + multiplicityCases.size());
    }

    private static void recordMultiplicity(List<Object> cases, MQClientInstance runtime,
        String name, boolean withoutSub) throws Exception {
        record(cases, runtime, name, withoutSub);
        @SuppressWarnings("unchecked")
        Map<String, Object> item = (Map<String, Object>) cases.get(cases.size() - 1);
        // Arbitrary JVM object-set iteration is not a fingerprint oracle.
        item.remove("fingerprint");
        com.alibaba.fastjson2.JSONObject body = (com.alibaba.fastjson2.JSONObject) item.get("body");
        int expectedCount = "one-after-unregister".equals(name) ? 1 : 2;
        if (body.getJSONArray("consumerDataSet").size() != expectedCount)
            throw new IllegalStateException("unexpected Java multiplicity: " + name);
        body.getJSONArray("consumerDataSet").sort(java.util.Comparator.comparing(
            com.alibaba.fastjson2.JSON::toJSONString)); // Preserve every duplicate.
    }
}
