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
        List<Object> direct = new ArrayList<>();
        for (String name : new String[] {"concurrent-success", "concurrent-later", "concurrent-null-status",
            "concurrent-throws", "concurrent-null-broker", "orderly-success", "orderly-commit",
            "orderly-rollback", "orderly-later", "orderly-null-status", "push-null-message",
            "push-null-service", "missing-group", "pull-group", "other-group", "null-group"}) {
            direct.add(direct(name));
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("serverBaselineContacted", false);
        fixture.put("runningInfoEntrypoint", "MQClientInstance.consumerRunningInfo(String)");
        fixture.put("runningInfo", runningInfo);
        fixture.put("directConsumeEntrypoint", "MQClientInstance.consumeMessageDirectly(MessageExt,String,String)");
        fixture.put("directConsume", direct);
        String json = JSON.toJSONString(fixture, JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path path = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(path))) throw new IllegalStateException("fixture drift: " + path);
        } else {
            Files.createDirectories(path.getParent()); Files.writeString(path, json);
        }
        System.out.println("Direct runtime consumer operations verified: " + runningInfo.size() + " Running Info cases");
    }

    private static Map<String, Object> direct(String name) throws Exception {
        ClientConfig config = new ClientConfig(); config.setClientIP("direct-runtime"); config.setNamesrvAddr(null);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        var push = new org.apache.rocketmq.client.consumer.DefaultMQPushConsumer("tenant%direct");
        push.setNamespace("tenant");
        var implementation = push.getDefaultMQPushConsumerImpl();
        var message = new org.apache.rocketmq.common.message.MessageExt();
        message.setTopic("tenant%orders"); message.setQueueId(3); message.setBody(new byte[] {1});
        List<Object> callbacks = new ArrayList<>();
        java.util.function.BiConsumer<List<org.apache.rocketmq.common.message.MessageExt>,
            org.apache.rocketmq.common.message.MessageQueue> record = (messages, queue) -> {
                Map<String, Object> item = new TreeMap<>();
                item.put("topic", messages.get(0).getTopic()); item.put("broker", messages.get(0).getBrokerName());
                item.put("queueTopic", queue.getTopic()); item.put("queueBroker", queue.getBrokerName());
                item.put("queueId", queue.getQueueId()); callbacks.add(item);
            };
        org.apache.rocketmq.client.impl.consumer.ConsumeMessageService service;
        if (name.startsWith("orderly")) {
            org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly listener = (messages, context) -> {
                record.accept(messages, context.getMessageQueue()); context.setAutoCommit(false);
                if (name.endsWith("null-status")) return null;
                if (name.endsWith("commit")) return org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus.COMMIT;
                if (name.endsWith("rollback")) return org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus.ROLLBACK;
                if (name.endsWith("later")) return org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                return org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus.SUCCESS;
            };
            service = new org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService(implementation, listener);
        } else {
            org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently listener = (messages, context) -> {
                record.accept(messages, context.getMessageQueue());
                if (name.endsWith("throws")) throw new IllegalStateException("listener failed");
                if (name.endsWith("null-status")) return null;
                return name.endsWith("later") ? org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus.RECONSUME_LATER
                    : org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            };
            service = new org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService(implementation, listener);
        }
        var serviceField = implementation.getClass().getDeclaredField("consumeMessageService");
        serviceField.setAccessible(true);
        serviceField.set(implementation, name.equals("push-null-service") ? null : service);
        var item = new TreeMap<String, Object>();
        try {
            String group = name.equals("null-group") ? null : name.equals("missing-group") ? "missing" : "selected";
            if (name.equals("pull-group")) {
                runtime.getConsumerTable().put("selected", new org.apache.rocketmq.client.consumer.DefaultMQPullConsumer("pull")
                    .getDefaultMQPullConsumerImpl());
            } else if (name.equals("other-group")) {
                runtime.getConsumerTable().put("selected", (MQConsumerInner) Proxy.newProxyInstance(
                    RuntimeConsumerOperationsFixtures.class.getClassLoader(), new Class<?>[] {MQConsumerInner.class},
                    (proxy, method, arguments) -> { throw new IllegalStateException("unexpected consumer call"); }));
            } else { runtime.getConsumerTable().put("selected", implementation); }
            String broker = name.endsWith("null-broker") ? null : "broker-a";
            boolean nullMessage = name.equals("push-null-message") || name.endsWith("group");
            item.put("name", name); item.put("group", group); item.put("broker", broker); item.put("nullMessage", nullMessage);
            try {
                var result = runtime.consumeMessageDirectly(nullMessage ? null : message, group, broker);
                item.put("error", null); item.put("resultPresent", result != null);
                if (result != null) {
                    // Only wall-clock duration and JVM exception rendering are
                    // nondeterministic/runtime-specific; keep result/order/counts.
                    item.put("result", result.getConsumeResult().name()); item.put("order", result.isOrder());
                    item.put("autoCommit", result.isAutoCommit()); item.put("remarkPresent", result.getRemark() != null);
                    if (result.getSpentTimeMills() < 0) throw new IllegalStateException("negative elapsed time");
                }
            } catch (Exception error) { item.put("error", error.getClass().getSimpleName()); }
            item.put("callbacks", callbacks);
            return item;
        } finally {
            service.shutdown(100);
            runtime.getConsumerTable().clear();
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true); cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
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
