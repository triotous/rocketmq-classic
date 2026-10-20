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
        List<Object> reset = new ArrayList<>();
        for (String name : new String[] {"missing-group", "pull-group", "null-group", "empty-push-null-inputs",
            "null-topic-active", "null-map-active", "null-map-other-topic", "null-value", "null-key-only",
            "unassigned-offset", "success", "per-queue-failure", "live-map-update", "live-new-queue", "orderly-success"}) {
            reset.add(reset(name));
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("serverBaselineContacted", false);
        fixture.put("runningInfoEntrypoint", "MQClientInstance.consumerRunningInfo(String)");
        fixture.put("runningInfo", runningInfo);
        fixture.put("directConsumeEntrypoint", "MQClientInstance.consumeMessageDirectly(MessageExt,String,String)");
        fixture.put("directConsume", direct);
        fixture.put("resetOffsetEntrypoint", "MQClientInstance.resetOffset(String,String,Map<MessageQueue,Long>)");
        fixture.put("resetOffset", reset);
        fixture.put("resetStoreHandoff", reset("store-handoff"));
        fixture.put("resetOffsetSynchronized", java.lang.reflect.Modifier.isSynchronized(
            MQClientInstance.class.getMethod("resetOffset", String.class, String.class, Map.class).getModifiers()));
        String json = JSON.toJSONString(fixture, JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path path = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(path))) throw new IllegalStateException("fixture drift: " + path);
        } else {
            Files.createDirectories(path.getParent()); Files.writeString(path, json);
        }
        System.out.println("Direct runtime consumer operations verified: " + runningInfo.size()
            + " Running Info, " + direct.size() + " direct-consume, " + reset.size() + " reset cases");
    }

    private static Map<String, Object> reset(String name) throws Exception {
        ClientConfig config = new ClientConfig(); config.setClientIP("reset-runtime"); config.setNamesrvAddr(null);
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        var facade = new org.apache.rocketmq.client.consumer.DefaultMQPushConsumer("reset");
        facade.setMessageModel(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING);
        List<String> lifecycle = new ArrayList<>();
        var consumer = new org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl(facade, null) {
            @Override public void suspend() { super.suspend(); lifecycle.add("suspend"); }
            // Record the required finally action without running an unrelated
            // assignment round in this isolated, never-started runtime probe.
            @Override public void resume() { setPause(false); lifecycle.add("resume"); }
        };
        var values = new TreeMap<Integer, Long>(); var persisted = new TreeMap<Integer, Long>();
        Map<Integer, List<String>> events = new TreeMap<>();
        Long[] handoffValues = new Long[] {60L, 70L};
        var finalStore = (org.apache.rocketmq.client.consumer.store.OffsetStore) Proxy.newProxyInstance(
            RuntimeConsumerOperationsFixtures.class.getClassLoader(),
            new Class<?>[] {org.apache.rocketmq.client.consumer.store.OffsetStore.class}, (proxy, method, arguments) -> {
                if (!method.getName().equals("removeOffset")) throw new IllegalStateException("unexpected final-store call");
                int id = ((org.apache.rocketmq.common.message.MessageQueue) arguments[0]).getQueueId();
                events.computeIfAbsent(id, ignored -> new ArrayList<>()).add("remove:third"); handoffValues[1] = null; return null;
            });
        var nextStore = (org.apache.rocketmq.client.consumer.store.OffsetStore) Proxy.newProxyInstance(
            RuntimeConsumerOperationsFixtures.class.getClassLoader(),
            new Class<?>[] {org.apache.rocketmq.client.consumer.store.OffsetStore.class}, (proxy, method, arguments) -> {
                if (!method.getName().equals("persist")) throw new IllegalStateException("unexpected second-store call");
                int id = ((org.apache.rocketmq.common.message.MessageQueue) arguments[0]).getQueueId();
                events.computeIfAbsent(id, ignored -> new ArrayList<>()).add("persist:second");
                persisted.put(id, handoffValues[0]); consumer.setOffsetStore(finalStore); return null;
            });
        var store = (org.apache.rocketmq.client.consumer.store.OffsetStore) Proxy.newProxyInstance(
            RuntimeConsumerOperationsFixtures.class.getClassLoader(),
            new Class<?>[] {org.apache.rocketmq.client.consumer.store.OffsetStore.class}, (proxy, method, arguments) -> {
                var queue = (org.apache.rocketmq.common.message.MessageQueue) arguments[0]; int id = queue.getQueueId();
                var calls = events.computeIfAbsent(id, ignored -> new ArrayList<>());
                switch (method.getName()) {
                    case "updateOffset":
                        calls.add("update:" + arguments[1]); values.put(id, (Long) arguments[1]);
                        if (name.equals("store-handoff")) consumer.setOffsetStore(nextStore);
                        return null;
                    case "persist":
                        calls.add("persist");
                        if (name.equals("per-queue-failure") && id == 0) throw new IllegalStateException("persist failed");
                        persisted.put(id, values.get(id)); return null;
                    case "removeOffset": calls.add("remove"); values.remove(id); return null;
                    default: throw new IllegalStateException("unexpected store method: " + method.getName());
                }
            });
        consumer.setOffsetStore(store);
        boolean orderly = name.equals("orderly-success");
        var orderlyField = org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl.class.getDeclaredField("consumeOrderly");
        orderlyField.setAccessible(true); orderlyField.set(consumer, orderly);
        var table = consumer.getRebalanceImpl().getProcessQueueTable();
        Map<Integer, org.apache.rocketmq.client.impl.consumer.ProcessQueue> queues = new TreeMap<>();
        var cleared = new java.util.concurrent.CountDownLatch(1);
        var queue0 = new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 0);
        var queue1 = new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 1);
        if (!name.equals("empty-push-null-inputs")) {
            var processQueue = new org.apache.rocketmq.client.impl.consumer.ProcessQueue() {
                @Override public void clear() { super.clear(); cleared.countDown(); }
            };
            table.put(queue0, processQueue); queues.put(0, processQueue); values.put(0, 50L);
        }
        if (name.equals("per-queue-failure")) {
            var processQueue = new org.apache.rocketmq.client.impl.consumer.ProcessQueue();
            table.put(queue1, processQueue); queues.put(1, processQueue); values.put(1, 60L);
        }
        Map<org.apache.rocketmq.common.message.MessageQueue, Long> offsets = new java.util.LinkedHashMap<>();
        offsets.put(queue0, name.equals("null-value") ? null : 7L);
        if (name.equals("per-queue-failure") || name.equals("live-new-queue")) offsets.put(queue1, 9L);
        if (name.equals("null-key-only")) { offsets.clear(); offsets.put(null, 8L); }
        if (name.equals("unassigned-offset")) { offsets.clear(); offsets.put(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker-a", 9), 99L); }
        String topic = name.equals("empty-push-null-inputs") || name.equals("null-topic-active") ? null
            : name.equals("null-map-other-topic") ? "other" : "orders";
        if (name.startsWith("null-map") || name.equals("empty-push-null-inputs")) offsets = null;
        final var input = offsets;
        Map<String, Object> item = new TreeMap<>();
        try {
            String group = name.equals("null-group") ? null : name.equals("missing-group") ? "missing" : "selected";
            runtime.getConsumerTable().put("selected", name.equals("pull-group")
                ? new org.apache.rocketmq.client.consumer.DefaultMQPullConsumer("pull").getDefaultMQPullConsumerImpl() : consumer);
            item.put("name", name); item.put("topic", topic); item.put("group", group); item.put("orderly", orderly);
            item.put("offsets", resetInputRows(input));
            if (name.startsWith("live-")) {
                var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
                Thread worker = new Thread(() -> {
                    try { runtime.resetOffset(topic, group, input); } catch (Throwable error) { failure.set(error); }
                }, "reset-fixture");
                worker.start();
                if (!cleared.await(5, java.util.concurrent.TimeUnit.SECONDS)) throw new IllegalStateException("reset did not clear queue");
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                while (java.util.Arrays.stream(worker.getStackTrace()).noneMatch(frame ->
                    frame.getClassName().equals("java.util.concurrent.TimeUnit") && frame.getMethodName().equals("sleep"))) {
                    if (System.nanoTime() > deadline) { worker.interrupt(); throw new IllegalStateException("reset did not reach drain sleep"); }
                    Thread.yield();
                }
                if (name.equals("live-map-update")) { input.put(queue0, 77L); }
                else {
                    var processQueue = new org.apache.rocketmq.client.impl.consumer.ProcessQueue();
                    table.put(queue1, processQueue); queues.put(1, processQueue); values.put(1, 60L);
                }
                worker.interrupt(); worker.join(5000);
                if (worker.isAlive()) throw new IllegalStateException("reset worker did not finish");
                if (failure.get() != null) throw new IllegalStateException("live reset failed", failure.get());
                item.put("error", null);
            } else {
                // Java explicitly ignores interrupted drain sleeps. Avoid a
                // 10-second wall-clock wait while still invoking its real path.
                if (!orderly) Thread.currentThread().interrupt();
                try { runtime.resetOffset(topic, group, input); item.put("error", null); }
                catch (Exception error) { item.put("error", error.getClass().getSimpleName()); }
                finally { Thread.interrupted(); }
            }
            List<Object> rows = new ArrayList<>();
            for (var entry : queues.entrySet()) {
                int id = entry.getKey(); var queue = id == 0 ? queue0 : queue1;
                Map<String, Object> row = new TreeMap<>(); row.put("id", id);
                row.put("present", table.containsKey(queue)); row.put("dropped", entry.getValue().isDropped());
                row.put("value", values.get(id)); row.put("persisted", persisted.get(id)); rows.add(row);
            }
            Map<String, Object> normalizedEvents = new TreeMap<>();
            events.forEach((id, calls) -> normalizedEvents.put(id.toString(), calls));
            item.put("queues", rows); item.put("events", normalizedEvents); item.put("lifecycle", lifecycle); item.put("pausedAfter", consumer.isPause());
            if (name.equals("store-handoff")) item.put("storeValues", java.util.Arrays.asList(values.get(0), handoffValues[0], handoffValues[1]));
            return item;
        } finally {
            Thread.interrupted(); runtime.getConsumerTable().clear();
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure", MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true); cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
    }

    private static Object resetInputRows(Map<org.apache.rocketmq.common.message.MessageQueue, Long> offsets) {
        if (offsets == null) return null;
        List<Object> rows = new ArrayList<>();
        for (var entry : offsets.entrySet()) {
            Map<String, Object> row = new TreeMap<>();
            row.put("queueId", entry.getKey() == null ? null : entry.getKey().getQueueId());
            row.put("offset", entry.getValue()); rows.add(row);
        }
        return rows;
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
