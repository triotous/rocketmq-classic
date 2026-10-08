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
        fixture.put("entrypoint", "ClientConfig.buildMQClientId/cloneClientConfig/resetClientConfig/getNamesrvAddr/getNamespace/withNamespace/withoutNamespace/changeInstanceNameToPID; MQClientManager.getOrCreateMQClientInstance/removeClientFactory/getFactoryTable; MQClientInstance.constructor/getClientConfig/getNettyClientConfig/getProducerTable/getConsumerTable/selectProducer/selectConsumer/registerConsumer/unregisterConsumer; MQClientAPIImpl.getRemotingClient/NettyRemotingClient.publicExecutor (reflective capacity observation); RemotingCommand.encode (null language JSON/ROCKETMQ); Long.parseLong (RebalanceService property parser)");
        fixture.put("serverBaselineContacted", false);
        fixture.put("identities", identities);
        fixture.put("defaults", defaults);
        fixture.put("clone", clone);
        fixture.put("managed", managed);
        fixture.put("managerRemoval", managerRemoval(config));
        fixture.put("managerTable", managerTable(config));
        fixture.put("nullableSettings", nullableSettings());
        fixture.put("socksConstruction", socksConstruction());
        fixture.put("callbackThreadsConstruction", callbackThreadsConstruction());
        fixture.put("registryTables", registryTables());
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

    private static Map<String, Object> managerRemoval(ClientConfig config) throws Exception {
        org.apache.rocketmq.client.impl.MQClientManager manager = org.apache.rocketmq.client.impl.MQClientManager.getInstance();
        java.util.concurrent.ConcurrentMap<String, org.apache.rocketmq.client.impl.factory.MQClientInstance> live = manager.getFactoryTable();
        org.apache.rocketmq.client.impl.factory.MQClientInstance first = manager.getOrCreateMQClientInstance(config);
        org.apache.rocketmq.client.impl.factory.MQClientInstance replacement = null;
        Map<String, Object> result = new TreeMap<>();
        String id = first.getClientId();
        try {
            result.put("liveSeesFirst", live.get(id) == first);
            manager.removeClientFactory(id);
            result.put("absentAfterUnconditional", !live.containsKey(id));
            replacement = manager.getOrCreateMQClientInstance(config);
            result.put("replacementIdentity", replacement != first);
            result.put("liveSeesReplacement", live.get(id) == replacement);
            manager.removeClientFactory(id, first);
            result.put("staleIdentityKeepsReplacement", live.get(id) == replacement);
            manager.removeClientFactory(id);
            result.put("unconditionalRemovesReplacement", !live.containsKey(id));
            manager.removeClientFactory(id); // Absent key is a no-op.
            result.put("absentRemovalStaysAbsent", !live.containsKey(id));
        } finally {
            try {
                cleanupRuntime(first);
            } finally {
                try {
                    if (replacement != null) cleanupRuntime(replacement);
                } finally {
                    manager.removeClientFactory(id, first);
                    if (replacement != null) manager.removeClientFactory(id, replacement);
                }
            }
        }
        return result;
    }

    private static void cleanupRuntime(org.apache.rocketmq.client.impl.factory.MQClientInstance runtime) throws Exception {
        java.lang.reflect.Method cleanup = runtime.getClass().getDeclaredMethod("cleanupAfterConstructionFailure",
            org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
        cleanup.setAccessible(true);
        cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("manager removal fixture teardown"));
    }

    private static Map<String, Object> managerTable(ClientConfig config) throws Exception {
        org.apache.rocketmq.client.impl.MQClientManager manager = org.apache.rocketmq.client.impl.MQClientManager.getInstance();
        java.util.concurrent.ConcurrentMap<String, org.apache.rocketmq.client.impl.factory.MQClientInstance> live = manager.getFactoryTable();
        org.apache.rocketmq.client.impl.factory.MQClientInstance first = manager.getOrCreateMQClientInstance(config);
        manager.removeClientFactory(first.getClientId());
        org.apache.rocketmq.client.impl.factory.MQClientInstance second = manager.getOrCreateMQClientInstance(config);
        String id = first.getClientId();
        java.util.Set<String> keyView = live.keySet();
        java.util.Collection<org.apache.rocketmq.client.impl.factory.MQClientInstance> valuesView = live.values();
        java.util.Set<Map.Entry<String, org.apache.rocketmq.client.impl.factory.MQClientInstance>> entryView = live.entrySet();
        Map<String, Object> result = new TreeMap<>();
        try {
            result.put("retainedViewSeesSecond", live.get(id) == second);
            result.put("containsValueSecond", live.containsValue(second));
            result.put("containsValueAbsent", !live.containsValue(first));
            result.put("getOrDefaultPresent", live.getOrDefault(id, first) == second);
            result.put("getOrDefaultAbsent", live.getOrDefault("missing", first) == first);
            result.put("putReturnsPrevious", live.put(id, first) == second);
            result.put("managerSeesPut", manager.getOrCreateMQClientInstance(config) == first);
            result.put("putIfAbsentKeepsCurrent", live.putIfAbsent(id, second) == first && live.get(id) == first);
            result.put("staleReplaceFails", !live.replace(id, second, first));
            result.put("matchingReplaceSucceeds", live.replace(id, first, second));
            result.put("replaceReturnsPrevious", live.replace(id, first) == second);
            result.put("staleRemoveFails", !live.remove(id, second));
            result.put("matchingRemoveSucceeds", live.remove(id, first));
            result.put("absentReplaceReturnsNull", live.replace(id, second) == null);
            result.put("absentRemoveReturnsNull", live.remove(id) == null);
            result.put("absentPutIfAbsentReturnsNull", live.putIfAbsent(id, first) == null);
            live.put("", second); // Empty and arbitrary alias keys are allowed.
            result.put("sizeWithAlias", live.size());
            result.put("aliasHasSecond", live.get("") == second);
            Map<String, org.apache.rocketmq.client.impl.factory.MQClientInstance> bulk = new TreeMap<>();
            bulk.put("bulk-a", first);
            bulk.put("bulk-b", second);
            live.putAll(bulk);
            result.put("putAllAdds", live.get("bulk-a") == first && live.get("bulk-b") == second
                && live.size() == 4);
            Map<String, org.apache.rocketmq.client.impl.factory.MQClientInstance> expected = new TreeMap<>();
            expected.put(id, first);
            expected.put("", second);
            expected.put("bulk-a", first);
            expected.put("bulk-b", second);
            Map<String, Boolean> visited = new TreeMap<>();
            live.forEach((key, value) -> visited.put(key, expected.get(key) == value));
            result.put("forEachVisitsCurrentMappings", visited.size() == expected.size()
                && visited.entrySet().stream().allMatch(entry -> Boolean.TRUE.equals(entry.getValue()))
                && expected.keySet().containsAll(visited.keySet()));
            result.put("keyViewSeesBulk", keyView.contains("bulk-a") && keyView.size() == 4);
            result.put("valuesViewSeesBulk", valuesView.contains(first) && valuesView.contains(second)
                && valuesView.size() == 4);
            result.put("entryViewSeesBulk", entryView.contains(new java.util.AbstractMap.SimpleImmutableEntry<>("bulk-a", first))
                && entryView.size() == 4);
            result.put("keyViewRemovesBulk", keyView.remove("bulk-b"));
            result.put("entryViewRemovesBulk", entryView.remove(
                new java.util.AbstractMap.SimpleImmutableEntry<>("bulk-a", first)));
            result.put("valuesViewRemovesCurrent", valuesView.remove(first));
            result.put("viewsSeeRemovals", !keyView.contains("bulk-b") && !entryView.contains(
                new java.util.AbstractMap.SimpleImmutableEntry<>("bulk-a", first)) && !keyView.contains(id)
                && valuesView.size() == 1);
            live.clear();
            result.put("retainedViewEmptyAfterClear", live.isEmpty());
            result.put("collectionViewsEmptyAfterClear", keyView.isEmpty() && valuesView.isEmpty() && entryView.isEmpty());
        } finally {
            live.remove(id);
            live.remove("");
            try { cleanupRuntime(first); } finally { cleanupRuntime(second); }
        }
        return result;
    }

    private static <T> T tableOwner(Class<T> type) {
        return tableOwner(type, new Object());
    }

    private static final class TableOwnerHandler implements java.lang.reflect.InvocationHandler {
        private final Object equalityKey;
        TableOwnerHandler(Object equalityKey) { this.equalityKey = equalityKey; }
        @Override public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
            if ("equals".equals(method.getName())) {
                Object other = args[0];
                if (other == proxy) return true;
                if (other == null || !java.lang.reflect.Proxy.isProxyClass(other.getClass())) return false;
                java.lang.reflect.InvocationHandler otherHandler = java.lang.reflect.Proxy.getInvocationHandler(other);
                return otherHandler instanceof TableOwnerHandler
                    && java.util.Objects.equals(equalityKey, ((TableOwnerHandler) otherHandler).equalityKey);
            }
            if ("hashCode".equals(method.getName())) return equalityKey.hashCode();
            throw new IllegalStateException("map fixture must not invoke owner callback: " + method.getName());
        }
    }

    private static <T> T tableOwner(Class<T> type, Object equalityKey) {
        return type.cast(java.lang.reflect.Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
            new TableOwnerHandler(equalityKey)));
    }

    private static <T> Map<String, Object> tableMutations(java.util.concurrent.ConcurrentMap<String, T> table,
        T first, T second, java.util.function.Function<String, T> select) {
        Map<String, Object> result = new TreeMap<>();
        result.put("initialEmpty", table.isEmpty());
        result.put("firstPutAbsent", table.put("group", first) == null);
        result.put("selectorSeesFirst", select.apply("group") == first);
        result.put("putReturnsFirst", table.put("group", second) == first);
        result.put("selectorSeesReplacement", select.apply("group") == second);
        result.put("putIfAbsentKeepsSecond", table.putIfAbsent("group", first) == second);
        result.put("staleReplaceRejected", !table.replace("group", first, second));
        result.put("conditionalReplaceAccepted", table.replace("group", second, first));
        result.put("replaceReturnsFirst", table.replace("group", second) == first);
        result.put("staleRemoveRejected", !table.remove("group", first));
        result.put("conditionalRemoveAccepted", table.remove("group", second));
        result.put("absentReplaceNull", table.replace("group", first) == null);
        result.put("absentRemoveNull", table.remove("group") == null);
        result.put("putIfAbsentAbsentNull", table.putIfAbsent("group", first) == null);
        result.put("emptyKeyAccepted", table.put("", second) == null);
        result.put("sizeTwo", table.size() == 2);
        table.clear();
        result.put("retainedViewSeesClear", table.isEmpty());
        result.put("selectorSeesClear", select.apply("group") == null);
        result.put("putAfterClearAccepted", table.put("group", second) == null && select.apply("group") == second);
        table.clear();
        return result;
    }

    private static <T> Map<String, Object> tableDefaultOperations(
        java.util.concurrent.ConcurrentMap<String, T> table, T first, T second) {
        Map<String, Object> result = new TreeMap<>();
        table.clear();
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        result.put("computeIfAbsentInsert", table.computeIfAbsent("a", key -> {
            calls.incrementAndGet(); return first;
        }) == first);
        result.put("computeIfAbsentNoRemap", table.computeIfAbsent("a", key -> {
            calls.incrementAndGet(); return second;
        }) == first && calls.get() == 1);
        result.put("computeIfPresentUpdate", table.computeIfPresent("a", (key, old) -> second) == second);
        result.put("computeIfPresentAbsent", table.computeIfPresent("missing", (key, old) -> second) == null);
        result.put("getOrDefaultPresent", table.getOrDefault("a", first) == second);
        result.put("getOrDefaultAbsent", table.getOrDefault("missing", second) == second);
        result.put("computeInsert", table.compute("b", (key, old) -> first) == first);
        result.put("computeRemove", table.compute("b", (key, old) -> null) == null && !table.containsKey("b"));
        result.put("mergeInsert", table.merge("c", first, (old, value) -> second) == first);
        result.put("mergeUpdate", table.merge("c", second, (old, value) -> second) == second);
        table.replaceAll((key, old) -> first);
        result.put("replaceAll", table.get("a") == first && table.get("c") == first);
        java.util.concurrent.atomic.AtomicInteger visited = new java.util.concurrent.atomic.AtomicInteger();
        table.forEach((key, value) -> visited.incrementAndGet());
        result.put("forEachCount", visited.get());
        Map<String, T> additions = new TreeMap<>();
        additions.put("d", second);
        additions.put("e", first);
        table.putAll(additions);
        result.put("putAllAdds", table.get("d") == second && table.get("e") == first && table.size() == 4);
        result.put("keys", new java.util.TreeSet<>(table.keySet()));
        result.put("valuesCount", table.values().size());
        table.clear();
        return result;
    }

    private static <T> Map<String, Object> tableLiveViews(
        java.util.concurrent.ConcurrentMap<String, T> table, T first, T second) {
        Map<String, Object> result = new TreeMap<>();
        table.clear();
        java.util.Set<String> keys = table.keySet();
        java.util.Collection<T> values = table.values();
        java.util.Set<Map.Entry<String, T>> entries = table.entrySet();
        table.put("a", first);
        result.put("retainedViewsSeeInitialPut", keys.contains("a") && values.contains(first)
            && entries.contains(new java.util.AbstractMap.SimpleImmutableEntry<>("a", first)));
        table.put("b", second);
        result.put("retainedViewsSeeLaterPut", keys.size() == 2 && values.size() == 2 && entries.size() == 2);
        result.put("keyViewRemove", keys.remove("a") && !table.containsKey("a"));
        result.put("valueViewRemove", values.remove(second) && !table.containsKey("b"));
        table.put("c", first);
        result.put("entryViewRemove", entries.remove(new java.util.AbstractMap.SimpleImmutableEntry<>("c", first))
            && !table.containsKey("c"));
        table.put("d", second);
        result.put("retainedViewsSeeRepopulation", keys.contains("d") && values.contains(second)
            && entries.contains(new java.util.AbstractMap.SimpleImmutableEntry<>("d", second)));
        keys.clear();
        result.put("keyViewClear", table.isEmpty());
        table.put("e", first);
        values.clear();
        result.put("valueViewClear", table.isEmpty());
        table.put("f", second);
        entries.clear();
        result.put("entryViewClear", table.isEmpty());

        table.put("remove-key", first);
        table.put("keep-key", second);
        result.put("keyViewRemoveIf", keys.removeIf(key -> key.equals("remove-key"))
            && !table.containsKey("remove-key"));
        table.clear();
        table.put("keep-key", first);
        table.put("drop-key", second);
        result.put("keyViewRetainAll", keys.retainAll(java.util.Collections.singleton("keep-key"))
            && table.size() == 1 && table.containsKey("keep-key"));

        table.clear();
        table.put("remove-value", first);
        table.put("keep-value", second);
        result.put("valueViewRemoveIf", values.removeIf(value -> value == first)
            && !table.containsKey("remove-value"));
        table.clear();
        table.put("keep-value", second);
        table.put("drop-value", first);
        result.put("valueViewRetainAll", values.retainAll(java.util.Collections.singleton(second))
            && table.size() == 1 && table.containsKey("keep-value"));

        table.clear();
        table.put("remove-entry", first);
        table.put("keep-entry", second);
        result.put("entryViewRemoveIf", entries.removeIf(entry -> entry.getKey().equals("remove-entry"))
            && !table.containsKey("remove-entry"));
        table.clear();
        table.put("keep-entry", second);
        table.put("drop-entry", first);
        result.put("entryViewRetainAll", entries.retainAll(java.util.Collections.singleton(
            new java.util.AbstractMap.SimpleImmutableEntry<>("keep-entry", second)))
            && table.size() == 1 && table.containsKey("keep-entry"));

        table.clear();
        table.put("remove-all-key", first);
        table.put("keep-all-key", second);
        result.put("keyViewRemoveAll", keys.removeAll(java.util.Collections.singleton("remove-all-key"))
            && !table.containsKey("remove-all-key"));
        table.clear();
        table.put("remove-all-value", first);
        table.put("keep-all-value", second);
        result.put("valueViewRemoveAll", values.removeAll(java.util.Collections.singleton(first))
            && !table.containsKey("remove-all-value"));
        table.clear();
        table.put("remove-all-entry", first);
        table.put("keep-all-entry", second);
        result.put("entryViewRemoveAll", entries.removeAll(java.util.Collections.singleton(
            new java.util.AbstractMap.SimpleImmutableEntry<>("remove-all-entry", first)))
            && !table.containsKey("remove-all-entry"));
        return result;
    }

    private static Map<String, Object> registryTables() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("registry-tables");
        config.setNamesrvAddr("");
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime =
            new org.apache.rocketmq.client.impl.factory.MQClientInstance(config, 0, config.buildMQClientId());
        Map<String, Object> result = new TreeMap<>();
        try {
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerA = tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class);
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerB = tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class);
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerA = tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class);
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerB = tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class);
            result.put("producer", tableMutations(runtime.getProducerTable(), producerA, producerB, runtime::selectProducer));
            result.put("consumer", tableMutations(runtime.getConsumerTable(), consumerA, consumerB, runtime::selectConsumer));
            result.put("producerDefaultOperations",
                tableDefaultOperations(runtime.getProducerTable(), producerA, producerB));
            result.put("consumerDefaultOperations",
                tableDefaultOperations(runtime.getConsumerTable(), consumerA, consumerB));
            result.put("producerLiveViews", tableLiveViews(runtime.getProducerTable(), producerA, producerB));
            result.put("consumerLiveViews", tableLiveViews(runtime.getConsumerTable(), consumerA, consumerB));
            Map<String, Object> equalValues = new TreeMap<>();
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerEqualA =
                tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class, "producer-equal");
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerEqualB =
                tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class, "producer-equal");
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerReplacement =
                tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class, "producer-replacement");
            org.apache.rocketmq.client.impl.producer.MQProducerInner producerOther =
                tableOwner(org.apache.rocketmq.client.impl.producer.MQProducerInner.class, "producer-other");
            runtime.getProducerTable().put("equal-replace", producerEqualA);
            equalValues.put("producerContainsEqualValue", runtime.getProducerTable().containsValue(producerEqualB));
            equalValues.put("producerReplaceByEqualValue",
                runtime.getProducerTable().replace("equal-replace", producerEqualB, producerReplacement));
            runtime.getProducerTable().put("equal-remove", producerEqualA);
            equalValues.put("producerRemoveByEqualValue",
                runtime.getProducerTable().remove("equal-remove", producerEqualB));
            runtime.getProducerTable().put("equal-values-remove", producerEqualA);
            equalValues.put("producerValuesRemoveByEqualValue",
                runtime.getProducerTable().values().remove(producerEqualB));
            runtime.getProducerTable().put("equal-entry-remove", producerEqualA);
            Map.Entry<String, org.apache.rocketmq.client.impl.producer.MQProducerInner> producerEntry =
                new java.util.AbstractMap.SimpleImmutableEntry<>("equal-entry-remove", producerEqualB);
            equalValues.put("producerEntryContainsEqualValue", runtime.getProducerTable().entrySet().contains(producerEntry));
            equalValues.put("producerEntryRemoveByEqualValue", runtime.getProducerTable().entrySet().remove(producerEntry));
            runtime.getProducerTable().put("equal-values-remove-all", producerEqualA);
            equalValues.put("producerValuesRemoveAllByEqualValue",
                runtime.getProducerTable().values().removeAll(java.util.Collections.singleton(producerEqualB)));
            runtime.getProducerTable().put("equal-values-retain-all", producerEqualA);
            runtime.getProducerTable().put("other-values-retain-all", producerOther);
            equalValues.put("producerValuesRetainAllByEqualValue",
                runtime.getProducerTable().values().retainAll(java.util.Collections.singleton(producerEqualB))
                    && runtime.getProducerTable().containsKey("equal-values-retain-all")
                    && !runtime.getProducerTable().containsKey("other-values-retain-all"));
            runtime.getProducerTable().put("equal-entry-retain-all", producerEqualA);
            runtime.getProducerTable().put("other-entry-retain-all", producerOther);
            equalValues.put("producerEntryRetainAllByEqualValue",
                runtime.getProducerTable().entrySet().retainAll(java.util.Collections.singleton(
                    new java.util.AbstractMap.SimpleImmutableEntry<>("equal-entry-retain-all", producerEqualB)))
                    && runtime.getProducerTable().containsKey("equal-entry-retain-all")
                    && !runtime.getProducerTable().containsKey("other-entry-retain-all"));
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerEqualA =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, "consumer-equal");
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerEqualB =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, "consumer-equal");
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerReplacement =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, "consumer-replacement");
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumerOther =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, "consumer-other");
            runtime.getConsumerTable().put("equal-replace", consumerEqualA);
            equalValues.put("consumerContainsEqualValue", runtime.getConsumerTable().containsValue(consumerEqualB));
            equalValues.put("consumerReplaceByEqualValue",
                runtime.getConsumerTable().replace("equal-replace", consumerEqualB, consumerReplacement));
            runtime.getConsumerTable().put("equal-remove", consumerEqualA);
            equalValues.put("consumerRemoveByEqualValue",
                runtime.getConsumerTable().remove("equal-remove", consumerEqualB));
            runtime.getConsumerTable().put("equal-values-remove", consumerEqualA);
            equalValues.put("consumerValuesRemoveByEqualValue",
                runtime.getConsumerTable().values().remove(consumerEqualB));
            runtime.getConsumerTable().put("equal-entry-remove", consumerEqualA);
            Map.Entry<String, org.apache.rocketmq.client.impl.consumer.MQConsumerInner> consumerEntry =
                new java.util.AbstractMap.SimpleImmutableEntry<>("equal-entry-remove", consumerEqualB);
            equalValues.put("consumerEntryContainsEqualValue", runtime.getConsumerTable().entrySet().contains(consumerEntry));
            equalValues.put("consumerEntryRemoveByEqualValue", runtime.getConsumerTable().entrySet().remove(consumerEntry));
            runtime.getConsumerTable().put("equal-values-remove-all", consumerEqualA);
            equalValues.put("consumerValuesRemoveAllByEqualValue",
                runtime.getConsumerTable().values().removeAll(java.util.Collections.singleton(consumerEqualB)));
            runtime.getConsumerTable().put("equal-values-retain-all", consumerEqualA);
            runtime.getConsumerTable().put("other-values-retain-all", consumerOther);
            equalValues.put("consumerValuesRetainAllByEqualValue",
                runtime.getConsumerTable().values().retainAll(java.util.Collections.singleton(consumerEqualB))
                    && runtime.getConsumerTable().containsKey("equal-values-retain-all")
                    && !runtime.getConsumerTable().containsKey("other-values-retain-all"));
            runtime.getConsumerTable().put("equal-entry-retain-all", consumerEqualA);
            runtime.getConsumerTable().put("other-entry-retain-all", consumerOther);
            equalValues.put("consumerEntryRetainAllByEqualValue",
                runtime.getConsumerTable().entrySet().retainAll(java.util.Collections.singleton(
                    new java.util.AbstractMap.SimpleImmutableEntry<>("equal-entry-retain-all", consumerEqualB)))
                    && runtime.getConsumerTable().containsKey("equal-entry-retain-all")
                    && !runtime.getConsumerTable().containsKey("other-entry-retain-all"));
            result.put("customEquality", equalValues);
            java.util.concurrent.ConcurrentMap<String, org.apache.rocketmq.client.impl.consumer.MQConsumerInner> retained = runtime.getConsumerTable();
            result.put("consumerRegisterAccepted", runtime.registerConsumer("registered", consumerA));
            result.put("consumerDirectPutReturnsRegistered", retained.put("registered", consumerB) == consumerA);
            result.put("consumerRegisterAfterDirectPutRejected", !runtime.registerConsumer("registered", consumerA));
            runtime.unregisterConsumer("registered"); // Empty Broker table: no outbound RPC.
            result.put("consumerUnregisterRemovesCurrentMapping", !retained.containsKey("registered"));
        } finally { cleanupRuntime(runtime); }
        return result;
    }

    private static List<Object> callbackThreadsConstruction() throws Exception {
        List<Object> cases = new ArrayList<>();
        for (int threads : new int[] {Integer.MIN_VALUE, -1, 0, 1, 4, Integer.MAX_VALUE}) {
            ClientConfig config = new ClientConfig();
            config.setClientIP("callback-threads-" + threads);
            config.setNamesrvAddr("");
            config.setClientCallbackExecutorThreads(threads);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", threads);
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime = null;
            try {
                runtime = new org.apache.rocketmq.client.impl.factory.MQClientInstance(config, 0, config.buildMQClientId());
                item.put("outcome", "OK");
                item.put("raw", runtime.getNettyClientConfig().getClientCallbackExecutorThreads());
                org.apache.rocketmq.remoting.netty.NettyRemotingClient remoting =
                    (org.apache.rocketmq.remoting.netty.NettyRemotingClient) runtime.getMQClientAPIImpl().getRemotingClient();
                java.lang.reflect.Field field = remoting.getClass().getDeclaredField("publicExecutor");
                field.setAccessible(true);
                java.util.concurrent.ThreadPoolExecutor executor = (java.util.concurrent.ThreadPoolExecutor) field.get(remoting);
                item.put("effective", executor.getCorePoolSize());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            } finally {
                if (runtime != null) cleanupRuntime(runtime);
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> socksConstruction() throws Exception {
        List<Object> cases = new ArrayList<>();
        String[] inputs = {null, "", " ", "\t\r\n", "\f", "\b", "\u000b", "\u00a0", "\u2003", "null", "{}", "invalid",
            "{'10.0.0.0/8':{'addr':'proxy:1080'}}",
            "{10.0.0.0/8:{addr:'proxy:1080'}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",}}",
            "{/* comment */\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"extra\":7}}",
            "{\"10.0.0.0/8\":{}}", "{\"10.0.0.0/8\":null}",
            "{\"10.0.0.0/8\":{\"addr\":123}}",
            "{\"10.0.0.0/8\":{\"addr\":true}}",
            "{\"10.0.0.0/8\":{\"addr\":null}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"username\":123}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"password\":false}}",
            "{\"10.0.0.0/8\":{\"addr\":[]}}"};
        for (int i = 0; i < inputs.length; i++) {
            ClientConfig config = new ClientConfig();
            config.setClientIP("socks-construction-" + i);
            config.setNamesrvAddr("");
            config.setSocksProxyConfig(inputs[i]);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", inputs[i]);
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime = null;
            try {
                runtime = new org.apache.rocketmq.client.impl.factory.MQClientInstance(config, 0, config.buildMQClientId());
                item.put("outcome", "OK");
                item.put("rawPreserved", java.util.Objects.equals(inputs[i], runtime.getNettyClientConfig().getSocksProxyConfig()));
                if (i >= 19) {
                    java.lang.reflect.Field remotingField = org.apache.rocketmq.client.impl.MQClientAPIImpl.class.getDeclaredField("remotingClient");
                    remotingField.setAccessible(true);
                    Object remoting = remotingField.get(runtime.getMQClientAPIImpl());
                    java.lang.reflect.Field proxyField = org.apache.rocketmq.remoting.netty.NettyRemotingClient.class.getDeclaredField("proxyMap");
                    proxyField.setAccessible(true);
                    @SuppressWarnings("unchecked")
                    Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> routes =
                        (Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>) proxyField.get(remoting);
                    org.apache.rocketmq.remoting.proxy.SocksProxyConfig route = routes.get("10.0.0.0/8");
                    item.put("parsedAddr", route == null ? null : route.getAddr());
                    item.put("parsedUsername", route == null ? null : route.getUsername());
                    item.put("parsedPassword", route == null ? null : route.getPassword());
                }
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            } finally {
                if (runtime != null) cleanupRuntime(runtime);
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> nullableSettings() throws Exception {
        ClientConfig input = new ClientConfig();
        input.setClientIP("nullable-settings");
        input.setNamesrvAddr("");
        input.setAccessChannel(null);
        input.setSocksProxyConfig(null);
        input.setLanguage(null);
        Map<String, Object> result = new TreeMap<>();
        result.put("inputAccessNull", input.getAccessChannel() == null);
        result.put("inputSocksNull", input.getSocksProxyConfig() == null);
        result.put("inputLanguageNull", input.getLanguage() == null);
        ClientConfig copy = input.cloneClientConfig();
        result.put("cloneAccess", copy.getAccessChannel().toString());
        result.put("cloneSocksNull", copy.getSocksProxyConfig() == null);
        result.put("cloneLanguageNull", copy.getLanguage() == null);
        ClientConfig receiver = new ClientConfig();
        receiver.setAccessChannel(org.apache.rocketmq.client.AccessChannel.CLOUD);
        receiver.resetClientConfig(input);
        result.put("resetAccess", receiver.getAccessChannel().toString());
        result.put("resetSocksNull", receiver.getSocksProxyConfig() == null);
        result.put("resetLanguageNull", receiver.getLanguage() == null);
        org.apache.rocketmq.client.impl.factory.MQClientInstance runtime =
            new org.apache.rocketmq.client.impl.factory.MQClientInstance(copy, 0, copy.buildMQClientId());
        try {
            result.put("runtimeConstructed", true);
            result.put("runtimeSocksNull", runtime.getNettyClientConfig().getSocksProxyConfig() == null);
            result.put("runtimeLanguageNull", runtime.getClientConfig().getLanguage() == null);
        } finally { cleanupRuntime(runtime); }
        for (org.apache.rocketmq.remoting.protocol.SerializeType type : org.apache.rocketmq.remoting.protocol.SerializeType.values()) {
            org.apache.rocketmq.remoting.protocol.RemotingCommand command =
                org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(34, null);
            command.setLanguage(null);
            command.setSerializeTypeCurrentRPC(type);
            Map<String, Object> outcome = new TreeMap<>();
            try {
                java.nio.ByteBuffer bytes = command.encode();
                outcome.put("outcome", "OK");
                bytes.getInt();
                int headerLength = bytes.getInt() & 0x00ffffff;
                byte[] header = new byte[headerLength]; bytes.get(header);
                if (type == org.apache.rocketmq.remoting.protocol.SerializeType.JSON) {
                    com.alibaba.fastjson2.JSONObject json = com.alibaba.fastjson2.JSON.parseObject(new String(header, java.nio.charset.StandardCharsets.UTF_8));
                    outcome.put("languageKeyPresent", json.containsKey("language"));
                    outcome.put("decodedLanguage", org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(header,
                        org.apache.rocketmq.remoting.protocol.RemotingCommand.class).getLanguage().toString());
                    byte[] explicitNull = "{\"code\":34,\"language\":null}"
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    outcome.put("explicitNullDecodedLanguageNull",
                        org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(explicitNull,
                            org.apache.rocketmq.remoting.protocol.RemotingCommand.class).getLanguage() == null);
                }
            } catch (RuntimeException error) { outcome.put("outcome", error.getClass().getSimpleName()); }
            result.put(type.name(), outcome);
        }
        return result;
    }
}
