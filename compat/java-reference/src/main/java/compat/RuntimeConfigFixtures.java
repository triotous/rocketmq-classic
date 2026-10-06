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
        fixture.put("entrypoint", "ClientConfig.buildMQClientId/cloneClientConfig/resetClientConfig/getNamesrvAddr/getNamespace/withNamespace/withoutNamespace/changeInstanceNameToPID; MQClientManager.getOrCreateMQClientInstance/removeClientFactory/getFactoryTable; MQClientInstance.constructor/getClientConfig/getNettyClientConfig; MQClientAPIImpl.getRemotingClient/NettyRemotingClient.publicExecutor (reflective capacity observation); RemotingCommand.encode (null language JSON/ROCKETMQ); Long.parseLong (RebalanceService property parser)");
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
        Map<String, Object> result = new TreeMap<>();
        try {
            result.put("retainedViewSeesSecond", live.get(id) == second);
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
            live.clear();
            result.put("retainedViewEmptyAfterClear", live.isEmpty());
        } finally {
            live.remove(id);
            live.remove("");
            try { cleanupRuntime(first); } finally { cleanupRuntime(second); }
        }
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
        String[] inputs = {null, "", " ", "\t\r\n", "\f", "\b", "\u000b", "\u00a0", "\u2003", "null", "{}", "invalid"};
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
                }
            } catch (RuntimeException error) { outcome.put("outcome", error.getClass().getSimpleName()); }
            result.put(type.name(), outcome);
        }
        return result;
    }
}
