package compat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.client.ClientConfig;

/** Executes pinned ClientConfig methods and local SOCKS transport paths without a Broker. */
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
            managed.put("configGetterSameObject", actual == runtime.getClientConfig());
            actual.setMqClientApiTimeout(4321);
            managed.put("configMutationVisible", runtime.getClientConfig().getMqClientApiTimeout() == 4321);
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
        fixture.put("entrypoint", "ClientConfig.buildMQClientId/toString/cloneClientConfig/resetClientConfig/getNamesrvAddr/getNamespace/withNamespace/withoutNamespace/queuesWithNamespace/changeInstanceNameToPID; MessageQueue.setTopic/setBrokerName/setQueueId; MQClientManager.getOrCreateMQClientInstance/removeClientFactory/getFactoryTable; MQClientInstance.constructor/getClientConfig/getNettyClientConfig/getProducerTable/getConsumerTable/selectProducer/selectConsumer/registerConsumer/unregisterConsumer; MQClientAPIImpl.getRemotingClient/NettyRemotingClient.publicExecutor (reflective capacity observation); NettyRemotingClient.start/invokeSync (local authenticated and no-auth SOCKS5 round trips); RemotingCommand.encode/getType (null language JSON/ROCKETMQ); LanguageCode/SerializeType/TlsMode/RequestType/RemotingCommandType/NettyEventType generated enum and custom value lookups; Long.parseLong (RebalanceService property parser)");
        fixture.put("serverBaselineContacted", false);
        fixture.put("networkUtilLocalAddress", org.apache.rocketmq.common.utils.NetworkUtil.getLocalAddress());
        List<String> localAddressCandidates = new ArrayList<>();
        for (java.net.InetAddress address : org.apache.rocketmq.common.utils.NetworkUtil.getLocalInetAddressList()) {
            localAddressCandidates.add(address.getHostAddress());
        }
        fixture.put("networkUtilLocalAddressCandidates", localAddressCandidates);
        fixture.put("clientConfigAutoClientIP", new ClientConfig().getClientIP());
        fixture.put("identities", identities);
        fixture.put("defaults", defaults);
        fixture.put("nameServerAddressCases", nameServerAddressCases());
        fixture.put("clone", clone);
        fixture.put("copyFields", clientConfigCopies());
        fixture.put("clientConfigToString", clientConfigToString());
        fixture.put("messageQueueSetters", messageQueueSetters());
        fixture.put("queuesWithNamespace", queuesWithNamespace());
        fixture.put("managed", managed);
        fixture.put("managerRemoval", managerRemoval(config));
        fixture.put("managerSharing", managerSharing());
        fixture.put("managerTable", managerTable(config));
        fixture.put("nullableSettings", nullableSettings());
        fixture.put("accessChannelMethodCases", accessChannelMethodCases());
        fixture.put("serviceNameCases", serviceNameCases());
        fixture.put("serviceThreadCases", serviceThreadCases());
        fixture.put("serviceLifecycleCases", serviceLifecycleCases());
        fixture.put("languageCodeMethodCases", languageCodeMethodCases());
        fixture.put("serializeTypeMethodCases", serializeTypeMethodCases());
        fixture.put("compressionTypeMethodCases", compressionTypeMethodCases());
        fixture.put("clientStatusEnumMethodCases", clientStatusEnumMethodCases());
        fixture.put("protocolEnumMethodCases", protocolEnumMethodCases());
        fixture.put("tlsModeMethodCases", tlsModeMethodCases());
        fixture.put("remotingEnumMethodCases", remotingEnumMethodCases());
        fixture.put("nettyEventTypeMethodCases", enumMethodCases(
            org.apache.rocketmq.remoting.netty.NettyEventType::values,
            org.apache.rocketmq.remoting.netty.NettyEventType::valueOf,
            new String[] {null, "", "CONNECT", "connect", "IDLE", "ACTIVE", "UNKNOWN"}));
        fixture.put("socksLeadingCodeUnits", socksLeadingCodeUnits());
        fixture.put("socksControlBoundaryCases", socksControlBoundaryCases());
        fixture.put("socksUnicodeTriviaCases", socksUnicodeTriviaCases());
        fixture.put("socksSubstituteEofObservations", socksSubstituteEofObservations());
        fixture.put("socksConstruction", socksConstruction());
        fixture.put("socksConnection", socksConnection(true));
        fixture.put("socksConnectionNoAuth", socksConnection(false));
        fixture.put("socksCidr", socksCidr());
        fixture.put("socksProxySelection", socksProxySelection());
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

    private static Map<String, Object> nameServerAddressCases() {
        Map<String, Object> cases = new TreeMap<>();
        cases.put("systemPropertyKey", MixAll.NAMESRV_ADDR_PROPERTY);
        cases.put("environmentKey", MixAll.NAMESRV_ADDR_ENV);
        String previous = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY);
        try {
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "system:9876");
            cases.put("systemPropertyValue", new ClientConfig().getNamesrvAddr());
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "");
            cases.put("explicitBlankPropertyValue", new ClientConfig().getNamesrvAddr());
        } finally {
            if (previous == null) {
                System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
            } else {
                System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, previous);
            }
        }
        return cases;
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

    private static Map<String, Object> managerSharing() throws Exception {
        org.apache.rocketmq.client.impl.MQClientManager manager = org.apache.rocketmq.client.impl.MQClientManager.getInstance();
        ClientConfig firstConfig = new ClientConfig();
        firstConfig.setClientIP("198.51.100.223");
        firstConfig.setInstanceName("phase6-manager-sharing");
        firstConfig.setNamesrvAddr("");
        org.apache.rocketmq.remoting.RPCHook firstHook = new org.apache.rocketmq.remoting.RPCHook() {
            @Override
            public void doBeforeRequest(String remoteAddr, org.apache.rocketmq.remoting.protocol.RemotingCommand request) { }
            @Override
            public void doAfterResponse(String remoteAddr, org.apache.rocketmq.remoting.protocol.RemotingCommand request,
                org.apache.rocketmq.remoting.protocol.RemotingCommand response) { }
        };
        org.apache.rocketmq.remoting.RPCHook laterHook = new org.apache.rocketmq.remoting.RPCHook() {
            @Override
            public void doBeforeRequest(String remoteAddr, org.apache.rocketmq.remoting.protocol.RemotingCommand request) { }
            @Override
            public void doAfterResponse(String remoteAddr, org.apache.rocketmq.remoting.protocol.RemotingCommand request,
                org.apache.rocketmq.remoting.protocol.RemotingCommand response) { }
        };
        org.apache.rocketmq.client.impl.factory.MQClientInstance first =
            manager.getOrCreateMQClientInstance(firstConfig, firstHook);
        ClientConfig laterConfig = firstConfig.cloneClientConfig();
        laterConfig.setNamesrvAddr("different-nameserver:9876");
        laterConfig.setNamespaceV2("different-namespace");
        laterConfig.setUseTLS(true);
        laterConfig.setMqClientApiTimeout(1234);
        org.apache.rocketmq.client.impl.factory.MQClientInstance later = null;
        Map<String, Object> result = new TreeMap<>();
        try {
            later = manager.getOrCreateMQClientInstance(laterConfig, laterHook);
            ClientConfig retained = first.getClientConfig();
            @SuppressWarnings("unchecked")
            List<org.apache.rocketmq.remoting.RPCHook> registeredHooks = (List<org.apache.rocketmq.remoting.RPCHook>)
                first.getMQClientAPIImpl().getRemotingClient().getClass().getMethod("getRPCHook")
                    .invoke(first.getMQClientAPIImpl().getRemotingClient());
            result.put("sameClientId", first.getClientId().equals(later.getClientId()));
            result.put("sameRuntimeIdentity", first == later);
            result.put("firstHookRetained", registeredHooks.contains(firstHook));
            result.put("laterHookIgnored", !registeredHooks.contains(laterHook));
            result.put("retainedNameserver", retained.getNamesrvAddr());
            result.put("retainedNamespaceV2", retained.getNamespaceV2());
            result.put("retainedTls", retained.isUseTLS());
            result.put("retainedTimeout", retained.getMqClientApiTimeout());
            result.put("laterConfigUnchanged", "different-nameserver:9876".equals(laterConfig.getNamesrvAddr())
                && "different-namespace".equals(laterConfig.getNamespaceV2()) && laterConfig.isUseTLS()
                && laterConfig.getMqClientApiTimeout() == 1234);
        } finally {
            try {
                cleanupRuntime(first);
            } finally {
                manager.removeClientFactory(first.getClientId(), first);
            }
        }
        return result;
    }

    private static Map<String, Object> clientConfigCopies() throws Exception {
        ClientConfig source = new ClientConfig();
        source.setNamesrvAddr("changed-nameserver");
        source.setClientIP(null);
        source.setInstanceName(null);
        source.setClientCallbackExecutorThreads(Integer.MIN_VALUE);
        source.setNamespace("changed-namespace");
        source.setNamespaceV2("changed-v2");
        source.setAccessChannel(org.apache.rocketmq.client.AccessChannel.CLOUD);
        source.setPollNameServerInterval(Integer.MIN_VALUE);
        source.setHeartbeatBrokerInterval(Integer.MAX_VALUE);
        source.setPersistConsumerOffsetInterval(-1);
        source.setPullTimeDelayMillsWhenException(Long.MIN_VALUE);
        source.setTraceMsgBatchNum(-10);
        source.setUnitMode(true);
        source.setUnitName("changed-unit");
        source.setDecodeReadBody(false);
        source.setDecodeDecompressBody(false);
        source.setVipChannelEnabled(true);
        source.setUseHeartbeatV2(true);
        source.setUseTLS(true);
        source.setSocksProxyConfig("changed-proxy");
        source.setMqClientApiTimeout(Integer.MIN_VALUE);
        source.setDetectTimeout(-200);
        source.setDetectInterval(Integer.MAX_VALUE);
        source.setLanguage(org.apache.rocketmq.remoting.protocol.LanguageCode.CPP);
        source.setEnableStreamRequestType(true);
        source.setSendLatencyEnable(true);
        source.setStartDetectorEnable(true);
        source.setEnableHeartbeatChannelEventListener(false);
        source.setEnableConcurrentHeartbeat(true);
        source.setConcurrentHeartbeatThreadPoolSize(-7);
        source.setEnableTrace(true);
        source.setTraceTopic("changed-trace");
        source.setMaxPageSizeInGetMetadata(-2000);

        ClientConfig copy = source.cloneClientConfig();
        ClientConfig reset = new ClientConfig();
        reset.setNamespace(null);
        reset.setAccessChannel(org.apache.rocketmq.client.AccessChannel.CLOUD);
        reset.setTraceMsgBatchNum(42);
        reset.setMaxPageSizeInGetMetadata(43);
        reset.resetClientConfig(source);

        Map<String, Object> result = new TreeMap<>();
        result.put("source", clientConfigFields(source));
        result.put("clone", clientConfigFields(copy));
        result.put("reset", clientConfigFields(reset));
        result.put("sourceGetters", clientConfigGetters(source));
        result.put("cloneGetters", clientConfigGetters(copy));
        result.put("resetGetters", clientConfigGetters(reset));
        return result;
    }

    private static Map<String, Object> clientConfigGetters(ClientConfig config) {
        Map<String, Object> fields = new TreeMap<>();
        fields.put("namesrvAddr", config.getNamesrvAddr());
        fields.put("clientIP", config.getClientIP());
        fields.put("instanceName", config.getInstanceName());
        fields.put("clientCallbackExecutorThreads", config.getClientCallbackExecutorThreads());
        fields.put("namespace", config.getNamespace());
        fields.put("namespaceV2", config.getNamespaceV2());
        fields.put("accessChannel", config.getAccessChannel());
        fields.put("pollNameServerInterval", config.getPollNameServerInterval());
        fields.put("heartbeatBrokerInterval", config.getHeartbeatBrokerInterval());
        fields.put("persistConsumerOffsetInterval", config.getPersistConsumerOffsetInterval());
        fields.put("pullTimeDelayMillsWhenException", config.getPullTimeDelayMillsWhenException());
        fields.put("traceMsgBatchNum", config.getTraceMsgBatchNum());
        fields.put("unitMode", config.isUnitMode());
        fields.put("unitName", config.getUnitName());
        fields.put("decodeReadBody", config.isDecodeReadBody());
        fields.put("decodeDecompressBody", config.isDecodeDecompressBody());
        fields.put("vipChannelEnabled", config.isVipChannelEnabled());
        fields.put("useHeartbeatV2", config.isUseHeartbeatV2());
        fields.put("useTLS", config.isUseTLS());
        fields.put("socksProxyConfig", config.getSocksProxyConfig());
        fields.put("mqClientApiTimeout", config.getMqClientApiTimeout());
        fields.put("detectTimeout", config.getDetectTimeout());
        fields.put("detectInterval", config.getDetectInterval());
        fields.put("language", config.getLanguage());
        fields.put("enableStreamRequestType", config.isEnableStreamRequestType());
        fields.put("sendLatencyEnable", config.isSendLatencyEnable());
        fields.put("startDetectorEnable", config.isStartDetectorEnable());
        fields.put("enableHeartbeatChannelEventListener", config.isEnableHeartbeatChannelEventListener());
        fields.put("enableConcurrentHeartbeat", config.isEnableConcurrentHeartbeat());
        fields.put("concurrentHeartbeatThreadPoolSize", config.getConcurrentHeartbeatThreadPoolSize());
        fields.put("enableTrace", config.isEnableTrace());
        fields.put("traceTopic", config.getTraceTopic());
        fields.put("maxPageSizeInGetMetadata", config.getMaxPageSizeInGetMetadata());
        return fields;
    }

    private static Map<String, Object> clientConfigFields(ClientConfig config) throws Exception {
        Map<String, Object> fields = new TreeMap<>();
        for (java.lang.reflect.Field field : ClientConfig.class.getDeclaredFields()) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())
                || "namespaceInitialized".equals(field.getName())) {
                continue;
            }
            field.setAccessible(true);
            Object value = field.get(config);
            fields.put(field.getName(), value instanceof Enum<?> ? value.toString() : value);
        }
        return fields;
    }

    private static List<Object> clientConfigToString() throws Exception {
        List<Object> cases = new ArrayList<>();
        ClientConfig nullable = new ClientConfig();
        nullable.setNamesrvAddr(null);
        nullable.setClientIP(null);
        nullable.setInstanceName(null);
        nullable.setClientCallbackExecutorThreads(-17);
        nullable.setNamespaceV2(null);
        nullable.setAccessChannel(null);
        nullable.setPollNameServerInterval(-1);
        nullable.setHeartbeatBrokerInterval(Integer.MIN_VALUE);
        nullable.setPersistConsumerOffsetInterval(Integer.MAX_VALUE);
        nullable.setPullTimeDelayMillsWhenException(Long.MIN_VALUE);
        nullable.setUnitMode(true);
        nullable.setUnitName("unit'quoted");
        nullable.setDecodeReadBody(false);
        nullable.setDecodeDecompressBody(true);
        nullable.setVipChannelEnabled(true);
        nullable.setUseHeartbeatV2(false);
        nullable.setUseTLS(true);
        nullable.setSocksProxyConfig("proxy-secret");
        nullable.setMqClientApiTimeout(-3);
        nullable.setDetectTimeout(4);
        nullable.setDetectInterval(-5);
        nullable.setLanguage(null);
        nullable.setEnableStreamRequestType(true);
        nullable.setSendLatencyEnable(false);
        nullable.setStartDetectorEnable(true);
        nullable.setEnableHeartbeatChannelEventListener(false);
        nullable.setEnableTrace(true);
        nullable.setTraceTopic("trace'quoted");
        nullable.setEnableConcurrentHeartbeat(true);
        nullable.setConcurrentHeartbeatThreadPoolSize(7);
        nullable.setTraceMsgBatchNum(81);
        nullable.setMaxPageSizeInGetMetadata(82);
        Map<String, Object> nullCase = new TreeMap<>();
        nullCase.put("case", "null-fields-uninitialized-namespace");
        nullCase.put("input", clientConfigToStringInput(nullable));
        nullCase.put("toString", nullable.toString());
        cases.add(nullCase);

        ClientConfig populated = new ClientConfig();
        populated.setNamesrvAddr("ns.example:9876");
        populated.setClientIP("198.51.100.7");
        populated.setInstanceName("instance");
        populated.setClientCallbackExecutorThreads(4);
        populated.setNamespace("tenant");
        populated.setNamespaceV2("tenant-v2");
        populated.setAccessChannel(org.apache.rocketmq.client.AccessChannel.CLOUD);
        populated.setPollNameServerInterval(30_000);
        populated.setHeartbeatBrokerInterval(30_001);
        populated.setPersistConsumerOffsetInterval(5_000);
        populated.setPullTimeDelayMillsWhenException(1_000L);
        populated.setUnitMode(false);
        populated.setUnitName("unit");
        populated.setDecodeReadBody(true);
        populated.setDecodeDecompressBody(false);
        populated.setVipChannelEnabled(false);
        populated.setUseHeartbeatV2(true);
        populated.setUseTLS(false);
        populated.setSocksProxyConfig("{\"user\":\"fixture-secret\"}");
        populated.setMqClientApiTimeout(3_000);
        populated.setDetectTimeout(200);
        populated.setDetectInterval(2_000);
        populated.setLanguage(org.apache.rocketmq.remoting.protocol.LanguageCode.CPP);
        populated.setEnableStreamRequestType(false);
        populated.setSendLatencyEnable(true);
        populated.setStartDetectorEnable(false);
        populated.setEnableHeartbeatChannelEventListener(true);
        populated.setEnableTrace(false);
        populated.setTraceTopic("trace-topic");
        populated.setEnableConcurrentHeartbeat(false);
        populated.setConcurrentHeartbeatThreadPoolSize(8);
        populated.setTraceMsgBatchNum(83);
        populated.setMaxPageSizeInGetMetadata(84);
        Map<String, Object> populatedCase = new TreeMap<>();
        populatedCase.put("case", "populated-fields-initialized-namespace");
        populatedCase.put("input", clientConfigToStringInput(populated));
        populatedCase.put("toString", populated.toString());
        cases.add(populatedCase);
        for (org.apache.rocketmq.remoting.protocol.LanguageCode language
            : org.apache.rocketmq.remoting.protocol.LanguageCode.values()) {
            ClientConfig languageCase = populated.cloneClientConfig();
            languageCase.setLanguage(language);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "language-" + language.name());
            item.put("input", clientConfigToStringInput(languageCase));
            item.put("toString", languageCase.toString());
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> clientConfigToStringInput(ClientConfig config) throws Exception {
        Map<String, Object> fields = clientConfigFields(config);
        java.lang.reflect.Field namespaceInitialized = ClientConfig.class.getDeclaredField("namespaceInitialized");
        namespaceInitialized.setAccessible(true);
        fields.put("namespaceInitialized", namespaceInitialized.getBoolean(config));
        return fields;
    }

    private static Map<String, Object> messageQueueSetters() {
        org.apache.rocketmq.common.message.MessageQueue queue =
            new org.apache.rocketmq.common.message.MessageQueue();
        Map<String, Object> result = new TreeMap<>();
        result.put("defaults", queueRow(queue));
        queue.setTopic("topic");
        queue.setBrokerName("broker");
        queue.setQueueId(-17);
        result.put("populated", queueRow(queue));
        queue.setTopic(null);
        queue.setBrokerName(null);
        queue.setQueueId(Integer.MIN_VALUE);
        result.put("nullable", queueRow(queue));
        return result;
    }

    private static List<Object> queuesWithNamespace() {
        List<Object> cases = new ArrayList<>();
        cases.add(queueNamespaceCase("null-collection-null-namespace", null, null));
        cases.add(queueNamespaceCase("null-collection-empty-namespace", "", null));
        cases.add(queueNamespaceCase("null-collection-populated-namespace", "tenant", null));

        List<org.apache.rocketmq.common.message.MessageQueue> emptyNamespace = new ArrayList<>();
        emptyNamespace.add(new org.apache.rocketmq.common.message.MessageQueue("topic", "broker", 1));
        emptyNamespace.add(null);
        cases.add(queueNamespaceCase("null-element-empty-namespace", "", emptyNamespace));

        List<org.apache.rocketmq.common.message.MessageQueue> partial = new ArrayList<>();
        partial.add(new org.apache.rocketmq.common.message.MessageQueue("topic", "broker", 1));
        partial.add(null);
        partial.add(new org.apache.rocketmq.common.message.MessageQueue("after", "broker", 2));
        cases.add(queueNamespaceCase("null-element-after-mutation", "tenant", partial));

        List<org.apache.rocketmq.common.message.MessageQueue> populated = new ArrayList<>();
        populated.add(new org.apache.rocketmq.common.message.MessageQueue("topic", "broker", 1));
        populated.add(new org.apache.rocketmq.common.message.MessageQueue("tenant%existing", "broker", 2));
        populated.add(new org.apache.rocketmq.common.message.MessageQueue(null, "broker", 3));
        cases.add(queueNamespaceCase("populated-collection", "tenant", populated));
        return cases;
    }

    private static Map<String, Object> queueNamespaceCase(
        String name, String namespace, Collection<org.apache.rocketmq.common.message.MessageQueue> queues) {
        ClientConfig config = new ClientConfig();
        config.setNamespace(namespace);
        Map<String, Object> item = new TreeMap<>();
        item.put("case", name);
        item.put("namespace", namespace);
        item.put("inputQueues", queueRows(queues));
        try {
            Collection<org.apache.rocketmq.common.message.MessageQueue> returned =
                config.queuesWithNamespace(queues);
            item.put("outcome", "OK");
            item.put("returnedSameCollection", returned == queues);
        } catch (Throwable error) {
            item.put("outcome", error.getClass().getSimpleName());
            item.put("returnedSameCollection", false);
        }
        item.put("outputQueues", queueRows(queues));
        return item;
    }

    private static List<Object> queueRows(
        Collection<org.apache.rocketmq.common.message.MessageQueue> queues) {
        if (queues == null) return null;
        List<Object> result = new ArrayList<>();
        for (org.apache.rocketmq.common.message.MessageQueue queue : queues) {
            result.add(queue == null ? null : queueRow(queue));
        }
        return result;
    }

    private static Map<String, Object> queueRow(org.apache.rocketmq.common.message.MessageQueue queue) {
        Map<String, Object> result = new TreeMap<>();
        result.put("topic", queue.getTopic());
        result.put("brokerName", queue.getBrokerName());
        result.put("queueId", queue.getQueueId());
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
            result.put("keyViewRemovesBulk", keyView.removeIf("bulk-b"::equals));
            result.put("entryViewRemovesBulk", entryView.removeIf(entry -> entry.getKey().equals("bulk-a")
                && entry.getValue() == first));
            result.put("valuesViewRemovesCurrent", valuesView.removeIf(value -> value == first));
            result.put("viewsSeeRemovals", !keyView.contains("bulk-b") && !entryView.contains(
                new java.util.AbstractMap.SimpleImmutableEntry<>("bulk-a", first)) && !keyView.contains(id)
                && valuesView.size() == 1);
            live.put("bulk-c", first);
            live.put("bulk-d", second);
            result.put("keyViewRemoveAll", keyView.removeAll(java.util.Collections.singleton("bulk-c")));
            result.put("valuesViewRemoveAll", valuesView.removeAll(java.util.Collections.singleton(second))
                && live.isEmpty());
            live.put("keep", first);
            live.put("drop", second);
            result.put("entryViewRetainAll", entryView.retainAll(java.util.Collections.singleton(
                new java.util.AbstractMap.SimpleImmutableEntry<>("keep", first)))
                && live.size() == 1 && live.get("keep") == first);
            result.put("computeIfAbsentMaps", live.computeIfAbsent("compute-a", key -> first) == first);
            result.put("computeIfAbsentKeepsCurrent", live.computeIfAbsent("compute-a", key -> second) == first);
            result.put("computeIfAbsentNullLeavesAbsent", live.computeIfAbsent("compute-null", key -> null) == null
                && !live.containsKey("compute-null"));
            result.put("computeIfPresentReplaces", live.computeIfPresent("compute-a", (key, value) -> second) == second
                && live.get("compute-a") == second);
            result.put("computeIfPresentNullRemoves", live.computeIfPresent("compute-a", (key, value) -> null) == null
                && !live.containsKey("compute-a"));
            result.put("computeReplaces", live.compute("compute-a", (key, value) -> first) == first
                && live.get("compute-a") == first);
            result.put("computeAbsentInserts", live.compute("compute-b", (key, value) -> second) == second);
            result.put("computeNullRemoves", live.compute("compute-b", (key, value) -> null) == null
                && !live.containsKey("compute-b"));
            result.put("computeAbsentNullLeavesAbsent", live.compute("compute-null-absent", (key, value) -> null) == null
                && !live.containsKey("compute-null-absent"));
            result.put("mergeCombines", live.merge("compute-a", second, (oldValue, newValue) -> newValue) == second);
            result.put("mergeAbsentInserts", live.merge("merge-new", first, (oldValue, newValue) -> null) == first);
            live.put("merge-remove", second);
            result.put("mergeNullRemoves", live.merge("merge-remove", first, (oldValue, newValue) -> null) == null
                && !live.containsKey("merge-remove"));
            live.replaceAll((key, value) -> second);
            result.put("replaceAllUpdatesMappings", live.size() == 3
                && live.values().stream().allMatch(value -> value == second));
            live.clear();
            result.put("retainedViewEmptyAfterClear", live.isEmpty());
            result.put("collectionViewsEmptyAfterClear", keyView.isEmpty() && valuesView.isEmpty() && entryView.isEmpty());
            live.put("iterator-key", first);
            java.util.Iterator<String> keyIterator = keyView.iterator();
            keyIterator.next();
            keyIterator.remove();
            result.put("keyIteratorRemove", live.isEmpty());
            live.put("iterator-value", first);
            java.util.Iterator<org.apache.rocketmq.client.impl.factory.MQClientInstance> valueIterator = valuesView.iterator();
            valueIterator.next();
            valueIterator.remove();
            result.put("valuesIteratorRemove", live.isEmpty());
            live.put("iterator-entry", first);
            java.util.Iterator<Map.Entry<String, org.apache.rocketmq.client.impl.factory.MQClientInstance>> entryIterator =
                entryView.iterator();
            entryIterator.next();
            entryIterator.remove();
            result.put("entryIteratorRemove", live.isEmpty());
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
        private static Object equalityValue(Object key) {
            if (key instanceof java.util.concurrent.atomic.AtomicReference<?>) {
                return ((java.util.concurrent.atomic.AtomicReference<?>) key).get();
            }
            return key;
        }
        @Override public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
            if ("equals".equals(method.getName())) {
                Object other = args[0];
                if (other == proxy) return true;
                if (other == null || !java.lang.reflect.Proxy.isProxyClass(other.getClass())) return false;
                java.lang.reflect.InvocationHandler otherHandler = java.lang.reflect.Proxy.getInvocationHandler(other);
                return otherHandler instanceof TableOwnerHandler
                    && java.util.Objects.equals(equalityValue(equalityKey),
                        equalityValue(((TableOwnerHandler) otherHandler).equalityKey));
            }
            if ("hashCode".equals(method.getName())) return equalityValue(equalityKey).hashCode();
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
            java.util.concurrent.atomic.AtomicReference<Object> storedKey =
                new java.util.concurrent.atomic.AtomicReference<>("dynamic-before");
            java.util.concurrent.atomic.AtomicReference<Object> probeKey =
                new java.util.concurrent.atomic.AtomicReference<>("dynamic-before");
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner dynamicStored =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, storedKey);
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner dynamicProbe =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, probeKey);
            runtime.getConsumerTable().put("dynamic-equality", dynamicStored);
            equalValues.put("dynamicContainsBeforeMutation",
                runtime.getConsumerTable().containsValue(dynamicProbe));
            storedKey.set("dynamic-after");
            equalValues.put("dynamicContainsAfterStoredMutation",
                !runtime.getConsumerTable().containsValue(dynamicProbe));
            probeKey.set("dynamic-after");
            equalValues.put("dynamicContainsAfterBothMutations",
                runtime.getConsumerTable().containsValue(dynamicProbe));
            java.util.concurrent.atomic.AtomicReference<Object> replaceStoredKey =
                new java.util.concurrent.atomic.AtomicReference<>("replace-before");
            java.util.concurrent.atomic.AtomicReference<Object> replaceExpectedKey =
                new java.util.concurrent.atomic.AtomicReference<>("replace-before");
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner replaceStored =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, replaceStoredKey);
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner replaceExpected =
                tableOwner(org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class, replaceExpectedKey);
            runtime.getConsumerTable().put("dynamic-replace", replaceStored);
            replaceExpectedKey.set("stale");
            equalValues.put("dynamicReplaceRejectsChangedExpected",
                !runtime.getConsumerTable().replace("dynamic-replace", replaceExpected, consumerReplacement));
            replaceStoredKey.set("replace-after");
            replaceExpectedKey.set("replace-after");
            equalValues.put("dynamicReplaceMatchesCurrentEquality",
                runtime.getConsumerTable().replace("dynamic-replace", replaceExpected, consumerReplacement));
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

    /** Direct Fastjson 2.0.63 source-path probe for every possible leading UTF-16 unit. */
    private static Map<String, Object> socksLeadingCodeUnits() {
        Map<String, Object> cases = new TreeMap<>();
        String route = "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}";
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        for (String[] probe : new String[][] {{"route", route}, {"null", "null"}}) {
            List<Object> accepted = new ArrayList<>();
            for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
                String input = (char) codeUnit + probe[1];
                try {
                    com.alibaba.fastjson2.JSON.parseObject(input, type);
                    accepted.add(codeUnit);
                } catch (RuntimeException rejected) {
                    // The compact golden stores successful units; native checks the whole BMP.
                }
            }
            cases.put(probe[0], accepted);
        }
        return cases;
    }

    /** Direct Fastjson whitespace scan for C0 units at each JSON token boundary. */
    private static Map<String, Object> socksControlBoundaryCases() {
        Map<String, Object> result = new TreeMap<>();
        String[][] boundaries = {
            {"root_after_open", "{", "\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}"},
            {"root_before_colon", "{\"10.0.0.0/8\"", ":{\"addr\":\"proxy:1080\"}}"},
            {"root_after_colon", "{\"10.0.0.0/8\":", "{\"addr\":\"proxy:1080\"}}"},
            {"proxy_after_open", "{\"10.0.0.0/8\":{", "\"addr\":\"proxy:1080\"}}"},
            {"proxy_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"", "}}"},
            {"root_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}", "}"}
        };
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        for (String[] boundary : boundaries) {
            List<Object> accepted = new ArrayList<>();
            for (int codeUnit = 0; codeUnit <= 32; codeUnit++) {
                String input = boundary[1] + (char) codeUnit + boundary[2];
                try {
                    com.alibaba.fastjson2.JSON.parseObject(input, type);
                    accepted.add(codeUnit);
                } catch (RuntimeException rejected) {
                    // Keep only accepted C0 units in the compact source oracle.
                }
            }
            result.put(boundary[0], accepted);
        }
        return result;
    }

    /** Direct Java trivia candidates across Unicode whitespace and control categories. */
    private static Map<String, Object> socksUnicodeTriviaCases() {
        Map<String, Object> result = new TreeMap<>();
        String[][] boundaries = {
            {"root_after_open", "{", "\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}"},
            {"root_before_colon", "{\"10.0.0.0/8\"", ":{\"addr\":\"proxy:1080\"}}"},
            {"root_after_colon", "{\"10.0.0.0/8\":", "{\"addr\":\"proxy:1080\"}}"},
            {"proxy_after_open", "{\"10.0.0.0/8\":{", "\"addr\":\"proxy:1080\"}}"},
            {"proxy_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"", "}}"},
            {"root_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}", "}"}
        };
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        for (String[] boundary : boundaries) {
            List<Object> cases = new ArrayList<>();
            for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
                char character = (char) codeUnit;
                if (!Character.isWhitespace(character) && !Character.isSpaceChar(character)
                    && !Character.isISOControl(character)) {
                    continue;
                }
                String input = boundary[1] + character + boundary[2];
                Map<String, Object> item = new TreeMap<>();
                item.put("codeUnit", codeUnit);
                try {
                    com.alibaba.fastjson2.JSON.parseObject(input, type);
                    item.put("accepted", true);
                } catch (RuntimeException rejected) {
                    item.put("accepted", false);
                }
                cases.add(item);
            }
            result.put(boundary[0], cases);
        }
        return result;
    }

    private static Map<String, Object> socksSubstituteEofObservations() {
        Map<String, Object> result = new TreeMap<>();
        String[][] boundaries = {
            {"root_after_open", "{", "\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}"},
            {"root_before_close", "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}", "}"}
        };
        com.alibaba.fastjson2.TypeReference<Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>> type =
            new com.alibaba.fastjson2.TypeReference<>() { };
        for (String[] boundary : boundaries) {
            Map<String, Object> item = new TreeMap<>();
            String input = boundary[1] + (char) 0x1a + boundary[2];
            try {
                Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> parsed =
                    com.alibaba.fastjson2.JSON.parseObject(input, type);
                item.put("outcome", "OK");
                if (parsed == null) {
                    item.put("keys", null);
                } else {
                    List<String> keys = new ArrayList<>(parsed.keySet());
                    keys.sort(String::compareTo);
                    item.put("keys", keys);
                    org.apache.rocketmq.remoting.proxy.SocksProxyConfig route = parsed.get("10.0.0.0/8");
                    item.put("addr", route == null ? null : route.getAddr());
                }
            } catch (RuntimeException rejected) {
                item.put("outcome", rejected.getClass().getSimpleName());
            }
            result.put(boundary[0], item);
        }
        return result;
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
            "{\"10.0.0.0/8\":{\"addr\":[]}}",
            "{\"10.0.0.0/8\":{addr:'proxy:1080',username:user,password:pass}}",
            "{\"10.0.0.0/8\":{\"addr\":0x2a}}",
            "{\"10.0.0.0/8\":{\"addr\":undefined}}",
            "{\"10.0.0.0/8\":{\"addr\":NaN}}",
            "{\"10.0.0.0/8\":{\"addr\":new Date(0)}}",
            "{\"10.0.0.0/8\":{\"addr\":\"first\",\"addr\":\"last\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"} // route comment\n}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\x3a1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":Infinity}}",
            "{\"10.0.0.0/8\":{\"addr\":-Infinity}}",
            "{\"10.0.0.0/8\":{addr:\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{'addr':\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":+123}}",
            "{\"10.0.0.0/8\":{\"addr\":0123}}",
            "{\"10.0.0.0/8\":{\"addr\":0b10}}",
            "{\"10.0.0.0/8\":{\"addr\":+0123}}",
            "{\"10.0.0.0/8\":{\"addr\":-0123}}",
            "{\"10.0.0.0/8\":{\"addr\":0123.5}}",
            "{\"10.0.0.0/8\":{\"addr\":0o10}}",
            "{\"10.0.0.0/8\":{\"addr\":+0x10}}",
            "{\"10.0.0.0/8\":{\"addr\":[1,true,null,\"x\"]}}",
            "{\"10.0.0.0/8\":{\"addr\":{\"host\":\"proxy\",\"port\":1080}}}",
            "{\"10.0.0.0/8\":{\"addr\":{\"port\":1080,\"host\":\"proxy\"}}}",
            "{\"10.0.0.0/8\":{\"addr\":{\"quote\":\"a\\\"b\",\"newline\":\"a\\nb\",\"unicode\":\"λ\",\"slash\":\"/\"}}}",
            "{\"10.0.0.0/8\":{\"addr\":-.5}}",
            "{\"10.0.0.0/8\":{\"addr\":1e3}}",
            "{\"10.0.0.0/8\":{\"addr\":1E+3}}",
            "{\"10.0.0.0/8\":{\"addr\":1e-3}}",
            "{\"10.0.0.0/8\":{\"addr\":-0.0}}",
            "{\"10.0.0.0/8\":{\"addr\":1.234567890123456789}}",
            "{\"10.0.0.0/8\":{\"addr\":1e309}}",
            "{\"10.0.0.0/8\":{\"addr\":1e-1000}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\u003a1080\"}}",
            "{\"10.0.0.0/8\":{\"\\u0061ddr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\/1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "x" + "\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "x1\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "xG0\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "x0G\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "xG1\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "u123\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "u12G4\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "u1G34\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "uG234\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy" + ((char) 92) + "'single\"}}",
            "{\"10.0.0.0/8\":{'addr':'proxy" + ((char) 92) + "'single'}}",
            "{\"10.0.0.0/8\":{'addr':'proxy" + ((char) 92) + "\"double'}}",
            "{\"10.0.0.0/8\":{\"addr\":\"line" + ((char) 92) + "\nnext\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"line" + ((char) 92) + "\r\nnext\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"trailing" + ((char) 92),
            "{\"10.0.0.0/8\":{\"addr\":\"\\uD83D\\uDE80\"}}",
            "{\"10.0.0.0/8\":{\"addr\":1e-4}}",
            "{\"10.0.0.0/8\":{\"addr\":1e-7}}",
            "{\"10.0.0.0/8\":{\"addr\":1e6}}",
            "{\"10.0.0.0/8\":{\"addr\":1e7}}",
            "{\"10.0.0.0/8\":{\"addr\":1.25E+4}}",
            "{\"10.0.0.0/8\":{\"addr\":-0e2}}",
            "{\"10.0.0.0/8\":{\"addr\":1.2300}}",
            "{\"10.0.0.0/8\":{\"addr\":-.5e-3}}",
            "{\"10.0.0.0/8\":{\"addr\":1.e3}}",
            "{\"10.0.0.0/8\":{\"addr\":+.5e3}}",
            "{\"10.0.0.0/8\":{\"addr\":-1e309}}",
            "{\"10.0.0.0/8\":{\"addr\":.5e-3}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\v:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\0:1080\"}}",
            "\uFEFF{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\f\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\u2028\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "// leading comment\n{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":/* value comment */{\"addr\":\"proxy:1080\"}}",
            "{\u000b\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy\\q:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"raw\u0001control\"}}",
            "{} trailing",
            "{}/* trailing comment */",
            "{\"10.0.0.0/8\":{\"addr\":\"line\u2028separator\"}}",
            "/* \"\u2028 */{\"10.0.0.0/8\":{\"addr\":\"inside\u2028string\"}}",
            "{\"10.0.0.0/8\":// value comment\n{\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"http://proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy/*literal*/:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}// trailing line comment\n}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}/* between route and root close */}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",/* field separator */\"username\":\"u\"}}",
            "{// comment with digits 123\r\"10.0.0.0/8\":{\"addr\":1e-4}}",
            "{// comment with digits 123\u2028\"10.0.0.0/8\":{\"addr\":1e-4}}",
            "{// comment with digits 123\u2029\"10.0.0.0/8\":{\"addr\":1e-4}}",
            "{\"10.0.0.0/8\":{\"username\":\"u\"// field comment\r,\"addr\":1e3}}",
            "{\"10.0.0.0/8\":{\"addr\":1D}}",
            "{\"10.0.0.0/8\":{\"addr\":1.25F}}",
            "{\"10.0.0.0/8\":{\"addr\":123L}}",
            "{\"10.0.0.0/8\":{\"addr\":1e7F}}",
            "{\"10.0.0.0/8\":{\"addr\":1e-4F}}",
            "{\"10.0.0.0/8\":{\"addr\":-0.0F}}",
            "{\"10.0.0.0/8\":{\"addr\":1e7D}}",
            "{\"10.0.0.0/8\":{\"addr\":-0.0D}}",
            "{\"10.0.0.0/8\":{\"addr\":-123L}}",
            "{\"10.0.0.0/8\":{\"addr\":9223372036854775807L}}",
            "{\"10.0.0.0/8\":{\"addr\":-9223372036854775808L}}",
            "{\"10.0.0.0/8\":{\"addr\":9223372036854775808L}}",
            "{\"10.0.0.0/8\":{\"addr\":\"" + ((char) 92) + "uD800\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"" + ((char) 92) + "uDC00\"}}"};
        List<String> allInputs = new ArrayList<>(java.util.Arrays.asList(inputs));
        for (int escaped = 0; escaped <= 0x7f; escaped++) {
            String input = "{\"10.0.0.0/8\":{\"addr\":\"A" + ((char) 92) + ((char) escaped) + "Z\"}}";
            allInputs.add(input);
        }
        for (int escaped = 0x80; escaped <= 0xff; escaped++) {
            String input = "{\"10.0.0.0/8\":{\"addr\":\"A" + ((char) 92) + ((char) escaped) + "Z\"}}";
            allInputs.add(input);
        }
        String slash = String.valueOf((char) 92);
        String[] unicodeEscapeInputs = {
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u1" + ((char) 0x03bb) + "34Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12" + ((char) 0x03bb) + "4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12" + ((char) 0x0661) + "4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12" + ((char) 0xff11) + "4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u1" + new String(Character.toChars(0x1d7da)) + "34Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "x" + ((char) 0x03bb) + "0Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "x0" + ((char) 0x0661) + "Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12\t4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12\n4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + new String(Character.toChars(0x1d7da)) + "Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u" + ((char) 0x03bb) + "234Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u123" + ((char) 0x03bb) + "Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u12" + new String(Character.toChars(0x1d7da)) + "4Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "u123" + new String(Character.toChars(0x1d7da)) + "Z\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"A" + slash + "uD83" + new String(Character.toChars(0x1d7da)) + "Z\"}}"
        };
        allInputs.addAll(java.util.Arrays.asList(unicodeEscapeInputs));
        allInputs.add("\u00a0{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{\u2003\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{\"10.0.0.0/8\":\u3000{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}\u2029}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}\uFEFF");
        allInputs.add("[]");
        allInputs.add("\"text\"");
        allInputs.add("true");
        allInputs.add("42");
        allInputs.add("[{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}]");
        allInputs.add("{\"10.0.0.0/8\":[]}");
        allInputs.add("{\"10.0.0.0/8\":\"proxy:1080\"}");
        allInputs.add("{\"10.0.0.0/8\":7}");
        allInputs.add("{\"10.0.0.0/8\":true}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"first\"},\"10.0.0.0/8\":{\"addr\":\"last\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":proxy}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":proxy:1080}}");
        allInputs.add("{\"10.0.0.0/8\":{addr:\"proxy:1080\",},}");
        allInputs.add("{'10.0.0.0/8':{'addr':'proxy:1080',},}");
        allInputs.add("/* lead */{\"10.0.0.0/8\":/* route */{\"addr\":\"proxy:1080\"}/* inner */}/* tail */");
        allInputs.add("// lead\r{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"username\":\"u\"// field\r,\"addr\":\"proxy:1080\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":[\"first\",\"last\",]}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",,}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}} // eof comment");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"first\",\"addr\":\"last\",\"username\":\"u1\",\"username\":\"u2\",\"password\":\"p1\",\"password\":\"p2\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"username\":\"first\",\"username\":null,\"password\":false}}");
        allInputs.add("{\"10.0.0.0/8\":{\"ADDR\":\"upper-proxy:1080\",\"USERNAME\":\"upper-user\",\"PASSWORD\":\"upper-password\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"userName\":\"camel-user\",\"password\":\"pw\"}}");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"} // eof with root open");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\" // eof with route open");
        allInputs.add("{\"10.0.0.0/8\":{\"addr\":[\"first\",\"last\" // eof with array open");
        allInputs.add("{0x2a:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{0b10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{0o10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{default:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{true:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{false:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{null:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1.5:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e2:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{+10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{0123:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{123L:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1.25F:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1D:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e7F:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1:{\"addr\":\"first\"},01:{\"addr\":\"last\"}}");
        allInputs.add("{01:{\"addr\":\"first\"},1:{\"addr\":\"last\"}}");
        allInputs.add("{1:{\"addr\":\"first\"},01:{\"addr\":\"middle\"},1:{\"addr\":\"last\"}}");
        allInputs.add("{1:{\"addr\":\"first\"},01:{\"addr\":\"middle\"},01:{\"addr\":\"last\"}}");
        allInputs.add("{1:{\"addr\":\"first\"},\"1\":{\"addr\":\"last\"}}");
        allInputs.add("{\"1\":{\"addr\":\"first\"},1:{\"addr\":\"last\"}}");
        allInputs.add("{true:{\"addr\":\"first\"},\"true\":{\"addr\":\"last\"}}");
        allInputs.add("{\"true\":{\"addr\":\"first\"},true:{\"addr\":\"last\"}}");
        allInputs.add("{null:{\"addr\":\"null-key\"},'null':{\"addr\":\"string-key\"}}");
        allInputs.add("{-0x2a:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{+0b10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-0o10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{.5F:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-10:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-0:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-0.0:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-.5:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{+.5:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{.5:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e+2:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e-2:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1E7:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e-4F:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-10L:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{+10L:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1.0D:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e+7D:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{NaN:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{Infinity:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-Infinity:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{undefined:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{TRUE:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{False:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{NULL:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{Null:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{falsehood:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e309:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-1e309:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e-1000:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-1e-1000:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{1e-1000:{\"addr\":\"first\"},0.0:{\"addr\":\"last\"}}");
        allInputs.add("{0.0:{\"addr\":\"first\"},1e-1000:{\"addr\":\"last\"}}");
        allInputs.add("{1e309:{\"addr\":\"first\"},'Infinity':{\"addr\":\"last\"}}");
        allInputs.add("{-0e-1000:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-1e-1000D:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-1e-1000F:{\"addr\":\"proxy:1080\"}}");
        allInputs.add("{-1e-1000D:{\"addr\":\"first\"},0.0:{\"addr\":\"last\"}}");
        for (int i = 0; i < allInputs.size(); i++) {
            String input = allInputs.get(i);
            ClientConfig config = new ClientConfig();
            config.setClientIP("socks-construction-" + i);
            config.setNamesrvAddr("");
            config.setSocksProxyConfig(input);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", input);
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime = null;
            try {
                runtime = new org.apache.rocketmq.client.impl.factory.MQClientInstance(config, 0, config.buildMQClientId());
                item.put("outcome", "OK");
                item.put("rawPreserved", java.util.Objects.equals(input, runtime.getNettyClientConfig().getSocksProxyConfig()));
                if (i >= 19 && "OK".equals(item.get("outcome"))) {
                    java.lang.reflect.Field remotingField = org.apache.rocketmq.client.impl.MQClientAPIImpl.class.getDeclaredField("remotingClient");
                    remotingField.setAccessible(true);
                    Object remoting = remotingField.get(runtime.getMQClientAPIImpl());
                    java.lang.reflect.Field proxyField = org.apache.rocketmq.remoting.netty.NettyRemotingClient.class.getDeclaredField("proxyMap");
                    proxyField.setAccessible(true);
                    @SuppressWarnings("unchecked")
                    Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> routes =
                        (Map<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>) proxyField.get(remoting);
                    if (i >= allInputs.size() - 60) {
                        List<String> routeKeys = new ArrayList<>(routes.keySet());
                        routeKeys.sort(java.util.Comparator.nullsFirst(java.util.Comparator.naturalOrder()));
                        item.put("routeKeys", routeKeys);
                    }
                    if (routes.containsKey("1")) {
                        item.put("routeKeyOneAddr", routes.get("1").getAddr());
                    }
                    if (routes.containsKey("true")) {
                        item.put("routeKeyTrueAddr", routes.get("true").getAddr());
                    }
                    if (routes.containsKey(null)) {
                        item.put("routeKeyNullAddr", routes.get(null).getAddr());
                    }
                    if (routes.containsKey("null")) {
                        item.put("routeKeyStringNullAddr", routes.get("null").getAddr());
                    }
                    org.apache.rocketmq.remoting.proxy.SocksProxyConfig route = routes.get("10.0.0.0/8");
                    item.put("routePresent", route != null);
                    String parsedAddr = route == null ? null : route.getAddr();
                    if (hasUnpairedSurrogate(parsedAddr)) {
                        List<Integer> codeUnits = new ArrayList<>();
                        for (char codeUnit : parsedAddr.toCharArray()) codeUnits.add((int) codeUnit);
                        item.put("parsedAddrUtf16", codeUnits);
                    } else {
                        item.put("parsedAddr", parsedAddr);
                    }
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

    private static boolean hasUnpairedSurrogate(String value) {
        if (value == null) return false;
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (Character.isHighSurrogate(current)) {
                if (i + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(i + 1))) return true;
                i++;
            } else if (Character.isLowSurrogate(current)) {
                return true;
            }
        }
        return false;
    }

    private static List<Object> socksCidr() {
        List<Object> cases = new ArrayList<>();
        String[][] inputs = {
            {"10.20.4.5", "10.20.0.0/16"}, {"10.21.4.5", "10.20.0.0/16"},
            {"10.20.4.5", "10.20.0.0/0"}, {"10.20.4.5", "10.20.0.0/33"},
            {"10.20.4.5", "10.20.0.0/-1"}, {"10.20.4.5", "266.20.0.0/16"},
            {"10.20.4.5", "10.20.0.0/16/ignored"}, {"10.20.4.5.99", "10.20.0.0/16"},
            {"10.20.4.5.", "10.20.0.0/16"}, {"10.20.4.", "10.20.0.0/16"},
            {"10.20.4.5", "10.20.0.0/not-a-prefix"}, {"10.20.4.5", "10.20.0.0"}
        };
        for (String[] input : inputs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("address", input[0]);
            item.put("cidr", input[1]);
            try {
                item.put("outcome", "OK");
                item.put("match", org.apache.rocketmq.remoting.common.RemotingHelper.ipInCIDR(input[0], input[1]));
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> socksConnection(boolean usePasswordAuth) throws Exception {
        final String username = "oracle-user";
        final String password = "oracle-pass";
        Map<String, Object> result = new TreeMap<>();
        try (java.net.ServerSocket brokerListener =
                 new java.net.ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"));
             java.net.ServerSocket proxyListener =
                 new java.net.ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))) {
            brokerListener.setSoTimeout(8000);
            proxyListener.setSoTimeout(8000);
            java.util.concurrent.ExecutorService workers =
                java.util.concurrent.Executors.newCachedThreadPool();
            try {
            final int brokerPort = brokerListener.getLocalPort();
            final int proxyPort = proxyListener.getLocalPort();
            java.util.concurrent.Future<Map<String, Object>> brokerOutcome = workers.submit(() -> {
                try (java.net.Socket broker = brokerListener.accept()) {
                    broker.setSoTimeout(8000);
                    byte[] frame = readFrame(broker.getInputStream());
                    org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                        org.apache.rocketmq.remoting.protocol.RemotingCommand.decode(
                            java.util.Arrays.copyOfRange(frame, 4, frame.length));
                    if (request.getCode() != 1234 || !java.util.Arrays.equals(request.getBody(),
                        "socks-oracle-request".getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
                        throw new IllegalStateException("unexpected Java SOCKS remoting request");
                    }
                    org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                        org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(0, "socks-oracle-ok");
                    response.setOpaque(request.getOpaque());
                    response.setSerializeTypeCurrentRPC(request.getSerializeTypeCurrentRPC());
                    java.nio.ByteBuffer encoded = response.encode();
                    byte[] bytes = new byte[encoded.remaining()];
                    encoded.get(bytes);
                    broker.getOutputStream().write(bytes);
                    broker.getOutputStream().flush();
                    Map<String, Object> observed = new TreeMap<>();
                    observed.put("requestCode", request.getCode());
                    observed.put("requestBodyBytes", request.getBody() == null ? 0 : request.getBody().length);
                    observed.put("opaqueMatched", request.getOpaque() == response.getOpaque());
                    return observed;
                }
            });
            java.util.concurrent.Future<Map<String, Object>> proxyOutcome = workers.submit(() -> {
                try (java.net.Socket client = proxyListener.accept()) {
                    client.setSoTimeout(8000);
                    java.io.InputStream clientInput = client.getInputStream();
                    java.io.OutputStream clientOutput = client.getOutputStream();
                    int version = clientInput.read();
                    int methodCount = clientInput.read();
                    byte[] methods = readExactly(clientInput, methodCount);
                    boolean offeredPasswordAuth = false;
                    boolean offeredNoAuth = false;
                    for (byte method : methods) {
                        offeredPasswordAuth |= method == 2;
                        offeredNoAuth |= method == 0;
                    }
                    if (version != 5 || (usePasswordAuth && !offeredPasswordAuth)
                        || (!usePasswordAuth && !offeredNoAuth)) {
                        throw new java.io.IOException("required SOCKS5 authentication method was not offered");
                    }
                    int authVersion = -1;
                    String receivedUsername = null;
                    String receivedPassword = null;
                    if (usePasswordAuth) {
                        clientOutput.write(new byte[] {5, 2});
                        clientOutput.flush();
                        authVersion = clientInput.read();
                        int usernameLength = clientInput.read();
                        receivedUsername = new String(readExactly(clientInput, usernameLength),
                            java.nio.charset.StandardCharsets.UTF_8);
                        int passwordLength = clientInput.read();
                        receivedPassword = new String(readExactly(clientInput, passwordLength),
                            java.nio.charset.StandardCharsets.UTF_8);
                        if (authVersion != 1 || !username.equals(receivedUsername)
                            || !password.equals(receivedPassword)) {
                            throw new java.io.IOException("SOCKS5 username/password did not match");
                        }
                        clientOutput.write(new byte[] {1, 0});
                    } else {
                        clientOutput.write(new byte[] {5, 0});
                    }
                    clientOutput.flush();

                    byte[] requestPrefix = readExactly(clientInput, 4);
                    int addressType = requestPrefix[3] & 0xff;
                    String targetHost;
                    if (addressType == 1) {
                        targetHost = java.net.InetAddress.getByAddress(readExactly(clientInput, 4)).getHostAddress();
                    } else if (addressType == 3) {
                        targetHost = new String(readExactly(clientInput, clientInput.read()),
                            java.nio.charset.StandardCharsets.UTF_8);
                    } else if (addressType == 4) {
                        targetHost = java.net.InetAddress.getByAddress(readExactly(clientInput, 16)).getHostAddress();
                    } else {
                        throw new java.io.IOException("unsupported SOCKS5 address type " + addressType);
                    }
                    int targetPort = (clientInput.read() << 8) | clientInput.read();
                    if ((requestPrefix[0] & 0xff) != 5 || (requestPrefix[1] & 0xff) != 1) {
                        throw new java.io.IOException("unexpected SOCKS5 connect request");
                    }
                    if (!"127.0.0.1".equals(targetHost) || targetPort != brokerPort) {
                        throw new java.io.IOException("SOCKS5 connect target did not match local Broker");
                    }

                    try (java.net.Socket upstream = new java.net.Socket(
                        "127.0.0.1", brokerPort)) {
                        upstream.setSoTimeout(8000);
                        clientOutput.write(new byte[] {5, 0, 0, 1, 127, 0, 0, 1, 0, 0});
                        clientOutput.flush();
                        java.util.concurrent.Future<Long> upload = workers.submit(() ->
                            transfer(clientInput, upstream.getOutputStream()));
                        long downloaded = transfer(upstream.getInputStream(), clientOutput);
                        long uploaded = upload.get(5, java.util.concurrent.TimeUnit.SECONDS);
                        Map<String, Object> observed = new TreeMap<>();
                        observed.put("authAccepted", usePasswordAuth
                            ? authVersion == 1 && username.equals(receivedUsername)
                                && password.equals(receivedPassword)
                            : offeredNoAuth);
                        observed.put("authenticationMode", usePasswordAuth ? "USERNAME_PASSWORD" : "NO_AUTH");
                        observed.put("connectTargetHost", targetHost);
                        observed.put("connectTargetMatched", "127.0.0.1".equals(targetHost)
                            && targetPort == brokerPort);
                        observed.put("requestBytesForwarded", uploaded > 0);
                        observed.put("responseBytesForwarded", downloaded > 0);
                        if (uploaded == 0 || downloaded == 0) {
                            throw new java.io.IOException("SOCKS5 tunnel did not forward both directions");
                        }
                        return observed;
                    }
                }
            });

            org.apache.rocketmq.remoting.netty.NettyClientConfig config =
                new org.apache.rocketmq.remoting.netty.NettyClientConfig();
            config.setUseTLS(false);
            config.setClientSocketSndBufSize(64 * 1024);
            config.setClientSocketRcvBufSize(64 * 1024);
            String proxy = "{\"addr\":\"127.0.0.1:" + proxyPort + "\""
                + (usePasswordAuth ? ",\"username\":\"" + username + "\",\"password\":\""
                    + password + "\"" : "") + "}";
            config.setSocksProxyConfig("{\"127.0.0.0/8\":" + proxy + "}");
            org.apache.rocketmq.remoting.netty.NettyRemotingClient client =
                new org.apache.rocketmq.remoting.netty.NettyRemotingClient(config);
            try {
                client.start();
                org.apache.rocketmq.remoting.protocol.RemotingCommand request =
                    org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(1234, null);
                request.setBody("socks-oracle-request".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                org.apache.rocketmq.remoting.protocol.RemotingCommand response =
                    client.invokeSync("127.0.0.1:" + brokerPort, request, 7000);
                if (response == null || response.getCode() != 0
                    || !"socks-oracle-ok".equals(response.getRemark())) {
                    throw new IllegalStateException("Java SOCKS remoting response did not match");
                }
                if (response.getOpaque() != request.getOpaque()) {
                    throw new IllegalStateException("Java SOCKS remoting response opaque did not match request");
                }
                result.put("outcome", "OK");
                result.put("responseCode", response.getCode());
                result.put("responseRemark", response.getRemark());
                result.put("responseOpaqueMatched", true);
            } catch (Exception error) {
                result.put("clientError", error.getClass().getSimpleName() + ": " + error.getMessage());
                try {
                    result.put("proxyDiagnostic", proxyOutcome.get(1, java.util.concurrent.TimeUnit.SECONDS));
                } catch (Exception proxyError) {
                    result.put("proxyDiagnostic", proxyError.getClass().getSimpleName() + ": " + proxyError.getMessage());
                }
                try {
                    result.put("brokerDiagnostic", brokerOutcome.get(1, java.util.concurrent.TimeUnit.SECONDS));
                } catch (Exception brokerError) {
                    result.put("brokerDiagnostic", brokerError.getClass().getSimpleName() + ": " + brokerError.getMessage());
                }
                System.err.println("SOCKS connection fixture diagnostics: " + result);
                throw error;
            } finally {
                client.shutdown();
            }
            result.putAll(proxyOutcome.get(8, java.util.concurrent.TimeUnit.SECONDS));
            result.putAll(brokerOutcome.get(8, java.util.concurrent.TimeUnit.SECONDS));
            } finally {
                workers.shutdownNow();
                workers.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            }
        }
        return result;
    }

    private static byte[] readFrame(java.io.InputStream input) throws java.io.IOException {
        byte[] lengthBytes = readExactly(input, 4);
        int length = java.nio.ByteBuffer.wrap(lengthBytes).getInt();
        if (length < 4 || length > 1024 * 1024) {
            throw new java.io.IOException("invalid remoting frame length " + length);
        }
        byte[] frame = new byte[4 + length];
        System.arraycopy(lengthBytes, 0, frame, 0, 4);
        System.arraycopy(readExactly(input, length), 0, frame, 4, length);
        return frame;
    }

    private static byte[] readExactly(java.io.InputStream input, int length) throws java.io.IOException {
        if (length < 0 || length > 65535) throw new java.io.IOException("invalid read length " + length);
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) throw new java.io.EOFException("truncated stream input");
        return bytes;
    }

    private static long transfer(java.io.InputStream input, java.io.OutputStream output) throws java.io.IOException {
        byte[] buffer = new byte[8192];
        long count = 0;
        int read;
        while ((read = input.read(buffer)) >= 0) {
            output.write(buffer, 0, read);
            count += read;
        }
        output.flush();
        return count;
    }

    private static List<Object> socksProxySelection() throws Exception {
        List<Object> cases = new ArrayList<>();
        String[][] inputs = {
            {"{\"10.0.0.0/8\":{\"addr\":\"proxy-a:1080\"}}", "10.20.1.2:9876", "10.20.1.2"},
            {"{\"10.0.0.0/8\":{\"addr\":\"proxy-a:1080\"}}", "192.0.2.3:9876", "192.0.2.3"},
            {"{\"0.0.0.0/0\":{\"addr\":\"proxy-default:1080\"}}", "broker.example:9876", "broker.example"},
            {"{\"0.0.0.0/0\":{\"addr\":\"proxy-default:1080\"}}", "[2001:db8::1]:9876", "[2001:db8::1]"},
            {"{\"10.20.0.0/33\":{\"addr\":\"proxy-shift:1080\"}}", "10.20.4.5:9876", "10.20.4.5"},
            {"{\"10.0.0.0/8\":null}", "10.20.1.2:9876", "10.20.1.2"},
            {"{\"not-a-cidr\":{\"addr\":\"proxy-invalid:1080\"}}", "10.20.1.2:9876", "10.20.1.2"},
            {"{}", "192.0.2.3:9876", "192.0.2.3"}
        };
        for (int i = 0; i < inputs.length; i++) {
            ClientConfig config = new ClientConfig();
            config.setClientIP("socks-selection-" + i);
            config.setNamesrvAddr("");
            config.setSocksProxyConfig(inputs[i][0]);
            Map<String, Object> item = new TreeMap<>();
            item.put("config", inputs[i][0]);
            item.put("address", inputs[i][1]);
            item.put("host", inputs[i][2]);
            org.apache.rocketmq.client.impl.factory.MQClientInstance runtime = null;
            try {
                runtime = new org.apache.rocketmq.client.impl.factory.MQClientInstance(
                    config, 0, config.buildMQClientId());
                java.lang.reflect.Field remotingField =
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class.getDeclaredField("remotingClient");
                remotingField.setAccessible(true);
                Object remoting = remotingField.get(runtime.getMQClientAPIImpl());
                java.lang.reflect.Method select = remoting.getClass().getDeclaredMethod("getProxy", String.class);
                select.setAccessible(true);
                @SuppressWarnings("unchecked")
                Map.Entry<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig> selected =
                    (Map.Entry<String, org.apache.rocketmq.remoting.proxy.SocksProxyConfig>) select.invoke(remoting, inputs[i][1]);
                item.put("outcome", "OK");
                item.put("proxyCidr", selected == null ? null : selected.getKey());
                item.put("proxyAddress", selected == null || selected.getValue() == null
                    ? null : selected.getValue().getAddr());
            } catch (java.lang.reflect.InvocationTargetException error) {
                item.put("outcome", error.getCause().getClass().getSimpleName());
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
                    byte[] unknownName = "{\"code\":34,\"language\":\"not-a-language\"}"
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    org.apache.rocketmq.remoting.protocol.RemotingCommand unknownDecoded =
                        org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(unknownName,
                            org.apache.rocketmq.remoting.protocol.RemotingCommand.class);
                    outcome.put("unknownLanguageDecodedNull", unknownDecoded.getLanguage() == null);
                }
            } catch (RuntimeException error) { outcome.put("outcome", error.getClass().getSimpleName()); }
            result.put(type.name(), outcome);
        }
        return result;
    }

    private static List<Object> languageCodeMethodCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.LanguageCode[] values =
            org.apache.rocketmq.remoting.protocol.LanguageCode.values();
        List<String> names = new ArrayList<>();
        for (org.apache.rocketmq.remoting.protocol.LanguageCode value : values) {
            names.add(value.name());
        }
        for (org.apache.rocketmq.remoting.protocol.LanguageCode value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("values", names);
            item.put("valueOfName",
                org.apache.rocketmq.remoting.protocol.LanguageCode.valueOf(value.name()).name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            org.apache.rocketmq.remoting.protocol.LanguageCode[] mutated =
                org.apache.rocketmq.remoting.protocol.LanguageCode.values();
            mutated[0] = org.apache.rocketmq.remoting.protocol.LanguageCode.NODE_JS;
            item.put("valuesDefensiveCopy",
                org.apache.rocketmq.remoting.protocol.LanguageCode.values()[0]
                    == org.apache.rocketmq.remoting.protocol.LanguageCode.JAVA);
            item.put("code", value.getCode());
            org.apache.rocketmq.remoting.protocol.LanguageCode fromByte =
                org.apache.rocketmq.remoting.protocol.LanguageCode.valueOf(value.getCode());
            org.apache.rocketmq.remoting.protocol.LanguageCode fromName =
                org.apache.rocketmq.remoting.protocol.LanguageCode.getCode(value.name());
            item.put("valueOfByte", fromByte == null ? null : fromByte.name());
            item.put("getCodeByName", fromName == null ? null : fromName.name());
            cases.add(item);
        }
        for (byte input : new byte[] {0, 13, 14, 127, -1, -128}) {
            org.apache.rocketmq.remoting.protocol.LanguageCode decoded =
                org.apache.rocketmq.remoting.protocol.LanguageCode.valueOf(input);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "byte");
            item.put("input", input);
            item.put("decoded", decoded == null ? null : decoded.name());
            cases.add(item);
        }
        for (String input : new String[] {null, "", "JAVA", "java", "NODE_JS", "UNKNOWN"}) {
            org.apache.rocketmq.remoting.protocol.LanguageCode decoded =
                org.apache.rocketmq.remoting.protocol.LanguageCode.getCode(input);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "name");
            item.put("input", input);
            item.put("decoded", decoded == null ? null : decoded.name());
            cases.add(item);
        }
        for (String input : new String[] {null, "", "JAVA", "java", "NODE_JS", "UNKNOWN"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "enumNameLookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded",
                    org.apache.rocketmq.remoting.protocol.LanguageCode.valueOf(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> serializeTypeMethodCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.SerializeType[] values =
            org.apache.rocketmq.remoting.protocol.SerializeType.values();
        List<String> names = new ArrayList<>();
        for (org.apache.rocketmq.remoting.protocol.SerializeType value : values) {
            names.add(value.name());
        }
        for (org.apache.rocketmq.remoting.protocol.SerializeType value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("code", value.getCode());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("values", names);
            item.put("valueOfName",
                org.apache.rocketmq.remoting.protocol.SerializeType.valueOf(value.name()).name());
            org.apache.rocketmq.remoting.protocol.SerializeType fromByte =
                org.apache.rocketmq.remoting.protocol.SerializeType.valueOf(value.getCode());
            item.put("valueOfByte", fromByte == null ? null : fromByte.name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            org.apache.rocketmq.remoting.protocol.SerializeType[] mutated =
                org.apache.rocketmq.remoting.protocol.SerializeType.values();
            mutated[0] = org.apache.rocketmq.remoting.protocol.SerializeType.ROCKETMQ;
            item.put("valuesDefensiveCopy",
                org.apache.rocketmq.remoting.protocol.SerializeType.values()[0]
                    == org.apache.rocketmq.remoting.protocol.SerializeType.JSON);
            cases.add(item);
        }
        for (byte input : new byte[] {0, 1, 2, 127, -1, -128}) {
            org.apache.rocketmq.remoting.protocol.SerializeType decoded =
                org.apache.rocketmq.remoting.protocol.SerializeType.valueOf(input);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "byte");
            item.put("input", input);
            item.put("decoded", decoded == null ? null : decoded.name());
            cases.add(item);
        }
        for (String input : new String[] {null, "", "JSON", "json", "ROCKETMQ", "UNKNOWN"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "enumNameLookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded",
                    org.apache.rocketmq.remoting.protocol.SerializeType.valueOf(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> compressionTypeMethodCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.common.compression.CompressionType[] values =
            org.apache.rocketmq.common.compression.CompressionType.values();
        List<String> names = new ArrayList<>();
        for (org.apache.rocketmq.common.compression.CompressionType value : values) {
            names.add(value.name());
        }
        for (org.apache.rocketmq.common.compression.CompressionType value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("values", names);
            item.put("value", value.getValue());
            item.put("compressionFlag", value.getCompressionFlag());
            item.put("valueOfName",
                org.apache.rocketmq.common.compression.CompressionType.valueOf(value.name()).name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            org.apache.rocketmq.common.compression.CompressionType[] mutated =
                org.apache.rocketmq.common.compression.CompressionType.values();
            mutated[0] = org.apache.rocketmq.common.compression.CompressionType.ZLIB;
            item.put("valuesDefensiveCopy",
                org.apache.rocketmq.common.compression.CompressionType.values()[0]
                    == org.apache.rocketmq.common.compression.CompressionType.LZ4);
            cases.add(item);
        }
        for (String input : new String[] {null, "", "LZ4", "lz4", "ZSTD", "ZLIB", "UNKNOWN"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "enumNameLookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded",
                    org.apache.rocketmq.common.compression.CompressionType.valueOf(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        for (String input : new String[] {null, "", "LZ4", " lz4 ", " zstd ", "ZLIB", "\u00a0ZLIB\u00a0", "snappy"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "of");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded", org.apache.rocketmq.common.compression.CompressionType.of(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        for (int input : new int[] {Integer.MIN_VALUE, -1, 0, 1, 2, 3, 4, 7, 8, Integer.MAX_VALUE}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "findByValue");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded", org.apache.rocketmq.common.compression.CompressionType.findByValue(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        for (int input : new int[] {-1, 0, 1, 2, 3, 4, 7, 8, 255}) {
            int flag = (input << 8) | 0x4000;
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "messageSysFlag");
            item.put("input", input);
            item.put("flag", flag);
            try {
                item.put("outcome", "OK");
                item.put("decoded", org.apache.rocketmq.common.sysflag.MessageSysFlag.getCompressionType(flag).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> clientStatusEnumMethodCases() {
        Map<String, Object> cases = new TreeMap<>();
        cases.put("PullStatus", enumMethodCases(
            org.apache.rocketmq.client.consumer.PullStatus::values,
            org.apache.rocketmq.client.consumer.PullStatus::valueOf,
            new String[] {null, "", "FOUND", "found", "NO_NEW_MSG", "UNKNOWN"}));
        cases.put("PopStatus", enumMethodCases(
            org.apache.rocketmq.client.consumer.PopStatus::values,
            org.apache.rocketmq.client.consumer.PopStatus::valueOf,
            new String[] {null, "", "FOUND", "found", "POLLING_FULL", "POLLING_NOT_FOUND", "UNKNOWN"}));
        cases.put("AckStatus", enumMethodCases(
            org.apache.rocketmq.client.consumer.AckStatus::values,
            org.apache.rocketmq.client.consumer.AckStatus::valueOf,
            new String[] {null, "", "OK", "ok", "NO_EXIST", "UNKNOWN"}));
        cases.put("SendStatus", enumMethodCases(
            org.apache.rocketmq.client.producer.SendStatus::values,
            org.apache.rocketmq.client.producer.SendStatus::valueOf,
            new String[] {null, "", "SEND_OK", "send_ok", "FLUSH_DISK_TIMEOUT", "SLAVE_NOT_AVAILABLE", "UNKNOWN"}));
        return cases;
    }

    private static Map<String, Object> protocolEnumMethodCases() {
        Map<String, Object> cases = new TreeMap<>();
        cases.put("MessageRequestMode", enumMethodCases(
            org.apache.rocketmq.common.message.MessageRequestMode::values,
            org.apache.rocketmq.common.message.MessageRequestMode::valueOf,
            new String[] {null, "", "PULL", "pull", "UNKNOWN"}));
        cases.put("LiteSubscriptionAction", enumMethodCases(
            org.apache.rocketmq.common.lite.LiteSubscriptionAction::values,
            org.apache.rocketmq.common.lite.LiteSubscriptionAction::valueOf,
            new String[] {null, "", "PARTIAL_ADD", "partial_add", "UNKNOWN"}));
        cases.put("OffsetOptionType", enumMethodCases(
            org.apache.rocketmq.common.lite.OffsetOption.Type::values,
            org.apache.rocketmq.common.lite.OffsetOption.Type::valueOf,
            new String[] {null, "", "POLICY", "policy", "TAIL_N", "UNKNOWN"}));
        cases.put("ConsumeMessageResult", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.body.CMResult::values,
            org.apache.rocketmq.remoting.protocol.body.CMResult::valueOf,
            new String[] {null, "", "CR_SUCCESS", "cr_success", "CR_RETURN_NULL", "UNKNOWN"}));
        cases.put("ConsumeType", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType::values,
            org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType::valueOf,
            new String[] {null, "", "CONSUME_ACTIVELY", "consume_actively", "UNKNOWN"}));
        cases.put("MessageModel", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel::values,
            org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel::valueOf,
            new String[] {null, "", "BROADCASTING", "broadcasting", "UNKNOWN"}));
        cases.put("ConsumeFromWhere", enumMethodCases(
            org.apache.rocketmq.common.consumer.ConsumeFromWhere::values,
            org.apache.rocketmq.common.consumer.ConsumeFromWhere::valueOf,
            new String[] {null, "", "CONSUME_FROM_LAST_OFFSET", "consume_from_last_offset", "UNKNOWN"}));
        return cases;
    }

    private static Map<String, Object> tlsModeMethodCases() {
        Map<String, Object> result = new TreeMap<>();
        List<Object> enumCases = new ArrayList<>();
        org.apache.rocketmq.remoting.common.TlsMode[] values =
            org.apache.rocketmq.remoting.common.TlsMode.values();
        List<String> names = new ArrayList<>();
        for (org.apache.rocketmq.remoting.common.TlsMode value : values) {
            names.add(value.name());
        }
        for (org.apache.rocketmq.remoting.common.TlsMode value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("getName", value.getName());
            item.put("values", names);
            item.put("valueOfName", org.apache.rocketmq.remoting.common.TlsMode.valueOf(value.name()).name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            org.apache.rocketmq.remoting.common.TlsMode[] mutated =
                org.apache.rocketmq.remoting.common.TlsMode.values();
            mutated[0] = values[values.length - 1];
            item.put("valuesDefensiveCopy",
                org.apache.rocketmq.remoting.common.TlsMode.values()[0] == values[0]);
            enumCases.add(item);
        }
        for (String input : new String[] {
            null, "", "DISABLED", "disabled", "permissive", "enforcing", "unknown"
        }) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "lookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded", org.apache.rocketmq.remoting.common.TlsMode.valueOf(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            enumCases.add(item);
        }
        result.put("enum", enumCases);

        List<Object> parseCases = new ArrayList<>();
        for (String input : new String[] {
            null, "", "disabled", "permissive", "enforcing", "DISABLED", "ENFORCING",
            " enforcing", "enforcing ", "other"
        }) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "parse");
            item.put("input", input);
            org.apache.rocketmq.remoting.common.TlsMode parsed =
                org.apache.rocketmq.remoting.common.TlsMode.parse(input);
            item.put("parsed", parsed.name());
            item.put("getName", parsed.getName());
            parseCases.add(item);
        }
        result.put("parse", parseCases);
        return result;
    }

    private static Map<String, Object> remotingEnumMethodCases() {
        Map<String, Object> result = new TreeMap<>();
        result.put("RemotingCommandType", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.RemotingCommandType::values,
            org.apache.rocketmq.remoting.protocol.RemotingCommandType::valueOf,
            new String[] {null, "", "REQUEST_COMMAND", "request_command", "RESPONSE_COMMAND", "UNKNOWN"}));
        result.put("RequestType", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.RequestType::values,
            name -> org.apache.rocketmq.remoting.protocol.RequestType.valueOf(name),
            new String[] {null, "", "STREAM", "stream", "UNKNOWN"}));
        result.put("RequestSource", requestSourceMethodCases());
        result.put("MessageQueueRouteState", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.route.MessageQueueRouteState::values,
            org.apache.rocketmq.remoting.protocol.route.MessageQueueRouteState::valueOf,
            new String[] {null, "", "Expired", "ReadOnly", "Normal", "WriteOnly", "expired", "UNKNOWN"}));

        List<Object> requestTypeBytes = new ArrayList<>();
        for (byte input : new byte[] {Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", input);
            item.put("case", "byteLookup");
            org.apache.rocketmq.remoting.protocol.RequestType decoded =
                org.apache.rocketmq.remoting.protocol.RequestType.valueOf(input);
            item.put("outcome", "OK");
            item.put("decoded", decoded == null ? null : decoded.name());
            item.put("getCode", decoded == null ? null : decoded.getCode());
            requestTypeBytes.add(item);
        }
        result.put("requestTypeBytes", requestTypeBytes);

        List<Object> commandTypes = new ArrayList<>();
        org.apache.rocketmq.remoting.protocol.RemotingCommand request =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(1, null);
        addCommandTypeCase(commandTypes, "request", request);
        org.apache.rocketmq.remoting.protocol.RemotingCommand response =
            org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand(1, "remark");
        addCommandTypeCase(commandTypes, "response", response);
        request.markResponseType();
        addCommandTypeCase(commandTypes, "markedRequest", request);
        result.put("commandGetType", commandTypes);
        return result;
    }

    private static Map<String, Object> requestSourceMethodCases() {
        Map<String, Object> result = new TreeMap<>();
        result.put("enum", enumMethodCases(
            org.apache.rocketmq.remoting.protocol.RequestSource::values,
            org.apache.rocketmq.remoting.protocol.RequestSource::valueOf,
            new String[] {null, "", "SDK", "PROXY_FOR_ORDER", "proxy_for_order", "UNKNOWN"}));
        result.put("systemPropertyKey",
            org.apache.rocketmq.remoting.protocol.RequestSource.SYSTEM_PROPERTY_KEY);

        List<Object> values = new ArrayList<>();
        for (org.apache.rocketmq.remoting.protocol.RequestSource source :
            org.apache.rocketmq.remoting.protocol.RequestSource.values()) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "value");
            item.put("name", source.name());
            item.put("value", source.getValue());
            values.add(item);
        }
        result.put("values", values);

        List<Object> integers = new ArrayList<>();
        for (Integer input : new Integer[] {
            null, Integer.MIN_VALUE, -2, -1, 0, 1, 2, 3, Integer.MAX_VALUE
        }) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", input);
            item.put("valid", org.apache.rocketmq.remoting.protocol.RequestSource.isValid(input));
            item.put("parsed",
                org.apache.rocketmq.remoting.protocol.RequestSource.parseInteger(input).name());
            integers.add(item);
        }
        result.put("integers", integers);
        return result;
    }

    private static void addCommandTypeCase(
        List<Object> cases,
        String label,
        org.apache.rocketmq.remoting.protocol.RemotingCommand command) {
        Map<String, Object> item = new TreeMap<>();
        item.put("case", label);
        item.put("type", command.getType().name());
        item.put("isResponse", command.isResponseType());
        cases.add(item);
    }

    private static <E extends Enum<E>> List<Object> enumMethodCases(
        java.util.function.Supplier<E[]> valuesMethod,
        java.util.function.Function<String, E> valueOfMethod,
        String[] lookupInputs) {
        List<Object> cases = new ArrayList<>();
        E[] values = valuesMethod.get();
        List<String> names = new ArrayList<>();
        for (E value : values) names.add(value.name());
        for (E value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("values", names);
            item.put("valueOfName", valueOfMethod.apply(value.name()).name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            E[] mutated = valuesMethod.get();
            mutated[0] = values[values.length - 1];
            item.put("valuesDefensiveCopy", valuesMethod.get()[0] == values[0]);
            cases.add(item);
        }
        for (String input : lookupInputs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "lookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded", valueOfMethod.apply(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> accessChannelMethodCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.client.AccessChannel[] values =
            org.apache.rocketmq.client.AccessChannel.values();
        List<String> names = new ArrayList<>();
        for (org.apache.rocketmq.client.AccessChannel value : values) {
            names.add(value.name());
        }
        for (org.apache.rocketmq.client.AccessChannel value : values) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "constant");
            item.put("name", value.name());
            item.put("ordinal", value.ordinal());
            item.put("text", value.toString());
            item.put("values", names);
            item.put("valueOf", org.apache.rocketmq.client.AccessChannel.valueOf(value.name()).name());
            item.put("compareNext", value.compareTo(values[(value.ordinal() + 1) % values.length]));
            org.apache.rocketmq.client.AccessChannel[] mutated =
                org.apache.rocketmq.client.AccessChannel.values();
            mutated[0] = org.apache.rocketmq.client.AccessChannel.CLOUD;
            item.put("valuesDefensiveCopy",
                org.apache.rocketmq.client.AccessChannel.values()[0]
                    == org.apache.rocketmq.client.AccessChannel.LOCAL);
            cases.add(item);
        }
        for (String input : new String[] {null, "", "LOCAL", "local", "CLOUD", "UNKNOWN"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", "lookup");
            item.put("input", input);
            try {
                item.put("outcome", "OK");
                item.put("decoded", org.apache.rocketmq.client.AccessChannel.valueOf(input).name());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
                item.put("message", error.getMessage());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> serviceNameCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.client.impl.consumer.RebalanceService rebalance =
            new org.apache.rocketmq.client.impl.consumer.RebalanceService(null);
        org.apache.rocketmq.client.impl.consumer.PullMessageService pull =
            new org.apache.rocketmq.client.impl.consumer.PullMessageService(null);
        Map<String, Object> rebalanceCase = new TreeMap<>();
        rebalanceCase.put("class", "RebalanceService");
        rebalanceCase.put("serviceName", rebalance.getServiceName());
        cases.add(rebalanceCase);
        Map<String, Object> pullCase = new TreeMap<>();
        pullCase.put("class", "PullMessageService");
        pullCase.put("serviceName", pull.getServiceName());
        cases.add(pullCase);
        return cases;
    }

    private static List<Object> serviceThreadCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.common.ServiceThread[] services = {
            new org.apache.rocketmq.client.impl.consumer.RebalanceService(null),
            new org.apache.rocketmq.client.impl.consumer.PullMessageService(null)
        };
        for (org.apache.rocketmq.common.ServiceThread service : services) {
            Map<String, Object> row = new TreeMap<>();
            row.put("class", service.getServiceName());
            row.put("joinTimeMillis", service.getJoinTime());
            row.put("initiallyDaemon", service.isDaemon());
            row.put("initiallyStopped", service.isStopped());
            service.setDaemon(true);
            row.put("daemonAfterSet", service.isDaemon());
            service.wakeup();
            row.put("stoppedAfterWakeup", service.isStopped());
            service.makeStop();
            row.put("stoppedAfterMakeStopBeforeStart", service.isStopped());
            service.shutdown();
            row.put("stoppedAfterShutdownBeforeStart", service.isStopped());
            if (service instanceof org.apache.rocketmq.client.impl.consumer.PullMessageService) {
                org.apache.rocketmq.client.impl.consumer.PullMessageService pull =
                    (org.apache.rocketmq.client.impl.consumer.PullMessageService) service;
                row.put("scheduledExecutorShutdown", pull.getScheduledExecutorService().isShutdown());
            }
            cases.add(row);
        }
        return cases;
    }

    private static List<Object> serviceLifecycleCases() throws InterruptedException {
        List<Object> cases = new ArrayList<>();
        WorkerRunProbe rebalanceProbe = new WorkerRunProbe();
        org.apache.rocketmq.client.impl.consumer.RebalanceService rebalance =
            new org.apache.rocketmq.client.impl.consumer.RebalanceService(null) {
                @Override
                public void run() {
                    rebalanceProbe.run();
                }
            };
        cases.add(serviceLifecycleCase(rebalance, rebalanceProbe));

        WorkerRunProbe pullProbe = new WorkerRunProbe();
        org.apache.rocketmq.client.impl.consumer.PullMessageService pull =
            new org.apache.rocketmq.client.impl.consumer.PullMessageService(null) {
                @Override
                public void run() {
                    pullProbe.run();
                }
            };
        cases.add(serviceLifecycleCase(pull, pullProbe));
        return cases;
    }

    private static Map<String, Object> serviceLifecycleCase(
        org.apache.rocketmq.common.ServiceThread service, WorkerRunProbe probe
    ) throws InterruptedException {
        service.start();
        if (!probe.started.await(5, TimeUnit.SECONDS)) {
            probe.release.countDown();
            service.shutdown(true);
            throw new IllegalStateException("ServiceThread probe did not start: " + service.getServiceName());
        }
        service.start();
        boolean stoppedAfterStart = service.isStopped();
        service.makeStop();
        boolean stoppedAfterMakeStop = service.isStopped();
        boolean threadAliveAfterMakeStop = probe.thread.get().isAlive();
        probe.release.countDown();
        service.shutdown(true);

        Map<String, Object> row = new TreeMap<>();
        row.put("class", service.getServiceName());
        row.put("runsAfterTwoStarts", probe.runs.get());
        row.put("stoppedAfterStart", stoppedAfterStart);
        row.put("stoppedAfterMakeStop", stoppedAfterMakeStop);
        row.put("threadAliveAfterMakeStop", threadAliveAfterMakeStop);
        row.put("stoppedAfterShutdown", service.isStopped());
        row.put("threadAliveAfterShutdown", probe.thread.get().isAlive());
        if (service instanceof org.apache.rocketmq.client.impl.consumer.PullMessageService) {
            org.apache.rocketmq.client.impl.consumer.PullMessageService pull =
                (org.apache.rocketmq.client.impl.consumer.PullMessageService) service;
            row.put("scheduledExecutorShutdown", pull.getScheduledExecutorService().isShutdown());
        }
        return row;
    }

    private static final class WorkerRunProbe {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicReference<Thread> thread = new AtomicReference<>();
        private final AtomicInteger runs = new AtomicInteger();

        private void run() {
            thread.set(Thread.currentThread());
            runs.incrementAndGet();
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
