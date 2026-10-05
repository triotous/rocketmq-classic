package compat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/** Actual Java route converters/VIP transform; no client or server is started. */
public final class RuntimeRouteFixtures {
    private RuntimeRouteFixtures() { }

    private static final class RouteApi extends org.apache.rocketmq.client.impl.MQClientAPIImpl {
        TopicRouteData route;
        RouteApi(org.apache.rocketmq.client.ClientConfig config) {
            super(new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.rocketmq.remoting.RemotingClient.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> method.getReturnType() == boolean.class ? false : null));
        }
        @Override
        public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeout) {
            return route;
        }
    }

    private static Map<String, Object> runtimeNullBrokerMap() throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("route-null-map");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> item = new TreeMap<>();
        try {
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true); field.set(runtime, api);
            String initial = "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"old:10911\"}}],\"filterServerTable\":{}}";
            String invalid = "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":null}],\"filterServerTable\":{}}";
            item.put("initialInput", com.alibaba.fastjson2.JSON.parseObject(initial));
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(invalid));
            api.route = com.alibaba.fastjson2.JSON.parseObject(initial, TopicRouteData.class);
            item.put("initialChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            TopicRouteData before = runtime.getAnExistTopicRouteData("topic");
            api.route = com.alibaba.fastjson2.JSON.parseObject(invalid, TopicRouteData.class);
            try {
                item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
                item.put("outcome", "OK");
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            item.put("cachedRouteUnchanged", before.equals(runtime.getAnExistTopicRouteData("topic")));
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, original, new IllegalStateException("route fixture teardown"));
            api.shutdown();
        }
        return item;
    }

    public static void main(String[] args) throws Exception {
        String ordinary = "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":3,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":0}],"
            + "\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master:10911\",\"2\":\"slave:10911\"}}],\"filterServerTable\":{}}";
        List<Object> cases = new ArrayList<>();
        List<Map<String, Object>> inputs = new ArrayList<>();
        for (String order : new String[] {null, "", "broker:2", "broker:2:ignored;", ";;", "broker:invalid",
            "broker:٢", "broker:+２", "broker:1٢", "broker:२", "broker:߂", "broker:²",
            "broker:𝟚", "broker: 2", "broker:", "broker:-２１４７４８３６４８", "broker:２１４７４８３６４８"}) {
            Map<String, Object> input = new TreeMap<>(com.alibaba.fastjson2.JSON.parseObject(ordinary));
            input.put("orderTopicConf", order);
            inputs.add(input);
        }
        for (String extra : new String[] {
            "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":2147483647,\"writeQueueNums\":2147483647,\"perm\":0,\"topicSysFlag\":0}]}",
            "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":0,\"writeQueueNums\":2147483647,\"perm\":2,\"topicSysFlag\":0}],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"2\":\"slave:10911\"}}]}",
            "{\"orderTopicConf\":\"broker:2\",\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":1,\"writeQueueNums\":2147483647,\"perm\":6,\"topicSysFlag\":0}]}",
            "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":2,\"writeQueueNums\":3,\"perm\":6,\"topicSysFlag\":0},{\"brokerName\":\"broker\",\"readQueueNums\":1,\"writeQueueNums\":1,\"perm\":6,\"topicSysFlag\":0}]}"
        }) {
            Map<String, Object> input = new TreeMap<>(com.alibaba.fastjson2.JSON.parseObject(ordinary));
            input.putAll(com.alibaba.fastjson2.JSON.parseObject(extra));
            inputs.add(input);
        }
        for (String scope : new String[] {"__global__", "__custom", "__", null}) {
            Map<String, Object> input = new TreeMap<>(com.alibaba.fastjson2.JSON.parseObject(ordinary));
            input.put("queueDatas", com.alibaba.fastjson2.JSON.parseArray("[{\"brokerName\":\"broker\",\"readQueueNums\":2147483647,\"writeQueueNums\":2147483647,\"perm\":6,\"topicSysFlag\":0}]"));
            Map<String, Object> oldMapping = new TreeMap<>();
            oldMapping.put("topic", "topic"); oldMapping.put("bname", "old-broker"); oldMapping.put("scope", scope);
            oldMapping.put("epoch", 1); oldMapping.put("totalQueues", scope == null ? Integer.MAX_VALUE : 5); oldMapping.put("currIdMap", Map.of("0",0));
            Map<String, Object> newMapping = new TreeMap<>(oldMapping);
            newMapping.put("bname", "new-broker"); newMapping.put("epoch", 2); newMapping.put("totalQueues", scope == null ? Integer.MAX_VALUE : 2);
            input.put("topicQueueMappingByBroker", new TreeMap<>(Map.of("old",oldMapping,"new",newMapping)));
            inputs.add(input);
        }
        for (Map<String, Object> input : inputs) {
            TopicRouteData route = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(input, com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue), TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", input);
            try {
                org.apache.rocketmq.client.impl.producer.TopicPublishInfo publish = MQClientInstance.topicRouteData2TopicPublishInfo("topic", route);
                List<MessageQueue> publishQueues = new ArrayList<>(publish.getMessageQueueList());
                if (route.getOrderTopicConf() == null && route.getTopicQueueMappingByBroker() != null && !route.getTopicQueueMappingByBroker().isEmpty()) {
                    // Canonicalize equal-ID HashMap order, not Java's queue-ID ordering.
                    publishQueues.sort(Comparator.comparingInt(MessageQueue::getQueueId).thenComparing(MessageQueue::getBrokerName));
                }
                item.put("publish", publishQueues);
                item.put("ordered", publish.isOrderTopic());
                List<MessageQueue> subscribe = new ArrayList<>(MQClientInstance.topicRouteData2TopicSubscribeInfo("topic", route));
                subscribe.sort(Comparator.naturalOrder());
                item.put("subscribe", subscribe);
                List<Map<String, Object>> endpoints = new ArrayList<>();
                org.apache.rocketmq.remoting.rpc.ClientMetadata.topicRouteData2EndpointsForStaticTopic("topic", route).entrySet().stream()
                    .sorted(Map.Entry.comparingByKey()).forEach(entry -> endpoints.add(new TreeMap<>(Map.of("queue",entry.getKey(),"broker",entry.getValue()))));
                item.put("endpoints", endpoints);
                item.put("outcome", "OK");
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        Map<String, String> vip = new TreeMap<>();
        for (String address : new String[] {"broker:10911", "[2001:db8::1]:10911", "2001:db8::1:10911"}) {
            vip.put(address, MixAll.brokerVIPChannel(true, address));
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "MQClientInstance.topicRouteData2TopicPublishInfo/topicRouteData2TopicSubscribeInfo/updateTopicRouteInfoFromNameServer/getAnExistTopicRouteData; ClientMetadata.topicRouteData2EndpointsForStaticTopic; MixAll.brokerVIPChannel; Integer.parseInt/Character.digit");
        fixture.put("serverBaselineContacted", false);
        fixture.put("cases", cases);
        fixture.put("vip", vip);
        List<Integer> digitZeros = new ArrayList<>();
        for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
            if (Character.digit((char) codeUnit, 10) == 0) digitZeros.add(codeUnit);
        }
        for (int zero : digitZeros) {
            for (int digit = 0; digit < 10; digit++) {
                if (Character.digit((char) (zero + digit), 10) != digit)
                    throw new IllegalStateException("noncontiguous decimal digits");
            }
        }
        for (int codeUnit = 0; codeUnit <= Character.MAX_VALUE; codeUnit++) {
            int digit = Character.digit((char) codeUnit, 10);
            if (digit >= 0 && !digitZeros.contains(codeUnit - digit))
                throw new IllegalStateException("unrecorded decimal digit");
        }
        fixture.put("integerDigitZeroCodeUnits", digitZeros);
        List<Object> integerCases = new ArrayList<>();
        for (String text : new String[] {"+2147483647", "2147483648", "-2147483648", "-2147483649",
            "٠٠", "--2", "", "+", "-２１４７４８３６４８", "2\n", "𝟚"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("input", text);
            try {
                item.put("value", Integer.parseInt(text));
                item.put("outcome", "OK");
            } catch (NumberFormatException error) {
                item.put("outcome", "NumberFormatException");
            }
            integerCases.add(item);
        }
        fixture.put("integers", integerCases);
        fixture.put("runtimeNullBrokerMap", runtimeNullBrokerMap());
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("Runtime route/VIP invoked cases verified: " + cases.size());
    }
}
