package compat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
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
        int requests;
        final List<String> requestedTopics = new ArrayList<>();
        RouteApi(org.apache.rocketmq.client.ClientConfig config) {
            super(new org.apache.rocketmq.remoting.netty.NettyClientConfig(), null, null, config, null,
                ignored -> (org.apache.rocketmq.remoting.RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.rocketmq.remoting.RemotingClient.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.remoting.RemotingClient.class},
                    (proxy, method, values) -> method.getReturnType() == boolean.class ? false : null));
        }
        @Override
        public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeout) {
            requests++;
            requestedTopics.add(topic);
            return route;
        }
    }

    private static Map<String, Object> nullSubscriptionTopicRefresh() throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("route-null-subscription-topic");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> item = new TreeMap<>();
        try {
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true);
            field.set(runtime, api);
            org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData nullTopic =
                new org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData();
            org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData named =
                new org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData();
            named.setTopic("topic");
            java.util.Set<org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData> subscriptions =
                new java.util.HashSet<>(java.util.Arrays.asList(nullTopic, named));
            org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumer =
                (org.apache.rocketmq.client.impl.consumer.MQConsumerInner) java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class.getClassLoader(),
                    new Class<?>[] {org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class},
                    (proxy, method, values) -> "subscriptions".equals(method.getName())
                        ? subscriptions : method.getReturnType() == boolean.class ? false : null);
            if (!runtime.registerConsumer("consumer", consumer))
                throw new IllegalStateException("consumer registration");
            runtime.updateTopicRouteInfoFromNameServer();
            item.put("requests", api.requests);
            item.put("requestedNull", api.requestedTopics.contains(null));
            item.put("requestedNamed", api.requestedTopics.contains("topic"));
            api.route = new TopicRouteData();
            try {
                runtime.updateTopicRouteInfoFromNameServer((String) null);
                item.put("successRouteOutcome", "OK");
            } catch (RuntimeException error) {
                item.put("successRouteOutcome", error.getClass().getSimpleName());
            }
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("null subscription topic fixture teardown")); }
            finally { api.shutdown(); }
        }
        return item;
    }

    private static List<Object> runtimeNullBrokerValues() throws Exception {
        List<Object> cases = new ArrayList<>();
        for (boolean slavePresent : new boolean[] {false, true}) {
            org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
            config.setClientIP("route-null-values-" + slavePresent);
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
            RouteApi api = new RouteApi(config);
            Map<String, Object> item = new TreeMap<>();
            try {
                java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
                field.setAccessible(true); field.set(runtime, api);
                String input = "{\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":2,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":0}],"
                    + "\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":null"
                    + (slavePresent ? ",\"1\":\"slave:10911\"" : "") + "}}],\"filterServerTable\":{}}";
                item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
                api.route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
                item.put("decodedMasterKeyPresent", api.route.getBrokerDatas().get(0).getBrokerAddrs().containsKey(0L));
                item.put("decodedMasterNull", api.route.getBrokerDatas().get(0).getBrokerAddrs().get(0L) == null);
                item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
                TopicRouteData cached = runtime.getAnExistTopicRouteData("topic");
                item.put("cachedMasterKeyPresent", cached.getBrokerDatas().get(0).getBrokerAddrs().containsKey(0L));
                item.put("cachedMasterNull", cached.getBrokerDatas().get(0).getBrokerAddrs().get(0L) == null);
                item.put("publishQueueCount", MQClientInstance.topicRouteData2TopicPublishInfo("topic", cached).getMessageQueueList().size());
                item.put("subscribeQueueCount", MQClientInstance.topicRouteData2TopicSubscribeInfo("topic", cached).size());
                item.put("publishAddress", runtime.findBrokerAddressInPublish("broker"));
                org.apache.rocketmq.client.impl.FindBrokerResult admin = runtime.findBrokerAddressInAdmin("broker");
                item.put("adminAddress", admin == null ? null : admin.getBrokerAddr());
                org.apache.rocketmq.client.impl.FindBrokerResult exact = runtime.findBrokerAddressInSubscribe("broker", 0L, true);
                item.put("subscribeExactMasterAddress", exact == null ? null : exact.getBrokerAddr());
                boolean onlyAllowedChoices = true;
                for (int sample = 0; sample < 128; sample++) {
                    String address = runtime.findBrokerAddrByTopic("topic");
                    onlyAllowedChoices &= address == null || (slavePresent && "slave:10911".equals(address));
                }
                item.put("topicSelectionOnlyAllowedChoices", onlyAllowedChoices);
                item.put("outcome", "OK");
            } finally {
                java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                try { cleanup.invoke(runtime, original, new IllegalStateException("null address-value fixture teardown")); }
                finally { api.shutdown(); }
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> runtimeNullNestedRouteElements() throws Exception {
        String[][] inputs = new String[][] {
            {"null-queue", "{\"queueDatas\":[null],\"brokerDatas\":[],\"filterServerTable\":{}}"},
            {"null-broker", "{\"queueDatas\":[],\"brokerDatas\":[null],\"filterServerTable\":{}}"},
            {"null-static-mapping", "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{},"
                + "\"topicQueueMappingByBroker\":{\"broker\":null}}"},
            {"null-filter-list", "{\"queueDatas\":[],\"brokerDatas\":[],"
                + "\"filterServerTable\":{\"broker\":null}}"}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] entry : inputs) {
            org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
            config.setClientIP("route-null-nested-" + entry[0]);
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
            RouteApi api = new RouteApi(config);
            Map<String, Object> item = new TreeMap<>();
            try {
                java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
                field.setAccessible(true);
                field.set(runtime, api);
                item.put("id", entry[0]);
                item.put("input", com.alibaba.fastjson2.JSON.parseObject(entry[1]));
                api.route = com.alibaba.fastjson2.JSON.parseObject(entry[1], TopicRouteData.class);
                try {
                    item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
                    item.put("outcome", "OK");
                } catch (RuntimeException error) {
                    item.put("outcome", error.getClass().getSimpleName());
                }
                item.put("cachedRouteExists", runtime.getAnExistTopicRouteData("topic") != null);
                item.put("requests", api.requests);
            } finally {
                java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                try { cleanup.invoke(runtime, original, new IllegalStateException("null nested route fixture teardown")); }
                finally { api.shutdown(); }
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> nullableRouteCollections() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{\"queueDatas\":null,\"brokerDatas\":[],\"filterServerTable\":{}}",
            "{\"queueDatas\":[],\"brokerDatas\":null,\"filterServerTable\":{}}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":null}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{}}"
        }) {
            TopicRouteData route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
            TopicRouteData copy = new TopicRouteData(route);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("decodedQueueNull", route.getQueueDatas() == null);
            item.put("decodedBrokerNull", route.getBrokerDatas() == null);
            item.put("decodedFilterNull", route.getFilterServerTable() == null);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(route.toJson());
            item.put("encodedHasQueue", encoded.containsKey("queueDatas"));
            item.put("encodedHasBroker", encoded.containsKey("brokerDatas"));
            item.put("encodedHasFilter", encoded.containsKey("filterServerTable"));
            item.put("encodedHasMapping", encoded.containsKey("topicQueueMappingByBroker"));
            item.put("copyQueueEmpty", copy.getQueueDatas().isEmpty());
            item.put("copyBrokerEmpty", copy.getBrokerDatas().isEmpty());
            item.put("copyFilterEmpty", copy.getFilterServerTable().isEmpty());
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> nullableNestedRouteFields() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{\"queueDatas\":[{}],\"brokerDatas\":[{}]}",
            "{\"queueDatas\":[{\"brokerName\":null}],\"brokerDatas\":[{\"cluster\":null,\"brokerName\":null}]}",
            "{\"queueDatas\":[{\"brokerName\":\"broker\",\"perm\":6}],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\"}]}",
            "{\"queueDatas\":[{\"brokerName\":null,\"readQueueNums\":1,\"perm\":4}],\"brokerDatas\":[{\"brokerName\":\"other\"}]}"
        }) {
            TopicRouteData route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(route.toJson());
            com.alibaba.fastjson2.JSONObject queue = encoded.getJSONArray("queueDatas").getJSONObject(0);
            com.alibaba.fastjson2.JSONObject broker = encoded.getJSONArray("brokerDatas").getJSONObject(0);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("queueNameNull", route.getQueueDatas().get(0).getBrokerName() == null);
            item.put("brokerNameNull", route.getBrokerDatas().get(0).getBrokerName() == null);
            item.put("clusterNull", route.getBrokerDatas().get(0).getCluster() == null);
            item.put("queueRead", route.getQueueDatas().get(0).getReadQueueNums());
            item.put("queueWrite", route.getQueueDatas().get(0).getWriteQueueNums());
            item.put("queuePerm", route.getQueueDatas().get(0).getPerm());
            item.put("queueFlag", route.getQueueDatas().get(0).getTopicSysFlag());
            item.put("encodedHasQueueName", queue.containsKey("brokerName"));
            item.put("encodedHasBrokerName", broker.containsKey("brokerName"));
            item.put("encodedHasCluster", broker.containsKey("cluster"));
            java.util.Set<MessageQueue> subscriptions = MQClientInstance.topicRouteData2TopicSubscribeInfo("topic", route);
            item.put("subscribeQueueCount", subscriptions.size());
            item.put("subscribeQueueBrokerNull", subscriptions.stream().anyMatch(queueValue -> queueValue.getBrokerName() == null));
            item.put("subscribeJsonBrokerOmitted", subscriptions.stream().anyMatch(queueValue ->
                !com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(queueValue)).containsKey("brokerName")));
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> nullableMessageQueueFields() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{}",
            "{\"topic\":null,\"brokerName\":null,\"queueId\":0}",
            "{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":-2}"
        }) {
            MessageQueue queue = com.alibaba.fastjson2.JSON.parseObject(input, MessageQueue.class);
            com.alibaba.fastjson2.JSONObject encoded =
                com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(queue));
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("topicNull", queue.getTopic() == null);
            item.put("brokerNull", queue.getBrokerName() == null);
            item.put("queueId", queue.getQueueId());
            item.put("encodedHasTopic", encoded.containsKey("topic"));
            item.put("encodedHasBroker", encoded.containsKey("brokerName"));
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> messageQueueCompareToCases() {
        List<Object> cases = new ArrayList<>();
        MessageQueue[][] pairs = new MessageQueue[][] {
            {new MessageQueue(null, "b", 0), new MessageQueue("t", "b", 0)},
            {new MessageQueue("t", "b", 0), new MessageQueue(null, "b", 0)},
            {new MessageQueue("t", null, 0), new MessageQueue("t", "b", 0)},
            {new MessageQueue("t", "b", 0), new MessageQueue("t", null, 0)},
            {new MessageQueue("a", "b", 0), new MessageQueue("c", "b", 0)},
            {new MessageQueue("abc", "b", 0), new MessageQueue("a", "b", 0)},
            {new MessageQueue("\uD800\uDC00", "b", 0), new MessageQueue("\uE000", "b", 0)},
            {new MessageQueue("t", "a", 0), new MessageQueue("t", "c", 0)},
            {new MessageQueue("t", "b", Integer.MAX_VALUE), new MessageQueue("t", "b", -1)},
            {new MessageQueue("t", "b", Integer.MIN_VALUE), new MessageQueue("t", "b", 1)},
            {new MessageQueue("t", "b", 5), new MessageQueue("t", "b", 5)}
        };
        for (MessageQueue[] pair : pairs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("left", pair[0]);
            item.put("right", pair[1]);
            try {
                item.put("result", pair[0].compareTo(pair[1]));
                item.put("outcome", "OK");
            } catch (NullPointerException error) {
                item.put("outcome", "NullPointerException");
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> messageQueueMethodCases() {
        MessageQueue[] queues = new MessageQueue[] {
            new MessageQueue(),
            new MessageQueue("topic", "broker", -17),
            new MessageQueue("\uD834\uDD1E", "Br\u00e9", Integer.MIN_VALUE)
        };
        List<Object> cases = new ArrayList<>();
        for (int index = 0; index < queues.length; index++) {
            MessageQueue queue = queues[index];
            Map<String, Object> item = new TreeMap<>();
            item.put("case", new String[] {"defaults", "populated", "unicode-boundary"}[index]);
            item.put("queue", messageQueueRow(queue));
            item.put("hashCode", queue.hashCode());
            item.put("toString", queue.toString());
            item.put("equalsSelf", queue.equals(queue));
            item.put("equalsCopy", queue.equals(new MessageQueue(queue)));
            item.put("equalsNull", queue.equals(null));
            item.put("equalsWrongType", queue.equals("not a MessageQueue"));
            item.put("equalsTopicChanged", queue.equals(new MessageQueue(
                queue.getTopic() == null ? "topic" : queue.getTopic() + "x",
                queue.getBrokerName(), queue.getQueueId())));
            item.put("equalsBrokerChanged", queue.equals(new MessageQueue(
                queue.getTopic(),
                queue.getBrokerName() == null ? "broker" : queue.getBrokerName() + "x",
                queue.getQueueId())));
            item.put("equalsQueueIdChanged", queue.equals(new MessageQueue(
                queue.getTopic(), queue.getBrokerName(), queue.getQueueId() + 1)));
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> messageQueueRow(MessageQueue queue) {
        Map<String, Object> result = new TreeMap<>();
        result.put("topic", queue.getTopic());
        result.put("brokerName", queue.getBrokerName());
        result.put("queueId", queue.getQueueId());
        return result;
    }

    private static List<Object> nullableStaticMappingFields() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{}",
            "{\"topic\":null,\"bname\":null,\"scope\":null,\"currIdMap\":null}",
            "{\"topic\":\"T\",\"bname\":\"B\",\"scope\":\"S\",\"currIdMap\":{\"0\":1}}"
        }) {
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo mapping =
                com.alibaba.fastjson2.JSON.parseObject(input,
                    org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(mapping.toJson());
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("topicNull", mapping.getTopic() == null);
            item.put("bnameNull", mapping.getBname() == null);
            item.put("scopeNull", mapping.getScope() == null);
            item.put("currIdMapNull", mapping.getCurrIdMap() == null);
            item.put("currIdMapEmpty", mapping.getCurrIdMap() != null && mapping.getCurrIdMap().isEmpty());
            item.put("encodedHasTopic", encoded.containsKey("topic"));
            item.put("encodedHasBname", encoded.containsKey("bname"));
            item.put("encodedHasScope", encoded.containsKey("scope"));
            item.put("encodedHasCurrIdMap", encoded.containsKey("currIdMap"));
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> staticMappingMapEntryCases() {
        List<Object> cases = new ArrayList<>();
        for (String[] testCase : new String[][] {
            {"null-value", "{\"currIdMap\":{\"0\":null}}"},
            {"null-key", "{\"currIdMap\":{\"null\":1}}"},
            {"invalid-key", "{\"currIdMap\":{\"not-a-number\":1}}"},
            {"overflow-key", "{\"currIdMap\":{\"2147483648\":1}}"},
            {"invalid-value", "{\"currIdMap\":{\"0\":\"not-a-number\"}}"},
            {"integer-limits", "{\"currIdMap\":{\"-2147483648\":-2147483648,\"2147483647\":2147483647}}"}
        }) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", testCase[0]);
            item.put("rawInput", testCase[1]);
            try {
                org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo mapping =
                    com.alibaba.fastjson2.JSON.parseObject(testCase[1],
                        org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class);
                item.put("outcome", "accepted");
                item.put("mapSize", mapping.getCurrIdMap() == null ? null : mapping.getCurrIdMap().size());
                item.put("hasKeyZero", mapping.getCurrIdMap() != null && mapping.getCurrIdMap().containsKey(0));
                item.put("keyZeroValue", mapping.getCurrIdMap() == null ? null : mapping.getCurrIdMap().get(0));
            } catch (Throwable error) {
                item.put("outcome", "rejected");
                item.put("error", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> routeDtoInputCase(String id, String type, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("id", id);
        item.put("type", type);
        item.put("rawInput", rawInput);
        try {
            Map<String, Object> fields = new TreeMap<>();
            switch (type) {
                case "queueData": {
                    org.apache.rocketmq.remoting.protocol.route.QueueData value =
                        com.alibaba.fastjson2.JSON.parseObject(rawInput,
                            org.apache.rocketmq.remoting.protocol.route.QueueData.class);
                    fields.put("brokerName", value.getBrokerName());
                    fields.put("readQueueNums", value.getReadQueueNums());
                    fields.put("writeQueueNums", value.getWriteQueueNums());
                    fields.put("perm", value.getPerm());
                    fields.put("topicSysFlag", value.getTopicSysFlag());
                    break;
                }
                case "messageQueue": {
                    MessageQueue value = com.alibaba.fastjson2.JSON.parseObject(rawInput, MessageQueue.class);
                    fields.put("topic", value.getTopic());
                    fields.put("brokerName", value.getBrokerName());
                    fields.put("queueId", value.getQueueId());
                    break;
                }
                case "brokerData": {
                    org.apache.rocketmq.remoting.protocol.route.BrokerData value =
                        com.alibaba.fastjson2.JSON.parseObject(rawInput,
                            org.apache.rocketmq.remoting.protocol.route.BrokerData.class);
                    fields.put("cluster", value.getCluster());
                    fields.put("brokerName", value.getBrokerName());
                    fields.put("zoneName", value.getZoneName());
                    fields.put("enableActingMaster", value.isEnableActingMaster());
                    Map<String, Object> addresses = new TreeMap<>();
                    if (value.getBrokerAddrs() != null) {
                        value.getBrokerAddrs().forEach((key, address) -> addresses.put(key.toString(), address));
                    }
                    fields.put("brokerAddrs", value.getBrokerAddrs() == null ? null : addresses);
                    break;
                }
                case "mapping": {
                    org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo value =
                        com.alibaba.fastjson2.JSON.parseObject(rawInput,
                            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class);
                    fields.put("topic", value.getTopic());
                    fields.put("bname", value.getBname());
                    fields.put("scope", value.getScope());
                    fields.put("totalQueues", value.getTotalQueues());
                    fields.put("epoch", value.getEpoch());
                    fields.put("dirty", value.isDirty());
                    Map<String, Object> ids = new TreeMap<>();
                    if (value.getCurrIdMap() != null) {
                        value.getCurrIdMap().forEach((key, number) -> ids.put(key.toString(), number));
                    }
                    fields.put("currIdMap", value.getCurrIdMap() == null ? null : ids);
                    break;
                }
                case "topicRouteData": {
                    TopicRouteData value = com.alibaba.fastjson2.JSON.parseObject(rawInput, TopicRouteData.class);
                    fields.putAll(routeJson(value));
                    break;
                }
                default: throw new IllegalArgumentException("unknown DTO: " + type);
            }
            item.put("outcome", "ACCEPTED");
            item.put("fields", fields);
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> routeDtoInputCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(routeDtoInputCase("queue-scalars", "queueData",
            "{\"brokerName\":123,\"readQueueNums\":\"2\",\"writeQueueNums\":1.9,\"perm\":true,\"topicSysFlag\":null}"));
        cases.add(routeDtoInputCase("queue-empty-object-number", "queueData",
            "{\"readQueueNums\":{}}"));
        cases.add(routeDtoInputCase("queue-decimal-overflow-number", "queueData",
            "{\"readQueueNums\":2147483648.0}"));
        cases.add(routeDtoInputCase("message-queue-scalars", "messageQueue",
            "{\"topic\":123,\"brokerName\":false,\"queueId\":\"2\"}"));
        cases.add(routeDtoInputCase("message-queue-structured-strings", "messageQueue",
            "{\"topic\":[],\"brokerName\":{}}"));
        cases.add(routeDtoInputCase("message-queue-id-null", "messageQueue",
            "{\"queueId\":null}"));
        cases.add(routeDtoInputCase("message-queue-id-minimum", "messageQueue",
            "{\"queueId\":-2147483648}"));
        cases.add(routeDtoInputCase("message-queue-id-maximum", "messageQueue",
            "{\"queueId\":2147483647}"));
        cases.add(routeDtoInputCase("message-queue-id-fraction", "messageQueue",
            "{\"queueId\":-1.9}"));
        cases.add(routeDtoInputCase("message-queue-unicode-decimal-id", "messageQueue",
            "{\"queueId\":\"１２\"}"));
        cases.add(routeDtoInputCase("message-queue-id-overflow", "messageQueue",
            "{\"queueId\":2147483648}"));
        cases.add(routeDtoInputCase("broker-scalars", "brokerData",
            "{\"cluster\":true,\"brokerName\":7,\"zoneName\":1.5,\"brokerAddrs\":{\"+1\":true,\"2\":3,\"3\":null},\"enableActingMaster\":\"TRUE\"}"));
        cases.add(routeDtoInputCase("broker-address-decimal-key", "brokerData",
            "{\"brokerAddrs\":{\"1.0\":0}}"));
        cases.add(routeDtoInputCase("broker-unicode-decimal-address-key", "brokerData",
            "{\"brokerAddrs\":{\"١٢\":\"master\"}}"));
        cases.add(routeDtoInputCase("broker-boolean-minus-one", "brokerData",
            "{\"enableActingMaster\":-1}"));
        cases.add(routeDtoInputCase("broker-boolean-other-number", "brokerData",
            "{\"enableActingMaster\":2}"));
        cases.add(routeDtoInputCase("broker-boolean-decimal-number", "brokerData",
            "{\"enableActingMaster\":1.0}"));
        cases.add(routeDtoInputCase("broker-boolean-numeric-string", "brokerData",
            "{\"enableActingMaster\":\"1\"}"));
        cases.add(routeDtoInputCase("broker-boolean-null", "brokerData",
            "{\"enableActingMaster\":null}"));
        cases.add(routeDtoInputCase("mapping-scalars", "mapping",
            "{\"topic\":42,\"bname\":true,\"scope\":3,\"totalQueues\":\"2\",\"epoch\":1.9,\"dirty\":\"TRUE\",\"currIdMap\":{\"01\":\"2\",\"2\":1.9,\"3\":true}}"));
        cases.add(routeDtoInputCase("mapping-exponent-string", "mapping",
            "{\"epoch\":\"1e2\"}"));
        cases.add(routeDtoInputCase("mapping-long-minimum", "mapping",
            "{\"epoch\":\"-9223372036854775808\"}"));
        cases.add(routeDtoInputCase("mapping-unicode-decimal-id-key", "mapping",
            "{\"currIdMap\":{\"１２\":1}}"));
        cases.add(routeDtoInputCase("queue-unicode-decimal-string", "queueData",
            "{\"readQueueNums\":\"１２\",\"writeQueueNums\":\"-١٢\"}"));
        cases.add(routeDtoInputCase("mapping-unicode-decimal-strings", "mapping",
            "{\"totalQueues\":\"१२\",\"epoch\":\"-１２\"}"));
        cases.add(routeDtoInputCase("mapping-long-overflow", "mapping",
            "{\"epoch\":9223372036854775808}"));
        cases.add(routeDtoInputCase("topic-route-nested-scalars", "topicRouteData",
            "{\"orderTopicConf\":7,\"queueDatas\":[{\"brokerName\":123,\"readQueueNums\":\"2\",\"writeQueueNums\":1.9,\"perm\":true,\"topicSysFlag\":null}],"
                + "\"brokerDatas\":[{\"cluster\":true,\"brokerName\":7,\"zoneName\":1.5,\"brokerAddrs\":{\"1.0\":true},\"enableActingMaster\":1}],"
                + "\"filterServerTable\":{\"broker\":[1,true,null],\"empty\":[],\"missing\":null},"
                + "\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":42,\"totalQueues\":\"2\",\"bname\":true,\"epoch\":\"1e2\",\"dirty\":\"TRUE\",\"currIdMap\":{\"01\":\"2\"}}}}"));
        cases.add(routeDtoInputCase("queue-int-overflow", "queueData",
            "{\"readQueueNums\":2147483648}"));
        cases.add(routeDtoInputCase("mapping-null-id", "mapping",
            "{\"currIdMap\":{\"1\":null}}"));
        cases.add(routeDtoInputCase("broker-invalid-boolean", "brokerData",
            "{\"enableActingMaster\":\"yes\"}"));
        cases.add(routeDtoInputCase("queue-fastjson-single-quote-unquoted-key-trailing-comma", "queueData",
            "{brokerName:'broker',readQueueNums:'2',}"));
        cases.add(routeDtoInputCase("broker-fastjson-comments-unquoted-map-key", "brokerData",
            "/* route */ {cluster:'cluster',brokerName:'broker',brokerAddrs:{0:'master:10911',},} // tail\n"));
        cases.add(routeDtoInputCase("mapping-fastjson-line-comment", "mapping",
            "{topic:'topic', // mapping fields\n bname:'broker',totalQueues:2,currIdMap:{'0':0,},}"));
        cases.add(routeDtoInputCase("queue-fastjson-long-suffix", "queueData",
            "{readQueueNums:2L,writeQueueNums:3L,perm:4L,topicSysFlag:5L}"));
        cases.add(routeDtoInputCase("queue-fastjson-decimal-suffix", "queueData",
            "{readQueueNums:2.9D,writeQueueNums:3.9F}"));
        cases.add(routeDtoInputCase("queue-fastjson-leading-plus-number", "queueData",
            "{\"readQueueNums\":+2}"));
        cases.add(routeDtoInputCase("queue-fastjson-leading-zero-number", "queueData",
            "{\"readQueueNums\":02}"));
        cases.add(routeDtoInputCase("queue-fastjson-leading-plus-fraction", "queueData",
            "{\"writeQueueNums\":+2.9}"));
        cases.add(routeDtoInputCase("queue-fastjson-leading-zero-fraction", "queueData",
            "{\"writeQueueNums\":02.9}"));
        cases.add(routeDtoInputCase("mapping-fastjson-leading-plus-exponent", "mapping",
            "{\"epoch\":+2e1}"));
        cases.add(routeDtoInputCase("mapping-fastjson-leading-zero-exponent", "mapping",
            "{\"epoch\":02e1}"));
        cases.add(routeDtoInputCase("queue-fastjson-nan-number", "queueData",
            "{\"readQueueNums\":NaN}"));
        cases.add(routeDtoInputCase("queue-fastjson-infinity-number", "queueData",
            "{\"readQueueNums\":Infinity}"));
        cases.add(routeDtoInputCase("queue-fastjson-hex-number", "queueData",
            "{\"readQueueNums\":0x10}"));
        cases.add(routeDtoInputCase("mapping-fastjson-long-suffix", "mapping",
            "{epoch:9223372036854775807L,totalQueues:2L,currIdMap:{0:1L}}"));
        cases.add(routeDtoInputCase("route-filter-string-as-list", "topicRouteData",
            "{filterServerTable:{broker:'server:10911'}}"));
        cases.add(routeDtoInputCase("route-filter-number-as-list", "topicRouteData",
            "{filterServerTable:{broker:1}}"));
        cases.add(routeDtoInputCase("route-null-and-scalar-queue-rows", "topicRouteData",
            "{queueDatas:[null,'broker'],brokerDatas:[]}"));
        cases.add(routeDtoInputCase("route-null-mapping-value", "topicRouteData",
            "{topicQueueMappingByBroker:{broker:null}}"));
        cases.add(routeDtoInputCase("broker-address-array-shape", "brokerData",
            "{brokerAddrs:[1,2]}"));
        cases.add(routeDtoInputCase("mapping-id-array-shape", "mapping",
            "{currIdMap:[1,2]}"));
        return cases;
    }

    private static List<Object> nullStaticMappingConversion() {
        List<Object> cases = new ArrayList<>();
        for (String mapping : new String[] {
            "{\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":\"broker\",\"currIdMap\":null}",
            "{\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":null,\"currIdMap\":{\"0\":0}}",
            "{\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":null,\"currIdMap\":{}}",
            "{\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":\"broker\"}"
        }) {
            String input = "{\"queueDatas\":[],\"brokerDatas\":[],\"topicQueueMappingByBroker\":{\"broker\":" + mapping + "}}";
            TopicRouteData route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            try {
                Map<MessageQueue, String> endpoints =
                    org.apache.rocketmq.remoting.rpc.ClientMetadata.topicRouteData2EndpointsForStaticTopic("topic", route);
                item.put("outcome", "OK");
                item.put("endpoints", endpoints.toString());
            } catch (RuntimeException error) {
                item.put("outcome", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> overlappingStaticMappingOwners() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"scope\",\"bname\":\"A\",\"epoch\":1,\"totalQueues\":2,\"currIdMap\":{\"0\":0}},\"beta\":{\"scope\":\"scope\",\"bname\":\"B\",\"epoch\":2,\"totalQueues\":1,\"currIdMap\":{\"0\":0}}}}",
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"scope\",\"bname\":\"A\",\"epoch\":2,\"totalQueues\":2,\"currIdMap\":{\"0\":0}},\"beta\":{\"scope\":\"scope\",\"bname\":\"B\",\"epoch\":1,\"totalQueues\":1,\"currIdMap\":{\"0\":0}}}}",
            "{\"topicQueueMappingByBroker\":{\"alpha\":{\"scope\":\"scope\",\"bname\":\"A\",\"epoch\":1,\"totalQueues\":2,\"currIdMap\":{\"0\":0}},\"beta\":{\"scope\":\"scope\",\"bname\":\"B\",\"epoch\":1,\"totalQueues\":1,\"currIdMap\":{\"0\":0}}}}"
        }) {
            TopicRouteData route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
            Map<MessageQueue, String> endpoints =
                org.apache.rocketmq.remoting.rpc.ClientMetadata.topicRouteData2EndpointsForStaticTopic("topic", route);
            List<Map<String, Object>> rows = new ArrayList<>();
            for (Map.Entry<MessageQueue, String> entry : endpoints.entrySet()) {
                Map<String, Object> row = new TreeMap<>();
                row.put("queueId", entry.getKey().getQueueId());
                row.put("brokerName", entry.getKey().getBrokerName());
                row.put("owner", entry.getValue());
                rows.add(row);
            }
            rows.sort(Comparator.comparingInt(row -> (Integer) row.get("queueId")));
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            item.put("endpoints", rows);
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> runtimeNullableCollections() throws Exception {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{\"queueDatas\":null,\"brokerDatas\":[],\"filterServerTable\":{}}",
            "{\"queueDatas\":null,\"brokerDatas\":[],\"filterServerTable\":{},\"orderTopicConf\":\"broker:1\"}",
            "{\"queueDatas\":null,\"brokerDatas\":[],\"filterServerTable\":{},\"topicQueueMappingByBroker\":{\"broker\":{\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":\"broker\"}}}",
            "{\"queueDatas\":[],\"brokerDatas\":null,\"filterServerTable\":{}}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":null}",
            "{\"queueDatas\":null,\"brokerDatas\":[],\"filterServerTable\":{},\"orderTopicConf\":\"broker:1\"}",
            "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":null,\"brokerAddrs\":{}}]}",
            "{\"queueDatas\":[],\"brokerDatas\":[{\"cluster\":null,\"brokerName\":\"broker\",\"brokerAddrs\":{}}]}",
            "{\"queueDatas\":[{\"brokerName\":null,\"readQueueNums\":1,\"perm\":4}],\"brokerDatas\":[]}",
            "{\"queueDatas\":[{\"brokerName\":null},{\"brokerName\":\"broker\"}],\"brokerDatas\":[]}"
        }) {
            org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
            config.setClientIP("route-null-collections-" + cases.size());
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
            RouteApi api = new RouteApi(config);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(input));
            try {
                java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
                field.setAccessible(true); field.set(runtime, api);
                api.route = com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class);
                boolean registeredConsumer = cases.size() == 5;
                item.put("registeredConsumer", registeredConsumer);
                if (registeredConsumer) {
                    org.apache.rocketmq.client.impl.consumer.MQConsumerInner consumer =
                        (org.apache.rocketmq.client.impl.consumer.MQConsumerInner) java.lang.reflect.Proxy.newProxyInstance(
                            org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class.getClassLoader(),
                            new Class<?>[] {org.apache.rocketmq.client.impl.consumer.MQConsumerInner.class},
                            (proxy, method, values) -> method.getReturnType() == boolean.class ? false : null);
                    runtime.getConsumerTable().put("consumer", consumer);
                }
                try {
                    item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
                    item.put("outcome", "OK");
                } catch (RuntimeException error) {
                    item.put("outcome", error.getClass().getSimpleName());
                }
                TopicRouteData cached = runtime.getAnExistTopicRouteData("topic");
                item.put("cached", cached != null);
                if (cached != null) {
                    item.put("cachedQueueNull", cached.getQueueDatas() == null);
                    item.put("cachedBrokerNull", cached.getBrokerDatas() == null);
                    item.put("cachedFilterNull", cached.getFilterServerTable() == null);
                }
            } finally {
                java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                cleanup.invoke(runtime, original, new IllegalStateException("route fixture teardown"));
                api.shutdown();
            }
            cases.add(item);
        }
        return cases;
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

    private static Map<String, Object> runtimeEmptyEndpointTransition(
        Map<String, Object> initial, Map<String, Object> ordinary) throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("route-endpoint-transition");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> item = new TreeMap<>();
        try {
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true); field.set(runtime, api);
            item.put("initialInput", initial); item.put("input", ordinary);
            api.route = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
            item.put("initialChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            List<MessageQueue> queues = new ArrayList<>(org.apache.rocketmq.remoting.rpc.ClientMetadata
                .topicRouteData2EndpointsForStaticTopic("topic", api.route).keySet());
            queues.sort(Comparator.naturalOrder());
            if (queues.isEmpty()) throw new IllegalStateException("transition requires static endpoints");
            List<Object> before = new ArrayList<>();
            for (MessageQueue queue : queues) before.add(new TreeMap<>(Map.of(
                "queue", queue, "broker", runtime.getBrokerNameFromMessageQueue(queue))));
            item.put("before", before);
            api.route = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(ordinary), TopicRouteData.class);
            item.put("changed", runtime.updateTopicRouteInfoFromNameServer("topic"));
            List<Object> after = new ArrayList<>();
            for (MessageQueue queue : queues) after.add(new TreeMap<>(Map.of(
                "queue", queue, "broker", runtime.getBrokerNameFromMessageQueue(queue))));
            item.put("after", after);
            item.put("retained", before.equals(after));
            item.put("rawRouteHasStaticMappings", runtime.getAnExistTopicRouteData("topic").getTopicQueueMappingByBroker() != null
                && !runtime.getAnExistTopicRouteData("topic").getTopicQueueMappingByBroker().isEmpty());
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, original, new IllegalStateException("endpoint fixture teardown"));
            api.shutdown();
        }
        return item;
    }

    private static List<Object> runtimeRawOfflineCleanup(Map<String, Object> initial) throws Exception {
        List<Object> cases = new ArrayList<>();
        for (boolean priorBroker : new boolean[] {false, true}) {
            org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
            config.setClientIP("raw-offline-cleanup-" + priorBroker);
            MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
            org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
            RouteApi api = new RouteApi(config);
            Map<String, Object> item = new TreeMap<>();
            try {
                java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
                field.setAccessible(true); field.set(runtime, api);
                api.route = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
                item.put("priorBroker", priorBroker);
                item.put("initialInput", initial);
                if (priorBroker) runtime.updateTopicRouteInfoFromNameServer("topic");
                item.put("publishAddressBefore", runtime.findBrokerAddressInPublish("broker"));
                TopicRouteData raw = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
                raw.getBrokerDatas().get(0).setBrokerAddrs(null);
                runtime.getTopicRouteTable().clear();
                runtime.getTopicRouteTable().put("raw", raw);
                item.put("rawInput", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(raw,
                    com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue, com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
                java.lang.reflect.Method clean = MQClientInstance.class.getDeclaredMethod("cleanOfflineBroker");
                clean.setAccessible(true);
                try {
                    clean.invoke(runtime);
                    item.put("outcome", "OK");
                } catch (java.lang.reflect.InvocationTargetException error) {
                    item.put("outcome", error.getCause().getClass().getSimpleName());
                }
                item.put("publishAddressAfter", runtime.findBrokerAddressInPublish("broker"));
                item.put("rawObjectRetained", runtime.getAnExistTopicRouteData("raw") == raw);
                item.put("rawNullMapRetained", runtime.getAnExistTopicRouteData("raw").getBrokerDatas().get(0).getBrokerAddrs() == null);
                item.put("fetchCount", api.requests);
            } finally {
                java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                    org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
                cleanup.setAccessible(true);
                try { cleanup.invoke(runtime, original, new IllegalStateException("raw offline cleanup fixture teardown")); }
                finally { api.shutdown(); }
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> runtimeTopicRouteTable(Map<String, Object> initial) throws Exception {
        org.apache.rocketmq.client.ClientConfig config = new org.apache.rocketmq.client.ClientConfig();
        config.setClientIP("route-live-table");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        org.apache.rocketmq.client.impl.MQClientAPIImpl original = runtime.getMQClientAPIImpl();
        RouteApi api = new RouteApi(config);
        Map<String, Object> result = new TreeMap<>();
        try {
            java.lang.reflect.Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
            field.setAccessible(true); field.set(runtime, api);
            TopicRouteData first = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
            TopicRouteData second = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
            second.getBrokerDatas().get(0).getBrokerAddrs().put(0L, "replacement:10911");
            TopicRouteData equalSecond = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(second), TopicRouteData.class);
            result.put("initialInput", initial);
            result.put("replacementInput", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(second,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            java.util.concurrent.ConcurrentMap<String, TopicRouteData> table = runtime.getTopicRouteTable();
            Map<String, Object> primitives = new TreeMap<>();
            primitives.put("initialEmpty", table.isEmpty());
            primitives.put("firstPutAbsent", table.put("topic", first) == null);
            primitives.put("getOrDefaultPresent", table.getOrDefault("topic", second) == first);
            primitives.put("getOrDefaultAbsent", table.getOrDefault("missing", second) == second);
            primitives.put("getterSeesFirst", runtime.getAnExistTopicRouteData("topic") == first);
            primitives.put("putReturnsFirst", table.put("topic", second) == first);
            primitives.put("retainedGetterSeesReplacement", runtime.getTopicRouteTable() == table && runtime.getAnExistTopicRouteData("topic") == second);
            primitives.put("putIfAbsentKeepsSecond", table.putIfAbsent("topic", first) == second);
            primitives.put("unequalReplaceRejected", !table.replace("topic", first, second));
            primitives.put("equalCopyReplaceAccepted", equalSecond != second && table.replace("topic", equalSecond, first));
            primitives.put("replaceReturnsFirst", table.replace("topic", second) == first);
            primitives.put("unequalRemoveRejected", !table.remove("topic", first));
            primitives.put("equalCopyRemoveAccepted", table.remove("topic", equalSecond));
            primitives.put("absentReplaceNull", table.replace("topic", first) == null);
            primitives.put("absentRemoveNull", table.remove("topic") == null);
            primitives.put("putIfAbsentAbsentNull", table.putIfAbsent("topic", first) == null);
            primitives.put("emptyKeyAccepted", table.put("", second) == null);
            primitives.put("sizeTwo", table.size() == 2 && table.containsKey(""));
            table.clear();
            primitives.put("retainedViewSeesClear", table.isEmpty());
            primitives.put("getterSeesClear", runtime.getAnExistTopicRouteData("topic") == null);
            primitives.put("putAfterClearAccepted", table.put("topic", second) == null && runtime.getAnExistTopicRouteData("topic") == second);
            Map<String, TopicRouteData> bulkValues = new TreeMap<>();
            bulkValues.put("bulk-a", first);
            bulkValues.put("bulk-b", second);
            table.putAll(bulkValues);
            primitives.put("putAllAdds", table.get("bulk-a") == first && table.get("bulk-b") == second
                && table.size() == 3);
            TopicRouteData absentValue = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(second), TopicRouteData.class);
            absentValue.getBrokerDatas().get(0).setBrokerName("absent-broker");
            primitives.put("containsValueEqualCopy", table.containsValue(equalSecond));
            primitives.put("containsValueAbsent", !table.containsValue(absentValue));
            Map<String, TopicRouteData> expectedRoutes = new TreeMap<>();
            expectedRoutes.put("topic", second);
            expectedRoutes.put("bulk-a", first);
            expectedRoutes.put("bulk-b", second);
            Map<String, Boolean> visitedRoutes = new TreeMap<>();
            table.forEach((key, value) -> visitedRoutes.put(key, expectedRoutes.get(key) == value));
            primitives.put("forEachVisitsCurrentMappings", visitedRoutes.size() == expectedRoutes.size()
                && visitedRoutes.values().stream().allMatch(Boolean::booleanValue));
            primitives.put("computeIfAbsentMaps", table.computeIfAbsent("compute-a", key -> first) == first);
            primitives.put("computeIfAbsentKeepsCurrent", table.computeIfAbsent("compute-a", key -> second) == first);
            primitives.put("computeIfAbsentNullLeavesAbsent", table.computeIfAbsent("compute-null", key -> null) == null
                && !table.containsKey("compute-null"));
            primitives.put("computeIfPresentReplaces", table.computeIfPresent("compute-a", (key, value) -> second) == second
                && table.get("compute-a") == second);
            primitives.put("computeIfPresentNullRemoves", table.computeIfPresent("compute-a", (key, value) -> null) == null
                && !table.containsKey("compute-a"));
            primitives.put("computeReplaces", table.compute("compute-a", (key, value) -> first) == first
                && table.get("compute-a") == first);
            primitives.put("computeAbsentInserts", table.compute("compute-b", (key, value) -> second) == second);
            primitives.put("computeNullRemoves", table.compute("compute-b", (key, value) -> null) == null
                && !table.containsKey("compute-b"));
            primitives.put("computeAbsentNullLeavesAbsent", table.compute("compute-null-absent", (key, value) -> null) == null
                && !table.containsKey("compute-null-absent"));
            primitives.put("mergeCombines", table.merge("compute-a", second, (oldValue, newValue) -> newValue) == second);
            primitives.put("mergeAbsentInserts", table.merge("merge-new", first, (oldValue, newValue) -> null) == first);
            table.put("merge-remove", second);
            primitives.put("mergeNullRemoves", table.merge("merge-remove", first, (oldValue, newValue) -> null) == null
                && !table.containsKey("merge-remove"));
            table.replaceAll((key, value) -> key.equals("topic") ? second : first);
            primitives.put("replaceAllUpdatesMappings", table.size() == 5 && table.get("topic") == second
                && table.get("bulk-a") == first && table.get("compute-a") == first && table.get("merge-new") == first);
            table.remove("compute-a");
            table.remove("merge-new");
            table.clear();

            table.put("key-remove", first);
            table.put("key-keep", second);
            table.put("key-retain", first);
            primitives.put("keyViewContains", table.keySet().contains("key-keep"));
            primitives.put("keyViewRemove", table.keySet().remove("key-remove") && !table.containsKey("key-remove"));
            primitives.put("keyViewRemoveIf", table.keySet().removeIf(key -> key.equals("key-keep"))
                && !table.containsKey("key-keep"));
            table.put("key-all", second);
            table.put("key-retain-all", first);
            primitives.put("keyViewRemoveAll", table.keySet().removeAll(java.util.Set.of("key-all"))
                && !table.containsKey("key-all"));
            primitives.put("keyViewRetainAll", table.keySet().retainAll(java.util.Set.of("key-retain-all"))
                && table.size() == 1 && table.containsKey("key-retain-all"));

            table.clear();
            table.put("value-a", first);
            table.put("value-b", second);
            table.put("value-c", second);
            primitives.put("valuesViewContainsEqualCopy", table.values().contains(equalSecond));
            primitives.put("valuesViewRemoveEqualCopy", table.values().remove(equalSecond)
                && table.values().stream().filter(equalSecond::equals).count() == 1);
            primitives.put("valuesViewRemoveIf", table.values().removeIf(value -> value.equals(first))
                && !table.containsKey("value-a"));
            table.clear();
            table.put("value-all", second);
            table.put("value-retain-drop", second);
            table.put("value-retain-keep", first);
            primitives.put("valuesViewRemoveAllEqual", table.values().removeAll(java.util.List.of(equalSecond))
                && !table.containsKey("value-all") && !table.containsKey("value-retain-drop"));
            table.clear();
            table.put("value-retain-drop", second);
            table.put("value-retain-keep", first);
            primitives.put("valuesViewRetainAllEqual", table.values().retainAll(java.util.List.of(first))
                && table.size() == 1 && table.containsKey("value-retain-keep"));

            java.util.Map.Entry<String, TopicRouteData> entryKeep =
                new java.util.AbstractMap.SimpleImmutableEntry<>("entry-keep", equalSecond);
            table.clear();
            table.put("entry-remove", first);
            table.put("entry-keep", second);
            primitives.put("entryViewContainsEqualCopy", table.entrySet().contains(entryKeep));
            primitives.put("entryViewRemoveEqualCopy", table.entrySet().remove(
                new java.util.AbstractMap.SimpleImmutableEntry<>("entry-remove", first))
                && !table.containsKey("entry-remove"));
            primitives.put("entryViewRemoveIf", table.entrySet().removeIf(entry -> entry.getKey().equals("entry-keep"))
                && !table.containsKey("entry-keep"));
            table.put("entry-all", second);
            table.put("entry-retain-drop", second);
            table.put("entry-retain-keep", first);
            primitives.put("entryViewRemoveAllEqual", table.entrySet().removeAll(java.util.Set.of(
                new java.util.AbstractMap.SimpleImmutableEntry<>("entry-all", equalSecond)))
                && !table.containsKey("entry-all"));
            primitives.put("entryViewRetainAllEqual", table.entrySet().retainAll(java.util.Set.of(
                new java.util.AbstractMap.SimpleImmutableEntry<>("entry-retain-keep", first)))
                && table.size() == 1 && table.containsKey("entry-retain-keep"));

            table.put("key-view-clear", first);
            table.keySet().clear();
            primitives.put("keyViewClear", table.isEmpty());
            table.put("values-view-clear", first);
            table.values().clear();
            primitives.put("valuesViewClear", table.isEmpty());
            table.put("entry-view-clear", first);
            table.entrySet().clear();
            primitives.put("entryViewClear", table.isEmpty());

            table.clear();
            table.put("iterator-key", first);
            java.util.Iterator<String> keyIterator = table.keySet().iterator();
            keyIterator.next();
            keyIterator.remove();
            primitives.put("keyIteratorRemove", table.isEmpty());
            table.put("iterator-value", first);
            java.util.Iterator<TopicRouteData> valueIterator = table.values().iterator();
            valueIterator.next();
            valueIterator.remove();
            primitives.put("valuesIteratorRemove", table.isEmpty());
            table.put("iterator-entry", first);
            java.util.Iterator<java.util.Map.Entry<String, TopicRouteData>> entryIterator = table.entrySet().iterator();
            entryIterator.next();
            entryIterator.remove();
            primitives.put("entryIteratorRemove", table.isEmpty());
            table.clear();
            result.put("primitives", primitives);

            Map<String, Object> mutableAlias = new TreeMap<>();
            TopicRouteData aliasValue = new TopicRouteData();
            aliasValue.setOrderTopicConf("broker:1");
            mutableAlias.put("putRetainsObjectIdentity",
                table.put("mutable-alias", aliasValue) == null
                    && table.get("mutable-alias") == aliasValue
                    && runtime.getAnExistTopicRouteData("mutable-alias") == aliasValue);
            TopicRouteData aliasFromTable = table.get("mutable-alias");
            aliasValue.setOrderTopicConf("broker:2");
            mutableAlias.put("originalMutationVisibleThroughTable",
                table.get("mutable-alias").getOrderTopicConf().equals("broker:2"));
            aliasFromTable.setOrderTopicConf("broker:3");
            mutableAlias.put("tableValueMutationVisibleThroughOriginal",
                aliasValue.getOrderTopicConf().equals("broker:3"));
            result.put("mutableAlias", mutableAlias);
            table.clear();

            api.route = first;
            result.put("refreshChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            result.put("retainedViewSeesRefresh", table.get("topic") == runtime.getAnExistTopicRouteData("topic"));
            result.put("initialPublishAddress", runtime.findBrokerAddressInPublish("broker"));
            TopicRouteData cached = table.get("topic");
            result.put("directPutReturnsCached", table.put("topic", second) == cached);
            result.put("topicAddressAfterDirectPut", runtime.findBrokerAddrByTopic("topic"));
            result.put("publishAddressAfterDirectPut", runtime.findBrokerAddressInPublish("broker"));
            int beforeQuery = api.requests;
            result.put("queryReturnsDirectValueWithoutFetch", runtime.queryTopicRouteData("topic") == second && api.requests == beforeQuery);
            table.clear();
            result.put("getterAbsentAfterClear", runtime.getAnExistTopicRouteData("topic") == null);
            result.put("topicAddressAfterClear", runtime.findBrokerAddrByTopic("topic"));
            result.put("publishAddressAfterClear", runtime.findBrokerAddressInPublish("broker"));
            java.lang.reflect.Method clean = MQClientInstance.class.getDeclaredMethod("cleanOfflineBroker");
            clean.setAccessible(true); clean.invoke(runtime);
            result.put("publishAddressAfterOfflineClean", runtime.findBrokerAddressInPublish("broker"));
            result.put("queryAfterClearRefetches", runtime.queryTopicRouteData("topic") != null && api.requests == beforeQuery + 1);
            result.put("retainedViewSeesQueryRefresh", table.containsKey("topic"));

            TopicRouteData metadataOnly = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(first), TopicRouteData.class);
            metadataOnly.getBrokerDatas().get(0).setCluster("other-cluster");
            metadataOnly.getBrokerDatas().get(0).setZoneName("other-zone");
            metadataOnly.getBrokerDatas().get(0).setEnableActingMaster(true);
            result.put("metadataOnlyInput", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(metadataOnly,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            result.put("brokerMetadataIgnoredByRouteEquals", first.equals(metadataOnly));
            table.put("metadata", first);
            result.put("metadataEqualConditionalRemoveAccepted", table.remove("metadata", metadataOnly));
            TopicRouteData beforeMetadataRefresh = table.get("topic");
            api.route = metadataOnly;
            result.put("metadataOnlyRefreshChanged", runtime.updateTopicRouteInfoFromNameServer("topic"));
            result.put("metadataOnlyRefreshKeepsCachedObject", table.get("topic") == beforeMetadataRefresh);

            TopicRouteData invalid = com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(initial), TopicRouteData.class);
            invalid.getBrokerDatas().get(0).setBrokerAddrs(null);
            result.put("rawPutAcceptsNullBrokerMap", table.put("invalid", invalid) == null && runtime.getAnExistTopicRouteData("invalid") == invalid);
            int beforeInvalidQuery = api.requests;
            result.put("queryReturnsInvalidRawValueWithoutValidation", runtime.queryTopicRouteData("invalid") == invalid && api.requests == beforeInvalidQuery);
            api.route = invalid;
            result.put("unchangedInvalidRawRefreshChanged", runtime.updateTopicRouteInfoFromNameServer("invalid"));
            result.put("unchangedInvalidRawRefreshKeepsCachedObject", table.get("invalid") == invalid);
            table.clear();
        } finally {
            java.lang.reflect.Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            try { cleanup.invoke(runtime, original, new IllegalStateException("live route-table fixture teardown")); }
            finally { api.shutdown(); }
        }
        return result;
    }

    private static org.apache.rocketmq.remoting.protocol.route.BrokerData brokerData(
        String cluster, String name, HashMap<Long, String> addresses, boolean actingMaster, String zone) {
        org.apache.rocketmq.remoting.protocol.route.BrokerData data =
            new org.apache.rocketmq.remoting.protocol.route.BrokerData();
        data.setCluster(cluster);
        data.setBrokerName(name);
        data.setBrokerAddrs(addresses);
        data.setEnableActingMaster(actingMaster);
        data.setZoneName(zone);
        return data;
    }

    private static List<Object> brokerDataSelectAddrCases() {
        String[] inputs = new String[] {
            "{\"brokerAddrs\":{\"0\":\"master\",\"1\":\"slave\"}}",
            "{\"brokerAddrs\":{\"1\":\"one\",\"2\":\"two\"}}",
            "{\"brokerAddrs\":{\"0\":null,\"2\":\"slave\"}}",
            "{\"brokerAddrs\":{\"1\":null}}",
            "{\"brokerAddrs\":{}}",
            "{\"brokerAddrs\":null}",
            "{}"
        };
        List<Object> cases = new ArrayList<>();
        for (int index = 0; index < inputs.length; index++) {
            org.apache.rocketmq.remoting.protocol.route.BrokerData data =
                com.alibaba.fastjson2.JSON.parseObject(inputs[index],
                    org.apache.rocketmq.remoting.protocol.route.BrokerData.class);
            Map<String, Object> item = new TreeMap<>();
            TreeSet<String> addresses = new TreeSet<>();
            boolean nullAddress = false;
            String exception = null;
            for (int sample = 0; sample < 256; sample++) {
                try {
                    String address = data.selectBrokerAddr();
                    if (address == null) nullAddress = true;
                    else addresses.add(address);
                } catch (RuntimeException error) {
                    exception = error.getClass().getSimpleName();
                    break;
                }
            }
            item.put("id", "select-" + index);
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(inputs[index]));
            item.put("addresses", new ArrayList<>(addresses));
            item.put("nullAddress", nullAddress);
            item.put("exception", exception);
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> brokerDataEqualityCases() {
        HashMap<Long, String> master = new HashMap<>();
        master.put(0L, "master:10911");
        HashMap<Long, String> otherMaster = new HashMap<>();
        otherMaster.put(0L, "other:10911");
        HashMap<Long, String> withNull = new HashMap<>();
        withNull.put(0L, null);
        withNull.put(2L, "slave:10911");
        List<Object> cases = new ArrayList<>();
        addBrokerDataEqualityCase(cases, "metadata-only", brokerData("cluster-a", "broker", master, false, "zone-a"),
            brokerData("cluster-b", "broker", master, true, "zone-b"));
        addBrokerDataEqualityCase(cases, "broker-name", brokerData("cluster", "broker-a", master, false, null),
            brokerData("cluster", "broker-b", master, false, null));
        addBrokerDataEqualityCase(cases, "addresses", brokerData("cluster", "broker", master, false, null),
            brokerData("cluster", "broker", otherMaster, false, null));
        addBrokerDataEqualityCase(cases, "null-map-vs-empty", brokerData(null, "broker", null, false, null),
            brokerData(null, "broker", new HashMap<>(), false, null));
        addBrokerDataEqualityCase(cases, "both-null-map", brokerData("a", null, null, false, "a"),
            brokerData("b", null, null, true, "b"));
        addBrokerDataEqualityCase(cases, "null-address-value", brokerData(null, "broker", withNull, false, null),
            brokerData("other", "broker", withNull, true, "other"));
        return cases;
    }

    private static void addBrokerDataEqualityCase(List<Object> cases, String id,
        org.apache.rocketmq.remoting.protocol.route.BrokerData left,
        org.apache.rocketmq.remoting.protocol.route.BrokerData right) {
        Map<String, Object> item = new TreeMap<>();
        item.put("id", id);
        item.put("left", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(left,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        item.put("right", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(right,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        item.put("equals", left.equals(right));
        item.put("hashEqual", left.hashCode() == right.hashCode());
        cases.add(item);
    }

    private static List<Object> brokerDataHashCodeCases() {
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(Long.MIN_VALUE, "\uD800\uDC00");
        addresses.put(Long.MAX_VALUE, null);
        addresses.put(0L, "\uD83D\uDE00");
        List<Object> cases = new ArrayList<>();
        addBrokerDataHashCodeCase(cases, "null-fields", brokerData(null, null, null, false, null));
        addBrokerDataHashCodeCase(cases, "unicode-name", brokerData(null, "\uD800\uDC00-name",
            new HashMap<>(), true, "ignored"));
        addBrokerDataHashCodeCase(cases, "long-keys-unicode-values",
            brokerData("ignored-cluster", "broker", addresses, false, "ignored-zone"));
        return cases;
    }

    private static List<Object> brokerDataToStringCases() {
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(2L, "slave:10911");
        addresses.put(0L, "master:10911");
        addresses.put(1L, null);
        List<Object> cases = new ArrayList<>();
        addBrokerDataToStringCase(cases, brokerData(null, null, null, false, null));
        addBrokerDataToStringCase(cases, brokerData("cluster", "broker", addresses, true, "zone"));
        return cases;
    }

    private static void addBrokerDataToStringCase(List<Object> cases,
        org.apache.rocketmq.remoting.protocol.route.BrokerData data) {
        Map<String, Object> item = new TreeMap<>();
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(data,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        item.put("toString", data.toString());
        cases.add(item);
    }

    private static void addBrokerDataHashCodeCase(List<Object> cases, String id,
        org.apache.rocketmq.remoting.protocol.route.BrokerData data) {
        Map<String, Object> item = new TreeMap<>();
        item.put("id", id);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(data,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
        item.put("hashCode", data.hashCode());
        cases.add(item);
    }

    private static org.apache.rocketmq.remoting.protocol.route.QueueData queueData(
        String brokerName, int read, int write, int perm, int flag) {
        org.apache.rocketmq.remoting.protocol.route.QueueData data =
            new org.apache.rocketmq.remoting.protocol.route.QueueData();
        data.setBrokerName(brokerName);
        data.setReadQueueNums(read);
        data.setWriteQueueNums(write);
        data.setPerm(perm);
        data.setTopicSysFlag(flag);
        return data;
    }

    private static List<Object> queueDataMethodCases() {
        org.apache.rocketmq.remoting.protocol.route.QueueData[][] pairs = new org.apache.rocketmq.remoting.protocol.route.QueueData[][] {
            {queueData(null, 0, 0, 0, 0), queueData(null, 0, 0, 0, 0)},
            {queueData("a", 1, 2, 4, 8), queueData("c", 1, 2, 4, 8)},
            {queueData("a", 1, 2, 4, 8), queueData("abcd", 1, 2, 4, 8)},
            {queueData("\uD800\uDC00", 1, 2, 4, 8), queueData("\uE000", 1, 2, 4, 8)},
            {queueData("same", 1, 2, 4, 8), queueData("same", 9, 8, 7, 6)}
        };
        List<Object> cases = new ArrayList<>();
        for (org.apache.rocketmq.remoting.protocol.route.QueueData[] pair : pairs) {
            Map<String, Object> item = new TreeMap<>();
            item.put("left", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(pair[0],
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue)));
            item.put("right", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(pair[1],
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue)));
            item.put("equals", pair[0].equals(pair[1]));
            item.put("leftHashCode", pair[0].hashCode());
            item.put("rightHashCode", pair[1].hashCode());
            item.put("leftToString", pair[0].toString());
            item.put("rightToString", pair[1].toString());
            try {
                item.put("result", pair[0].compareTo(pair[1]));
                item.put("outcome", "OK");
            } catch (NullPointerException error) {
                item.put("outcome", "NullPointerException");
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> topicQueueMappingMethodsCases() {
        String populated = "{\"topic\":\"topic\",\"scope\":\"__global__\",\"totalQueues\":3,"
            + "\"bname\":\"broker\",\"epoch\":4,\"dirty\":false,\"currIdMap\":{\"0\":0,\"1\":2,\"2\":4}}";
        String[][] pairs = new String[][] {
            {"{}", "{}"},
            {"{\"topic\":null,\"scope\":null,\"totalQueues\":0,\"bname\":null,\"epoch\":0,\"dirty\":false,\"currIdMap\":null}",
                "{\"topic\":null,\"scope\":null,\"totalQueues\":0,\"bname\":null,\"epoch\":0,\"dirty\":false,\"currIdMap\":{}}"},
            {populated, populated},
            {populated, populated.replace("\"dirty\":false", "\"dirty\":true")}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] pair : pairs) {
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo left =
                com.alibaba.fastjson2.JSON.parseObject(pair[0],
                    org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class);
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo right =
                com.alibaba.fastjson2.JSON.parseObject(pair[1],
                    org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("left", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(left,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("right", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(right,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("equals", left.equals(right));
            item.put("leftHashCode", left.hashCode());
            item.put("rightHashCode", right.hashCode());
            item.put("leftToString", left.toString());
            item.put("rightToString", right.toString());
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> topicRouteDataMethodCases() {
        String populated = "{\"orderTopicConf\":\"broker:2\",\"queueDatas\":[{\"brokerName\":\"broker\","
            + "\"readQueueNums\":2,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":1}],"
            + "\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\","
            + "\"brokerAddrs\":{\"0\":\"master:10911\"},\"zoneName\":\"zone\",\"enableActingMaster\":false}],"
            + "\"filterServerTable\":{\"broker\":[\"filter-a\",\"filter-b\"]},"
            + "\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":\"topic\",\"scope\":\"__global__\","
            + "\"totalQueues\":2,\"bname\":\"broker\",\"epoch\":1,\"dirty\":false,\"currIdMap\":{\"0\":0,\"1\":1}}}}";
        String metadataOnly = populated.replace("\"cluster\":\"cluster\"", "\"cluster\":\"other\"")
            .replace("\"zoneName\":\"zone\"", "\"zoneName\":\"other\"")
            .replace("\"enableActingMaster\":false", "\"enableActingMaster\":true");
        String differentQueue = populated.replace("\"perm\":6", "\"perm\":4");
        String nullCollections = "{\"queueDatas\":null,\"brokerDatas\":null,\"filterServerTable\":null,"
            + "\"topicQueueMappingByBroker\":null}";
        String emptyCollections = "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{},"
            + "\"topicQueueMappingByBroker\":{}}";
        String nullElements = "{\"queueDatas\":[null],\"brokerDatas\":[null],"
            + "\"filterServerTable\":{\"broker\":[\"filter-a\",null]},"
            + "\"topicQueueMappingByBroker\":{\"broker\":null}}";
        String nullFilterList = "{\"filterServerTable\":{\"broker\":null}}";
        String[][] pairs = new String[][] {
            {"{}", "{}"},
            {nullCollections, emptyCollections},
            {populated, metadataOnly},
            {populated, differentQueue},
            {nullElements, nullElements},
            {nullElements, emptyCollections},
            {nullFilterList, nullFilterList}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] pair : pairs) {
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData left =
                com.alibaba.fastjson2.JSON.parseObject(pair[0],
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData right =
                com.alibaba.fastjson2.JSON.parseObject(pair[1],
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("left", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(left,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("right", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(right,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("equals", left.equals(right));
            item.put("leftHashCode", left.hashCode());
            item.put("rightHashCode", right.hashCode());
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> topicRouteDataToStringCases() {
        String populated = "{\"orderTopicConf\":\"broker:1\",\"queueDatas\":[{\"brokerName\":\"broker\","
            + "\"readQueueNums\":1,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":0}],"
            + "\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\","
            + "\"brokerAddrs\":{\"0\":\"master:10911\"}}],"
            + "\"filterServerTable\":{\"broker\":[\"filter-a\",\"filter-b\"]},"
            + "\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":\"topic\",\"scope\":\"__global__\","
            + "\"totalQueues\":2,\"bname\":\"broker\",\"epoch\":1,\"dirty\":false,\"currIdMap\":{\"0\":0}}}}";
        String nullCollections = "{\"orderTopicConf\":null,\"queueDatas\":null,\"brokerDatas\":null,"
            + "\"filterServerTable\":null,\"topicQueueMappingByBroker\":null}";
        String nullElements = "{\"queueDatas\":[null],\"brokerDatas\":[null],"
            + "\"filterServerTable\":{\"broker\":[\"filter-a\",null]},"
            + "\"topicQueueMappingByBroker\":{\"broker\":null}}";
        String nullFilterList = "{\"filterServerTable\":{\"broker\":null}}";
        String[] inputs = new String[] {"{}", nullCollections, populated, nullElements, nullFilterList};
        List<Object> cases = new ArrayList<>();
        for (String input : inputs) {
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData route =
                com.alibaba.fastjson2.JSON.parseObject(input,
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(route,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
            item.put("toString", route.toString());
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> brokerDataCompareToCases() {
        List<Object> cases = new ArrayList<>();
        String[][] pairs = new String[][] {
            {"alpha", "beta"},
            {"a", "abcd"},
            {"\uD800\uDC00", "\uE000"},
            {"same", "same"},
            {null, "broker"},
            {"broker", null}
        };
        for (String[] pair : pairs) {
            org.apache.rocketmq.remoting.protocol.route.BrokerData left = brokerData(
                "left-cluster", pair[0], null, false, "left-zone");
            org.apache.rocketmq.remoting.protocol.route.BrokerData right = brokerData(
                "right-cluster", pair[1], null, true, "right-zone");
            Map<String, Object> item = new TreeMap<>();
            item.put("left", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(left,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue)));
            item.put("right", com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(right,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue)));
            try {
                item.put("result", left.compareTo(right));
                item.put("outcome", "OK");
            } catch (NullPointerException error) {
                item.put("outcome", "NullPointerException");
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> topicRouteDataChangedCases() {
        String emptyPrefix = "{\"queueDatas\":[],\"filterServerTable\":{},\"brokerDatas\":";
        String singleNull = emptyPrefix + "[{\"brokerName\":null,\"brokerAddrs\":{}}]}";
        String multipleWithNull = emptyPrefix
            + "[{\"brokerName\":null,\"brokerAddrs\":{}},{\"brokerName\":\"broker\",\"brokerAddrs\":{}}]}";
        String ordered = emptyPrefix
            + "[{\"brokerName\":\"a\",\"brokerAddrs\":{}},{\"brokerName\":\"b\",\"brokerAddrs\":{}}]}";
        String reversed = emptyPrefix
            + "[{\"brokerName\":\"b\",\"brokerAddrs\":{}},{\"brokerName\":\"a\",\"brokerAddrs\":{}}]}";
        String nullQueue = "{\"queueDatas\":[{\"brokerName\":null}],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String multipleQueuesWithNull = "{\"queueDatas\":[{\"brokerName\":null},{\"brokerName\":\"broker\"}],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String orderedQueues = "{\"queueDatas\":[{\"brokerName\":\"a\"},{\"brokerName\":\"b\"}],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String reversedQueues = "{\"queueDatas\":[{\"brokerName\":\"b\"},{\"brokerName\":\"a\"}],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String oneNullQueue = "{\"queueDatas\":[null],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String multipleWithNullQueue = "{\"queueDatas\":[null,{\"brokerName\":\"broker\"}],\"filterServerTable\":{},\"brokerDatas\":[]}";
        String oneNullBroker = "{\"queueDatas\":[],\"filterServerTable\":{},\"brokerDatas\":[null]}";
        String multipleWithNullBroker = "{\"queueDatas\":[],\"filterServerTable\":{},\"brokerDatas\":[null,{\"brokerName\":\"broker\",\"brokerAddrs\":{}}]}";
        String[][] pairs = new String[][] {
            {singleNull, singleNull},
            {multipleWithNull, multipleWithNull},
            {reversed, ordered},
            {nullQueue, nullQueue},
            {multipleQueuesWithNull, multipleQueuesWithNull},
            {reversedQueues, orderedQueues},
            {oneNullQueue, oneNullQueue},
            {multipleWithNullQueue, multipleWithNullQueue},
            {oneNullBroker, oneNullBroker},
            {multipleWithNullBroker, multipleWithNullBroker}
        };
        List<Object> cases = new ArrayList<>();
        for (String[] pair : pairs) {
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData current =
                com.alibaba.fastjson2.JSON.parseObject(pair[0],
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData old =
                com.alibaba.fastjson2.JSON.parseObject(pair[1],
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("current", com.alibaba.fastjson2.JSON.parseObject(pair[0]));
            item.put("old", com.alibaba.fastjson2.JSON.parseObject(pair[1]));
            try {
                item.put("changed", current.topicRouteDataChanged(old));
                item.put("outcome", "OK");
            } catch (NullPointerException error) {
                item.put("outcome", "NullPointerException");
            }
            cases.add(item);
        }
        org.apache.rocketmq.remoting.protocol.route.TopicRouteData current =
            com.alibaba.fastjson2.JSON.parseObject("{}",
                org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
        Map<String, Object> nullOld = new TreeMap<>();
        nullOld.put("current", com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(current,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue)));
        nullOld.put("old", null);
        nullOld.put("changed", current.topicRouteDataChanged(null));
        nullOld.put("outcome", "OK");
        cases.add(nullOld);
        return cases;
    }

    private static List<Object> topicRouteDataCloneCases() {
        String[] inputs = new String[] {
            "{}",
            "{\"queueDatas\":null,\"brokerDatas\":null,\"filterServerTable\":null}",
            "{\"queueDatas\":[null],\"brokerDatas\":[null],\"filterServerTable\":{\"broker\":null}}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{},\"topicQueueMappingByBroker\":{\"broker\":null}}",
            "{\"queueDatas\":[{\"brokerName\":\"broker\"}],\"brokerDatas\":[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master\"}}],"
                + "\"filterServerTable\":{\"broker\":[\"filter\"]},\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":\"topic\","
                + "\"scope\":\"__global__\",\"totalQueues\":1,\"bname\":\"broker\",\"epoch\":1,\"dirty\":false,\"currIdMap\":{\"0\":0}}}}",
            "{\"queueDatas\":[],\"brokerDatas\":[null],\"filterServerTable\":{}}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{\"broker\":null}}",
            "{\"queueDatas\":[],\"brokerDatas\":[],\"filterServerTable\":{},\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":\"topic\","
                + "\"totalQueues\":1,\"bname\":\"broker\",\"epoch\":1,\"currIdMap\":null}}}"
        };
        List<Object> cases = new ArrayList<>();
        for (int index = 0; index < inputs.length; index++) {
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData source =
                com.alibaba.fastjson2.JSON.parseObject(inputs[index],
                    org.apache.rocketmq.remoting.protocol.route.TopicRouteData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("id", "clone-" + index);
            item.put("input", com.alibaba.fastjson2.JSON.parseObject(inputs[index]));
            org.apache.rocketmq.remoting.protocol.route.TopicRouteData copy =
                new org.apache.rocketmq.remoting.protocol.route.TopicRouteData(source);
            item.put("copyConstructor", routeJson(copy));
            item.put("copyQueueElementShared", source.getQueueDatas() != null && !source.getQueueDatas().isEmpty()
                && source.getQueueDatas().get(0) == copy.getQueueDatas().get(0));
            try {
                org.apache.rocketmq.remoting.protocol.route.TopicRouteData clone =
                    source.cloneTopicRouteData();
                item.put("cloneOutcome", "OK");
                item.put("clone", routeJson(clone));
                item.put("cloneQueueElementShared", source.getQueueDatas() != null && !source.getQueueDatas().isEmpty()
                    && source.getQueueDatas().get(0) == clone.getQueueDatas().get(0));
            } catch (NullPointerException error) {
                item.put("cloneOutcome", "NullPointerException");
                item.put("clone", null);
                item.put("cloneQueueElementShared", false);
            }
            try {
                org.apache.rocketmq.remoting.protocol.route.TopicRouteData clone =
                    source.deepCloneTopicRouteData();
                item.put("deepCloneOutcome", "OK");
                item.put("deepClone", routeJson(clone));
                item.put("deepCloneQueueElementShared", source.getQueueDatas() != null && !source.getQueueDatas().isEmpty()
                    && source.getQueueDatas().get(0) == clone.getQueueDatas().get(0));
            } catch (NullPointerException error) {
                item.put("deepCloneOutcome", "NullPointerException");
                item.put("deepClone", null);
                item.put("deepCloneQueueElementShared", false);
            }
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> routeJson(org.apache.rocketmq.remoting.protocol.route.TopicRouteData route) {
        return new TreeMap<>(com.alibaba.fastjson2.JSON.parseObject(com.alibaba.fastjson2.JSON.toJSONString(route,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteNonStringKeyAsString)));
    }

    private static <T extends org.apache.rocketmq.remoting.protocol.RemotingSerializable>
        Map<String, Object> inheritedSerializationCase(String id, String rawInput, Class<T> type) {
        T value = com.alibaba.fastjson2.JSON.parseObject(rawInput, type);
        String instanceJson = value.toJson();
        String prettyJson = value.toJson(true);
        byte[] instanceBytes = value.encode();
        byte[] staticBytes = org.apache.rocketmq.remoting.protocol.RemotingSerializable.encode(value);
        byte[] featureBytes = value.encode(new com.alibaba.fastjson2.JSONWriter.Feature[0]);
        T decoded = org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(instanceBytes, type);
        T fromJson = org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson(instanceJson, type);
        List<T> decodedList = org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(
            com.alibaba.fastjson2.JSON.toJSONBytes(java.util.Collections.singletonList(value)), type);

        Map<String, Object> item = new TreeMap<>();
        item.put("id", id);
        item.put("rawInput", rawInput);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(rawInput));
        item.put("instanceJson", instanceJson);
        item.put("instancePrettyJson", prettyJson);
        item.put("staticJson", org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(value, false));
        item.put("staticPrettyJson", org.apache.rocketmq.remoting.protocol.RemotingSerializable.toJson(value, true));
        item.put("instanceEncodedJson", new String(instanceBytes, java.nio.charset.StandardCharsets.UTF_8));
        item.put("staticEncodedJson", new String(staticBytes, java.nio.charset.StandardCharsets.UTF_8));
        item.put("featureEncodedJson", new String(featureBytes, java.nio.charset.StandardCharsets.UTF_8));
        item.put("decodedJson", decoded.toJson());
        item.put("fromJsonJson", fromJson.toJson());
        item.put("decodedListJson", decodedList.get(0).toJson());
        item.put("decodedListCount", decodedList.size());
        return item;
    }

    private static List<Object> inheritedSerializationCases() {
        List<Object> cases = new ArrayList<>();
        String[][] routeInputs = new String[][] {
            {"route-default", "{}"},
            {"route-null", "{\"orderTopicConf\":null,\"queueDatas\":null,\"brokerDatas\":null,\"filterServerTable\":null,\"topicQueueMappingByBroker\":null}"},
            {"route-populated", "{\"orderTopicConf\":\"broker:2\",\"queueDatas\":[{\"brokerName\":\"broker\",\"readQueueNums\":2,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":1}],\"brokerDatas\":[{\"cluster\":\"cluster\",\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master:10911\",\"2\":\"slave:10911\"}}],\"filterServerTable\":{\"broker\":[\"filter-a\",\"filter-b\"]},\"topicQueueMappingByBroker\":{\"broker\":{\"topic\":\"topic\",\"totalQueues\":2,\"bname\":\"broker\",\"epoch\":1,\"dirty\":false,\"scope\":\"__global__\",\"currIdMap\":{\"0\":0,\"1\":1}}}}"}
        };
        for (String[] entry : routeInputs) {
            cases.add(inheritedSerializationCase(entry[0], entry[1], TopicRouteData.class));
        }
        String[][] mappingInputs = new String[][] {
            {"mapping-default", "{}"},
            {"mapping-null", "{\"topic\":null,\"totalQueues\":0,\"bname\":null,\"epoch\":0,\"dirty\":false,\"scope\":null,\"currIdMap\":null}"},
            {"mapping-populated", "{\"topic\":\"topic\",\"totalQueues\":2,\"bname\":\"broker\",\"epoch\":1,\"dirty\":false,\"scope\":\"__global__\",\"currIdMap\":{\"0\":0,\"1\":1}}"}
        };
        for (String[] entry : mappingInputs) {
            cases.add(inheritedSerializationCase(entry[0], entry[1],
                org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class));
        }
        return cases;
    }

    private static Map<String, Object> inheritedSerializationNullUtilities() {
        Map<String, Object> item = new TreeMap<>();
        byte[] jsonNull = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        item.put("decodeNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(null, TopicRouteData.class) == null);
        item.put("decodeJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(jsonNull, TopicRouteData.class) == null);
        item.put("fromJsonNullInputReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson(null, TopicRouteData.class) == null);
        item.put("fromJsonJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson("null", TopicRouteData.class) == null);
        item.put("decodeListNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(null, TopicRouteData.class) == null);
        item.put("decodeListJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(jsonNull, TopicRouteData.class) == null);
        List<TopicRouteData> nullMemberList = org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(
            "[null]".getBytes(java.nio.charset.StandardCharsets.UTF_8), TopicRouteData.class);
        item.put("decodeListNullMemberCount", nullMemberList.size());
        item.put("decodeListHasNullMember", nullMemberList.get(0) == null);

        Class<org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo> mappingType =
            org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo.class;
        item.put("mappingDecodeNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(null, mappingType) == null);
        item.put("mappingDecodeJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(jsonNull, mappingType) == null);
        item.put("mappingFromJsonNullInputReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson(null, mappingType) == null);
        item.put("mappingFromJsonJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.fromJson("null", mappingType) == null);
        item.put("mappingDecodeListNullBytesReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(null, mappingType) == null);
        item.put("mappingDecodeListJsonNullReturnsNull",
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(jsonNull, mappingType) == null);
        List<org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo> nullMappingMemberList =
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decodeList(
                "[null]".getBytes(java.nio.charset.StandardCharsets.UTF_8), mappingType);
        item.put("mappingDecodeListNullMemberCount", nullMappingMemberList.size());
        item.put("mappingDecodeListHasNullMember", nullMappingMemberList.get(0) == null);
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
        List<Map<String, Object>> staticInputs = new ArrayList<>();
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
            staticInputs.add(input);
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
        List<Object> vipCases = new ArrayList<>();
        for (String address : new String[] {"missing", "", ":10911", "broker:2", "broker:0", "broker:-1",
            "broker:65536", "broker:2147483647", "broker:-2147483648", "broker:+2", "broker: 2",
            "broker:١٠٩١١", "not:ipv6:10911", "broker:", "broker:999999999999"}) {
            Map<String, Object> item = new TreeMap<>();
            item.put("address", address);
            item.put("disabled", MixAll.brokerVIPChannel(false, address));
            try {
                item.put("enabledOutcome", "OK");
                item.put("enabledValue", MixAll.brokerVIPChannel(true, address));
            } catch (RuntimeException error) {
                item.put("enabledOutcome", error.getClass().getSimpleName());
                item.put("enabledValue", null);
            }
            vipCases.add(item);
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "MessageQueue.equals/hashCode/toString/compareTo; BrokerData.equals/hashCode; MQClientInstance.topicRouteData2TopicPublishInfo/topicRouteData2TopicSubscribeInfo/updateTopicRouteInfoFromNameServer/getTopicRouteTable/getAnExistTopicRouteData/queryTopicRouteData/cleanOfflineBroker/findBrokerAddressInPublish/findBrokerAddressInAdmin/findBrokerAddressInSubscribe/findBrokerAddrByTopic; ConcurrentMap put/putIfAbsent/replace/remove/clear; TopicRouteData inherited RemotingSerializable encode/toJson/decode/decodeList/fromJson; ClientMetadata.topicRouteData2EndpointsForStaticTopic; MixAll.brokerVIPChannel; Integer.parseInt/Character.digit");
        fixture.put("serverBaselineContacted", false);
        fixture.put("cases", cases);
        fixture.put("vip", vip);
        fixture.put("vipCases", vipCases);
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
        fixture.put("runtimeNullBrokerValues", runtimeNullBrokerValues());
        fixture.put("runtimeNullNestedRouteElements", runtimeNullNestedRouteElements());
        fixture.put("nullSubscriptionTopicRefresh", nullSubscriptionTopicRefresh());
        fixture.put("nullableRouteCollections", nullableRouteCollections());
        fixture.put("nullableNestedRouteFields", nullableNestedRouteFields());
        fixture.put("nullableMessageQueueFields", nullableMessageQueueFields());
        fixture.put("messageQueueCompareTo", messageQueueCompareToCases());
        fixture.put("messageQueueMethods", messageQueueMethodCases());
        fixture.put("nullableStaticMappingFields", nullableStaticMappingFields());
        fixture.put("staticMappingMapEntryCases", staticMappingMapEntryCases());
        fixture.put("routeDtoInputCases", routeDtoInputCases());
        fixture.put("nullStaticMappingConversion", nullStaticMappingConversion());
        fixture.put("overlappingStaticMappingOwners", overlappingStaticMappingOwners());
        fixture.put("runtimeNullableCollections", runtimeNullableCollections());
        fixture.put("runtimeEmptyEndpointTransition", runtimeEmptyEndpointTransition(staticInputs.get(0), inputs.get(0)));
        fixture.put("runtimeTopicRouteTable", runtimeTopicRouteTable(inputs.get(0)));
        fixture.put("runtimeRawOfflineCleanup", runtimeRawOfflineCleanup(inputs.get(0)));
        fixture.put("brokerDataEquality", brokerDataEqualityCases());
        fixture.put("brokerDataSelectAddr", brokerDataSelectAddrCases());
        fixture.put("brokerDataHashCode", brokerDataHashCodeCases());
        fixture.put("brokerDataToString", brokerDataToStringCases());
        fixture.put("queueDataMethods", queueDataMethodCases());
        fixture.put("topicQueueMappingMethods", topicQueueMappingMethodsCases());
        fixture.put("topicRouteDataMethods", topicRouteDataMethodCases());
        fixture.put("topicRouteDataToString", topicRouteDataToStringCases());
        fixture.put("inheritedSerializationCases", inheritedSerializationCases());
        fixture.put("inheritedSerializationNullUtilities", inheritedSerializationNullUtilities());
        fixture.put("brokerDataCompareTo", brokerDataCompareToCases());
        fixture.put("topicRouteDataChanged", topicRouteDataChangedCases());
        fixture.put("topicRouteDataClone", topicRouteDataCloneCases());
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
