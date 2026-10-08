package compat;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
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
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;

/** Invokes actual runtime registration and private heartbeat preparation.
 * The transport is never started; finally releases all constructed resources. */
public final class RuntimeHeartbeatFixtures {
    private RuntimeHeartbeatFixtures() { }

    private static List<Object> subscriptionFieldCases() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{}",
            "{\"topic\":null,\"subString\":null,\"tagsSet\":null,\"codeSet\":null,\"expressionType\":null,\"subVersion\":0}",
            "{\"topic\":\"T\",\"subString\":\"tag\",\"tagsSet\":[\"tag\"],\"codeSet\":[7],\"expressionType\":\"SQL92\",\"subVersion\":42}",
            "{\"topic\":\"null-members\",\"tagsSet\":[null,\"tag\",null],\"codeSet\":[null,7,null]}",
            "{\"topic\":\"filter-source\",\"filterClassSource\":\"source-from-json\"}"
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
            item.put("filterClassSource", value.getFilterClassSource());
            item.put("filterClassSourceNull", value.getFilterClassSource() == null);
            item.put("tagsEmpty", value.getTagsSet() != null && value.getTagsSet().isEmpty());
            item.put("codeEmpty", value.getCodeSet() != null && value.getCodeSet().isEmpty());
            item.put("tagsCount", value.getTagsSet() == null ? null : value.getTagsSet().size());
            item.put("codeCount", value.getCodeSet() == null ? null : value.getCodeSet().size());
            item.put("tagsHasNull", value.getTagsSet() != null && value.getTagsSet().contains(null));
            item.put("codeHasNull", value.getCodeSet() != null && value.getCodeSet().contains(null));
            item.put("versionCurrent", value.getSubVersion() >= before && value.getSubVersion() <= after);
            item.put("versionZero", value.getSubVersion() == 0L);
            item.put("versionFortyTwo", value.getSubVersion() == 42L);
            item.put("encodedHasTopic", encoded.containsKey("topic"));
            item.put("encodedHasSubString", encoded.containsKey("subString"));
            item.put("encodedHasTags", encoded.containsKey("tagsSet"));
            item.put("encodedHasCode", encoded.containsKey("codeSet"));
            item.put("encodedHasExpression", encoded.containsKey("expressionType"));
            item.put("encodedHasFilterClassSource", encoded.containsKey("filterClassSource"));
            item.put("encodedFilterClassSource", encoded.get("filterClassSource"));
            if (value.getTopic() != null && value.getTopic().equals("null-members")) {
                item.put("encodedTags", encoded.get("tagsSet"));
                item.put("encodedCode", encoded.get("codeSet"));
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> subscriptionConstructorCases() {
        List<Object> cases = new ArrayList<>();
        SubscriptionData[] values = {
            new SubscriptionData(),
            new SubscriptionData("topic", "tag"),
            new SubscriptionData(null, null)
        };
        String[] names = {"no-argument", "two-argument", "two-null-arguments"};
        for (int i = 0; i < values.length; i++) {
            SubscriptionData value = values[i];
            Map<String, Object> item = new TreeMap<>();
            item.put("case", names[i]);
            item.put("topic", value.getTopic());
            item.put("subString", value.getSubString());
            item.put("tagsEmpty", value.getTagsSet() != null && value.getTagsSet().isEmpty());
            item.put("codeEmpty", value.getCodeSet() != null && value.getCodeSet().isEmpty());
            item.put("expressionType", value.getExpressionType());
            item.put("versionCurrent", value.getSubVersion() > 0);
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> subscriptionMethods() {
        Map<String, Object> result = new TreeMap<>();
        result.put("subAll", SubscriptionData.SUB_ALL);
        List<Object> values = new ArrayList<>();
        for (String[] input : new String[][] {
            {"defaults", "{\"topic\":\"topic\",\"subString\":\"tag\",\"subVersion\":42}"},
            {"nullable", "{\"classFilterMode\":true,\"topic\":null,\"subString\":null,\"tagsSet\":null,\"codeSet\":null,\"subVersion\":-9,\"expressionType\":null}"},
            {"unicode", "{\"topic\":\"🚀\",\"subString\":\"Ω\",\"tagsSet\":[\"tag\"],\"codeSet\":[7],\"subVersion\":9223372036854775807,\"expressionType\":\"SQL92\"}"},
            {"sets", "{\"topic\":\"topic\",\"subString\":\"tagA||tagB\",\"tagsSet\":[\"alpha\",\"beta\"],\"codeSet\":[7,11],\"subVersion\":0,\"expressionType\":\"TAG\"}"},
            {"null-members", "{\"topic\":\"null-members\",\"subString\":\"tag\",\"tagsSet\":[null,\"alpha\"],\"codeSet\":[null,7],\"subVersion\":0,\"expressionType\":\"TAG\"}"}
        }) {
            SubscriptionData value = com.alibaba.fastjson2.JSON.parseObject(input[1], SubscriptionData.class);
            value.setFilterClassSource("ignored-in-text-and-equality");
            Map<String, Object> item = new TreeMap<>();
            item.put("case", input[0]);
            item.put("rawInput", input[1]);
            item.put("hashCode", value.hashCode());
            item.put("toString", value.toString());
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            item.put("encodedHasFilterClassSource", encoded.containsKey("filterClassSource"));
            item.put("encodedFilterClassSource", encoded.get("filterClassSource"));
            values.add(item);
        }
        result.put("values", values);

        String base = "{\"classFilterMode\":false,\"topic\":\"topic\",\"subString\":\"tag\","
            + "\"tagsSet\":[\"alpha\",\"beta\"],\"codeSet\":[7,11],\"subVersion\":42,"
            + "\"expressionType\":\"SQL92\"}";
        String nullSets = base.replace("\"tagsSet\":[\"alpha\",\"beta\"]", "\"tagsSet\":null")
            .replace("\"codeSet\":[7,11]", "\"codeSet\":null");
        String emptySets = base.replace("\"tagsSet\":[\"alpha\",\"beta\"]", "\"tagsSet\":[]")
            .replace("\"codeSet\":[7,11]", "\"codeSet\":[]");
        List<Object> equalCases = new ArrayList<>();
        for (String[] input : new String[][] {
            {"filter-class-source-ignored", base, base, "source-a", "source-b"},
            {"sub-version-compared-but-not-hashed", base, base.replace("\"subVersion\":42", "\"subVersion\":43"), "source", "source"},
            {"null-versus-empty-sets", nullSets, emptySets, "source", "source"},
            {"class-filter-mode", base, base.replace("\"classFilterMode\":false", "\"classFilterMode\":true"), "source", "source"},
            {"topic", base, base.replace("\"topic\":\"topic\"", "\"topic\":\"other\""), "source", "source"},
            {"expression-type", base, base.replace("\"expressionType\":\"SQL92\"", "\"expressionType\":\"TAG\""), "source", "source"},
            {"tags-set", base, base.replace("\"beta\"", "\"gamma\""), "source", "source"},
            {"tags-null-member", base, base.replace("\"tagsSet\":[\"alpha\",\"beta\"]", "\"tagsSet\":[\"alpha\",null]"), "source", "source"},
            {"code-set", base, base.replace("\"codeSet\":[7,11]", "\"codeSet\":[7,12]"), "source", "source"},
            {"code-null-member", base, base.replace("\"codeSet\":[7,11]", "\"codeSet\":[7,null]"), "source", "source"},
            {"subscription-string", base, base.replace("\"subString\":\"tag\"", "\"subString\":\"other\""), "source", "source"}
        }) {
            SubscriptionData left = com.alibaba.fastjson2.JSON.parseObject(input[1], SubscriptionData.class);
            SubscriptionData right = com.alibaba.fastjson2.JSON.parseObject(input[2], SubscriptionData.class);
            left.setFilterClassSource(input[3]);
            right.setFilterClassSource(input[4]);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", input[0]);
            item.put("leftInput", input[1]);
            item.put("rightInput", input[2]);
            item.put("leftFilterClassSource", input[3]);
            item.put("rightFilterClassSource", input[4]);
            item.put("equal", left.equals(right));
            item.put("leftHashCode", left.hashCode());
            item.put("rightHashCode", right.hashCode());
            equalCases.add(item);
        }
        result.put("equalCases", equalCases);

        List<Object> compareCases = new ArrayList<>();
        for (String[] input : new String[][] {
            {"null-concatenation", "{\"topic\":null,\"subString\":null}", "{\"topic\":\"null\",\"subString\":\"null\"}"},
            {"utf16-order", "{\"topic\":\"\uE000\",\"subString\":\"x\"}", "{\"topic\":\"🚀\",\"subString\":\"x\"}"},
            {"delimiter-collision", "{\"topic\":\"A@\",\"subString\":\"B\"}", "{\"topic\":\"A\",\"subString\":\"@B\"}"},
            {"prefix", "{\"topic\":\"a\",\"subString\":\"\"}", "{\"topic\":\"a\",\"subString\":\"x\"}"}
        }) {
            SubscriptionData left = com.alibaba.fastjson2.JSON.parseObject(input[1], SubscriptionData.class);
            SubscriptionData right = com.alibaba.fastjson2.JSON.parseObject(input[2], SubscriptionData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", input[0]);
            item.put("leftInput", input[1]);
            item.put("rightInput", input[2]);
            item.put("comparison", left.compareTo(right));
            compareCases.add(item);
        }
        result.put("compareCases", compareCases);
        return result;
    }

    private static List<Object> identityMethodCases() throws Exception {
        ProducerData producerLeft = new ProducerData();
        producerLeft.setGroupName("same");
        ProducerData producerRight = new ProducerData();
        producerRight.setGroupName("same");

        ConsumerData consumerLeft = new ConsumerData();
        consumerLeft.setGroupName("same");
        ConsumerData consumerRight = new ConsumerData();
        consumerRight.setGroupName("same");

        HeartbeatData heartbeatLeft = new HeartbeatData();
        heartbeatLeft.setClientID("same");
        HeartbeatData heartbeatRight = new HeartbeatData();
        heartbeatRight.setClientID("same");

        List<Object> cases = new ArrayList<>();
        for (Object[] pair : new Object[][] {
            {"ProducerData", producerLeft, producerRight},
            {"ConsumerData", consumerLeft, consumerRight},
            {"HeartbeatData", heartbeatLeft, heartbeatRight}
        }) {
            Object left = pair[1];
            Object right = pair[2];
            Map<String, Object> item = new TreeMap<>();
            item.put("class", pair[0]);
            item.put("equalsDeclaringClass", left.getClass().getMethod("equals", Object.class)
                .getDeclaringClass().getName());
            item.put("hashCodeDeclaringClass", left.getClass().getMethod("hashCode")
                .getDeclaringClass().getName());
            item.put("sameInstanceEquals", left.equals(left));
            item.put("equalFieldsDistinctInstanceEquals", left.equals(right));
            java.util.Set<Object> values = new java.util.HashSet<>();
            values.add(left);
            values.add(right);
            item.put("equalFieldsSetSize", values.size());
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> ownerFieldCases() {
        Map<String, Object> result = new TreeMap<>();
        List<Object> producers = new ArrayList<>();
        for (String input : new String[] {"{}", "{\"groupName\":null}", "{\"groupName\":\"group\"}"}) {
            ProducerData value = com.alibaba.fastjson2.JSON.parseObject(input, ProducerData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("rawInput", input);
            item.put("groupNull", value.getGroupName() == null);
            item.put("toString", value.toString());
            item.put("encodedHasGroup", encoded.containsKey("groupName"));
            producers.add(item);
        }
        result.put("producers", producers);
        List<Object> consumers = new ArrayList<>();
        for (String input : new String[] {
            "{}",
            "{\"groupName\":null,\"consumeType\":null,\"messageModel\":null,\"consumeFromWhere\":null,\"subscriptionDataSet\":null}",
            "{\"groupName\":\"group\",\"consumeType\":\"CONSUME_ACTIVELY\",\"messageModel\":\"CLUSTERING\",\"consumeFromWhere\":\"CONSUME_FROM_LAST_OFFSET\",\"subscriptionDataSet\":[],\"unitMode\":true}",
            "{\"groupName\":\"nested\",\"consumeType\":\"CONSUME_PASSIVELY\",\"messageModel\":\"BROADCASTING\",\"consumeFromWhere\":\"CONSUME_FROM_TIMESTAMP\",\"subscriptionDataSet\":[{\"topic\":\"topic\",\"subString\":\"tag\",\"tagsSet\":[\"tag\"],\"codeSet\":[7],\"subVersion\":42,\"expressionType\":\"SQL92\"}],\"unitMode\":false}"
        }) {
            ConsumerData value = com.alibaba.fastjson2.JSON.parseObject(input, ConsumerData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("rawInput", input);
            item.put("toString", value.toString());
            item.put("groupNull", value.getGroupName() == null);
            item.put("consumeTypeNull", value.getConsumeType() == null);
            item.put("messageModelNull", value.getMessageModel() == null);
            item.put("consumeFromWhereNull", value.getConsumeFromWhere() == null);
            item.put("subscriptionsNull", value.getSubscriptionDataSet() == null);
            item.put("subscriptionsEmpty", value.getSubscriptionDataSet() != null && value.getSubscriptionDataSet().isEmpty());
            item.put("unitMode", value.isUnitMode());
            item.put("encodedHasGroup", encoded.containsKey("groupName"));
            item.put("encodedHasConsumeType", encoded.containsKey("consumeType"));
            item.put("encodedHasMessageModel", encoded.containsKey("messageModel"));
            item.put("encodedHasConsumeFromWhere", encoded.containsKey("consumeFromWhere"));
            item.put("encodedHasSubscriptions", encoded.containsKey("subscriptionDataSet"));
            consumers.add(item);
        }
        result.put("consumers", consumers);
        return result;
    }

    private static List<Object> consumerEnumCases() {
        List<Object> cases = new ArrayList<>();
        for (ConsumeType value : ConsumeType.values())
            recordConsumerEnum(cases, "consumeType", value.name());
        for (MessageModel value : MessageModel.values())
            recordConsumerEnum(cases, "messageModel", value.name());
        for (ConsumeFromWhere value : ConsumeFromWhere.values())
            recordConsumerEnum(cases, "consumeFromWhere", value.name());

        for (String[] input : new String[][] {
            {"consumeType", "\"NOT_A_CONSUME_TYPE\""},
            {"consumeType", "\"consume_pop\""},
            {"consumeType", "99"},
            {"consumeType", "-1"},
            {"consumeType", "1.0"},
            {"consumeType", "1.9"},
            {"consumeType", "-0.5"},
            {"consumeType", "3.0"},
            {"messageModel", "\"NOT_A_MESSAGE_MODEL\""},
            {"messageModel", "99"},
            {"consumeFromWhere", "\"NOT_A_CONSUME_FROM_WHERE\""},
            {"consumeFromWhere", "99"}
        }) {
            recordConsumerEnumProbe(cases, input[0], input[1]);
        }
        for (int ordinal = 0; ordinal <= ConsumeType.values().length; ordinal++)
            recordConsumerEnumProbe(cases, "consumeType", Integer.toString(ordinal));
        for (int ordinal = 0; ordinal <= MessageModel.values().length; ordinal++)
            recordConsumerEnumProbe(cases, "messageModel", Integer.toString(ordinal));
        for (int ordinal = 0; ordinal <= ConsumeFromWhere.values().length; ordinal++)
            recordConsumerEnumProbe(cases, "consumeFromWhere", Integer.toString(ordinal));
        return cases;
    }

    private static List<Object> consumerEnumMethodCases() {
        List<Object> cases = new ArrayList<>();
        for (ConsumeType value : ConsumeType.values()) {
            Map<String, Object> item = new TreeMap<>();
            item.put("enum", "consumeType");
            item.put("constant", value.name());
            item.put("methodValue", value.getTypeCN());
            cases.add(item);
        }
        for (MessageModel value : MessageModel.values()) {
            Map<String, Object> item = new TreeMap<>();
            item.put("enum", "messageModel");
            item.put("constant", value.name());
            item.put("methodValue", value.getModeCN());
            cases.add(item);
        }
        return cases;
    }

    private static void recordConsumerEnumProbe(List<Object> cases, String field, String jsonValue) {
        String rawInput = "{\"" + field + "\":" + jsonValue + "}";
        Map<String, Object> item = new TreeMap<>();
        item.put("field", field);
        item.put("rawInput", rawInput);
        try {
            ConsumerData value = com.alibaba.fastjson2.JSON.parseObject(rawInput, ConsumerData.class);
            Object decoded = consumerEnumName(value, field);
            item.put("outcome", decoded == null ? "null" : "accepted");
            item.put("decodedName", decoded);
            item.put("encodedName", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value)).getString(field));
        } catch (Throwable error) {
            item.put("outcome", "rejected");
            item.put("error", error.getClass().getName());
        }
        cases.add(item);
    }

    private static void recordConsumerEnum(List<Object> cases, String field, String enumName) {
        String rawInput = "{\"" + field + "\":\"" + enumName + "\"}";
        ConsumerData value = com.alibaba.fastjson2.JSON.parseObject(rawInput, ConsumerData.class);
        Map<String, Object> item = new TreeMap<>();
        item.put("field", field);
        item.put("rawInput", rawInput);
        item.put("decodedName", consumerEnumName(value, field));
        item.put("encodedName", com.alibaba.fastjson2.JSON.parseObject(
            com.alibaba.fastjson2.JSON.toJSONString(value)).getString(field));
        item.put("outcome", "accepted");
        cases.add(item);
    }

    private static String consumerEnumName(ConsumerData value, String field) {
        switch (field) {
            case "consumeType": return value.getConsumeType() == null ? null : value.getConsumeType().name();
            case "messageModel": return value.getMessageModel() == null ? null : value.getMessageModel().name();
            case "consumeFromWhere": return value.getConsumeFromWhere() == null
                ? null : value.getConsumeFromWhere().name();
            default: throw new IllegalArgumentException("unknown consumer enum field: " + field);
        }
    }

    private static List<Object> heartbeatFieldCases() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{}",
            "{\"clientID\":null,\"producerDataSet\":null,\"consumerDataSet\":null,\"heartbeatFingerprint\":0,\"withoutSub\":false}",
            "{\"clientID\":\"client\",\"producerDataSet\":[{\"groupName\":\"p\"}],\"consumerDataSet\":[{\"groupName\":\"c\",\"subscriptionDataSet\":[{\"topic\":\"topic\",\"subString\":\"tag\",\"tagsSet\":[\"tag\"],\"codeSet\":[7],\"subVersion\":42,\"expressionType\":\"SQL92\"}]}],\"heartbeatFingerprint\":7,\"withoutSub\":true}",
            "{\"clientID\":\"duplicate-producers\",\"producerDataSet\":[{\"groupName\":\"same\"},{\"groupName\":\"same\"}],\"consumerDataSet\":[],\"heartbeatFingerprint\":0,\"withoutSub\":false}"
        }) {
            HeartbeatData value = com.alibaba.fastjson2.JSON.parseObject(input, HeartbeatData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("rawInput", input);
            item.put("toString", value.toString());
            item.put("clientNull", value.getClientID() == null);
            item.put("producersNull", value.getProducerDataSet() == null);
            item.put("consumersNull", value.getConsumerDataSet() == null);
            item.put("producerCount", value.getProducerDataSet() == null ? -1 : value.getProducerDataSet().size());
            item.put("consumerCount", value.getConsumerDataSet() == null ? -1 : value.getConsumerDataSet().size());
            item.put("fingerprint", value.getHeartbeatFingerprint());
            item.put("computedFingerprint", value.computeHeartbeatFingerprint());
            item.put("withoutSub", value.isWithoutSub());
            item.put("encodedHasClient", encoded.containsKey("clientID"));
            item.put("encodedHasProducers", encoded.containsKey("producerDataSet"));
            item.put("encodedHasConsumers", encoded.containsKey("consumerDataSet"));
            cases.add(item);
        }
        return cases;
    }

    private static Map<String, Object> heartbeatSerializationMethodCases() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        List<Object> cases = new ArrayList<>();
        Method featureEncode = null;
        for (Method method : HeartbeatData.class.getMethods()) {
            if ("encode".equals(method.getName()) && method.getParameterCount() == 1
                && method.getParameterTypes()[0].isArray()) {
                featureEncode = method;
                break;
            }
        }
        if (featureEncode == null) throw new IllegalStateException("inherited feature encode overload missing");
        result.put("featureEncodeParameterType", featureEncode.getParameterTypes()[0].getComponentType().getName());
        for (String[] testCase : new String[][] {
            {"default", "{}"},
            {"explicit-nulls", "{\"clientID\":null,\"producerDataSet\":null,"
                + "\"consumerDataSet\":null,\"heartbeatFingerprint\":0,\"withoutSub\":false}"},
            {"populated", "{\"clientID\":\"client\",\"producerDataSet\":[{\"groupName\":\"p\"}],"
                + "\"consumerDataSet\":[{\"groupName\":\"c\",\"consumeType\":\"CONSUME_ACTIVELY\","
                + "\"messageModel\":\"CLUSTERING\",\"consumeFromWhere\":\"CONSUME_FROM_LAST_OFFSET\","
                + "\"subscriptionDataSet\":[{\"topic\":\"topic\",\"subString\":\"tag\","
                + "\"tagsSet\":[\"tag\"],\"codeSet\":[7],\"subVersion\":42,"
                + "\"expressionType\":\"SQL92\"}],\"unitMode\":true}],"
                + "\"heartbeatFingerprint\":7,\"withoutSub\":true}"}
        }) {
            HeartbeatData value = com.alibaba.fastjson2.JSON.parseObject(testCase[1], HeartbeatData.class);
            byte[] instanceBytes = value.encode();
            String instanceJson = new String(instanceBytes, java.nio.charset.StandardCharsets.UTF_8);
            Object emptyFeatures = java.lang.reflect.Array.newInstance(
                featureEncode.getParameterTypes()[0].getComponentType(), 0);
            String featureJson = new String((byte[]) featureEncode.invoke(value, emptyFeatures),
                java.nio.charset.StandardCharsets.UTF_8);
            byte[] arrayBytes = com.alibaba.fastjson2.JSON.toJSONString(Collections.singletonList(value))
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            List<HeartbeatData> decodedList = HeartbeatData.decodeList(arrayBytes, HeartbeatData.class);
            Map<String, Object> item = new TreeMap<>();
            item.put("case", testCase[0]);
            item.put("rawInput", testCase[1]);
            item.put("instanceJson", instanceJson);
            item.put("instancePrettyJson", value.toJson(true));
            item.put("staticJson", HeartbeatData.toJson(value, false));
            item.put("staticPrettyJson", HeartbeatData.toJson(value, true));
            item.put("instanceEncodedJson", instanceJson);
            item.put("staticEncodedJson", new String(HeartbeatData.encode(value),
                java.nio.charset.StandardCharsets.UTF_8));
            item.put("featureEncodedJson", featureJson);
            item.put("decodedJson", HeartbeatData.decode(instanceBytes, HeartbeatData.class).toJson());
            item.put("fromJsonJson", HeartbeatData.fromJson(instanceJson, HeartbeatData.class).toJson());
            item.put("decodedListCount", decodedList.size());
            item.put("decodedListJson", decodedList.get(0).toJson());
            cases.add(item);
        }
        result.put("cases", cases);
        result.put("staticEncodeNullIsNull", HeartbeatData.encode((Object) null) == null);
        result.put("staticToJsonNull", HeartbeatData.toJson(null, false));
        result.put("decodeNullIsNull", HeartbeatData.decode(null, HeartbeatData.class) == null);
        result.put("decodeListNullIsNull", HeartbeatData.decodeList(null, HeartbeatData.class) == null);
        result.put("fromJsonNullIsNull", HeartbeatData.fromJson(null, HeartbeatData.class) == null);
        byte[] jsonNull = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        result.put("decodeJsonNullIsNull", HeartbeatData.decode(jsonNull, HeartbeatData.class) == null);
        result.put("decodeListJsonNullIsNull", HeartbeatData.decodeList(jsonNull, HeartbeatData.class) == null);
        result.put("fromJsonJsonNullIsNull", HeartbeatData.fromJson("null", HeartbeatData.class) == null);
        List<HeartbeatData> nullMemberList = HeartbeatData.decodeList(
            "[null,{}]".getBytes(java.nio.charset.StandardCharsets.UTF_8), HeartbeatData.class);
        result.put("decodeListPreservesNullMember", nullMemberList.size() == 2
            && nullMemberList.get(0) == null && nullMemberList.get(1) != null);
        return result;
    }

    private static List<Object> heartbeatNullElementCases() {
        List<Object> cases = new ArrayList<>();
        for (String[] testCase : new String[][] {
            {"producer-null", "{\"producerDataSet\":[null,{\"groupName\":\"p\"},null],\"consumerDataSet\":[]}"},
            {"consumer-null", "{\"producerDataSet\":[],\"consumerDataSet\":[null,{\"groupName\":\"c\"},null]}"},
            {"subscription-null", "{\"producerDataSet\":[],\"consumerDataSet\":[{\"groupName\":\"c\","
                + "\"subscriptionDataSet\":[null,{\"topic\":\"topic\",\"subString\":\"tag\","
                + "\"tagsSet\":[],\"codeSet\":[],\"expressionType\":\"TAG\",\"subVersion\":1},null]}]}"}
        }) {
            HeartbeatData value = com.alibaba.fastjson2.JSON.parseObject(testCase[1], HeartbeatData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("case", testCase[0]);
            item.put("rawInput", testCase[1]);
            item.put("toString", value.toString());
            item.put("producerCount", value.getProducerDataSet() == null ? null : value.getProducerDataSet().size());
            item.put("producerHasNull", value.getProducerDataSet() != null && value.getProducerDataSet().contains(null));
            item.put("consumerCount", value.getConsumerDataSet() == null ? null : value.getConsumerDataSet().size());
            item.put("consumerHasNull", value.getConsumerDataSet() != null && value.getConsumerDataSet().contains(null));
            item.put("encodedProducerDataSet", encoded.get("producerDataSet"));
            item.put("encodedConsumerDataSet", encoded.get("consumerDataSet"));
            if ("subscription-null".equals(testCase[0])) {
                ConsumerData consumer = value.getConsumerDataSet().iterator().next();
                item.put("subscriptionCount", consumer.getSubscriptionDataSet().size());
                item.put("subscriptionHasNull", consumer.getSubscriptionDataSet().contains(null));
            }
            try {
                item.put("computedFingerprint", value.computeHeartbeatFingerprint());
                item.put("fingerprintError", null);
            } catch (Throwable error) {
                item.put("computedFingerprint", null);
                item.put("fingerprintError", error.getClass().getSimpleName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> heartbeatAliasCases() {
        List<Object> cases = new ArrayList<>();
        for (String input : new String[] {
            "{\"clientId\":\"lower-d\",\"isWithoutSub\":true}",
            "{\"ClientID\":\"mixed-case\",\"WithoutSub\":true}",
            "{\"CLIENTID\":\"upper\",\"WITHOUTSUB\":true}"
        }) {
            HeartbeatData value = com.alibaba.fastjson2.JSON.parseObject(input, HeartbeatData.class);
            com.alibaba.fastjson2.JSONObject encoded = com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(value));
            Map<String, Object> item = new TreeMap<>();
            item.put("rawInput", input);
            item.put("clientID", value.getClientID());
            item.put("withoutSub", value.isWithoutSub());
            item.put("encodedClientID", encoded.get("clientID"));
            item.put("encodedWithoutSub", encoded.get("withoutSub"));
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> fingerprintNullCases() {
        List<Object> cases = new ArrayList<>();
        for (String name : new String[] {
            "empty", "null-producers", "null-consumers", "null-subscriptions"
        }) {
            HeartbeatData data = new HeartbeatData();
            if ("null-producers".equals(name)) data.setProducerDataSet(null);
            if ("null-consumers".equals(name)) data.setConsumerDataSet(null);
            if ("null-subscriptions".equals(name)) {
                ConsumerData consumer = new ConsumerData();
                consumer.setSubscriptionDataSet(null);
                data.getConsumerDataSet().add(consumer);
            }
            Map<String, Object> item = new TreeMap<>();
            item.put("case", name);
            item.put("body", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(data)));
            try {
                item.put("fingerprint", data.computeHeartbeatFingerprint());
            } catch (Throwable error) {
                item.put("error", error.getClass().getName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> primitiveNullCases() {
        List<Object> cases = new ArrayList<>();
        for (String[] input : new String[][] {
            {"subscription-version", "{\"subVersion\":null}", "subscription"},
            {"subscription-class-filter", "{\"classFilterMode\":null}", "subscription"},
            {"heartbeat-fingerprint", "{\"heartbeatFingerprint\":null}", "heartbeat"},
            {"heartbeat-without-sub", "{\"withoutSub\":null}", "heartbeat"},
            {"consumer-unit-mode", "{\"unitMode\":null}", "consumer"}
        }) {
            Map<String, Object> item = new TreeMap<>();
            item.put("case", input[0]);
            item.put("rawInput", input[1]);
            try {
                switch (input[2]) {
                    case "subscription":
                        SubscriptionData subscription = com.alibaba.fastjson2.JSON.parseObject(input[1], SubscriptionData.class);
                        item.put("value", "subscription-version".equals(input[0])
                            ? subscription.getSubVersion() : subscription.isClassFilterMode());
                        break;
                    case "heartbeat":
                        HeartbeatData heartbeat = com.alibaba.fastjson2.JSON.parseObject(input[1], HeartbeatData.class);
                        item.put("value", "heartbeat-fingerprint".equals(input[0])
                            ? heartbeat.getHeartbeatFingerprint() : heartbeat.isWithoutSub());
                        break;
                    default:
                        ConsumerData consumer = com.alibaba.fastjson2.JSON.parseObject(input[1], ConsumerData.class);
                        item.put("value", consumer.isUnitMode());
                }
            } catch (Throwable error) {
                item.put("error", error.getClass().getName());
            }
            cases.add(item);
        }
        return cases;
    }

    private static Object normalizeHeartbeatJson(Object value) {
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            Map<String, Object> normalized = new TreeMap<>();
            map.forEach((key, item) -> normalized.put(String.valueOf(key), normalizeHeartbeatJson(item)));
            return normalized;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            List<Object> normalized = new ArrayList<>();
            for (Object item : list) normalized.add(normalizeHeartbeatJson(item));
            normalized.sort(Comparator.comparing(com.alibaba.fastjson2.JSON::toJSONString));
            return normalized;
        }
        return value;
    }

    private static Map<String, Object> heartbeatDtoInputCase(String id, String type, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("id", id);
        item.put("type", type);
        item.put("rawInput", rawInput);
        try {
            Object value;
            if ("subscription".equals(type)) {
                value = com.alibaba.fastjson2.JSON.parseObject(rawInput, SubscriptionData.class);
            } else if ("producer".equals(type)) {
                value = com.alibaba.fastjson2.JSON.parseObject(rawInput, ProducerData.class);
            } else if ("consumer".equals(type)) {
                value = com.alibaba.fastjson2.JSON.parseObject(rawInput, ConsumerData.class);
            } else if ("heartbeat".equals(type)) {
                value = com.alibaba.fastjson2.JSON.parseObject(rawInput, HeartbeatData.class);
            } else {
                throw new IllegalArgumentException("unknown DTO: " + type);
            }
            String encoded = com.alibaba.fastjson2.JSON.toJSONString(value,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue);
            item.put("outcome", "ACCEPTED");
            item.put("fields", normalizeHeartbeatJson(com.alibaba.fastjson2.JSON.parseObject(encoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> heartbeatDtoInputCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(heartbeatDtoInputCase("subscription-scalars", "subscription",
            "{\"classFilterMode\":-1,\"topic\":42,\"subString\":true,\"tagsSet\":[1,true,[],{},null],\"codeSet\":[\"2\",1.9,true,null],\"subVersion\":\"1e2\",\"expressionType\":3,\"filterClassSource\":false}"));
        cases.add(heartbeatDtoInputCase("subscription-decimal-primitives", "subscription",
            "{\"classFilterMode\":1.0,\"subVersion\":1.9}"));
        cases.add(heartbeatDtoInputCase("subscription-long-maximum", "subscription",
            "{\"subVersion\":9223372036854775807}"));
        cases.add(heartbeatDtoInputCase("subscription-long-overflow", "subscription",
            "{\"subVersion\":9223372036854775808}"));
        cases.add(heartbeatDtoInputCase("subscription-invalid-code", "subscription",
            "{\"codeSet\":[\"not-an-integer\"],\"subVersion\":0}"));
        cases.add(heartbeatDtoInputCase("consumer-scalars", "consumer",
            "{\"groupName\":123,\"unitMode\":-1,\"subscriptionDataSet\":[{\"topic\":42,\"subVersion\":\"4\",\"classFilterMode\":1}]}"));
        cases.add(heartbeatDtoInputCase("consumer-unit-mode-other-number", "consumer",
            "{\"unitMode\":2}"));
        cases.add(heartbeatDtoInputCase("consumer-invalid-boolean", "consumer",
            "{\"unitMode\":\"yes\"}"));
        cases.add(heartbeatDtoInputCase("heartbeat-scalars", "heartbeat",
            "{\"clientID\":123,\"heartbeatFingerprint\":\"2\",\"withoutSub\":\"1\",\"producerDataSet\":[{\"groupName\":7}]}"));
        cases.add(heartbeatDtoInputCase("producer-string", "producer",
            "{\"groupName\":123}"));
        cases.add(heartbeatDtoInputCase("subscription-leading-plus-zero-version", "subscription",
            "{\"subVersion\":+02}"));
        cases.add(heartbeatDtoInputCase("heartbeat-leading-plus-zero-fingerprint", "heartbeat",
            "{\"heartbeatFingerprint\":+02}"));
        cases.add(heartbeatDtoInputCase("consumer-form-feed-and-leading-zero", "consumer",
            String.valueOf((char) 0x0c) + "{\"unitMode\":\f01}"));
        cases.add(heartbeatDtoInputCase("heartbeat-bom-prefix", "heartbeat",
            String.valueOf((char) 0xfeff) + "{\"withoutSub\":true}"));
        cases.add(heartbeatDtoInputCase("subscription-form-feed-before-version", "subscription",
            "{\"subVersion\":" + String.valueOf((char) 0x0c) + "+02}"));
        cases.add(heartbeatDtoInputCase("consumer-vertical-tab-prefix", "consumer",
            String.valueOf((char) 0x0b) + "{\"unitMode\":true}"));
        cases.add(heartbeatDtoInputCase("heartbeat-embedded-bom", "heartbeat",
            "{\"heartbeatFingerprint\":" + String.valueOf((char) 0xfeff) + "2}"));
        cases.add(heartbeatDtoInputCase("subscription-object-to-string", "subscription",
            "{\"subVersion\":0,\"topic\":{\"z\":1,\"a\":2}}"));
        cases.add(heartbeatDtoInputCase("subscription-array-to-string", "subscription",
            "{\"subVersion\":0,\"subString\":[true,null,\"x\",{\"z\":1,\"a\":2}]}"));
        cases.add(heartbeatDtoInputCase("producer-object-to-string", "producer",
            "{\"groupName\":{\"z\":1,\"a\":2}}"));
        cases.add(heartbeatDtoInputCase("heartbeat-object-to-string", "heartbeat",
            "{\"clientID\":{\"z\":1,\"a\":2}}"));
        return cases;
    }

    private static Map<String, Object> nullOwnerSubscriptionPreparation() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("heartbeat-null-owner-subscriptions");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        Map<String, Object> item = new TreeMap<>();
        try {
            MQConsumerInner consumer = (MQConsumerInner) Proxy.newProxyInstance(
                MQConsumerInner.class.getClassLoader(), new Class<?>[] {MQConsumerInner.class},
                (proxy, method, values) -> {
                    switch (method.getName()) {
                        case "groupName": return "null-subscriptions";
                        case "consumeType": return ConsumeType.CONSUME_ACTIVELY;
                        case "messageModel": return MessageModel.CLUSTERING;
                        case "consumeFromWhere": return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
                        case "isUnitMode": return false;
                        case "subscriptions": return null;
                        default: return method.getReturnType() == boolean.class ? false : null;
                    }
                });
            if (!runtime.registerConsumer("null-subscriptions", consumer))
                throw new IllegalStateException("consumer registration");
            Method prepare = MQClientInstance.class.getDeclaredMethod("prepareHeartbeatData", boolean.class);
            prepare.setAccessible(true);
            try {
                prepare.invoke(runtime, false);
                item.put("fullOutcome", "OK");
            } catch (java.lang.reflect.InvocationTargetException error) {
                item.put("fullOutcome", error.getCause().getClass().getSimpleName());
            }
            HeartbeatData simple = (HeartbeatData) prepare.invoke(runtime, true);
            ConsumerData owner = simple.getConsumerDataSet().stream()
                .filter(value -> "null-subscriptions".equals(value.getGroupName())).findFirst().orElseThrow();
            item.put("simpleSubscriptionCount", owner.getSubscriptionDataSet().size());
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
        return item;
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
        fixture.put("entrypoint", "MQClientInstance.registerProducer/registerConsumer/prepareHeartbeatData/unregisterProducer/unregisterConsumer; HeartbeatData/ConsumerData/ProducerData/SubscriptionData Fastjson scalar inputs; HeartbeatData.computeHeartbeatFingerprint/toString/duplicate-ProducerData decode/nullable producer-consumer-subscription set elements; ProducerData.toString; ConsumerData enum JSON names/toString; ConsumeType.getTypeCN/MessageModel.getModeCN; SubscriptionData.SUB_ALL/constructors/equals/hashCode/compareTo/toString; ProducerData/ConsumerData/HeartbeatData inherited Object equals/hashCode; HeartbeatData inherited RemotingSerializable encode/toJson/decode/decodeList/fromJson methods");
        fixture.put("serverBaselineContacted", false);
        fixture.put("cases", cases);
        fixture.put("multiplicityCases", multiplicityCases);
        fixture.put("subscriptionFieldCases", subscriptionFieldCases());
        fixture.put("subscriptionConstructorCases", subscriptionConstructorCases());
        fixture.put("subscriptionMethods", subscriptionMethods());
        fixture.put("identityMethodCases", identityMethodCases());
        fixture.put("ownerFieldCases", ownerFieldCases());
        fixture.put("consumerEnumCases", consumerEnumCases());
        fixture.put("consumerEnumMethodCases", consumerEnumMethodCases());
        fixture.put("heartbeatFieldCases", heartbeatFieldCases());
        fixture.put("heartbeatSerializationMethodCases", heartbeatSerializationMethodCases());
        List<Object> nullElementCases = heartbeatNullElementCases();
        fixture.put("heartbeatNullElementCases", nullElementCases);
        fixture.put("heartbeatAliasCases", heartbeatAliasCases());
        fixture.put("fingerprintNullCases", fingerprintNullCases());
        fixture.put("primitiveNullCases", primitiveNullCases());
        fixture.put("heartbeatDtoInputCases", heartbeatDtoInputCases());
        fixture.put("nullOwnerSubscriptionPreparation", nullOwnerSubscriptionPreparation());
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("Runtime heartbeat invoked cases verified: " + cases.size()
            + "; null-element cases: " + nullElementCases.size()
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
