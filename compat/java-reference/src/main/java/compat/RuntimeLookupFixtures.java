package compat;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/** Invokes actual topic broker selection and assignment decoding without a transport. */
public final class RuntimeLookupFixtures {
    private RuntimeLookupFixtures() { }

    public static void main(String[] args) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setClientIP("192.0.2.6");
        MQClientInstance runtime = new MQClientInstance(config, 0, config.buildMQClientId());
        List<Object> cases = new ArrayList<>();
        try {
            add(cases, runtime, "missing-topic", null, new String[] {});
            add(cases, runtime, "empty-brokers", "[]", new String[] {});
            add(cases, runtime, "master-preferred", "[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"0\":\"master:10911\",\"2\":\"slave:10911\"}}]", new String[] {"master:10911"});
            add(cases, runtime, "single-slave", "[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"2\":\"slave:10911\"}}]", new String[] {"slave:10911"});
            add(cases, runtime, "random-slaves", "[{\"brokerName\":\"broker\",\"brokerAddrs\":{\"2\":\"a:10911\",\"3\":\"b:10911\"}}]", new String[] {"a:10911", "b:10911"});
            add(cases, runtime, "random-brokers", "[{\"brokerName\":\"a\",\"brokerAddrs\":{\"0\":\"a:10911\",\"2\":\"ignored:10911\"}},{\"brokerName\":\"b\",\"brokerAddrs\":{\"2\":\"b:10911\"}}]", new String[] {"a:10911", "b:10911"});
            add(cases, runtime, "empty-addresses", "[{\"brokerName\":\"broker\",\"brokerAddrs\":{}}]", new String[] {});
            add(cases, runtime, "null-address-map", "[{\"brokerName\":\"broker\",\"brokerAddrs\":null}]", new String[] {});
        } finally {
            Method cleanup = MQClientInstance.class.getDeclaredMethod("cleanupAfterConstructionFailure",
                org.apache.rocketmq.client.impl.MQClientAPIImpl.class, Throwable.class);
            cleanup.setAccessible(true);
            cleanup.invoke(runtime, runtime.getMQClientAPIImpl(), new IllegalStateException("fixture teardown"));
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "MQClientInstance.findBrokerAddrByTopic/BrokerData.selectBrokerAddr/QueryAssignmentResponseBody.decode/GetConsumerListByGroupResponseBody.decode");
        fixture.put("serverBaselineContacted", false);
        fixture.put("cases", cases);
        String assignment = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},\"mode\":\"POP\",\"attachments\":{\"key\":\"value\"}}";
        org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody response =
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.decode(
                ("{\"messageQueueAssignments\":[" + assignment + "," + assignment + "]}").getBytes(java.nio.charset.StandardCharsets.UTF_8),
                org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.class);
        fixture.put("assignmentDuplicateCount", response.getMessageQueueAssignments().size());
        fixture.put("assignmentModeCases", assignmentModeCases());
        fixture.put("assignmentAttachmentCases", assignmentAttachmentCases());
        fixture.put("assignmentBodyCases", assignmentBodyCases());
        fixture.put("liteSubscriptionEnumCases", liteSubscriptionEnumCases());
        fixture.put("queryAssignmentEnumCases", queryAssignmentEnumCases());
        fixture.put("setRequestModeCases", setRequestModeCases());
        fixture.put("consumeDirectResultCases", consumeDirectResultCases());
        fixture.put("messageQueueInputCases", messageQueueInputCases());
        fixture.put("assignmentHashCollisionCases", assignmentHashCollisionCases());
        fixture.put("assignmentMethodCases", assignmentMethodCases());
        fixture.put("messageRequestModeMethodCases", messageRequestModeMethodCases());
        fixture.put("consumerListCases", consumerListCases());
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else {
            Files.createDirectories(file.getParent()); Files.writeString(file, json);
        }
        System.out.println("Runtime broker lookup verified: " + cases.size());
    }

    private static List<Object> assignmentModeCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(assignmentModeCase("missing", null));
        cases.add(assignmentModeCase("explicit-null", "null"));
        cases.add(assignmentModeCase("pull-name", "\"PULL\""));
        cases.add(assignmentModeCase("pop-name", "\"POP\""));
        cases.add(assignmentModeCase("lowercase-pop", "\"pop\""));
        cases.add(assignmentModeCase("mixed-case-pull", "\"Pull\""));
        cases.add(assignmentModeCase("ordinal-pull", "0"));
        cases.add(assignmentModeCase("ordinal-pop", "1"));
        cases.add(assignmentModeCase("numeric-string", "\"1\""));
        cases.add(assignmentModeCase("numeric-string-fraction", "\"1.9\""));
        cases.add(assignmentModeCase("fractional-ordinal", "1.9"));
        cases.add(assignmentModeCase("fractional-ordinal-precision-boundary", "0.9999999999999999999"));
        cases.add(assignmentModeCase("decimal-ordinal-wraps-to-zero", "4294967296.0"));
        cases.add(assignmentModeCase("integer-ordinal-wrap-boundary", "4294967296"));
        cases.add(assignmentModeCase("negative-fractional-ordinal", "-0.9"));
        cases.add(assignmentModeCase("negative-ordinal", "-1"));
        cases.add(assignmentModeCase("ordinal-out-of-range", "2"));
        cases.add(assignmentModeCase("long-overflow-ordinal", "9223372036854775808"));
        cases.add(assignmentModeCase("invalid-name", "\"NOT_A_MODE\""));
        cases.add(assignmentModeCase("boolean", "true"));
        return cases;
    }

    private static Map<String, Object> assignmentModeCase(String name, String modeValue) {
        String assignment = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2}"
            + (modeValue == null ? "" : ",\"mode\":" + modeValue) + "}";
        String body = "{\"messageQueueAssignments\":[" + assignment + "]}";
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(body));
        item.put("rawInput", body);
        try {
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody decoded =
                org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.decode(
                    body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.class);
            org.apache.rocketmq.common.message.MessageQueueAssignment value =
                decoded.getMessageQueueAssignments().iterator().next();
            item.put("outcome", "ACCEPTED");
            item.put("mode", value.getMode() == null ? null : value.getMode().name());
            byte[] encoded = decoded.encode();
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                new String(encoded, java.nio.charset.StandardCharsets.UTF_8)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> assignmentAttachmentCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(assignmentAttachmentCase("missing", null));
        cases.add(assignmentAttachmentCase("explicit-null", "null"));
        cases.add(assignmentAttachmentCase("scalar-values",
            "{\"number\":1,\"fraction\":1.9,\"boolean\":true,\"nullValue\":null}"));
        cases.add(assignmentAttachmentCase("numeric-string-values",
            "{\"plain\":9223372036854775807.123456789012345678901,\"exponent\":1e2}"));
        cases.add(assignmentAttachmentCase("nested-values",
            "{\"array\":[1,\"x\"],\"object\":{\"key\":\"value\"}}"));
        cases.add(assignmentAttachmentCase("array-map", "[]"));
        cases.add(assignmentAttachmentCase("scalar-map", "\"not-a-map\""));
        return cases;
    }

    private static Map<String, Object> assignmentAttachmentCase(String name, String attachmentsValue) {
        String assignment = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},\"mode\":\"POP\""
            + (attachmentsValue == null ? "" : ",\"attachments\":" + attachmentsValue) + "}";
        String body = "{\"messageQueueAssignments\":[" + assignment + "]}";
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(body));
        item.put("rawInput", body);
        try {
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody decoded =
                org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.decode(
                    body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.class);
            org.apache.rocketmq.common.message.MessageQueueAssignment value =
                decoded.getMessageQueueAssignments().iterator().next();
            item.put("outcome", "ACCEPTED");
            item.put("attachments", value.getAttachments());
            byte[] encoded = decoded.encode();
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                new String(encoded, java.nio.charset.StandardCharsets.UTF_8)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> assignmentBodyCases() {
        String queue = "\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2}";
        List<Object> cases = new ArrayList<>();
        cases.add(assignmentBodyCase("missing", null, false));
        cases.add(assignmentBodyCase("explicit-null", "null", true));
        cases.add(assignmentBodyCase("empty", "[]", true));
        cases.add(assignmentBodyCase("single-default", "[{}]", true));
        cases.add(assignmentBodyCase("null-element", "[null]", true));
        cases.add(assignmentBodyCase("duplicate-null-elements", "[null,null]", true));
        cases.add(assignmentBodyCase("string-element", "[\"assignment\"]", true));
        cases.add(assignmentBodyCase("number-element", "[7]", true));
        cases.add(assignmentBodyCase("boolean-element", "[true]", true));
        cases.add(assignmentBodyCase("array-element", "[[]]", true));
        cases.add(assignmentBodyCase("nested-array-object-element", "[[{}]]", true));
        cases.add(assignmentBodyCase("mixed-null-object-elements", "[null,{}]", true));
        cases.add(assignmentBodyCase("mixed-scalar-object-elements", "[7,{}]", true));
        cases.add(assignmentBodyCase("object-field", "{" + queue + "}", true));
        cases.add(assignmentBodyCase("string-field", "\"assignment\"", true));
        cases.add(assignmentBodyCase("number-field", "7", true));
        cases.add(assignmentBodyCase("boolean-field", "true", true));
        cases.add(assignmentBodyCase("identical-duplicates", "[{" + queue + "},{" + queue + "}]", true));
        String pull = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},"
            + "\"mode\":\"PULL\"}";
        String pop = "{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},"
            + "\"mode\":\"POP\"}";
        cases.add(assignmentBodyCase("same-queue-different-mode", "[" + pull + "," + pop + "]", true));
        cases.add(assignmentBodyRawCase("duplicate-root-field-last-wins",
            "{\"messageQueueAssignments\":[{}],\"messageQueueAssignments\":[]}"));
        cases.add(assignmentBodyCase("duplicate-nested-mode-last-wins",
            "[{\"messageQueue\":{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2},"
                + "\"mode\":\"PULL\",\"mode\":\"POP\"}]", true));
        cases.add(assignmentBodyRawCase("duplicate-nested-queue-field-last-wins",
            "{\"messageQueueAssignments\":[{\"messageQueue\":{\"topic\":\"first\","
                + "\"topic\":\"last\",\"brokerName\":\"broker\",\"queueId\":2}}]}"));
        String[] messageQueueValues = {"null", "\"queue\"", "7", "true", "[]", "{}",
            "{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2}"};
        String[] messageQueueNames = {"null", "string", "number", "boolean", "array", "empty-object", "object"};
        for (int i = 0; i < messageQueueValues.length; i++) {
            cases.add(assignmentBodyRawCase("message-queue-" + messageQueueNames[i],
                "{\"messageQueueAssignments\":[{\"messageQueue\":" + messageQueueValues[i] + ",\"mode\":\"PULL\"}]}"));
        }
        return cases;
    }

    private static List<Object> liteSubscriptionEnumCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(liteSubscriptionEnumCase("action-duplicate-last-wins", "{\"subscriptionSet\":[{\"action\":\"PARTIAL_ADD\",\"action\":\"COMPLETE_REMOVE\"}]}"));
        cases.add(liteSubscriptionEnumCase("client-id-duplicate-last-wins", "{\"subscriptionSet\":[{\"clientId\":\"first\",\"clientId\":\"last\"}]}"));
        cases.add(liteSubscriptionEnumCase("group-duplicate-last-wins", "{\"subscriptionSet\":[{\"group\":\"first\",\"group\":\"last\"}]}"));
        cases.add(liteSubscriptionEnumCase("topic-duplicate-last-wins", "{\"subscriptionSet\":[{\"topic\":\"first\",\"topic\":\"last\"}]}"));
        cases.add(liteSubscriptionEnumCase("lite-topic-set-duplicate-last-wins", "{\"subscriptionSet\":[{\"liteTopicSet\":[\"first\"],\"liteTopicSet\":[\"last\"]}]}"));
        cases.add(liteSubscriptionEnumCase("offset-option-duplicate-last-wins", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"POLICY\",\"value\":1},\"offsetOption\":{\"type\":\"TIMESTAMP\",\"value\":2}}]}"));
        cases.add(liteSubscriptionEnumCase("version-duplicate-last-wins", "{\"subscriptionSet\":[{\"version\":1,\"version\":2}]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-duplicate-last-wins", "{\"subscriptionSet\":[{\"topic\":\"first\"}],\"subscriptionSet\":[{\"topic\":\"last\"}]}"));
        cases.add(liteSubscriptionEnumCase("offset-type-duplicate-last-wins", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"POLICY\",\"type\":\"TIMESTAMP\",\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-value-duplicate-last-wins", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"POLICY\",\"value\":1,\"value\":2}}]}"));
        cases.add(liteSubscriptionEnumCase("action-name", "{\"subscriptionSet\":[{\"action\":\"PARTIAL_ADD\"}]}"));
        cases.add(liteSubscriptionEnumCase("action-ordinal", "{\"subscriptionSet\":[{\"action\":1}]}"));
        cases.add(liteSubscriptionEnumCase("action-numeric-string", "{\"subscriptionSet\":[{\"action\":\"1\"}]}"));
        cases.add(liteSubscriptionEnumCase("action-fractional", "{\"subscriptionSet\":[{\"action\":1.9}]}"));
        cases.add(liteSubscriptionEnumCase("action-out-of-range", "{\"subscriptionSet\":[{\"action\":4}]}"));
        cases.add(liteSubscriptionEnumCase("action-unknown-string", "{\"subscriptionSet\":[{\"action\":\"UNKNOWN\"}]}"));
        cases.add(liteSubscriptionEnumCase("offset-name", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"POLICY\",\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-missing-value", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"POLICY\"}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-ordinal", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":3,\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-numeric-string", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"3\",\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-fractional", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":2.9,\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-out-of-range", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":4,\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("offset-unknown-string", "{\"subscriptionSet\":[{\"offsetOption\":{\"type\":\"UNKNOWN\",\"value\":1}}]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-null", "{\"subscriptionSet\":null}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-empty", "{\"subscriptionSet\":[]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-single-object", "{\"subscriptionSet\":{\"topic\":\"object\"}}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-singleton-object-array", "{\"subscriptionSet\":[{\"topic\":\"array\"}]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-null-element", "{\"subscriptionSet\":[null]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-string-element", "{\"subscriptionSet\":[\"text\"]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-number-element", "{\"subscriptionSet\":[42]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-boolean-element", "{\"subscriptionSet\":[true]}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-scalar-string", "{\"subscriptionSet\":\"text\"}"));
        cases.add(liteSubscriptionEnumCase("subscription-set-scalar-number", "{\"subscriptionSet\":42}"));
        return cases;
    }

    private static Map<String, Object> liteSubscriptionEnumCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody decoded =
                com.alibaba.fastjson2.JSON.parseObject(rawInput,
                    org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> queryAssignmentEnumCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(queryAssignmentEnumCase("name", "{\"messageModel\":\"CLUSTERING\"}"));
        cases.add(queryAssignmentEnumCase("ordinal", "{\"messageModel\":1}"));
        cases.add(queryAssignmentEnumCase("numeric-string", "{\"messageModel\":\"1\"}"));
        cases.add(queryAssignmentEnumCase("fractional", "{\"messageModel\":2.9}"));
        cases.add(queryAssignmentEnumCase("unknown-name", "{\"messageModel\":\"UNKNOWN\"}"));
        cases.add(queryAssignmentEnumCase("unknown-numeric-string", "{\"messageModel\":\"3\"}"));
        cases.add(queryAssignmentEnumCase("out-of-range", "{\"messageModel\":3}"));
        cases.add(queryAssignmentEnumCase("explicit-null", "{\"messageModel\":null}"));
        cases.add(queryAssignmentEnumCase("duplicate-name-then-ordinal",
            "{\"messageModel\":\"BROADCASTING\",\"messageModel\":1}"));
        cases.add(queryAssignmentEnumCase("duplicate-ordinal-then-name",
            "{\"messageModel\":1,\"messageModel\":\"BROADCASTING\"}"));
        return cases;
    }

    private static Map<String, Object> queryAssignmentEnumCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody decoded =
                com.alibaba.fastjson2.JSON.parseObject(rawInput,
                    org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> setRequestModeCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(setRequestModeCase("missing", "{}"));
        cases.add(setRequestModeCase("name", "{\"mode\":\"POP\"}"));
        cases.add(setRequestModeCase("ordinal", "{\"mode\":1}"));
        cases.add(setRequestModeCase("numeric-string", "{\"mode\":\"1\"}"));
        cases.add(setRequestModeCase("fractional", "{\"mode\":1.9}"));
        cases.add(setRequestModeCase("unknown-name", "{\"mode\":\"UNKNOWN\"}"));
        cases.add(setRequestModeCase("unknown-numeric-string", "{\"mode\":\"2\"}"));
        cases.add(setRequestModeCase("out-of-range", "{\"mode\":2}"));
        cases.add(setRequestModeCase("explicit-null", "{\"mode\":null}"));
        cases.add(setRequestModeCase("boolean", "{\"mode\":true}"));
        cases.add(setRequestModeCase("primitive-default", "{\"topic\":\"orders\"}"));
        cases.add(setRequestModeCase("decimal-wrap", "{\"mode\":4294967296.0}"));
        cases.add(setRequestModeCase("duplicate-name-then-ordinal",
            "{\"mode\":\"PULL\",\"mode\":1}"));
        cases.add(setRequestModeCase("duplicate-ordinal-then-name",
            "{\"mode\":1,\"mode\":\"PULL\"}"));
        return cases;
    }

    private static Map<String, Object> setRequestModeCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody decoded =
                com.alibaba.fastjson2.JSON.parseObject(rawInput,
                    org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> consumeDirectResultCases() {
        List<Object> cases = new ArrayList<>();
        for (org.apache.rocketmq.remoting.protocol.body.CMResult value :
            org.apache.rocketmq.remoting.protocol.body.CMResult.values()) {
            cases.add(consumeDirectResultCase("name-" + value.name(),
                "{\"consumeResult\":\"" + value.name() + "\"}"));
        }
        for (int ordinal = 0;
            ordinal <= org.apache.rocketmq.remoting.protocol.body.CMResult.values().length;
            ordinal++) {
            cases.add(consumeDirectResultCase("ordinal-" + ordinal,
                "{\"consumeResult\":" + ordinal + "}"));
        }
        cases.add(consumeDirectResultCase("numeric-string", "{\"consumeResult\":\"1\"}"));
        cases.add(consumeDirectResultCase("fractional", "{\"consumeResult\":1.9}"));
        cases.add(consumeDirectResultCase("unknown-name", "{\"consumeResult\":\"UNKNOWN\"}"));
        cases.add(consumeDirectResultCase("unknown-numeric-string", "{\"consumeResult\":\"6\"}"));
        cases.add(consumeDirectResultCase("explicit-null", "{\"consumeResult\":null}"));
        cases.add(consumeDirectResultCase("boolean", "{\"consumeResult\":true}"));
        return cases;
    }

    private static Map<String, Object> consumeDirectResultCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult decoded =
                com.alibaba.fastjson2.JSON.parseObject(rawInput,
                    org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> messageQueueInputCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(messageQueueInputCase("default", "{}"));
        cases.add(messageQueueInputCase("normal", "{\"topic\":\"topic\",\"brokerName\":\"broker\",\"queueId\":2}"));
        cases.add(messageQueueInputCase("scalar-coercions", "{\"topic\":42,\"brokerName\":true,\"queueId\":1.9}"));
        cases.add(messageQueueInputCase("high-precision-topic", "{\"topic\":9223372036854775807.123456789012345678901}"));
        cases.add(messageQueueInputCase("queue-minimum", "{\"queueId\":-2147483648}"));
        cases.add(messageQueueInputCase("queue-maximum", "{\"queueId\":2147483647}"));
        cases.add(messageQueueInputCase("queue-integer-overflow", "{\"queueId\":2147483648}"));
        cases.add(messageQueueInputCase("queue-decimal-wrap", "{\"queueId\":2147483648.0}"));
        cases.add(messageQueueInputCase("queue-fraction", "{\"queueId\":-1.9}"));
        cases.add(messageQueueInputCase("queue-exponent-string", "{\"queueId\":\"1e2\"}"));
        cases.add(messageQueueInputCase("queue-null", "{\"queueId\":null}"));
        cases.add(messageQueueInputCase("queue-boolean", "{\"queueId\":true}"));
        cases.add(messageQueueInputCase("queue-empty-object", "{\"queueId\":{}}"));
        cases.add(messageQueueInputCase("queue-array", "{\"queueId\":[]}"));
        cases.add(messageQueueInputCase("topic-object", "{\"topic\":{\"k\":1}}"));
        cases.add(messageQueueInputCase("topic-null", "{\"topic\":null,\"brokerName\":null}"));
        cases.add(messageQueueInputCase("duplicate-topic-last-wins",
            "{\"topic\":\"first\",\"topic\":\"last\"}"));
        cases.add(messageQueueInputCase("duplicate-broker-name-last-wins",
            "{\"brokerName\":\"first\",\"brokerName\":\"last\"}"));
        cases.add(messageQueueInputCase("duplicate-queue-id-last-wins",
            "{\"queueId\":1,\"queueId\":2.9}"));
        return cases;
    }

    private static Map<String, Object> messageQueueInputCase(String name, String rawInput) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("rawInput", rawInput);
        try {
            org.apache.rocketmq.common.message.MessageQueue decoded =
                com.alibaba.fastjson2.JSON.parseObject(rawInput,
                    org.apache.rocketmq.common.message.MessageQueue.class);
            item.put("outcome", "ACCEPTED");
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                com.alibaba.fastjson2.JSON.toJSONString(decoded)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static String stringWithHashCode(int target) {
        long remaining = Integer.toUnsignedLong(target);
        char[] digits = new char[7];
        for (int index = digits.length - 1; index >= 0; index--) {
            digits[index] = (char) (remaining % 31);
            remaining /= 31;
        }
        if (remaining != 0) {
            throw new IllegalArgumentException("hash does not fit seven base-31 digits");
        }
        return new String(digits);
    }

    private static Map<String, Object> assignmentHashCollisionCases() {
        org.apache.rocketmq.common.message.MessageQueue queue =
            new org.apache.rocketmq.common.message.MessageQueue("topic", "broker", 2);
        org.apache.rocketmq.common.message.MessageQueueAssignment pull =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        pull.setMessageQueue(queue);
        org.apache.rocketmq.common.message.MessageQueueAssignment pop =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        pop.setMessageQueue(new org.apache.rocketmq.common.message.MessageQueue("topic", "broker", 2));
        pop.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);

        // MessageQueueAssignment.hashCode includes enum identity hash and attachments,
        // but equals compares only the queue. Choose an attachment hash that forces
        // an equal-hash pair for this JVM, without emitting the process-local hash.
        int attachmentHash = 31 * (pull.getMode().hashCode() - pop.getMode().hashCode());
        Map<String, String> attachments = new HashMap<>();
        attachments.put("", stringWithHashCode(attachmentHash));
        pop.setAttachments(attachments);

        Set<org.apache.rocketmq.common.message.MessageQueueAssignment> assignments = new HashSet<>();
        assignments.add(pull);
        assignments.add(pop);
        Map<String, Object> result = new TreeMap<>();
        result.put("sameQueueEquals", pull.equals(pop));
        result.put("equalHash", pull.hashCode() == pop.hashCode());
        result.put("setSize", assignments.size());
        result.put("collisionDeduplicates", assignments.size() == 1);
        return result;
    }

    private static Map<String, Object> assignmentBodyCase(String name, String rawValue, boolean includeField) {
        String body = includeField ? "{\"messageQueueAssignments\":" + rawValue + "}" : "{}";
        return assignmentBodyRawCase(name, body);
    }

    private static Map<String, Object> assignmentBodyRawCase(String name, String body) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(body));
        item.put("rawInput", body);
        try {
            org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody decoded =
                org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.decode(
                    body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody.class);
            item.put("outcome", "ACCEPTED");
            Set<org.apache.rocketmq.common.message.MessageQueueAssignment> assignmentSet =
                decoded.getMessageQueueAssignments();
            if (assignmentSet == null) {
                item.put("assignments", null);
            } else {
                List<org.apache.rocketmq.common.message.MessageQueueAssignment> assignments =
                    new ArrayList<>(assignmentSet);
                assignments.sort(
                    Comparator.nullsFirst(Comparator.comparing(com.alibaba.fastjson2.JSON::toJSONString)));
                item.put("assignments", assignments);
            }
            byte[] encoded = decoded.encode();
            Map<String, Object> encodedBody = com.alibaba.fastjson2.JSON.parseObject(
                new String(encoded, java.nio.charset.StandardCharsets.UTF_8));
            Object encodedAssignments = encodedBody.get("messageQueueAssignments");
            if (encodedAssignments instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<Object> encodedRows = (List<Object>) encodedAssignments;
                encodedRows.sort(
                    Comparator.nullsFirst(Comparator.comparing(com.alibaba.fastjson2.JSON::toJSONString)));
            }
            item.put("encoded", encodedBody);
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static List<Object> assignmentMethodCases() {
        List<Object> cases = new ArrayList<>();
        org.apache.rocketmq.common.message.MessageQueueAssignment empty =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        Map<String, Object> emptyCase = new TreeMap<>();
        emptyCase.put("name", "default");
        emptyCase.put("messageQueue", null);
        emptyCase.put("mode", empty.getMode() == null ? null : empty.getMode().name());
        emptyCase.put("attachments", empty.getAttachments());
        emptyCase.put("text", empty.toString());
        emptyCase.put("equalsSelf", empty.equals(empty));
        emptyCase.put("equalsNull", empty.equals(null));
        emptyCase.put("equalsWrongType", empty.equals("not-an-assignment"));
        try {
            empty.equals(new org.apache.rocketmq.common.message.MessageQueueAssignment());
            emptyCase.put("equalsDistinctNullQueue", "NO_EXCEPTION");
        } catch (RuntimeException error) {
            emptyCase.put("equalsDistinctNullQueue", error.getClass().getSimpleName());
        }
        cases.add(emptyCase);

        org.apache.rocketmq.common.message.MessageQueue queue =
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 2);
        org.apache.rocketmq.common.message.MessageQueueAssignment populated =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        populated.setMessageQueue(queue);
        populated.setMode(org.apache.rocketmq.common.message.MessageRequestMode.POP);
        Map<String, String> attachments = new TreeMap<>();
        attachments.put("key", "value");
        populated.setAttachments(attachments);
        Map<String, Object> populatedCase = new TreeMap<>();
        populatedCase.put("name", "populated");
        Map<String, Object> messageQueue = new TreeMap<>();
        messageQueue.put("topic", populated.getMessageQueue().getTopic());
        messageQueue.put("brokerName", populated.getMessageQueue().getBrokerName());
        messageQueue.put("queueId", populated.getMessageQueue().getQueueId());
        populatedCase.put("messageQueue", messageQueue);
        populatedCase.put("mode", populated.getMode().name());
        populatedCase.put("attachments", populated.getAttachments());
        populatedCase.put("text", populated.toString());
        org.apache.rocketmq.common.message.MessageQueueAssignment sameQueueDifferentMetadata =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        sameQueueDifferentMetadata.setMessageQueue(
            new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 2));
        sameQueueDifferentMetadata.setMode(org.apache.rocketmq.common.message.MessageRequestMode.PULL);
        populatedCase.put("equalsSameQueueDifferentMetadata", populated.equals(sameQueueDifferentMetadata));
        org.apache.rocketmq.common.message.MessageQueueAssignment differentQueue =
            new org.apache.rocketmq.common.message.MessageQueueAssignment();
        differentQueue.setMessageQueue(new org.apache.rocketmq.common.message.MessageQueue("orders", "broker", 3));
        populatedCase.put("equalsDifferentQueue", populated.equals(differentQueue));
        cases.add(populatedCase);
        return cases;
    }

    private static List<Object> messageRequestModeMethodCases() {
        List<Object> cases = new ArrayList<>();
        for (org.apache.rocketmq.common.message.MessageRequestMode mode :
            org.apache.rocketmq.common.message.MessageRequestMode.values()) {
            Map<String, Object> item = new TreeMap<>();
            item.put("constant", mode.name());
            item.put("getName", mode.getName());
            cases.add(item);
        }
        return cases;
    }

    private static List<Object> consumerListCases() {
        List<Object> cases = new ArrayList<>();
        cases.add(consumerListCase("missing", null, false));
        cases.add(consumerListCase("explicit-null", "null", true));
        cases.add(consumerListCase("empty", "[]", true));
        cases.add(consumerListCase("strings-with-duplicates", "[\"b\",\"a\",\"b\"]", true));
        cases.add(consumerListCase("scalar-elements", "[1,1.9,true,null]", true));
        cases.add(consumerListCase("high-precision-numeric-strings",
            "[9223372036854775807.123456789012345678901,1e2]", true));
        cases.add(consumerListCase("nested-elements", "[[1,\"x\"],{\"key\":\"value\"}]", true));
        cases.add(consumerListCase("object-field", "{\"id\":\"client\"}", true));
        cases.add(consumerListCase("string-field", "\"client\"", true));
        cases.add(consumerListCase("number-field", "7", true));
        cases.add(consumerListCase("fraction-field", "1.9", true));
        cases.add(consumerListCase("boolean-field", "true", true));
        cases.add(consumerListCase("empty-string-field", "\"\"", true));
        cases.add(consumerListRawCase("duplicate-field-last-wins",
            "{\"consumerIdList\":[\"first\"],\"consumerIdList\":[\"last\"]}"));
        cases.add(consumerListRawCase("duplicate-field-earlier-scalar",
            "{\"consumerIdList\":7,\"consumerIdList\":[\"last\"]}"));
        cases.add(consumerListRawCase("duplicate-field-last-scalar",
            "{\"consumerIdList\":[\"first\"],\"consumerIdList\":7}"));
        return cases;
    }

    private static Map<String, Object> consumerListCase(String name, String rawValue, boolean includeField) {
        String body = includeField ? "{\"consumerIdList\":" + rawValue + "}" : "{}";
        return consumerListRawCase(name, body);
    }

    private static Map<String, Object> consumerListRawCase(String name, String body) {
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name);
        item.put("input", com.alibaba.fastjson2.JSON.parseObject(body));
        item.put("rawInput", body);
        try {
            org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody decoded =
                org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody.decode(
                    body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody.class);
            item.put("outcome", "ACCEPTED");
            item.put("ids", decoded.getConsumerIdList());
            byte[] encoded = decoded.encode();
            item.put("encoded", com.alibaba.fastjson2.JSON.parseObject(
                new String(encoded, java.nio.charset.StandardCharsets.UTF_8)));
        } catch (RuntimeException error) {
            item.put("outcome", "REJECTED");
            item.put("error", error.getClass().getSimpleName());
        }
        return item;
    }

    private static void add(List<Object> cases, MQClientInstance runtime, String name, String brokers, String[] candidates) {
        String input = brokers == null ? null : "{\"queueDatas\":[],\"brokerDatas\":" + brokers + ",\"filterServerTable\":{}}";
        if (input != null) {
            com.alibaba.fastjson2.JSONObject json = com.alibaba.fastjson2.JSON.parseObject(input);
            for (Object broker : json.getJSONArray("brokerDatas")) {
                ((com.alibaba.fastjson2.JSONObject) broker).put("cluster", "cluster");
            }
            input = com.alibaba.fastjson2.JSON.toJSONString(json,
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue);
        }
        runtime.getTopicRouteTable().clear();
        if (input != null) runtime.getTopicRouteTable().put("topic", com.alibaba.fastjson2.JSON.parseObject(input, TopicRouteData.class));
        String outcome = "OK";
        try {
            for (int i = 0; i < 128; i++) {
                String result = runtime.findBrokerAddrByTopic("topic");
                if (result == null) outcome = "NULL";
                else if (!Arrays.asList(candidates).contains(result)) throw new AssertionError("unexpected address: " + result);
            }
        } catch (RuntimeException error) { outcome = error.getClass().getSimpleName(); }
        Map<String, Object> item = new TreeMap<>();
        item.put("name", name); item.put("input", input == null ? null : com.alibaba.fastjson2.JSON.parseObject(input));
        item.put("candidates", Arrays.asList(candidates)); item.put("outcome", outcome);
        cases.add(item);
    }
}
