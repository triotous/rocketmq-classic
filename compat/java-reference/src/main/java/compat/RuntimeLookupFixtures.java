package compat;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
        fixture.put("entrypoint", "MQClientInstance.findBrokerAddrByTopic/BrokerData.selectBrokerAddr/QueryAssignmentResponseBody.decode");
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
