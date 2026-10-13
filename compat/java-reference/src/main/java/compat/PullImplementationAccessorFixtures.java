package compat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** Checks the deprecated facade accessor and its retained implementation identity. */
public final class PullImplementationAccessorFixtures {
    private PullImplementationAccessorFixtures() { }

    private static void check(boolean condition, String label) {
        if (!condition) throw new IllegalStateException(label);
    }

    private static String queueKey(MessageQueue queue) {
        return String.valueOf(queue.getTopic()) + "|" + String.valueOf(queue.getBrokerName())
            + "|" + queue.getQueueId();
    }

    private static List<String> queueKeys(Iterable<MessageQueue> queues) {
        TreeSet<String> keys = new TreeSet<>();
        for (MessageQueue queue : queues) keys.add(queueKey(queue));
        return new ArrayList<>(keys);
    }

    private static MessageExt message(String topic) {
        MessageExt message = new MessageExt();
        message.setTopic(topic);
        return message;
    }

    private static List<String> messageTopics(Iterable<MessageExt> messages) {
        List<String> topics = new ArrayList<>();
        for (MessageExt message : messages) topics.add(message.getTopic());
        return topics;
    }

    public static void main(String[] args) throws Exception {
        check(args.length >= 1, "expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String group = "phase18-pull-implementation-accessor";
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(group);
        consumer.setNamespace("tenant");
        DefaultMQPullConsumerImpl first = consumer.getDefaultMQPullConsumerImpl();
        DefaultMQPullConsumerImpl second = consumer.getDefaultMQPullConsumerImpl();
        check(first != null, "implementation accessor non-null");
        check(first == second, "implementation accessor identity stable");
        check(group.equals(first.groupName()), "implementation retains configured group");
        check(first.getServiceState().name().equals("CREATE_JUST"), "initial implementation state");
        check(first.getOffsetStore() == null, "offset store absent before start");
        check(first.getPullAPIWrapper() == null, "pull API wrapper absent before start");
        check(first.getRebalanceImpl() != null, "rebalance implementation constructed");
        int maxReconsumeTimesDefault = consumer.getMaxReconsumeTimes();
        consumer.setMaxReconsumeTimes(-3);
        check(consumer.getMaxReconsumeTimes() == -3, "max reconsume setting mutation");

        Set<MessageQueue> parseInput = new HashSet<>();
        parseInput.add(new MessageQueue("tenant%orders", "broker-a", 0));
        parseInput.add(new MessageQueue("orders", "broker-a", 0));
        parseInput.add(new MessageQueue("%RETRY%tenant%group-a", "broker-b", 1));
        parseInput.add(new MessageQueue("other%orders", "broker-c", 2));
        parseInput.add(new MessageQueue(null, "broker-d", 3));
        parseInput.add(new MessageQueue("tenant%nullable-broker", null, 4));
        Set<MessageQueue> parseOutput = first.parseSubscribeMessageQueues(parseInput);

        first.resetTopic(null);
        List<MessageExt> emptyMessages = new ArrayList<>();
        first.resetTopic(emptyMessages);
        List<MessageExt> resetInput = new ArrayList<>(List.of(
            message("tenant%orders"),
            message("orders"),
            message("other%orders"),
            message("%RETRY%tenant%group-a"),
            message("%RETRY%other%group-a"),
            message("%DLQ%tenant%group-a")
        ));
        first.resetTopic(resetInput);

        DefaultMQPullConsumer noNamespaceConsumer = new DefaultMQPullConsumer(group + "-no-namespace");
        List<MessageExt> noNamespaceMessages = new ArrayList<>(List.of(message("tenant%orders")));
        noNamespaceConsumer.getDefaultMQPullConsumerImpl().resetTopic(noNamespaceMessages);

        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", "DefaultMQPullConsumer.getDefaultMQPullConsumerImpl");
        provenance.put("serverBaselineContacted", false);
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("provenance", provenance);
        fixture.put("consumerGroup", group);
        fixture.put("accessorNonNull", true);
        fixture.put("sameImplementationAcrossCalls", true);
        fixture.put("implementationGroup", first.groupName());
        fixture.put("serviceStateBeforeStart", first.getServiceState().name());
        fixture.put("offsetStoreIsNullBeforeStart", first.getOffsetStore() == null);
        fixture.put("pullApiWrapperIsNullBeforeStart", first.getPullAPIWrapper() == null);
        fixture.put("rebalanceImplementationPresent", first.getRebalanceImpl() != null);
        fixture.put("maxReconsumeTimesDefault", maxReconsumeTimesDefault);
        fixture.put("maxReconsumeTimesAfterMutation", consumer.getMaxReconsumeTimes());
        fixture.put("parseSubscribeMessageQueuesInput", queueKeys(parseInput));
        fixture.put("parseSubscribeMessageQueuesOutput", queueKeys(parseOutput));
        fixture.put("resetTopicNullListNoOp", true);
        fixture.put("resetTopicEmptyListOutput", messageTopics(emptyMessages));
        fixture.put("resetTopicInput", List.of(
            "tenant%orders", "orders", "other%orders", "%RETRY%tenant%group-a",
            "%RETRY%other%group-a", "%DLQ%tenant%group-a"));
        fixture.put("resetTopicOutput", messageTopics(resetInput));
        fixture.put("resetTopicNoNamespaceInput", List.of("tenant%orders"));
        fixture.put("resetTopicNoNamespaceOutput", messageTopics(noNamespaceMessages));
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
        if (verify) {
            check(Files.exists(path) && json.equals(Files.readString(path)), "fixture drift: " + path);
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
        System.out.println("Pull implementation accessor verified");
    }
}
