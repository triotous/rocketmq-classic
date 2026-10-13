package compat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;

/** Checks the deprecated facade accessor and its retained implementation identity. */
public final class PullImplementationAccessorFixtures {
    private PullImplementationAccessorFixtures() { }

    private static void check(boolean condition, String label) {
        if (!condition) throw new IllegalStateException(label);
    }

    public static void main(String[] args) throws Exception {
        check(args.length >= 1, "expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String group = "phase18-pull-implementation-accessor";
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(group);
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
