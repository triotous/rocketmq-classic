package compat;

import com.alibaba.fastjson2.JSON;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.common.ServiceState;

/** Invokes the deprecated Push consumer implementation accessor on Java 5.5.1. */
public final class PushImplementationAccessorFixtures {
    private PushImplementationAccessorFixtures() { }

    private static void check(boolean condition, String label) {
        if (!condition) throw new IllegalStateException(label);
    }

    public static void main(String[] args) throws Exception {
        check(args.length >= 1, "expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);

        String group = "phase18-push-implementation-accessor";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        DefaultMQPushConsumerImpl first = consumer.getDefaultMQPushConsumerImpl();
        DefaultMQPushConsumerImpl second = consumer.getDefaultMQPushConsumerImpl();
        check(first != null, "implementation accessor non-null");
        check(first == second, "implementation accessor identity stable");

        String initialServiceState = first.getServiceState().name();
        first.setServiceState(null);
        boolean serviceStateSetterAcceptsNull = first.getServiceState() == null;
        first.setServiceState(ServiceState.START_FAILED);
        boolean startFailedStateBlocksStart = false;
        try {
            first.start();
        } catch (MQClientException expectedStateFailure) {
            startFailedStateBlocksStart = true;
        }
        String serviceStateAfterRejectedStart = first.getServiceState().name();
        first.setServiceState(ServiceState.CREATE_JUST);
        int effectiveMaxReconsumeTimesDefault = first.getMaxReconsumeTimes();
        consumer.setMaxReconsumeTimes(7);
        int effectiveMaxReconsumeTimesCustom = first.getMaxReconsumeTimes();
        boolean hasConsumeHookBeforeRegistration = first.hasHook();
        List<String> directHookEvents = new ArrayList<>();
        first.registerConsumeMessageHook(new ConsumeMessageHook() {
            @Override
            public String hookName() {
                return "push-raw-hook-fixture";
            }

            @Override
            public void consumeMessageBefore(ConsumeMessageContext context) {
                directHookEvents.add("before:" + context.getStatus());
                context.setStatus("hook-before");
            }

            @Override
            public void consumeMessageAfter(ConsumeMessageContext context) {
                directHookEvents.add("after:" + context.getStatus() + ":" + context.isSuccess());
            }
        });
        ConsumeMessageContext directHookContext = new ConsumeMessageContext();
        directHookContext.setStatus("seed");
        first.executeHookBefore(directHookContext);
        directHookContext.setSuccess(true);
        first.executeHookAfter(directHookContext);
        boolean hasConsumeHookAfterRegistration = first.hasHook();

        Map<String, Object> fixture = new LinkedHashMap<>();
        fixture.put("implementationNonNull", first != null);
        fixture.put("implementationIdentityStable", first == second);
        fixture.put("configuredConsumerGroup", first.groupName());
        fixture.put("initialServiceState", initialServiceState);
        fixture.put("serviceStateSetterAcceptsNull", serviceStateSetterAcceptsNull);
        fixture.put("startFailedStateBlocksStart", startFailedStateBlocksStart);
        fixture.put("serviceStateAfterRejectedStart", serviceStateAfterRejectedStart);
        fixture.put("effectiveMaxReconsumeTimesDefault", effectiveMaxReconsumeTimesDefault);
        fixture.put("effectiveMaxReconsumeTimesCustom", effectiveMaxReconsumeTimesCustom);
        fixture.put("hasConsumeHookBeforeRegistration", hasConsumeHookBeforeRegistration);
        fixture.put("directHookEvents", directHookEvents);
        fixture.put("hasConsumeHookAfterRegistration", hasConsumeHookAfterRegistration);
        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) {
            check(Files.exists(path) && json.equals(Files.readString(path)), "fixture drift: " + path);
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
        System.out.println("Push implementation accessor verified");
    }
}
