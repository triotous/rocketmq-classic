package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

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
        String groupName = first.groupName();
        String messageModel = first.messageModel().name();
        String consumeType = first.consumeType().name();
        String consumeFromWhere = first.consumeFromWhere().name();
        boolean unitMode = first.isUnitMode();
        boolean messageQueueListenerInitiallyNull = first.getMessageQueueListener() == null;
        boolean pauseBeforeSet = first.isPause();
        first.setPause(true);
        boolean pauseAfterSetTrue = first.isPause();
        first.setPause(false);
        boolean pauseAfterSetFalse = first.isPause();
        boolean consumeOrderlyBeforeSet = first.isConsumeOrderly();
        first.setConsumeOrderly(true);
        boolean consumeOrderlyAfterSetTrue = first.isConsumeOrderly();
        first.setConsumeOrderly(false);
        boolean consumeOrderlyAfterSetFalse = first.isConsumeOrderly();
        long pullDelayBeforeSet = pullTimeDelayMillsWhenException(first);
        first.setPullTimeDelayMillsWhenException(73);
        long pullDelayAfterSet = pullTimeDelayMillsWhenException(first);
        int subscriptionCountBefore = first.subscriptions().size();
        Object subscriptionTable = first.getSubscriptionInner();
        first.subscribe("orders", "*");
        boolean subscriptionInnerIdentityStable = subscriptionTable == first.getSubscriptionInner();
        List<String> subscriptionTopicsAfterSubscribe = first.subscriptions().stream()
            .map(SubscriptionData::getTopic)
            .sorted()
            .collect(Collectors.toList());
        boolean offsetStoreInitiallyNull = first.getOffsetStore() == null;
        OffsetStore directOffsetStore = (OffsetStore) Proxy.newProxyInstance(
            OffsetStore.class.getClassLoader(),
            new Class<?>[] {OffsetStore.class},
            (proxy, method, methodArgs) -> null);
        first.setOffsetStore(directOffsetStore);
        boolean offsetStoreSetterPreservesIdentity = first.getOffsetStore() == directOffsetStore;
        first.setOffsetStore(null);
        boolean offsetStoreSetterAcceptsNull = first.getOffsetStore() == null;
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
        fixture.put("groupName", groupName);
        fixture.put("messageModel", messageModel);
        fixture.put("consumeType", consumeType);
        fixture.put("consumeFromWhere", consumeFromWhere);
        fixture.put("unitMode", unitMode);
        fixture.put("messageQueueListenerInitiallyNull", messageQueueListenerInitiallyNull);
        fixture.put("pauseBeforeSet", pauseBeforeSet);
        fixture.put("pauseAfterSetTrue", pauseAfterSetTrue);
        fixture.put("pauseAfterSetFalse", pauseAfterSetFalse);
        fixture.put("consumeOrderlyBeforeSet", consumeOrderlyBeforeSet);
        fixture.put("consumeOrderlyAfterSetTrue", consumeOrderlyAfterSetTrue);
        fixture.put("consumeOrderlyAfterSetFalse", consumeOrderlyAfterSetFalse);
        fixture.put("pullDelayBeforeSet", pullDelayBeforeSet);
        fixture.put("pullDelayAfterSet", pullDelayAfterSet);
        fixture.put("subscriptionCountBefore", subscriptionCountBefore);
        fixture.put("subscriptionInnerIdentityStable", subscriptionInnerIdentityStable);
        fixture.put("subscriptionTopicsAfterSubscribe", subscriptionTopicsAfterSubscribe);
        fixture.put("offsetStoreInitiallyNull", offsetStoreInitiallyNull);
        fixture.put("offsetStoreSetterPreservesIdentity", offsetStoreSetterPreservesIdentity);
        fixture.put("offsetStoreSetterAcceptsNull", offsetStoreSetterAcceptsNull);
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

    private static long pullTimeDelayMillsWhenException(DefaultMQPushConsumerImpl implementation)
        throws Exception {
        Field delay = DefaultMQPushConsumerImpl.class.getDeclaredField("pullTimeDelayMillsWhenException");
        delay.setAccessible(true);
        return delay.getLong(implementation);
    }
}
