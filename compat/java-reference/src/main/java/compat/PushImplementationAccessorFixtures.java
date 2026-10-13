package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageService;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.PopProcessQueue;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullRequest;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;

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
        Object firstRebalance = first.getRebalanceImpl();
        Object secondRebalance = first.getRebalanceImpl();
        check(firstRebalance != null, "rebalance implementation accessor non-null");
        check(firstRebalance == secondRebalance, "rebalance implementation identity stable");

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
        AtomicInteger adjustThreadPoolDecreaseCalls = new AtomicInteger();
        first.setConsumeMessageService((ConsumeMessageService) Proxy.newProxyInstance(
            ConsumeMessageService.class.getClassLoader(),
            new Class<?>[] {ConsumeMessageService.class},
            (proxy, method, methodArgs) -> {
                if ("decCorePoolSize".equals(method.getName())) {
                    adjustThreadPoolDecreaseCalls.incrementAndGet();
                }
                return "getCorePoolSize".equals(method.getName()) ? 7 : null;
            }));
        int adjustThreadPoolCorePoolSizeBefore = first.getConsumeMessageService().getCorePoolSize();
        first.adjustThreadPool();
        int adjustThreadPoolCorePoolSizeAfter = first.getConsumeMessageService().getCorePoolSize();
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
        boolean subscribeInfoNeededBeforeRoute = first.isSubscribeTopicNeedUpdate("orders");
        first.updateTopicSubscribeInfo("orders", Set.of(new MessageQueue("orders", "broker-a", 1)));
        boolean subscribeInfoNeededAfterRoute = first.isSubscribeTopicNeedUpdate("orders");
        boolean unregisteredTopicNeedsRoute = first.isSubscribeTopicNeedUpdate("unregistered");
        first.suspend();
        boolean tryRebalanceWhilePaused = first.tryRebalance();
        first.resume();
        RecordingMqClientInstance resetFactory = recordingMqClientInstance();
        first.setmQClientFactory(resetFactory);
        first.getRebalanceImpl().setmQClientFactory(resetFactory);
        SubscriptionData emptySubscription = new SubscriptionData();
        emptySubscription.setTopic("empty");
        first.getRebalanceImpl().getSubscriptionInner().put("empty", emptySubscription);
        MessageQueue resetQueueA = new MessageQueue("orders", "broker-a", 0);
        MessageQueue resetQueueB = new MessageQueue("orders", "broker-a", 1);
        first.getRebalanceImpl().getTopicSubscribeInfoTable().put(
            "orders", Set.of(resetQueueA, resetQueueB));
        first.getRebalanceImpl().getTopicSubscribeInfoTable().put("empty", Set.of());
        first.resetOffsetByTimeStamp(12345);
        boolean offsetStoreInitiallyNull = first.getOffsetStore() == null;
        List<String> offsetUpdates = new ArrayList<>();
        List<String> persistedQueues = new ArrayList<>();
        OffsetStore directOffsetStore = (OffsetStore) Proxy.newProxyInstance(
            OffsetStore.class.getClassLoader(),
            new Class<?>[] {OffsetStore.class},
            (proxy, method, methodArgs) -> {
                if ("updateOffset".equals(method.getName())) {
                    MessageQueue queue = (MessageQueue) methodArgs[0];
                    offsetUpdates.add(queue.getTopic() + "|" + queue.getBrokerName() + "|"
                        + queue.getQueueId() + "|" + methodArgs[1] + "|" + methodArgs[2]);
                } else if ("readOffset".equals(method.getName())) {
                    return 41L;
                } else if ("persistAll".equals(method.getName())) {
                    @SuppressWarnings("unchecked")
                    Set<MessageQueue> queues = (Set<MessageQueue>) methodArgs[0];
                    queues.stream()
                        .map(queue -> queue.getTopic() + "|" + queue.getBrokerName() + "|"
                            + queue.getQueueId())
                        .sorted()
                        .forEach(persistedQueues::add);
                }
                return null;
            });
        first.setOffsetStore(directOffsetStore);
        boolean offsetStoreSetterPreservesIdentity = first.getOffsetStore() == directOffsetStore;
        MessageQueue assignedQueue = new MessageQueue("orders", "broker-a", 2);
        MessageQueue unassignedQueue = new MessageQueue("orders", "broker-b", 3);
        first.updateConsumeOffset(assignedQueue, 41);
        first.updateConsumeOffset(unassignedQueue, 99);
        first.getRebalanceImpl().getProcessQueueTable().put(assignedQueue, new ProcessQueue());
        first.getRebalanceImpl().getPopProcessQueueTable().put(
            new MessageQueue("orders", "broker-a", 4), new PopProcessQueue());
        first.setServiceState(ServiceState.RUNNING);
        Map<String, Object> rawRunningInfo = rawRunningInfoResults(first, resetFactory);
        first.persistConsumerOffset();
        first.setServiceState(ServiceState.CREATE_JUST);
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

        List<String> filterHookEvents = new ArrayList<>();
        first.registerFilterMessageHook(new FilterMessageHook() {
            @Override
            public String hookName() {
                return "push-raw-filter-hook-fixture";
            }

            @Override
            public void filterMessage(FilterMessageContext context) {
                filterHookEvents.add(context.isUnitMode() + ":" + context.getMsgList().size());
            }
        });
        List<MessageExt> popMessages = new ArrayList<>();
        MessageExt popMessageA = new MessageExt();
        popMessageA.setTopic("orders");
        popMessages.add(popMessageA);
        MessageExt popMessageB = new MessageExt();
        popMessageB.setTopic("orders");
        popMessages.add(popMessageB);
        java.lang.reflect.Method processPopResult = DefaultMQPushConsumerImpl.class
            .getDeclaredMethod("processPopResult", PopResult.class, SubscriptionData.class);
        processPopResult.setAccessible(true);
        processPopResult.invoke(first, new PopResult(PopStatus.FOUND, popMessages), new SubscriptionData());

        Map<String, Object> topicHelpers = topicHelperResults();

        Map<String, Object> fixture = new LinkedHashMap<>();
        fixture.put("implementationNonNull", first != null);
        fixture.put("implementationIdentityStable", first == second);
        fixture.put("rebalanceImplementationNonNull", firstRebalance != null);
        fixture.put("rebalanceImplementationIdentityStable", firstRebalance == secondRebalance);
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
        fixture.put("adjustThreadPoolDecreaseCalls", adjustThreadPoolDecreaseCalls.get());
        fixture.put("adjustThreadPoolCorePoolSizeBefore", adjustThreadPoolCorePoolSizeBefore);
        fixture.put("adjustThreadPoolCorePoolSizeAfter", adjustThreadPoolCorePoolSizeAfter);
        fixture.put("pauseBeforeSet", pauseBeforeSet);
        fixture.put("pauseAfterSetTrue", pauseAfterSetTrue);
        fixture.put("pauseAfterSetFalse", pauseAfterSetFalse);
        fixture.put("consumeOrderlyBeforeSet", consumeOrderlyBeforeSet);
        fixture.put("consumeOrderlyAfterSetTrue", consumeOrderlyAfterSetTrue);
        fixture.put("consumeOrderlyAfterSetFalse", consumeOrderlyAfterSetFalse);
        fixture.put("pullDelayBeforeSet", pullDelayBeforeSet);
        fixture.put("pullDelayAfterSet", pullDelayAfterSet);
        fixture.put("timestampResetSearches", resetFactory.searches.stream()
            .sorted()
            .collect(Collectors.toList()));
        fixture.put("timestampResetCalls", resetFactory.resets);
        fixture.put("subscriptionCountBefore", subscriptionCountBefore);
        fixture.put("subscriptionInnerIdentityStable", subscriptionInnerIdentityStable);
        fixture.put("subscriptionTopicsAfterSubscribe", subscriptionTopicsAfterSubscribe);
        fixture.put("subscribeInfoNeededBeforeRoute", subscribeInfoNeededBeforeRoute);
        fixture.put("subscribeInfoNeededAfterRoute", subscribeInfoNeededAfterRoute);
        fixture.put("unregisteredTopicNeedsRoute", unregisteredTopicNeedsRoute);
        fixture.put("tryRebalanceWhilePaused", tryRebalanceWhilePaused);
        fixture.put("offsetStoreInitiallyNull", offsetStoreInitiallyNull);
        fixture.put("offsetStoreSetterPreservesIdentity", offsetStoreSetterPreservesIdentity);
        fixture.put("offsetUpdates", offsetUpdates);
        fixture.put("persistedQueues", persistedQueues);
        fixture.putAll(rawRunningInfo);
        fixture.put("offsetStoreSetterAcceptsNull", offsetStoreSetterAcceptsNull);
        fixture.put("hasConsumeHookBeforeRegistration", hasConsumeHookBeforeRegistration);
        fixture.put("directHookEvents", directHookEvents);
        fixture.put("hasConsumeHookAfterRegistration", hasConsumeHookAfterRegistration);
        fixture.put("pushPopFilterHookEvents", filterHookEvents);
        fixture.put("rawImplementationPublicMethodSignatures", publicMethodSignatures());
        fixture.putAll(classFilterSubscribeResults());
        fixture.put("pushConsumeInitModes", pushConsumeInitModes());
        fixture.putAll(topicHelpers);
        fixture.putAll(orderlyLockedPullAttempts());
        fixture.putAll(pushQueueThresholdResults());
        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) {
            check(Files.exists(path) && json.equals(Files.readString(path)), "fixture drift: " + path);
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
        System.out.println("Push implementation accessor verified");
    }

    private static List<String> publicMethodSignatures() {
        return Arrays.stream(DefaultMQPushConsumerImpl.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> !method.isBridge() && !method.isSynthetic())
            .map(PushImplementationAccessorFixtures::signature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static String signature(Method method) {
        String parameters = Arrays.stream(method.getParameterTypes())
            .map(Class::getSimpleName)
            .collect(Collectors.joining(","));
        return method.getName() + "(" + parameters + ")";
    }

    private static Map<String, Object> topicHelperResults() {
        String group = "phase18-push-topic-helpers";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamespace("tenant");
        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();

        Set<MessageQueue> subscribedQueues = Set.of(
            new MessageQueue("tenant%orders", "broker-a", 1),
            new MessageQueue("orders", "broker-b", 2),
            new MessageQueue("other%events", "broker-c", 3));
        List<String> parsedQueues = implementation.parseSubscribeMessageQueues(subscribedQueues).stream()
            .map(queue -> queue.getTopic() + "|" + queue.getBrokerName() + "|" + queue.getQueueId())
            .sorted()
            .collect(Collectors.toList());

        List<MessageExt> popRetryMessages = new ArrayList<>();
        for (String topic : List.of("%RETRY%" + group + "_orders", "%RETRY%other_orders", "orders")) {
            MessageExt message = new MessageExt();
            message.setTopic(topic);
            popRetryMessages.add(message);
        }
        implementation.tryResetPopRetryTopic(popRetryMessages, group);
        List<String> resetPopRetryTopics = popRetryMessages.stream()
            .map(MessageExt::getTopic)
            .collect(Collectors.toList());

        List<MessageExt> retryAndNamespaceMessages = new ArrayList<>();
        MessageExt retry = new MessageExt();
        retry.setTopic("%RETRY%" + group);
        MessageAccessor.putProperty(retry, MessageConst.PROPERTY_RETRY_TOPIC, "tenant%orders");
        retryAndNamespaceMessages.add(retry);
        MessageExt ordinary = new MessageExt();
        ordinary.setTopic("tenant%events");
        retryAndNamespaceMessages.add(ordinary);
        MessageExt otherNamespace = new MessageExt();
        otherNamespace.setTopic("other%events");
        retryAndNamespaceMessages.add(otherNamespace);
        implementation.resetRetryAndNamespace(retryAndNamespaceMessages, group);
        List<String> resetRetryAndNamespaceTopics = retryAndNamespaceMessages.stream()
            .map(MessageExt::getTopic)
            .collect(Collectors.toList());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("parsedSubscribeQueues", parsedQueues);
        result.put("resetPopRetryTopics", resetPopRetryTopics);
        result.put("resetRetryAndNamespaceTopics", resetRetryAndNamespaceTopics);
        return result;
    }

    private static Map<String, Object> classFilterSubscribeResults() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("phase18-class-filter-subscribe");
        consumer.setNamespace("tenant");
        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();
        consumer.subscribe("class-filter-orders", "compat.filters.OrderFilter", "source-v1");

        SubscriptionData beforeStart = implementation.getSubscriptionInner()
            .get("tenant%class-filter-orders");
        check(beforeStart != null, "class-filter subscription stored before client factory");

        RecordingMqClientInstance factory = recordingMqClientInstance();
        implementation.setmQClientFactory(factory);
        consumer.subscribe("class-filter-orders", "compat.filters.RiskFilter", "source-v2");
        SubscriptionData afterFactory = implementation.getSubscriptionInner()
            .get("tenant%class-filter-orders");
        check(afterFactory != null, "class-filter subscription stored with client factory");

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("classFilterBeforeStartTopic", beforeStart.getTopic());
        result.put("classFilterBeforeStartClassName", beforeStart.getSubString());
        result.put("classFilterBeforeStartMode", beforeStart.isClassFilterMode());
        result.put("classFilterBeforeStartSource", beforeStart.getFilterClassSource());
        result.put("classFilterAfterFactoryTopic", afterFactory.getTopic());
        result.put("classFilterAfterFactoryClassName", afterFactory.getSubString());
        result.put("classFilterAfterFactoryMode", afterFactory.isClassFilterMode());
        result.put("classFilterAfterFactorySource", afterFactory.getFilterClassSource());
        result.put("classFilterAfterFactoryHeartbeatCalls", factory.heartbeatCalls);
        com.alibaba.fastjson2.JSONObject wireData = JSON.parseObject(JSON.toJSONString(afterFactory));
        wireData.put("subVersion", 0L);
        result.put("classFilterAfterFactoryWireData", wireData);
        result.put("classFilterSourceSerialized",
            JSON.toJSONString(afterFactory).contains("filterClassSource"));
        return result;
    }

    private static Map<String, Object> pushConsumeInitModes() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("phase18-pop-init-mode");
        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();
        Map<String, Object> modes = new LinkedHashMap<>();
        for (ConsumeFromWhere policy : ConsumeFromWhere.values()) {
            consumer.setConsumeFromWhere(policy);
            modes.put(policy.name(), implementation.getRebalanceImpl().getConsumeInitMode());
        }
        return modes;
    }

    private static Map<String, Object> pushQueueThresholdResults() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("phase18-push-queue-thresholds");
        consumer.setPullThresholdForQueue(777);
        consumer.setPullThresholdSizeForQueue(77);
        consumer.setPullThresholdForTopic(20);
        consumer.setPullThresholdSizeForTopic(12);
        consumer.subscribe("orders", "*");
        consumer.subscribe("events", "*");

        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();
        RebalancePushImpl rebalance = (RebalancePushImpl) implementation.getRebalanceImpl();
        RecordingMqClientInstance factory = recordingMqClientInstance();
        implementation.setmQClientFactory(factory);
        rebalance.setmQClientFactory(factory);

        rebalance.messageQueueChanged("orders", Set.of(), Set.of());
        int zeroAssignmentQueueThreshold = consumer.getPullThresholdForQueue();
        int zeroAssignmentSizeThreshold = consumer.getPullThresholdSizeForQueue();

        MessageQueue orders0 = new MessageQueue("orders", "broker-a", 0);
        MessageQueue orders1 = new MessageQueue("orders", "broker-a", 1);
        MessageQueue events0 = new MessageQueue("events", "broker-b", 0);
        MessageQueue events1 = new MessageQueue("events", "broker-b", 1);
        rebalance.getProcessQueueTable().put(orders0, new ProcessQueue());
        rebalance.messageQueueChanged("orders", Set.of(orders0), Set.of(orders0));
        int singleAssignmentQueueThreshold = consumer.getPullThresholdForQueue();
        int singleAssignmentSizeThreshold = consumer.getPullThresholdSizeForQueue();

        rebalance.getProcessQueueTable().put(orders1, new ProcessQueue());
        rebalance.getProcessQueueTable().put(events0, new ProcessQueue());
        rebalance.getProcessQueueTable().put(events1, new ProcessQueue());
        rebalance.messageQueueChanged("orders", Set.of(orders0, orders1), Set.of(orders0, orders1));
        int fourAssignmentQueueThreshold = consumer.getPullThresholdForQueue();
        int fourAssignmentSizeThreshold = consumer.getPullThresholdSizeForQueue();

        rebalance.getProcessQueueTable().remove(events1);
        rebalance.messageQueueChanged("events", Set.of(events0), Set.of(events0));
        int threeAssignmentQueueThreshold = consumer.getPullThresholdForQueue();
        int threeAssignmentSizeThreshold = consumer.getPullThresholdSizeForQueue();

        rebalance.getProcessQueueTable().clear();
        rebalance.messageQueueChanged("orders", Set.of(), Set.of());
        int zeroAfterAssignmentQueueThreshold = consumer.getPullThresholdForQueue();
        int zeroAfterAssignmentSizeThreshold = consumer.getPullThresholdSizeForQueue();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("pushQueueThresholdZeroAssignment", List.of(
            zeroAssignmentQueueThreshold, zeroAssignmentSizeThreshold));
        result.put("pushQueueThresholdSingleAssignment", List.of(
            singleAssignmentQueueThreshold, singleAssignmentSizeThreshold));
        result.put("pushQueueThresholdFourAssignmentsAcrossTopics", List.of(
            fourAssignmentQueueThreshold, fourAssignmentSizeThreshold));
        result.put("pushQueueThresholdThreeAssignmentsAcrossTopics", List.of(
            threeAssignmentQueueThreshold, threeAssignmentSizeThreshold));
        result.put("pushQueueThresholdZeroAfterAssignment", List.of(
            zeroAfterAssignmentQueueThreshold, zeroAfterAssignmentSizeThreshold));
        result.put("pushQueueThresholdHeartbeatCalls", factory.heartbeatCalls);
        return result;
    }

    private static Map<String, Object> orderlyLockedPullAttempts() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("phase18-orderly-pull-offset");
        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();
        implementation.setServiceState(ServiceState.RUNNING);
        implementation.setConsumeOrderly(true);

        AtomicInteger storeReads = new AtomicInteger();
        OffsetStore offsetStore = (OffsetStore) Proxy.newProxyInstance(
            OffsetStore.class.getClassLoader(),
            new Class<?>[] {OffsetStore.class},
            (proxy, method, methodArgs) -> {
                if ("readOffset".equals(method.getName())) {
                    return storeReads.getAndIncrement() == 0 ? 15L : 21L;
                }
                return null;
            });
        implementation.setOffsetStore(offsetStore);

        MessageQueue queue = new MessageQueue("orders", "broker-a", 0);
        ProcessQueue processQueue = new ProcessQueue();
        processQueue.setLocked(true);
        PullRequest request = new PullRequest();
        request.setConsumerGroup("phase18-orderly-pull-offset");
        request.setMessageQueue(queue);
        request.setProcessQueue(processQueue);
        request.setNextOffset(8L);

        try {
            implementation.pullMessage(request);
        } catch (RuntimeException expectedAfterOffsetRefresh) {
            // No live client factory is installed; the empty subscription path reaches
            // delayed pull scheduling after the first-lock offset refresh.
        }
        long firstAttemptOffset = request.getNextOffset();
        boolean previouslyLockedAfterFirstAttempt = request.isPreviouslyLocked();
        int storeReadsAfterFirstAttempt = storeReads.get();

        try {
            implementation.pullMessage(request);
        } catch (RuntimeException expectedAfterOffsetRefresh) {
            // The second attempt remains on the same controlled no-factory path.
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("orderlyFirstLockedPullOffset", firstAttemptOffset);
        result.put("orderlyFirstLockedPullPreviouslyLocked", previouslyLockedAfterFirstAttempt);
        result.put("orderlyFirstLockedPullStoreReads", storeReadsAfterFirstAttempt);
        result.put("orderlySecondLockedPullOffset", request.getNextOffset());
        result.put("orderlySecondLockedPullStoreReads", storeReads.get());
        return result;
    }

    private static long pullTimeDelayMillsWhenException(DefaultMQPushConsumerImpl implementation)
        throws Exception {
        Field delay = DefaultMQPushConsumerImpl.class.getDeclaredField("pullTimeDelayMillsWhenException");
        delay.setAccessible(true);
        return delay.getLong(implementation);
    }

    private static Map<String, Object> rawRunningInfoResults(
        DefaultMQPushConsumerImpl implementation,
        RecordingMqClientInstance factory) throws Exception {
        try {
            factory.initializeStats();
            Field consumeServiceField = DefaultMQPushConsumerImpl.class
                .getDeclaredField("consumeMessageService");
            consumeServiceField.setAccessible(true);
            consumeServiceField.set(implementation, Proxy.newProxyInstance(
                ConsumeMessageService.class.getClassLoader(),
                new Class<?>[] {ConsumeMessageService.class},
                (proxy, method, methodArgs) -> "getCorePoolSize".equals(method.getName()) ? 7 : null));
            ConsumerRunningInfo info = implementation.consumerRunningInfo();
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("rawRunningInfoSubscriptionCount", info.getSubscriptionSet().size());
            result.put("rawRunningInfoQueueCount", info.getMqTable().size());
            result.put("rawRunningInfoPopQueueCount", info.getMqPopTable().size());
            result.put("rawRunningInfoStatusCount", info.getStatusTable().size());
            result.put("rawRunningInfoCorePoolSize",
                info.getProperties().getProperty("PROP_THREADPOOL_CORE_SIZE"));
            return result;
        } finally {
            factory.shutdownStats();
        }
    }

    private static RecordingMqClientInstance recordingMqClientInstance() throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);
        RecordingMqClientInstance factory = (RecordingMqClientInstance) unsafeClass
            .getMethod("allocateInstance", Class.class)
            .invoke(unsafe, RecordingMqClientInstance.class);
        factory.initialize();
        return factory;
    }

    private static final class RecordingMqClientInstance extends MQClientInstance {
        private List<String> searches;
        private List<Map<String, Object>> resets;
        private int heartbeatCalls;
        private MQAdminImpl admin;
        private ScheduledExecutorService statsScheduler;
        private ConsumerStatsManager stats;

        private RecordingMqClientInstance() {
            super(new ClientConfig(), 1, "phase18-push-offset-reset-fixture");
        }

        private void initialize() {
            searches = new ArrayList<>();
            resets = new ArrayList<>();
            heartbeatCalls = 0;
            this.admin = new MQAdminImpl(this) {
                @Override
                public long searchOffset(MessageQueue queue, long timestamp) throws MQClientException {
                    searches.add(queue.getTopic() + "|" + queue.getBrokerName() + "|"
                        + queue.getQueueId() + "|" + timestamp);
                    return 11;
                }
            };
        }

        @Override
        public MQAdminImpl getMQAdminImpl() {
            return admin;
        }

        @Override
        public ConsumerStatsManager getConsumerStatsManager() {
            return stats;
        }

        @Override
        public boolean sendHeartbeatToAllBrokerWithLock() {
            heartbeatCalls++;
            return true;
        }

        @Override
        public boolean sendHeartbeatToAllBrokerWithLockV2(boolean isRebalance) {
            heartbeatCalls++;
            return true;
        }

        private void initializeStats() {
            statsScheduler = Executors.newSingleThreadScheduledExecutor();
            stats = new ConsumerStatsManager(statsScheduler);
        }

        private void shutdownStats() {
            if (statsScheduler != null) statsScheduler.shutdownNow();
        }

        @Override
        public synchronized void resetOffset(
            String topic, String group, Map<MessageQueue, Long> offsetTable) {
            List<String> offsets = offsetTable.entrySet().stream()
                .map(entry -> entry.getKey().getTopic() + "|" + entry.getKey().getBrokerName()
                    + "|" + entry.getKey().getQueueId() + "|" + entry.getValue())
                .sorted()
                .collect(Collectors.toList());
            Map<String, Object> reset = new LinkedHashMap<>();
            reset.put("topic", topic);
            reset.put("group", group);
            reset.put("offsets", offsets);
            resets.add(reset);
        }
    }
}
