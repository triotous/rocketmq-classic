package compat;

import java.lang.reflect.Proxy;
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
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

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

    private static List<String> subscriptionRows(Iterable<SubscriptionData> subscriptions) {
        List<String> rows = new ArrayList<>();
        for (SubscriptionData subscription : subscriptions) {
            TreeSet<String> tags = new TreeSet<>(subscription.getTagsSet());
            TreeSet<Integer> codes = new TreeSet<>(subscription.getCodeSet());
            String version = subscription.getSubVersion() == 0 ? "0" : "nonzero";
            rows.add(subscription.getTopic() + "|" + subscription.getSubString() + "|"
                + subscription.getExpressionType() + "|" + subscription.isClassFilterMode() + "|"
                + version + "|" + tags + "|" + codes);
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private static SubscriptionData tagSubscription(String topic, String tag, long version) {
        SubscriptionData subscription = new SubscriptionData();
        subscription.setTopic(topic);
        subscription.setSubString(tag);
        subscription.setSubVersion(version);
        subscription.getTagsSet().add(tag);
        subscription.getCodeSet().add(tag.hashCode());
        return subscription;
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
        String groupNameBeforeStart = first.groupName();
        MessageModel messageModelBeforeMutation = first.messageModel();
        boolean unitModeBeforeMutation = first.isUnitMode();
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setUnitMode(true);
        MessageModel messageModelAfterMutation = first.messageModel();
        boolean unitModeAfterMutation = first.isUnitMode();
        ConsumeType consumeType = first.consumeType();
        ConsumeFromWhere consumeFromWhere = first.consumeFromWhere();
        consumer.setEnableRebalance(false);
        boolean disabledTryRebalanceReturnsTrue = first.tryRebalance();
        first.doRebalance();
        consumer.setEnableRebalance(true);
        List<String> directHookEvents = new ArrayList<>();
        first.registerConsumeMessageHook(new ConsumeMessageHook() {
            @Override
            public String hookName() {
                return "pull-raw-hook-entrypoint";
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
        List<String> initialSubscriptions = subscriptionRows(first.subscriptions());
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

        Map<String, SubscriptionData> subscriptionInner = first.getRebalanceImpl().getSubscriptionInner();
        String rebalanceTopic = "rebalance-topic";
        first.subscriptionAutomatically(rebalanceTopic);
        boolean unsubscribedTopicNeedsUpdate = first.isSubscribeTopicNeedUpdate("not-subscribed");
        boolean subscribedTopicNeedsUpdateBeforeInfo = first.isSubscribeTopicNeedUpdate(rebalanceTopic);
        Set<MessageQueue> rebalanceInfo = new HashSet<>();
        rebalanceInfo.add(new MessageQueue(rebalanceTopic, "broker-a", 0));
        first.updateTopicSubscribeInfo(rebalanceTopic, rebalanceInfo);
        boolean subscribeInfoStoredForSubscribedTopic = first.getRebalanceImpl()
            .getTopicSubscribeInfoTable().containsKey(rebalanceTopic);
        boolean subscribedTopicNeedsUpdateAfterInfo = first.isSubscribeTopicNeedUpdate(rebalanceTopic);
        first.updateTopicSubscribeInfo("not-subscribed", rebalanceInfo);
        boolean subscribeInfoStoredForUnsubscribedTopic = first.getRebalanceImpl()
            .getTopicSubscribeInfoTable().containsKey("not-subscribed");
        List<String> persistedQueues = new ArrayList<>();
        OffsetStore recordingOffsetStore = (OffsetStore) Proxy.newProxyInstance(
            OffsetStore.class.getClassLoader(), new Class<?>[] {OffsetStore.class},
            (proxy, method, arguments) -> {
                if ("persistAll".equals(method.getName())) {
                    for (Object queue : (Iterable<?>) arguments[0]) {
                        persistedQueues.add(queueKey((MessageQueue) queue));
                    }
                }
                return null;
            });
        MessageQueue assignedPersistQueue = new MessageQueue("assigned-topic", "broker-persist", 4);
        first.setOffsetStore(recordingOffsetStore);
        first.setServiceState(ServiceState.RUNNING);
        first.getRebalanceImpl().getProcessQueueTable().put(assignedPersistQueue, new ProcessQueue());
        first.persistConsumerOffset();
        first.getRebalanceImpl().getProcessQueueTable().clear();
        first.setServiceState(ServiceState.CREATE_JUST);
        first.setOffsetStore(null);
        persistedQueues.sort(String::compareTo);
        first.subscriptionAutomatically("raw-topic");
        SubscriptionData automaticSubscription = subscriptionInner.get("raw-topic");
        check(automaticSubscription != null, "automatic subscription inserted");
        check("raw-topic".equals(automaticSubscription.getTopic()), "automatic topic remains raw");
        first.subscriptionAutomatically("raw-topic");
        check(subscriptionInner.get("raw-topic") == automaticSubscription,
            "automatic subscription preserves existing entry");
        first.subscriptionAutomatically("");
        SubscriptionData emptyTopicSubscription = subscriptionInner.get("");
        check(emptyTopicSubscription != null, "empty topic subscription inserted");
        SubscriptionData existingSubscription = new SubscriptionData();
        existingSubscription.setTopic("existing-topic");
        existingSubscription.setSubString("tag-a");
        existingSubscription.setExpressionType("SQL92");
        existingSubscription.setSubVersion(7L);
        subscriptionInner.put("existing-topic", existingSubscription);
        first.subscriptionAutomatically("existing-topic");
        check(subscriptionInner.get("existing-topic") == existingSubscription,
            "automatic subscription preserves custom entry");
        first.unsubscribe("raw-topic");
        check(!subscriptionInner.containsKey("raw-topic"), "unsubscribe removes exact topic");
        first.unsubscribe("");
        check(!subscriptionInner.containsKey(""), "unsubscribe removes empty topic");
        first.unsubscribe("missing-topic");
        consumer.registerMessageQueueListener("registered-topic", null);
        String registeredTopic = "tenant%registered-topic";
        first.subscriptionAutomatically(registeredTopic);
        first.unsubscribe(registeredTopic);
        check(!subscriptionInner.containsKey(registeredTopic), "unsubscribe removes namespaced key as supplied");
        check(consumer.getRegisterTopics().contains(registeredTopic),
            "unsubscribe leaves facade topic registration unchanged");
        List<String> topicSubscriptions = subscriptionRows(first.subscriptions());
        consumer.addRegisterSubscriptions("explicit-topic", MessageSelector.byTag("red"));
        List<String> explicitSubscriptions = subscriptionRows(first.subscriptions());
        consumer.getRegisterSubscriptions().clear();
        consumer.getRegisterSubscriptions().add(tagSubscription("duplicate-topic", "red", 11));
        consumer.getRegisterSubscriptions().add(tagSubscription("duplicate-topic", "red", 12));
        List<String> duplicateVersionSubscriptions = subscriptionRows(first.subscriptions());

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
        fixture.put("subscriptionAutomaticallyTopic", automaticSubscription.getTopic());
        fixture.put("subscriptionAutomaticallyInputTopic", "raw-topic");
        fixture.put("subscriptionAutomaticallyExpression", automaticSubscription.getSubString());
        fixture.put("subscriptionAutomaticallyExpressionType", automaticSubscription.getExpressionType());
        fixture.put("subscriptionAutomaticallyClassFilterMode", automaticSubscription.isClassFilterMode());
        fixture.put("subscriptionAutomaticallyVersionPositive", automaticSubscription.getSubVersion() > 0);
        fixture.put("subscriptionAutomaticallyTags", new TreeSet<>(automaticSubscription.getTagsSet()));
        fixture.put("subscriptionAutomaticallyCodes", new TreeSet<>(automaticSubscription.getCodeSet()));
        fixture.put("subscriptionAutomaticallyIdempotent", true);
        fixture.put("subscriptionAutomaticallyEmptyTopicInput", "");
        fixture.put("subscriptionAutomaticallyEmptyTopicOutput", emptyTopicSubscription.getTopic());
        fixture.put("subscriptionAutomaticallyExistingTopic", "existing-topic");
        fixture.put("subscriptionAutomaticallyExistingExpression", "tag-a");
        fixture.put("subscriptionAutomaticallyExistingExpressionType", "SQL92");
        fixture.put("subscriptionAutomaticallyExistingVersion", existingSubscription.getSubVersion());
        fixture.put("subscriptionAutomaticallyCustomEntryPreserved",
            subscriptionInner.get("existing-topic") == existingSubscription);
        fixture.put("unsubscribeRemovedExactTopic", !subscriptionInner.containsKey("raw-topic"));
        fixture.put("unsubscribeRemovedEmptyTopic", !subscriptionInner.containsKey(""));
        fixture.put("unsubscribeLeavesOtherTopic", subscriptionInner.containsKey("existing-topic"));
        fixture.put("unsubscribeRegisteredTopic", registeredTopic);
        fixture.put("unsubscribeLeavesFacadeRegistration",
            consumer.getRegisterTopics().contains(registeredTopic));
        fixture.put("subscriptionAutomaticallyDoesNotNamespace", "raw-topic".equals(automaticSubscription.getTopic()));
        fixture.put("unsubscribedTopicNeedsUpdate", unsubscribedTopicNeedsUpdate);
        fixture.put("subscribedTopicNeedsUpdateBeforeInfo", subscribedTopicNeedsUpdateBeforeInfo);
        fixture.put("subscribeInfoStoredForSubscribedTopic", subscribeInfoStoredForSubscribedTopic);
        fixture.put("subscribedTopicNeedsUpdateAfterInfo", subscribedTopicNeedsUpdateAfterInfo);
        fixture.put("subscribeInfoStoredForUnsubscribedTopic", subscribeInfoStoredForUnsubscribedTopic);
        fixture.put("persistConsumerOffsetOutputQueues", persistedQueues);
        fixture.put("groupNameBeforeStart", groupNameBeforeStart);
        fixture.put("messageModelBeforeMutation", messageModelBeforeMutation.name());
        fixture.put("messageModelAfterMutation", messageModelAfterMutation.name());
        fixture.put("unitModeBeforeMutation", unitModeBeforeMutation);
        fixture.put("unitModeAfterMutation", unitModeAfterMutation);
        fixture.put("consumeType", consumeType.name());
        fixture.put("consumeFromWhere", consumeFromWhere.name());
        fixture.put("disabledTryRebalanceReturnsTrue", disabledTryRebalanceReturnsTrue);
        fixture.put("directHookEvents", directHookEvents);
        fixture.put("subscriptionsInitially", initialSubscriptions);
        fixture.put("subscriptionsFromTopics", topicSubscriptions);
        fixture.put("subscriptionsFromExplicit", explicitSubscriptions);
        fixture.put("subscriptionsWithSameSpecDifferentVersions", duplicateVersionSubscriptions);
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
