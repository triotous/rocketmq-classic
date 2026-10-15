package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.compression.Compressor;
import org.apache.rocketmq.common.compression.CompressionType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/** Offline Java 5.5.1 producer API and send-flow oracle. */
public final class ProducerCoreFixtures {
    private ProducerCoreFixtures() { }

    @FunctionalInterface
    private interface Check { void run() throws Exception; }

    private static Map<String, Object> row(Object input, boolean valid) {
        Map<String, Object> row = new TreeMap<>();
        row.put("input", input);
        row.put("valid", valid);
        return row;
    }

    private static boolean accepts(Check check) {
        try {
            check.run();
            return true;
        } catch (Exception expected) {
            return false;
        }
    }

    private static long totalAccumulatorHoldBytes(DefaultMQProducer producer) {
        try {
            Field producerAccumulator = DefaultMQProducer.class.getDeclaredField("produceAccumulator");
            producerAccumulator.setAccessible(true);
            Object accumulator = producerAccumulator.get(producer);
            Field totalHoldSize = accumulator.getClass().getDeclaredField("totalHoldSize");
            totalHoldSize.setAccessible(true);
            return totalHoldSize.getLong(accumulator);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not inspect pinned accumulator limit", error);
        }
    }

    private static boolean isBatchable(DefaultMQProducer producer, Message message) {
        try {
            Method canBatch = DefaultMQProducer.class.getDeclaredMethod("canBatch", Message.class);
            canBatch.setAccessible(true);
            return (Boolean) canBatch.invoke(producer, message);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not invoke pinned auto-batch eligibility", error);
        }
    }

    private static Semaphore asyncSendNumSemaphore(DefaultMQProducer producer) {
        try {
            Field implementationField = DefaultMQProducer.class.getDeclaredField("defaultMQProducerImpl");
            implementationField.setAccessible(true);
            Object implementation = implementationField.get(producer);
            Field semaphoreField = implementation.getClass().getDeclaredField("semaphoreAsyncSendNum");
            semaphoreField.setAccessible(true);
            return (Semaphore) semaphoreField.get(implementation);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not inspect pinned async-send semaphore", error);
        }
    }

    private static boolean acceptsMessage(DefaultMQProducer producer, String topic, byte[] body,
        String multiDispatch) {
        Message message = new Message();
        message.setTopic(topic);
        message.setBody(body);
        if (multiDispatch != null) {
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_INNER_MULTI_DISPATCH, multiDispatch);
        }
        return accepts(() -> Validators.checkMessage(message, producer));
    }

    private static Map<String, Object> produce() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer();
        Map<String, Object> result = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        provenance.put("entrypoints", List.of("DefaultMQProducer", "Validators", "TopicValidator",
            "DefaultMQProducerImpl.tryToCompressMessage", "MessageClientIDSetter.createUniqID",
            "MessageBatch.generateFromList", "MessageBatch.encode", "MQFaultStrategy",
            "LatencyFaultToleranceImpl.detectByOneRound", "MQClientAPIImpl.getMaxOffset"));
        provenance.put("serverContacted", false);
        result.put("provenance", provenance);
        result.put("producerImplementationApi", producerImplementationApi());
        result.put("producerFacadeApi", producerFacadeApi());
        result.put("topicPublishInfoApi", publicApiSurface(TopicPublishInfo.class));
        result.put("topicPublishInfoQueueFilterApi",
            publicApiSurface(TopicPublishInfo.QueueFilter.class));
        result.put("threadLocalIndexApi", threadLocalIndexApi());
        result.put("producerConstructorCases", producerConstructorCases());
        result.put("runtimeAccumulatorSettings", runtimeAccumulatorSettings());
        result.put("implementationHookExecution", implementationHookExecution());
        result.put("implementationStateAccessors", implementationStateAccessors());
        result.put("implementationSemaphoreSettings", implementationSemaphoreSettings());
        result.put("implementationPublishInfoTable", implementationPublishInfoTable());
        result.put("implementationPublishInfoTableAdvanced", implementationPublishInfoTableAdvanced());
        result.put("implementationQueueFilterSelection", implementationQueueFilterSelection());
        result.put("implementationSelectorFailure", implementationSelectorFailure());
        result.put("implementationSelectorMessageSnapshot", implementationSelectorMessageSnapshot());
        result.put("producerClientFactoryApi", publicApiSurface(MQClientInstance.class));
        result.put("mqFaultStrategyApi", publicApiSurface(MQFaultStrategy.class));
        result.put("mqFaultStrategyBrokerFilterApi", publicApiSurface(MQFaultStrategy.BrokerFilter.class));

        Map<String, Object> defaults = new TreeMap<>();
        defaults.put("producerGroup", producer.getProducerGroup());
        defaults.put("createTopicKey", producer.getCreateTopicKey());
        defaults.put("defaultTopicQueueNums", producer.getDefaultTopicQueueNums());
        defaults.put("sendMsgTimeout", producer.getSendMsgTimeout());
        defaults.put("sendMsgMaxTimeoutPerRequest", producer.getSendMsgMaxTimeoutPerRequest());
        defaults.put("compressMsgBodyOverHowmuch", producer.getCompressMsgBodyOverHowmuch());
        defaults.put("compressType", producer.getCompressType().name());
        defaults.put("compressLevel", producer.getCompressLevel());
        defaults.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        defaults.put("retryTimesWhenSendAsyncFailed", producer.getRetryTimesWhenSendAsyncFailed());
        defaults.put("retryAnotherBrokerWhenNotStoreOK", producer.isRetryAnotherBrokerWhenNotStoreOK());
        defaults.put("maxMessageSize", producer.getMaxMessageSize());
        defaults.put("sendMessageWithVIPChannel", producer.isSendMessageWithVIPChannel());
        defaults.put("autoBatch", producer.getAutoBatch());
        defaults.put("batchMaxDelayMs", producer.getBatchMaxDelayMs());
        defaults.put("batchMaxBytes", producer.getBatchMaxBytes());
        defaults.put("totalBatchMaxBytes", producer.getTotalBatchMaxBytes());
        defaults.put("enableBackpressureForAsyncMode", producer.isEnableBackpressureForAsyncMode());
        defaults.put("backPressureForAsyncSendNum", producer.getBackPressureForAsyncSendNum());
        defaults.put("backPressureForAsyncSendSize", producer.getBackPressureForAsyncSendSize());
        defaults.put("retryResponseCodes", new TreeSet<>(producer.getRetryResponseCodes()));
        result.put("defaults", defaults);
        result.put("executorSettings", executorSettings());
        result.put("checkForbiddenContext", checkForbiddenContext());
        result.put("sendMessageContext", sendMessageContext());

        Map<String, Object> batchSettings = new TreeMap<>();
        DefaultMQProducer configuredBatch = new DefaultMQProducer("batch-settings");
        configuredBatch.setInstanceName("configured-batch-settings");
        configuredBatch.batchMaxDelayMs(25);
        configuredBatch.batchMaxBytes(65_536);
        configuredBatch.totalBatchMaxBytes(1_048_576);
        configuredBatch.initProduceAccumulator();
        batchSettings.put("configuredBatchMaxDelayMs", configuredBatch.getBatchMaxDelayMs());
        batchSettings.put("configuredBatchMaxBytes", configuredBatch.getBatchMaxBytes());
        batchSettings.put("configuredTotalBatchMaxBytes", configuredBatch.getTotalBatchMaxBytes());
        batchSettings.put("configuredTotalBatchHoldBytes", totalAccumulatorHoldBytes(configuredBatch));
        result.put("batchSettings", batchSettings);

        DefaultMQProducer initializedAccumulator = new DefaultMQProducer("accumulator-defaults");
        initializedAccumulator.setInstanceName("accumulator-defaults");
        initializedAccumulator.initProduceAccumulator();
        Map<String, Object> accumulatorDefaults = new TreeMap<>();
        accumulatorDefaults.put("batchMaxDelayMs", initializedAccumulator.getBatchMaxDelayMs());
        accumulatorDefaults.put("batchMaxBytes", initializedAccumulator.getBatchMaxBytes());
        accumulatorDefaults.put("totalBatchMaxBytes", initializedAccumulator.getTotalBatchMaxBytes());
        accumulatorDefaults.put("totalBatchHoldBytes", totalAccumulatorHoldBytes(initializedAccumulator));
        result.put("accumulatorDefaults", accumulatorDefaults);

        DefaultMQProducer autoBatchLifecycle = new DefaultMQProducer("auto-batch-lifecycle");
        autoBatchLifecycle.setInstanceName("auto-batch-lifecycle");
        boolean autoBatchBeforeAccumulator = autoBatchLifecycle.getAutoBatch();
        autoBatchLifecycle.setAutoBatch(true);
        autoBatchLifecycle.initProduceAccumulator();
        Map<String, Object> autoBatch = new TreeMap<>();
        autoBatch.put("enabledBeforeAccumulator", autoBatchBeforeAccumulator);
        autoBatch.put("enabledAfterAccumulator", autoBatchLifecycle.getAutoBatch());
        autoBatchLifecycle.setAutoBatch(false);
        autoBatch.put("disabledAfterRuntimeToggle", autoBatchLifecycle.getAutoBatch());
        autoBatchLifecycle.setAutoBatch(true);
        autoBatch.put("enabledAfterRuntimeToggle", autoBatchLifecycle.getAutoBatch());
        result.put("autoBatchLifecycle", autoBatch);
        result.put("sendInputMutation", sendInputMutation());

        DefaultMQProducer compressorProducer = new DefaultMQProducer("compressor-settings");
        List<Map<String, Object>> compressorTypes = new ArrayList<>();
        byte[] compressorInput = new byte[] {0, 1, 2, 3, 127, (byte) 255};
        for (CompressionType type : CompressionType.values()) {
            compressorProducer.setCompressType(type);
            Compressor compressor = compressorProducer.getCompressor();
            byte[] compressed = compressor.compress(compressorInput,
                compressorProducer.getCompressLevel());
            Map<String, Object> compressorType = new TreeMap<>();
            compressorType.put("type", compressorProducer.getCompressType().name());
            compressorType.put("class", compressor.getClass().getSimpleName());
            compressorType.put("roundTrip",
                Arrays.equals(compressorInput, compressor.decompress(compressed)));
            compressorTypes.add(compressorType);
        }
        compressorProducer.setCompressType(CompressionType.ZLIB);
        Compressor retainedCompressor = compressorProducer.getCompressor();
        compressorProducer.setCompressType(CompressionType.LZ4);
        byte[] retainedCompressed = retainedCompressor.compress(compressorInput,
            compressorProducer.getCompressLevel());
        Map<String, Object> compressorSettings = new TreeMap<>();
        compressorSettings.put("types", compressorTypes);
        compressorSettings.put("currentTypeAfterUpdate",
            compressorProducer.getCompressType().name());
        compressorSettings.put("retainedRoundTripAfterUpdate",
            Arrays.equals(compressorInput, retainedCompressor.decompress(retainedCompressed)));
        compressorSettings.put("retainedClassAfterUpdate",
            retainedCompressor.getClass().getSimpleName());
        compressorSettings.put("currentClassAfterUpdate",
            compressorProducer.getCompressor().getClass().getSimpleName());
        result.put("compressorSettings", compressorSettings);

        DefaultMQProducer autoBatchEligibility = new DefaultMQProducer("auto-batch-eligibility");
        autoBatchEligibility.setInstanceName("auto-batch-eligibility");
        autoBatchEligibility.initProduceAccumulator();
        List<Map<String, Object>> autoBatchCases = new ArrayList<>();
        autoBatchCases.add(row("regular", isBatchable(autoBatchEligibility,
            new Message("orders", new byte[] {1}))));
        Message delayedAutoBatch = new Message("orders", new byte[] {1});
        MessageAccessor.putProperty(delayedAutoBatch, MessageConst.PROPERTY_TIMER_DELAY_MS, "100");
        autoBatchCases.add(row("delayed", isBatchable(autoBatchEligibility, delayedAutoBatch)));
        autoBatchCases.add(row("retry-topic", isBatchable(autoBatchEligibility,
            new Message("%RETRY%workers", new byte[] {1}))));
        Message groupedAutoBatch = new Message("orders", new byte[] {1});
        MessageAccessor.putProperty(groupedAutoBatch, MessageConst.PROPERTY_PRODUCER_GROUP, "transaction-producer");
        autoBatchCases.add(row("producer-group", isBatchable(autoBatchEligibility, groupedAutoBatch)));
        result.put("autoBatchEligibility", autoBatchCases);

        DefaultMQProducer backpressureSettings = new DefaultMQProducer("backpressure-settings");
        backpressureSettings.setBackPressureForAsyncSendNum(1);
        backpressureSettings.setBackPressureForAsyncSendSize(1);
        Map<String, Object> backpressure = new TreeMap<>();
        backpressure.put("clampedSendNum", backpressureSettings.getBackPressureForAsyncSendNum());
        backpressure.put("clampedSendSize", backpressureSettings.getBackPressureForAsyncSendSize());
        result.put("backpressureSettings", backpressure);
        result.put("runtimeSettings", runtimeSettings());
        result.put("perRequestTimeoutRetry", perRequestTimeoutRetry());
        result.put("missingBrokerAddressRetry", missingBrokerAddressRetry());
        result.put("missingBrokerAddressExplicit", missingBrokerAddressExplicit());
        result.put("checkForbiddenRetry", checkForbiddenRetry());
        result.put("checkForbiddenDefaultAsync", checkForbiddenDefaultAsync());
        result.put("checkForbiddenExplicitAsync", checkForbiddenExplicitAsync());
        result.put("checkForbiddenDeadline", checkForbiddenDeadline());

        List<Map<String, Object>> groups = new ArrayList<>();
        for (String value : Arrays.asList(" ", "g".repeat(120), "g".repeat(121), "bad.group", "a|b_%")) {
            groups.add(row(value, accepts(() -> Validators.checkGroup(value))));
        }
        result.put("groups", groups);

        List<Map<String, Object>> topics = new ArrayList<>();
        for (String value : Arrays.asList(" ", "t".repeat(127), "t".repeat(128), "bad.topic",
            "bad." + "t".repeat(124), "%RETRY%" + "t".repeat(121))) {
            topics.add(row(value, accepts(() -> Validators.checkTopic(value))));
        }
        result.put("topics", topics);

        List<Map<String, Object>> systemTopics = new ArrayList<>();
        for (String value : Arrays.asList("TBW102", "RMQ_SYS_TRACE_TOPIC", "SCHEDULE_TOPIC_XXXX", "rmq_sys_custom")) {
            Map<String, Object> row = new TreeMap<>();
            row.put("topic", value);
            row.put("system", TopicValidator.isSystemTopic(value));
            row.put("sendAllowed", accepts(() -> Validators.isNotAllowedSendTopic(value))
                && !TopicValidator.isNotAllowedSendTopic(value));
            // isNotAllowedSendTopic returns normally for both allowed and forbidden topics.
            row.put("sendForbidden", TopicValidator.isNotAllowedSendTopic(value));
            systemTopics.add(row);
        }
        result.put("systemTopics", systemTopics);

        List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(row("null-message", accepts(() -> Validators.checkMessage(null, producer))));
        messages.add(row("null-body", accepts(() -> Validators.checkMessage(new Message("orders", (byte[]) null), producer))));
        messages.add(row("empty-body", accepts(() -> Validators.checkMessage(new Message("orders", new byte[0]), producer))));
        messages.add(row("body-max", acceptsMessage(producer, "orders", new byte[producer.getMaxMessageSize()], null)));
        messages.add(row("body-max-plus-one", acceptsMessage(producer, "orders", new byte[producer.getMaxMessageSize() + 1], null)));
        messages.add(row("multi-dispatch-separator", acceptsMessage(producer, "orders", new byte[] {1}, "a/b")));
        result.put("messages", messages);

        Map<String, Object> compression = new TreeMap<>();
        int threshold = producer.getCompressMsgBodyOverHowmuch();
        compression.put("threshold", threshold);
        compression.put("cases", compressionCases(producer, threshold));
        result.put("compression", compression);
        result.put("runtimeCompressionSettings", runtimeCompressionSettings());
        result.put("compressionProperties", compressionProperties());

        Map<String, Object> faultStrategy = new TreeMap<>();
        faultStrategy.put("latencyMaxMillis", longs(producer.getLatencyMax()));
        faultStrategy.put("notAvailableDurationMillis", longs(producer.getNotAvailableDuration()));
        faultStrategy.put("sendLatencyFaultEnable", producer.isSendLatencyFaultEnable());
        faultStrategy.put("startDetectorEnable", producer.isStartDetectorEnable());
        faultStrategy.put("detectTimeoutMillis", producer.getDetectTimeout());
        faultStrategy.put("detectIntervalMillis", producer.getDetectInterval());
        faultStrategy.put("scheduleInitialDelaySeconds", 3);
        faultStrategy.put("schedulePeriodSeconds", 3);
        faultStrategy.put("probeRequestCode", RequestCode.GET_MAX_OFFSET);
        faultStrategy.put("probeQueueId", 0);
        faultStrategy.put("probeCommitted", true);
        result.put("faultStrategy", faultStrategy);
        result.put("runtimeFaultStrategy", runtimeFaultStrategy());
        result.put("runtimeFaultStrategyNullArrays", runtimeFaultStrategyNullArrays());

        String uniqueId = MessageClientIDSetter.createUniqID();
        int ipBytes = MessageClientIDSetter.getIPFromID(uniqueId).length;
        Map<String, Object> uniqueIdShape = new TreeMap<>();
        uniqueIdShape.put("hexUppercase", uniqueId.matches("[0-9A-F]+"));
        uniqueIdShape.put("length", uniqueId.length());
        uniqueIdShape.put("ipBytes", ipBytes);
        uniqueIdShape.put("processIdBytes", 2);
        uniqueIdShape.put("processIdentityBytes", 4);
        uniqueIdShape.put("monthTimestampBytes", 4);
        uniqueIdShape.put("counterBytes", 2);
        result.put("uniqueId", uniqueIdShape);
        result.put("uniqueIdHelpers", uniqueIdHelpers());

        result.put("batchValidation", batchCases());
        result.put("batchEncoding", batchEncoding());
        return result;
    }

    private static Map<String, Object> uniqueIdHelpers() {
        String ipv4 = "C000020A123456789ABC010203040005";
        String ipv6 = "20010DB8000000000000000000000020123456789ABC010203040005";
        Map<String, Object> result = new TreeMap<>();
        result.put("ipv4Id", ipv4);
        result.put("ipv4AddressBytes", toHex(MessageClientIDSetter.getIPFromID(ipv4)));
        result.put("ipv4Address", MessageClientIDSetter.getIPStrFromID(ipv4));
        result.put("ipv4Pid", MessageClientIDSetter.getPidFromID(ipv4));
        result.put("ipv4NearlyTimeMonthOffsetMillis",
            nearlyTimeMonthOffsetMillis(MessageClientIDSetter.getNearlyTimeFromID(ipv4)));
        result.put("ipv6Id", ipv6);
        result.put("ipv6AddressBytes", toHex(MessageClientIDSetter.getIPFromID(ipv6)));
        result.put("ipv6Address", MessageClientIDSetter.getIPStrFromID(ipv6));
        result.put("ipv6Pid", MessageClientIDSetter.getPidFromID(ipv6));
        result.put("ipv6NearlyTimeMonthOffsetMillis",
            nearlyTimeMonthOffsetMillis(MessageClientIDSetter.getNearlyTimeFromID(ipv6)));
        result.put("oddTrailingNibbleIgnored",
            toHex(MessageClientIDSetter.getIPFromID("C000020A1")));
        result.put("invalidHexNibbles", toHex(MessageClientIDSetter.getIPFromID("G0H0I0J0")));
        result.put("fakeIpLength", MessageClientIDSetter.createFakeIP().length);
        result.put("shortIpThrows", !"none".equals(failureType(
            () -> MessageClientIDSetter.getIPFromID("AA"))));
        result.put("shortPidThrows", !"none".equals(failureType(
            () -> MessageClientIDSetter.getPidFromID("AA"))));
        result.put("shortTimestampThrows", !"none".equals(failureType(
            () -> MessageClientIDSetter.getNearlyTimeFromID("AA"))));
        return result;
    }

    private static long nearlyTimeMonthOffsetMillis(Date nearlyTime) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(nearlyTime);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return nearlyTime.getTime() - calendar.getTimeInMillis();
    }

    private static Map<String, Object> sendInputMutation() {
        DefaultMQProducer producer = new DefaultMQProducer("send-input-mutation");
        producer.setInstanceName("send-input-mutation");
        producer.batchMaxBytes(1);
        producer.initProduceAccumulator();
        Message message = new Message("orders", new byte[] {1, 2, 3});
        String sendOutcome = "returned";
        try {
            producer.sendByAccumulator(message, null, null);
        } catch (Throwable error) {
            sendOutcome = error.getClass().getSimpleName();
        }
        String uniqueId = message.getProperty("UNIQ_KEY");

        Message directMessage = new Message("orders", new byte[] {1, 2, 3});
        String directSendOutcome = "returned";
        try {
            producer.sendDirect(directMessage, null, null);
        } catch (Throwable error) {
            directSendOutcome = error.getClass().getSimpleName();
        }
        String directUniqueId = directMessage.getProperty("UNIQ_KEY");

        Map<String, Object> result = new TreeMap<>();
        result.put("uniqueIdPresent", uniqueId != null);
        result.put("uniqueIdLength", uniqueId == null ? 0 : uniqueId.length());
        result.put("topic", message.getTopic());
        result.put("sendOutcome", sendOutcome);
        result.put("directUniqueIdPresent", directUniqueId != null);
        result.put("directSendOutcome", directSendOutcome);
        return result;
    }

    private static Map<String, Object> producerImplementationApi() {
        Object implementation = new DefaultMQProducer("producer-implementation-api")
            .getDefaultMQProducerImpl();
        Class<?> implementationClass = implementation.getClass();
        List<String> constructors = Arrays.stream(implementationClass.getConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(ProducerCoreFixtures::constructorSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        List<String> methods = Arrays.stream(implementationClass.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        Map<String, Object> result = new TreeMap<>();
        result.put("className", implementationClass.getName());
        result.put("publicConstructors", constructors);
        result.put("publicMethods", methods);
        result.put("publicMethodCount", methods.size());
        return result;
    }

    private static Map<String, Object> producerFacadeApi() {
        Map<String, Object> result = new TreeMap<>();
        result.put("DefaultMQProducer", publicApiSurface(DefaultMQProducer.class));
        result.put("MQProducer", publicApiSurface(MQProducer.class));
        result.put("MQAdmin", publicApiSurface(MQAdmin.class));
        return result;
    }

    private static Map<String, Object> threadLocalIndexApi() {
        ThreadLocalIndex index = new ThreadLocalIndex();
        Map<String, Object> result = new TreeMap<>();
        result.put("publicApi", publicApiSurface(ThreadLocalIndex.class));
        result.put("initialString", index.toString());
        int incremented = index.incrementAndGet();
        result.put("incrementedNonnegative", incremented >= 0);
        result.put("incrementMatchesMask",
            (threadLocalIndexValue(index) & Integer.MAX_VALUE) == incremented);
        index.reset();
        result.put("resetValueNonnegative", threadLocalIndexValue(index) >= 0);

        try {
            java.lang.reflect.Field field = ThreadLocalIndex.class.getDeclaredField("threadLocalIndex");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            ThreadLocal<Integer> values = (ThreadLocal<Integer>) field.get(index);
            values.set(Integer.MAX_VALUE);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("failed to seed ThreadLocalIndex overflow", error);
        }
        result.put("overflowFirst", index.incrementAndGet());
        result.put("overflowSecond", index.incrementAndGet());
        result.put("afterOverflowString", index.toString());
        return result;
    }

    private static int threadLocalIndexValue(ThreadLocalIndex index) {
        String text = index.toString();
        return Integer.parseInt(text.substring(text.indexOf('=') + 1, text.length() - 1));
    }

    private static Map<String, Object> producerConstructorCases() {
        RPCHook hook = new RPCHook() {
            @Override
            public void doBeforeRequest(String remoteAddr, RemotingCommand request) { }

            @Override
            public void doAfterResponse(String remoteAddr, RemotingCommand request,
                RemotingCommand response) { }
        };
        List<String> topics = new ArrayList<>(List.of("constructor-topic-a", "constructor-topic-b"));
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(producerConstructorCase("no-arg", new DefaultMQProducer(), null, null));
        cases.add(producerConstructorCase("rpc-hook", new DefaultMQProducer(hook), hook, null));
        cases.add(producerConstructorCase("group", new DefaultMQProducer("constructor-group"), null, null));
        cases.add(producerConstructorCase("group-rpc-hook",
            new DefaultMQProducer("constructor-group-hook", hook), hook, null));
        cases.add(producerConstructorCase("group-rpc-hook-topics",
            new DefaultMQProducer("constructor-group-topics", hook, topics), hook, topics));
        cases.add(producerConstructorCase("group-trace",
            new DefaultMQProducer("constructor-group-trace", true, "constructor-trace-topic"), null, null));
        cases.add(producerConstructorCase("group-rpc-hook-trace",
            new DefaultMQProducer("constructor-group-hook-trace", hook, true,
                "constructor-hook-trace-topic"), hook, null));
        cases.add(producerConstructorCase("group-rpc-hook-topics-trace",
            new DefaultMQProducer("constructor-group-full", hook, topics, true,
                "constructor-full-trace-topic"), hook, topics));
        cases.add(producerConstructorCase("namespace-group",
            new DefaultMQProducer("constructor-namespace", "constructor-legacy-group"), null, null));
        cases.add(producerConstructorCase("namespace-group-rpc-hook",
            new DefaultMQProducer("constructor-namespace-hook", "constructor-legacy-hook-group", hook),
            hook, null));
        cases.add(producerConstructorCase("namespace-group-rpc-hook-trace",
            new DefaultMQProducer("constructor-namespace-trace", "constructor-legacy-trace-group", hook,
                true, "constructor-legacy-trace-topic"), hook, null));
        Map<String, Object> result = new TreeMap<>();
        result.put("cases", cases);
        result.put("count", cases.size());
        List<Map<String, Object>> nullableCases = new ArrayList<>();
        nullableCases.add(producerConstructorCase("null-producer-group",
            new DefaultMQProducer((String) null), null, null));
        nullableCases.add(producerConstructorCase("null-producer-group-with-namespace",
            new DefaultMQProducer("constructor-null-group-namespace", (String) null), null, null));
        DefaultMQProducer nullGroupSetter = new DefaultMQProducer("constructor-group-before-null");
        String groupBeforeSetter = nullGroupSetter.getProducerGroup();
        nullGroupSetter.setProducerGroup(null);
        Map<String, Object> nullGroupSetterCase = producerConstructorCase(
            "null-producer-group-setter", nullGroupSetter, null, null);
        nullGroupSetterCase.put("producerGroupBeforeSetter", groupBeforeSetter);
        nullableCases.add(nullGroupSetterCase);
        nullableCases.add(producerConstructorCase("null-namespace",
            new DefaultMQProducer((String) null, "constructor-null-namespace-group"), null, null));
        nullableCases.add(producerConstructorCase("null-initial-topics",
            new DefaultMQProducer("constructor-null-topics", hook, (List<String>) null), hook, null));
        nullableCases.add(producerConstructorCase("null-trace-topic",
            new DefaultMQProducer("constructor-null-trace-topic", hook, true, null), hook, null));
        result.put("nullableCases", nullableCases);
        result.put("nullableCaseCount", nullableCases.size());
        result.put("nullProducerGroupStart", producerStartOutcome(new DefaultMQProducer((String) null)));
        result.put("nullNamespacedProducerGroupStart", producerStartOutcome(
            new DefaultMQProducer("constructor-null-group-namespace", (String) null)));
        return result;
    }

    private static Map<String, Object> producerStartOutcome(DefaultMQProducer producer) {
        Map<String, Object> result = new TreeMap<>();
        try {
            producer.start();
            result.put("failureClass", "");
            result.put("failureMessage", "");
        } catch (Throwable failure) {
            result.put("failureClass", failure.getClass().getName());
            result.put("failureMessage", failure.getMessage() == null ? "" : failure.getMessage());
        }
        result.put("serviceState", producer.getDefaultMQProducerImpl().getServiceState().name());
        return result;
    }

    private static Map<String, Object> producerConstructorCase(String name, DefaultMQProducer producer,
        RPCHook suppliedHook, List<String> suppliedTopics) {
        Map<String, Object> result = new TreeMap<>();
        result.put("name", name);
        result.put("producerGroupIsNull", producer.getProducerGroup() == null);
        result.put("producerGroup", producer.getProducerGroup() == null ? "" : producer.getProducerGroup());
        result.put("namespaceIsNull", producer.getNamespace() == null);
        result.put("namespace", producer.getNamespace() == null ? "" : producer.getNamespace());
        result.put("rpcHookSupplied", suppliedHook != null);
        result.put("rpcHookRetainedByFacade", suppliedHook != null
            && readProducerField(producer, "rpcHook") == suppliedHook);
        result.put("rpcHookRetainedByImplementation",
            suppliedHook != null && readImplementationField(producer, "rpcHook") == suppliedHook);
        result.put("topicsIsNull", producer.getTopics() == null);
        result.put("topics", producer.getTopics() == null ? List.of() : new ArrayList<>(producer.getTopics()));
        result.put("topicsInputIdentityRetained", suppliedTopics != null
            && suppliedTopics == producer.getTopics());
        result.put("enableTrace", producer.isEnableTrace());
        result.put("traceTopicIsNull", producer.getTraceTopic() == null);
        result.put("traceTopic", producer.getTraceTopic() == null ? "" : producer.getTraceTopic());
        return result;
    }

    private static Object readProducerField(DefaultMQProducer producer, String fieldName) {
        try {
            Field field = DefaultMQProducer.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(producer);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not inspect producer constructor field " + fieldName, error);
        }
    }

    private static Object readImplementationField(DefaultMQProducer producer, String fieldName) {
        try {
            Object implementation = producer.getDefaultMQProducerImpl();
            Field field = implementation.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(implementation);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Could not inspect producer implementation field " + fieldName, error);
        }
    }

    private static Map<String, Object> runtimeAccumulatorSettings() {
        DefaultMQProducer producer = new DefaultMQProducer("runtime-accumulator-settings");
        producer.setInstanceName("runtime-accumulator-settings");
        Map<String, Object> result = new TreeMap<>();
        result.put("beforeInitializationDelay", producer.getBatchMaxDelayMs());
        result.put("beforeInitializationBytes", producer.getBatchMaxBytes());
        result.put("beforeInitializationTotalBytes", producer.getTotalBatchMaxBytes());

        producer.initProduceAccumulator();
        result.put("initializedDelay", producer.getBatchMaxDelayMs());
        result.put("initializedBytes", producer.getBatchMaxBytes());
        result.put("initializedTotalGetterBytes", producer.getTotalBatchMaxBytes());
        result.put("initializedTotalHoldBytes", totalAccumulatorHoldBytes(producer));

        producer.batchMaxDelayMs(25);
        producer.batchMaxBytes(65_536);
        producer.totalBatchMaxBytes(1_048_576);
        result.put("updatedDelay", producer.getBatchMaxDelayMs());
        result.put("updatedBytes", producer.getBatchMaxBytes());
        result.put("updatedTotalGetterBytes", producer.getTotalBatchMaxBytes());
        result.put("updatedTotalHoldBytes", totalAccumulatorHoldBytes(producer));

        result.put("invalidDelayFailure", failureType(() -> producer.batchMaxDelayMs(0)));
        result.put("delayAfterInvalidUpdate", producer.getBatchMaxDelayMs());
        result.put("requestedDelayAfterInvalidUpdate", readProducerField(producer, "batchMaxDelayMs"));
        result.put("invalidBytesFailure", failureType(() -> producer.batchMaxBytes(0)));
        result.put("bytesAfterInvalidUpdate", producer.getBatchMaxBytes());
        result.put("requestedBytesAfterInvalidUpdate", readProducerField(producer, "batchMaxBytes"));
        result.put("invalidTotalBytesFailure", failureType(() -> producer.totalBatchMaxBytes(0)));
        result.put("totalHoldBytesAfterInvalidUpdate", totalAccumulatorHoldBytes(producer));
        result.put("requestedTotalBytesAfterInvalidUpdate", readProducerField(producer, "totalBatchMaxBytes"));
        result.put("reinitializationAfterInvalidUpdateFailure", failureType(producer::initProduceAccumulator));
        result.put("delayAfterReinitializationFailure", producer.getBatchMaxDelayMs());
        producer.shutdown();

        DefaultMQProducer configured = new DefaultMQProducer("configured-before-accumulator");
        configured.setInstanceName("configured-before-accumulator");
        configured.batchMaxDelayMs(25);
        configured.batchMaxBytes(65_536);
        configured.totalBatchMaxBytes(1_048_576);
        result.put("configuredBeforeInitializationDelay", configured.getBatchMaxDelayMs());
        result.put("configuredBeforeInitializationBytes", configured.getBatchMaxBytes());
        result.put("configuredBeforeInitializationTotalBytes", configured.getTotalBatchMaxBytes());
        configured.initProduceAccumulator();
        result.put("configuredAfterInitializationDelay", configured.getBatchMaxDelayMs());
        result.put("configuredAfterInitializationBytes", configured.getBatchMaxBytes());
        result.put("configuredAfterInitializationTotalGetterBytes", configured.getTotalBatchMaxBytes());
        result.put("configuredAfterInitializationTotalHoldBytes", totalAccumulatorHoldBytes(configured));
        configured.shutdown();

        DefaultMQProducer invalidBeforeInitialization = new DefaultMQProducer("invalid-before-accumulator");
        invalidBeforeInitialization.setInstanceName("invalid-before-accumulator");
        invalidBeforeInitialization.batchMaxDelayMs(0);
        result.put("invalidBeforeInitializationSetterFailure", "none");
        result.put("invalidBeforeInitializationInitFailure",
            failureType(invalidBeforeInitialization::initProduceAccumulator));
        result.put("invalidBeforeInitializationEffectiveDelay", invalidBeforeInitialization.getBatchMaxDelayMs());
        invalidBeforeInitialization.shutdown();
        return result;
    }

    private static String failureType(Check check) {
        try {
            check.run();
            return "none";
        } catch (Throwable error) {
            return error.getClass().getName();
        }
    }

    private static Map<String, Object> implementationPublishInfoTable() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-publish-info-table");
        var implementation = producer.getDefaultMQProducerImpl();
        ConcurrentMap<String, TopicPublishInfo> table = implementation.getTopicPublishInfoTable();
        TopicPublishInfo info = new TopicPublishInfo();
        MessageQueue original = new MessageQueue("orders", "broker-a", 0);
        MessageQueue replacement = new MessageQueue("orders", "broker-b", 1);
        List<MessageQueue> originalList = new ArrayList<>(List.of(original));
        info.setMessageQueueList(originalList);
        TopicRouteData route = new TopicRouteData();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("broker-b");
        queueData.setWriteQueueNums(3);
        route.setQueueDatas(new ArrayList<>(List.of(queueData)));
        info.setTopicRouteData(route);
        info.setOrderTopic(true);
        info.setHaveTopicRouterInfo(true);
        ThreadLocalIndex sendIndex = new ThreadLocalIndex();
        info.setSendWhichQueue(sendIndex);

        table.put("orders", info);
        TopicPublishInfo fromTable = table.get("orders");
        List<MessageQueue> retainedList = fromTable.getMessageQueueList();
        retainedList.clear();
        retainedList.add(replacement);

        Map<String, Object> result = new TreeMap<>();
        result.put("getterReturnsLiveMap", table == implementation.getTopicPublishInfoTable());
        result.put("tableValueRetainsIdentity", fromTable == info);
        result.put("queueListRetainsIdentity", retainedList == fromTable.getMessageQueueList());
        result.put("queueListMutationAffectsOk", fromTable.ok());
        result.put("queueListMutationAffectsSelection", fromTable.selectOneMessageQueue() == replacement);
        result.put("orderTopic", fromTable.isOrderTopic());
        result.put("haveTopicRouterInfo", fromTable.isHaveTopicRouterInfo());
        result.put("sendIndexRetainsIdentity", fromTable.getSendWhichQueue() == sendIndex);
        result.put("writeQueueNumsByBroker", fromTable.getWriteQueueNumsByBroker("broker-b"));
        result.put("routeDataRetainsIdentity", fromTable.getTopicRouteData() == route);

        List<MessageQueue> staleList = fromTable.getMessageQueueList();
        fromTable.setMessageQueueList(new ArrayList<>(List.of(original)));
        staleList.clear();
        result.put("listSetterReplacesAlias", fromTable.getMessageQueueList().size() == 1
            && fromTable.getMessageQueueList().get(0) == original);
        TopicPublishInfo replacementInfo = new TopicPublishInfo();
        TopicPublishInfo previous = table.put("orders", replacementInfo);
        result.put("tablePutReturnsPreviousIdentity", previous == info);
        result.put("replacementValueIsVisible", table.get("orders") == replacementInfo);
        result.put("toStringContainsState", fromTable.toString().contains("haveTopicRouterInfo=true"));
        table.remove("orders");
        result.put("removeAffectsRetainedMap", !table.containsKey("orders") && table.isEmpty());
        return result;
    }

    private static Map<String, Object> implementationPublishInfoTableAdvanced() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-publish-info-table-advanced");
        ConcurrentMap<String, TopicPublishInfo> table =
            producer.getDefaultMQProducerImpl().getTopicPublishInfoTable();
        TopicPublishInfo first = new TopicPublishInfo();
        TopicPublishInfo second = new TopicPublishInfo();
        TopicPublishInfo third = new TopicPublishInfo();
        Map<String, Object> result = new TreeMap<>();

        TopicPublishInfo computed = table.computeIfAbsent("compute", key -> first);
        TopicPublishInfo replaced = table.computeIfPresent("compute", (key, current) -> second);
        boolean computeIfPresentIdentity = replaced == second && table.get("compute") == second;
        TopicPublishInfo merged = table.merge("compute", third, (current, incoming) -> current);
        boolean mergeKeepsCurrentIdentity = merged == second && table.get("compute") == second;
        boolean computeRemoved = table.compute("compute", (key, current) -> null) == null
            && !table.containsKey("compute");
        result.put("computeIfAbsentIdentity", computed == first);
        result.put("computeIfPresentIdentity", computeIfPresentIdentity);
        result.put("mergeKeepsCurrentIdentity", mergeKeepsCurrentIdentity);
        result.put("computeNullRemoves", computeRemoved);

        table.put("replace", first);
        TopicPublishInfo replacedValue = table.replace("replace", second);
        boolean conditionalReplace = table.replace("replace", second, third)
            && table.get("replace") == third;
        result.put("replaceReturnsPreviousIdentity", replacedValue == first);
        result.put("conditionalReplaceUsesIdentity", conditionalReplace);
        table.replaceAll((key, current) -> second);
        result.put("replaceAllUsesLiveValues", table.get("replace") == second);

        Set<String> keys = table.keySet();
        table.put("key-view", first);
        result.put("keyViewRemovesFromMap", keys.remove("key-view") && !table.containsKey("key-view"));
        table.put("key-bulk-remove", first);
        table.put("key-bulk-retain", second);
        boolean keyBulk = keys.removeAll(Set.of("key-bulk-remove"))
            && !table.containsKey("key-bulk-remove")
            && keys.retainAll(Set.of("key-bulk-retain"))
            && !table.containsKey("replace") && table.containsKey("key-bulk-retain")
            && table.size() == 1;
        result.put("keyViewBulkMutation", keyBulk);

        Map<String, TopicPublishInfo> putAllRows = new LinkedHashMap<>();
        putAllRows.put("put-all-before", first);
        putAllRows.put("put-all-null", null);
        putAllRows.put("put-all-after", third);
        boolean putAllRejected = false;
        try {
            table.putAll(putAllRows);
        } catch (NullPointerException expected) {
            putAllRejected = true;
        }
        result.put("putAllNullValueKeepsEarlier", putAllRejected
            && table.containsKey("put-all-before") && !table.containsKey("put-all-after"));
        table.remove("put-all-before");

        var values = table.values();
        table.put("value-view", first);
        result.put("valuesViewRemovesByIdentity", values.remove(first) && !table.containsKey("value-view"));
        table.put("value-bulk-a", first);
        table.put("value-bulk-b", first);
        table.put("value-bulk-c", second);
        boolean valuesRemoveAll = values.removeAll(List.of(first))
            && !table.containsKey("value-bulk-a") && !table.containsKey("value-bulk-b")
            && table.containsKey("value-bulk-c");
        table.put("value-retain-a", first);
        table.put("value-retain-b", second);
        boolean valuesRetainAll = values.retainAll(List.of(first))
            && table.containsKey("value-retain-a") && !table.containsKey("value-retain-b")
            && !table.containsKey("replace") && !table.containsKey("key-bulk-retain")
            && !table.containsKey("value-bulk-c");
        result.put("valuesViewBulkMutation", valuesRemoveAll && valuesRetainAll);
        var entries = table.entrySet();
        table.put("entry-view", first);
        var entry = entries.stream().filter(candidate -> "entry-view".equals(candidate.getKey()))
            .findFirst().orElseThrow();
        TopicPublishInfo entryPrevious = entry.setValue(third);
        boolean entryWrite = entryPrevious == first && table.get("entry-view") == third;
        result.put("entryViewSetValueWritesThrough", entryWrite);
        result.put("entryViewRemoveUsesKeyAndIdentity",
            entries.remove(new java.util.AbstractMap.SimpleEntry<>("entry-view", third))
                && !table.containsKey("entry-view"));
        table.put("entry-bulk-remove", first);
        table.put("entry-bulk-retain", third);
        boolean entryRemoveAll = entries.removeAll(Set.of(
            new java.util.AbstractMap.SimpleEntry<>("entry-bulk-remove", first)));
        boolean entryRetainAll = entries.retainAll(Set.of(
            new java.util.AbstractMap.SimpleEntry<>("entry-bulk-retain", third)));
        result.put("entryViewBulkMutation", entryRemoveAll && entryRetainAll
            && table.size() == 1 && table.get("entry-bulk-retain") == third);
        table.put("key-remove-if", first);
        result.put("keyViewRemoveIfMutation",
            keys.removeIf("key-remove-if"::equals) && !table.containsKey("key-remove-if"));
        table.put("value-remove-if-match", first);
        table.put("value-remove-if-keep", second);
        result.put("valuesViewRemoveIfMutation", values.removeIf(value -> value == first)
            && !table.containsKey("value-remove-if-match")
            && table.get("value-remove-if-keep") == second);
        table.put("entry-remove-if-match", first);
        table.put("entry-remove-if-keep", third);
        result.put("entryViewRemoveIfMutation", entries.removeIf(candidate ->
            "entry-remove-if-match".equals(candidate.getKey()))
            && !table.containsKey("entry-remove-if-match")
            && table.get("entry-remove-if-keep") == third);
        table.clear();
        result.put("clearAffectsRetainedViews", keys.isEmpty() && values.isEmpty() && entries.isEmpty());

        result.put("getOrDefaultMissingIdentity", table.getOrDefault("missing", second) == second);
        table.put("map-op-present", first);
        result.put("getOrDefaultPresentIdentity",
            table.getOrDefault("map-op-present", second) == first);
        result.put("containsValueIdentity", table.containsValue(first));
        TopicPublishInfo putIfAbsentPrevious = table.putIfAbsent("map-op-present", second);
        result.put("putIfAbsentKeepsCurrentIdentity",
            putIfAbsentPrevious == first && table.get("map-op-present") == first);
        TopicPublishInfo absentPrevious = table.putIfAbsent("map-op-absent", second);
        result.put("putIfAbsentInserts", absentPrevious == null
            && table.get("map-op-absent") == second);
        result.put("conditionalRemoveUsesIdentity",
            table.remove("map-op-absent", second) && !table.containsKey("map-op-absent"));
        table.put("for-each-a", first);
        table.put("for-each-b", second);
        List<String> forEachRows = new ArrayList<>();
        table.forEach((key, value) -> forEachRows.add(key + ":" + (value == first ? "first" : "other")));
        result.put("forEachVisitsLiveEntries", forEachRows.size() == 3
            && forEachRows.contains("map-op-present:first")
            && forEachRows.contains("for-each-a:first")
            && forEachRows.contains("for-each-b:other"));
        result.put("sizeAndEmptyMatch", table.size() == 3 && !table.isEmpty());
        table.clear();
        result.put("clearEmptiesLiveMap", table.isEmpty() && table.size() == 0);

        table.put("nullable", first);
        result.put("nullGetKey", nullPointerClass(() -> table.get(null)));
        result.put("nullGetOrDefaultKey", nullPointerClass(() -> table.getOrDefault(null, second)));
        result.put("nullContainsKey", nullPointerClass(() -> table.containsKey(null)));
        result.put("nullContainsValue", nullPointerClass(() -> table.containsValue(null)));
        result.put("nullPutKey", nullPointerClass(() -> table.put(null, second)));
        result.put("nullPutValue", nullPointerClass(() -> table.put("nullable-value", null)));
        result.put("nullPutIfAbsentKey", nullPointerClass(() -> table.putIfAbsent(null, second)));
        result.put("nullPutIfAbsentValue", nullPointerClass(() -> table.putIfAbsent("nullable", null)));
        result.put("nullPutAllMap", nullPointerClass(() -> table.putAll(null)));
        result.put("nullRemoveKey", nullPointerClass(() -> table.remove(null)));
        result.put("nullConditionalRemoveValue", !table.remove("nullable", null));
        result.put("nullReplaceKey", nullPointerClass(() -> table.replace(null, second)));
        result.put("nullReplaceValue", nullPointerClass(() -> table.replace("nullable", null)));
        result.put("nullConditionalReplaceExpected",
            nullPointerClass(() -> table.replace("nullable", null, second)));
        result.put("nullConditionalReplaceValue",
            nullPointerClass(() -> table.replace("nullable", first, null)));
        result.put("nullComputeIfAbsentKey",
            nullPointerClass(() -> table.computeIfAbsent(null, key -> second)));
        result.put("nullComputeIfPresentKey",
            nullPointerClass(() -> table.computeIfPresent(null, (key, old) -> second)));
        result.put("nullComputeKey", nullPointerClass(() -> table.compute(null, (key, old) -> second)));
        result.put("nullMergeKey", nullPointerClass(() -> table.merge(null, second, (old, value) -> value)));
        result.put("nullMergeValue",
            nullPointerClass(() -> table.merge("nullable", null, (old, value) -> value)));
        result.put("nullComputeIfAbsentCallback",
            nullPointerClass(() -> table.computeIfAbsent("nullable", null)));
        result.put("nullComputeIfPresentCallback",
            nullPointerClass(() -> table.computeIfPresent("nullable", null)));
        result.put("nullComputeCallback", nullPointerClass(() -> table.compute("nullable", null)));
        result.put("nullMergeCallback",
            nullPointerClass(() -> table.merge("nullable", second, null)));
        result.put("nullForEachCallback", nullPointerClass(() -> table.forEach(null)));
        result.put("nullReplaceAllCallback", nullPointerClass(() -> table.replaceAll(null)));
        table.put("compute-present-null", second);
        result.put("computeIfAbsentNullResultAllowed",
            table.computeIfAbsent("compute-absent-null", key -> null) == null
                && !table.containsKey("compute-absent-null"));
        result.put("computeIfPresentNullResultRemoves",
            table.computeIfPresent("compute-present-null", (key, old) -> null) == null
                && !table.containsKey("compute-present-null"));
        table.put("compute-null-result", second);
        result.put("computeNullResultRemoves",
            table.compute("compute-null-result", (key, old) -> null) == null
                && !table.containsKey("compute-null-result"));
        table.put("merge-null-result", second);
        result.put("mergeNullResultRemoves",
            table.merge("merge-null-result", first, (old, value) -> null) == null
                && !table.containsKey("merge-null-result"));
        table.put("replace-all-null-result", first);
        result.put("nullReplaceAllResult",
            nullPointerClass(() -> table.replaceAll((key, value) -> null)));
        result.put("nullReplaceAllResultPreservesMapping",
            table.get("replace-all-null-result") == first);
        result.put("nullGetOrDefaultFallbackAllowed",
            table.getOrDefault("missing", null) == null);
        result.put("nullArgumentsPreserveMapping", table.get("nullable") == first);
        result.put("nullKeySetContains", nullPointerClass(() -> table.keySet().contains(null)));
        result.put("nullKeySetRemove", nullPointerClass(() -> table.keySet().remove(null)));
        result.put("nullValuesContains", nullPointerClass(() -> table.values().contains(null)));
        result.put("nullValuesRemove", !table.values().remove(null));
        result.put("nullEntrySetContains", !table.entrySet().contains(null));
        result.put("nullEntrySetRemove", !table.entrySet().remove(null));
        result.put("nullKeyEntrySetContains",
            !table.entrySet().contains(new java.util.AbstractMap.SimpleEntry<>(null, first)));
        result.put("nullValueEntrySetContains",
            !table.entrySet().contains(new java.util.AbstractMap.SimpleEntry<>("nullable", null)));
        result.put("nullKeyEntrySetRemove",
            !table.entrySet().remove(new java.util.AbstractMap.SimpleEntry<>(null, first)));
        result.put("nullValueEntrySetRemove",
            !table.entrySet().remove(new java.util.AbstractMap.SimpleEntry<>("nullable", null)));
        result.put("nullKeySetRemoveAll", nullPointerClass(() -> table.keySet().removeAll(null)));
        result.put("nullKeySetRetainAll", nullPointerClass(() -> table.keySet().retainAll(null)));
        result.put("nullValuesRemoveAll", nullPointerClass(() -> table.values().removeAll(null)));
        result.put("nullValuesRetainAll", nullPointerClass(() -> table.values().retainAll(null)));
        result.put("nullEntrySetRemoveAll", nullPointerClass(() -> table.entrySet().removeAll(null)));
        result.put("nullEntrySetRetainAll", nullPointerClass(() -> table.entrySet().retainAll(null)));
        result.put("nullKeySetRemoveIf", nullPointerClass(() -> table.keySet().removeIf(null)));
        result.put("nullValuesRemoveIf", nullPointerClass(() -> table.values().removeIf(null)));
        result.put("nullEntrySetRemoveIf", nullPointerClass(() -> table.entrySet().removeIf(null)));
        var nullableEntry = table.entrySet().stream()
            .filter(candidate -> "nullable".equals(candidate.getKey())).findFirst().orElseThrow();
        result.put("nullEntrySetValue", nullPointerClass(() -> nullableEntry.setValue(null)));
        result.put("nullEntrySetValuePreservesMapping",
            nullableEntry.getValue() == first && table.get("nullable") == first);
        return result;
    }

    private static Map<String, Object> implementationQueueFilterSelection() {
        TopicPublishInfo info = new TopicPublishInfo();
        MessageQueue queue = new MessageQueue("orders", "broker-a", 2);
        info.setMessageQueueList(new ArrayList<>(List.of(queue)));
        Map<String, Object> result = new TreeMap<>();
        result.put("nullFilterArraySelection", queueId(info.selectOneMessageQueue((TopicPublishInfo.QueueFilter[]) null)));
        result.put("emptyFilterArraySelection", queueId(info.selectOneMessageQueue(new TopicPublishInfo.QueueFilter[0])));
        result.put("nullFilterWithQueue", nullPointerClass(() -> info.selectOneMessageQueue((TopicPublishInfo.QueueFilter) null)));

        TopicPublishInfo empty = new TopicPublishInfo();
        empty.setMessageQueueList(new ArrayList<>());
        result.put("nullFilterWithEmptyQueueList", queueId(empty.selectOneMessageQueue((TopicPublishInfo.QueueFilter) null)));

        TopicPublishInfo missing = new TopicPublishInfo();
        missing.setMessageQueueList(null);
        result.put("nullFilterWithNullQueueList", queueId(missing.selectOneMessageQueue((TopicPublishInfo.QueueFilter) null)));

        List<String> filterOrder = new ArrayList<>();
        TopicPublishInfo.QueueFilter[] filters = {
            candidate -> {
                filterOrder.add("reject");
                return false;
            },
            candidate -> {
                filterOrder.add("accept");
                return true;
            }
        };
        result.put("nonShortCircuitSelection", queueId(info.selectOneMessageQueue(filters)));
        result.put("nonShortCircuitFilterOrder", filterOrder);
        return result;
    }

    private static Map<String, Object> implementationSelectorFailure() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-selector-failure");
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        org.apache.rocketmq.client.ClientConfig clientConfig = new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("producer-selector-failure");
        MQClientInstance factory = new MQClientInstance(clientConfig, 0, clientConfig.buildMQClientId());
        try {
            Field factoryField = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
            factoryField.setAccessible(true);
            factoryField.set(implementation, factory);
            implementation.setServiceState(ServiceState.RUNNING);

            TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
            topicPublishInfo.setMessageQueueList(new ArrayList<>(List.of(
                new MessageQueue("orders", "broker-a", 0))));
            implementation.updateTopicPublishInfo("orders", topicPublishInfo);

            Map<String, Object> result = new TreeMap<>();
            Message message = new Message("orders", new byte[] {1});
            result.put("invoke", selectorFailureCase(implementation, message, true));
            result.put("send", selectorFailureCase(implementation, message, false));
            return result;
        } finally {
            implementation.setServiceState(ServiceState.SHUTDOWN_ALREADY);
            Field factoryState = MQClientInstance.class.getDeclaredField("serviceState");
            factoryState.setAccessible(true);
            factoryState.set(factory, ServiceState.RUNNING);
            factory.shutdown();
        }
    }

    private static Map<String, Object> implementationSelectorMessageSnapshot() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-selector-snapshot");
        producer.setNamespace("tenant");
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        org.apache.rocketmq.client.ClientConfig clientConfig = new org.apache.rocketmq.client.ClientConfig();
        clientConfig.setClientIP("producer-selector-snapshot");
        clientConfig.setNamespace("tenant");
        MQClientInstance factory = new MQClientInstance(clientConfig, 0, clientConfig.buildMQClientId());
        try {
            Field factoryField = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
            factoryField.setAccessible(true);
            factoryField.set(implementation, factory);
            implementation.setServiceState(ServiceState.RUNNING);

            TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
            topicPublishInfo.setMessageQueueList(new ArrayList<>(List.of(
                new MessageQueue("tenant%orders", "broker-a", 0))));
            implementation.updateTopicPublishInfo("tenant%orders", topicPublishInfo);

            Message message = new Message("tenant%orders", new byte[] {1, 2});
            message.setFlag(7);
            message.setTransactionId("source-transaction-id");
            MessageAccessor.putProperty(message, "fixture-key", "fixture-value");
            Map<String, Object> result = new TreeMap<>();
            MessageQueue selected = implementation.invokeMessageQueueSelector(message,
                (queues, selectorMessage, argument) -> {
                    result.put("selectorMessageIsDistinct", selectorMessage != message);
                    result.put("selectorTopic", selectorMessage.getTopic());
                    result.put("selectorBodyShared", selectorMessage.getBody() == message.getBody());
                    result.put("selectorFlag", selectorMessage.getFlag());
                    result.put("selectorPropertiesShared",
                        selectorMessage.getProperties() == message.getProperties());
                    result.put("selectorProperty", selectorMessage.getProperty("fixture-key"));
                    result.put("selectorTransactionIdIsNull", selectorMessage.getTransactionId() == null);
                    result.put("selectorQueueTopic", queues.get(0).getTopic());
                    return queues.get(0);
                }, new Object(), 10_000);
            result.put("selectedQueueTopic", selected.getTopic());
            result.put("sourceTopic", message.getTopic());
            result.put("sourceTransactionId", message.getTransactionId());
            return result;
        } finally {
            implementation.setServiceState(ServiceState.SHUTDOWN_ALREADY);
            Field factoryState = MQClientInstance.class.getDeclaredField("serviceState");
            factoryState.setAccessible(true);
            factoryState.set(factory, ServiceState.RUNNING);
            factory.shutdown();
        }
    }

    private static Map<String, Object> selectorFailureCase(DefaultMQProducerImpl implementation,
        Message message, boolean invokeOnly) {
        Map<String, Object> result = new TreeMap<>();
        AtomicInteger selectorCalls = new AtomicInteger();
        List<Integer> queueCounts = new ArrayList<>();
        Object selectorArgument = new Object();
        MessageQueueSelector selector = (queues, selectedMessage, argument) -> {
            selectorCalls.incrementAndGet();
            queueCounts.add(queues.size());
            if (selectedMessage == message || !"orders".equals(selectedMessage.getTopic())
                || argument != selectorArgument) {
                throw new IllegalStateException("selector inputs did not match");
            }
            throw new IllegalStateException("fixture-selector-failure");
        };
        Throwable failure = null;
        try {
            if (invokeOnly) {
                implementation.invokeMessageQueueSelector(message, selector, selectorArgument, 10_000);
            } else {
                implementation.send(message, selector, selectorArgument, 10_000);
            }
        } catch (Throwable error) {
            failure = error;
        }
        result.put("selectorCalls", selectorCalls.get());
        result.put("queueCounts", queueCounts);
        result.put("errorClass", failure == null ? "none" : failure.getClass().getSimpleName());
        result.put("errorMessage", failure == null ? "" : failure.getMessage());
        Throwable cause = failure == null ? null : failure.getCause();
        result.put("causeClass", cause == null ? "none" : cause.getClass().getSimpleName());
        result.put("causeMessage", cause == null ? "" : cause.getMessage());
        return result;
    }

    private static Integer queueId(MessageQueue queue) {
        return queue == null ? null : queue.getQueueId();
    }

    private static String nullPointerClass(Runnable action) {
        try {
            action.run();
            return "none";
        } catch (NullPointerException expected) {
            return expected.getClass().getSimpleName();
        }
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "(" + Arrays.stream(constructor.getParameterTypes())
            .map(Class::getName)
            .collect(java.util.stream.Collectors.joining(",")) + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getName() + " " + method.getDeclaringClass().getName() + "."
            + method.getName() + "(" + Arrays.stream(method.getParameterTypes())
            .map(Class::getName)
            .collect(java.util.stream.Collectors.joining(",")) + ")";
    }

    private static Map<String, Object> publicApiSurface(Class<?> apiType) {
        List<String> constructors = Arrays.stream(apiType.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(ProducerCoreFixtures::constructorSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        List<String> declaredMethods = Arrays.stream(apiType.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(ProducerCoreFixtures::methodSignature)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        List<String> methods = Arrays.stream(apiType.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(ProducerCoreFixtures::methodSignature)
            .distinct()
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        Map<String, Object> result = new TreeMap<>();
        result.put("className", apiType.getName());
        result.put("publicConstructors", constructors);
        result.put("declaredPublicMethods", declaredMethods);
        result.put("declaredPublicMethodCount", declaredMethods.size());
        result.put("publicMethods", methods);
        result.put("publicMethodCount", methods.size());
        return result;
    }

    private static Map<String, Object> implementationHookExecution() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-implementation-hooks");
        var implementation = producer.getDefaultMQProducerImpl();
        Map<String, Object> result = new TreeMap<>();
        result.put("checkForbiddenInitiallyPresent", implementation.hasCheckForbiddenHook());
        result.put("sendMessageInitiallyPresent", implementation.hasSendMessageHook());

        List<String> checkEvents = new ArrayList<>();
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "fixture-check-first";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) {
                checkEvents.add("first");
                MessageAccessor.putProperty(context.getMessage(), "DIRECT_CHECK_HOOK", "mutated");
                context.getMq().setQueueId(4);
            }
        });
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "fixture-check-reject";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
                checkEvents.add("reject:" + context.getMessage().getProperty("DIRECT_CHECK_HOOK"));
                throw new MQClientException("fixture forbidden", null);
            }
        });
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "fixture-check-last";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) {
                checkEvents.add("unreached");
            }
        });
        Message checkMessage = new Message("orders", new byte[] {1});
        MessageQueue checkQueue = new MessageQueue("orders", "broker-a", 1);
        CheckForbiddenContext checkContext = new CheckForbiddenContext();
        checkContext.setGroup("producer-implementation-hooks");
        checkContext.setMessage(checkMessage);
        checkContext.setMq(checkQueue);
        checkContext.setBrokerAddr("127.0.0.1:10911");
        checkContext.setCommunicationMode(CommunicationMode.SYNC);
        String checkFailure = null;
        try {
            implementation.executeCheckForbiddenHook(checkContext);
        } catch (MQClientException expected) {
            checkFailure = expected.getMessage();
        }
        result.put("checkForbiddenPresentAfterRegistration", implementation.hasCheckForbiddenHook());
        result.put("checkEvents", checkEvents);
        result.put("checkFailure", checkFailure);
        result.put("checkMessageMarker", checkMessage.getProperty("DIRECT_CHECK_HOOK"));
        result.put("checkQueueId", checkQueue.getQueueId());

        List<String> sendEvents = new ArrayList<>();
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "fixture-send-first";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendEvents.add("before:first");
                MessageAccessor.putProperty(context.getMessage(), "DIRECT_SEND_HOOK", "before");
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendEvents.add("after:first");
                MessageAccessor.putProperty(context.getMessage(), "DIRECT_SEND_HOOK", "after");
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "fixture-send-throws";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendEvents.add("before:throws");
                throw new IllegalStateException("fixture before failure");
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendEvents.add("after:throws");
                throw new IllegalStateException("fixture after failure");
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "fixture-send-last";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendEvents.add("before:last:" + context.getMessage().getProperty("DIRECT_SEND_HOOK"));
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendEvents.add("after:last:" + context.getMessage().getProperty("DIRECT_SEND_HOOK"));
            }
        });
        Message sendMessage = new Message("orders", new byte[] {2});
        SendMessageContext sendContext = new SendMessageContext();
        sendContext.setProducer(implementation);
        sendContext.setProducerGroup("producer-implementation-hooks");
        sendContext.setMessage(sendMessage);
        sendContext.setMq(new MessageQueue("orders", "broker-a", 2));
        sendContext.setBrokerAddr("127.0.0.1:10911");
        sendContext.setCommunicationMode(CommunicationMode.SYNC);
        implementation.executeSendMessageHookBefore(sendContext);
        implementation.executeSendMessageHookAfter(sendContext);
        result.put("sendMessagePresentAfterRegistration", implementation.hasSendMessageHook());
        result.put("sendEvents", sendEvents);
        result.put("sendMessageMarker", sendMessage.getProperty("DIRECT_SEND_HOOK"));
        return result;
    }

    private static Map<String, Object> implementationStateAccessors() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-implementation-state");
        producer.setUnitMode(true);
        var implementation = producer.getDefaultMQProducerImpl();
        Map<String, Object> result = new TreeMap<>();
        result.put("initialServiceState", implementation.getServiceState().name());
        result.put("serviceStateValues", Arrays.stream(ServiceState.values())
            .map(ServiceState::name)
            .collect(java.util.stream.Collectors.toList()));
        result.put("serviceStateOrdinals", Arrays.stream(ServiceState.values())
            .map(ServiceState::ordinal)
            .collect(java.util.stream.Collectors.toList()));
        result.put("defaultProducerIdentity", implementation.getDefaultMQProducer() == producer);
        result.put("defaultProducerImplIdentity", producer.getDefaultMQProducerImpl() == implementation);
        result.put("defaultProducerGroup", implementation.getDefaultMQProducer().getProducerGroup());
        result.put("unitMode", implementation.isUnitMode());
        result.put("clientFactoryInitiallyNull",
            implementation.getMqClientFactory() == null && implementation.getmQClientFactory() == null);
        implementation.setServiceState(null);
        result.put("nullServiceState", implementation.getServiceState() == null);
        return result;
    }

    private static Map<String, Object> implementationSemaphoreSettings() throws InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("producer-implementation-semaphores");
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        Map<String, Object> result = new TreeMap<>();
        result.put("initialCountAvailable", implementation.getSemaphoreAsyncSendNumAvailablePermits());
        result.put("initialByteAvailable", implementation.getSemaphoreAsyncSendSizeAvailablePermits());

        Semaphore currentCount = asyncSendNumSemaphore(producer);
        currentCount.acquire(2);
        implementation.setSemaphoreAsyncSendNum(7);
        asyncSendNumSemaphore(producer).release(2);
        result.put("countAfterRawReplaceAndInFlightRelease",
            implementation.getSemaphoreAsyncSendNumAvailablePermits());
        producer.setBackPressureForAsyncSendNum(20);
        result.put("countAfterFacadeResize", implementation.getSemaphoreAsyncSendNumAvailablePermits());
        result.put("facadeCountLimitAfterResize", producer.getBackPressureForAsyncSendNum());

        implementation.setSemaphoreAsyncSendNum(-3);
        implementation.setSemaphoreAsyncSendSize(4096);
        result.put("negativeCountAvailable", implementation.getSemaphoreAsyncSendNumAvailablePermits());
        result.put("byteAvailableAfterRawReplace", implementation.getSemaphoreAsyncSendSizeAvailablePermits());
        implementation.setSemaphoreAsyncSendSize(-8192);
        result.put("negativeByteAvailable", implementation.getSemaphoreAsyncSendSizeAvailablePermits());
        result.put("facadeByteLimitUnchanged", producer.getBackPressureForAsyncSendSize());
        return result;
    }

    private static Map<String, Object> executorSettings() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-executor-fixture");
        ExecutorService defaultExecutor = producer.getDefaultMQProducerImpl().getAsyncSenderExecutor();
        ExecutorService customExecutor = Executors.newSingleThreadExecutor();
        Map<String, Object> result = new TreeMap<>();
        result.put("defaultAsyncExecutorStable",
            defaultExecutor == producer.getDefaultMQProducerImpl().getAsyncSenderExecutor());
        producer.setAsyncSenderExecutor(customExecutor);
        result.put("customAsyncExecutorRetained",
            customExecutor == producer.getDefaultMQProducerImpl().getAsyncSenderExecutor());
        producer.setAsyncSenderExecutor(null);
        result.put("nullAsyncExecutorRestoresDefault",
            defaultExecutor == producer.getDefaultMQProducerImpl().getAsyncSenderExecutor());
        try {
            producer.setCallbackExecutor(null);
            result.put("callbackExecutorBeforeStart", "no-exception");
        } catch (NullPointerException exception) {
            result.put("callbackExecutorBeforeStart", exception.getClass().getSimpleName());
        } finally {
            customExecutor.shutdownNow();
        }
        return result;
    }

    private static Map<String, Object> runtimeSettings() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-runtime-settings");
        Map<String, Object> result = new TreeMap<>();
        producer.setProducerGroup("producer-runtime-group");
        producer.setSendMessageWithVIPChannel(true);
        producer.setTopics(new ArrayList<>(List.of("startup-topic")));
        result.put("producerGroupAfterSetter", producer.getProducerGroup());
        result.put("sendMessageWithVIPChannelAfterSetter", producer.isSendMessageWithVIPChannel());
        result.put("topicsAfterSetter", new ArrayList<>(producer.getTopics()));
        producer.setTopics(null);
        result.put("topicsIsNullAfterSetter", producer.getTopics() == null);
        producer.setCreateTopicKey("runtime-default-topic");
        producer.setDefaultTopicQueueNums(7);
        producer.setSendMsgTimeout(6_100);
        producer.setSendMsgMaxTimeoutPerRequest(800);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setRetryTimesWhenSendAsyncFailed(4);
        producer.setRetryAnotherBrokerWhenNotStoreOK(true);
        producer.setMaxMessageSize(123_456);
        result.put("createTopicKey", producer.getCreateTopicKey());
        result.put("defaultTopicQueueNums", producer.getDefaultTopicQueueNums());
        result.put("sendMsgTimeout", producer.getSendMsgTimeout());
        result.put("sendMsgMaxTimeoutPerRequest", producer.getSendMsgMaxTimeoutPerRequest());
        result.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        result.put("retryTimesWhenSendAsyncFailed", producer.getRetryTimesWhenSendAsyncFailed());
        result.put("retryAnotherBrokerWhenNotStoreOK", producer.isRetryAnotherBrokerWhenNotStoreOK());
        result.put("maxMessageSize", producer.getMaxMessageSize());

        producer.setEnableBackpressureForAsyncMode(true);
        result.put("backpressureEnabledAfterToggle", producer.isEnableBackpressureForAsyncMode());

        try {
            Semaphore beforeResize = asyncSendNumSemaphore(producer);
            beforeResize.acquire(7);
            producer.setBackPressureForAsyncSendNum(12);
            result.put("sendNumPermitsAfterResize", producer.getDefaultMQProducerImpl()
                .getSemaphoreAsyncSendNumAvailablePermits());
            Semaphore afterResize = asyncSendNumSemaphore(producer);
            afterResize.release(7);
            result.put("sendNumPermitsAfterInFlightRelease", producer.getDefaultMQProducerImpl()
                .getSemaphoreAsyncSendNumAvailablePermits());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Could not inspect pinned async-send permit adjustment", error);
        }

        producer.setBackPressureForAsyncSendNum(12);
        producer.setBackPressureForAsyncSendSize(2 * 1024 * 1024);
        result.put("sendNumAfterUpdate", producer.getBackPressureForAsyncSendNum());
        result.put("sendSizeAfterUpdate", producer.getBackPressureForAsyncSendSize());
        result.put("sendSizePermitsAfterUpdate", producer.getDefaultMQProducerImpl()
            .getSemaphoreAsyncSendSizeAvailablePermits());
        producer.setBackPressureForAsyncSendNum(1);
        producer.setBackPressureForAsyncSendSize(1);
        result.put("clampedSendNumAfterUpdate", producer.getBackPressureForAsyncSendNum());
        result.put("clampedSendSizeAfterUpdate", producer.getBackPressureForAsyncSendSize());

        Set<Integer> liveRetryCodes = producer.getRetryResponseCodes();
        liveRetryCodes.add(9001);
        result.put("setMutationVisibleThroughGetter", producer.getRetryResponseCodes().contains(9001));
        producer.addRetryResponseCode(9002);
        result.put("addMethodVisibleThroughExistingSet", liveRetryCodes.contains(9002));
        liveRetryCodes.remove(9001);
        result.put("setRemovalVisibleThroughGetter", !producer.getRetryResponseCodes().contains(9001));
        return result;
    }

    private static void shutdownUnstartedClientExecutors(MQClientInstance client) {
        client.getScheduledExecutorService().shutdownNow();
    }

    private static MQClientInstance installProducerSendFixtureRoute(
        DefaultMQProducer producer, DefaultMQProducerImpl implementation, String topic, String clientId)
        throws Exception {
        implementation.setServiceState(ServiceState.RUNNING);
        MQClientInstance client = new MQClientInstance(producer, 0, clientId);
        Field clientFactory = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        clientFactory.setAccessible(true);
        clientFactory.set(implementation, client);

        Field brokerAddressTable = MQClientInstance.class.getDeclaredField("brokerAddrTable");
        brokerAddressTable.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String, HashMap<Long, String>> addresses =
            (ConcurrentMap<String, HashMap<Long, String>>) brokerAddressTable.get(client);
        addresses.put("broker-a", new HashMap<>(Map.of(MixAll.MASTER_ID, "127.0.0.1:10911")));
        addresses.put("broker-b", new HashMap<>(Map.of(MixAll.MASTER_ID, "127.0.0.1:20911")));

        TopicPublishInfo topicInfo = new TopicPublishInfo();
        topicInfo.setMessageQueueList(List.of(
            new MessageQueue(topic, "broker-a", 0),
            new MessageQueue(topic, "broker-b", 0)));
        implementation.updateTopicPublishInfo(topic, topicInfo);
        return client;
    }

    private static Map<String, Object> perRequestTimeoutRetry() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-timeout-retry-fixture");
        producer.setSendMsgTimeout(3_000);
        List<Map<String, Object>> configuredValues = new ArrayList<>();
        for (int timeoutMs : List.of(-2, -1, 0, 125, 6_000)) {
            producer.setSendMsgMaxTimeoutPerRequest(timeoutMs);
            Map<String, Object> value = new TreeMap<>();
            value.put("input", timeoutMs);
            value.put("readback", producer.getSendMsgMaxTimeoutPerRequest());
            configuredValues.add(value);
        }
        producer.setSendMsgMaxTimeoutPerRequest(0);
        producer.setRetryTimesWhenSendFailed(1);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-timeout-retry-fixture");

        AtomicInteger beforeAttempts = new AtomicInteger();
        AtomicInteger afterAttempts = new AtomicInteger();
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "per-request-timeout-retry-fixture";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                beforeAttempts.incrementAndGet();
                try {
                    // Ensure sendKernelImpl consumes the zero request budget before transport.
                    Thread.sleep(5);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                afterAttempts.incrementAndGet();
            }
        });

        String outcome;
        try {
            implementation.send(new Message("orders", new byte[] {1}));
            outcome = "success";
        } catch (Exception expected) {
            outcome = expected.getClass().getSimpleName();
        } finally {
            shutdownUnstartedClientExecutors(client);
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        result.put("sendMsgTimeout", producer.getSendMsgTimeout());
        result.put("sendMsgMaxTimeoutPerRequest", producer.getSendMsgMaxTimeoutPerRequest());
        result.put("configuredValues", configuredValues);
        result.put("beforeAttempts", beforeAttempts.get());
        result.put("afterAttempts", afterAttempts.get());
        result.put("outcome", outcome);
        return result;
    }

    private static Map<String, Object> checkForbiddenRetry() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-check-forbidden-retry-fixture");
        producer.setSendMsgTimeout(3_000);
        producer.setRetryTimesWhenSendFailed(1);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-check-forbidden-retry-fixture");

        AtomicInteger checkCalls = new AtomicInteger();
        AtomicInteger sendBeforeCalls = new AtomicInteger();
        AtomicInteger sendAfterCalls = new AtomicInteger();
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "check-forbidden-retry-fixture";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
                checkCalls.incrementAndGet();
                throw new MQClientException("fixture forbidden", null);
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "send-hook-after-forbidden-retry-fixture";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendBeforeCalls.incrementAndGet();
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendAfterCalls.incrementAndGet();
            }
        });

        String outcome;
        String finalMessage = "";
        int responseCode = -1;
        try {
            implementation.send(new Message("orders", new byte[] {1}));
            outcome = "success";
        } catch (MQClientException expected) {
            outcome = expected.getClass().getSimpleName();
            finalMessage = expected.getMessage();
            responseCode = expected.getResponseCode();
        } finally {
            shutdownUnstartedClientExecutors(client);
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        result.put("checkCalls", checkCalls.get());
        result.put("sendBeforeCalls", sendBeforeCalls.get());
        result.put("sendAfterCalls", sendAfterCalls.get());
        result.put("outcome", outcome);
        result.put("responseCode", responseCode);
        result.put("messageContainsAttemptCount", finalMessage.contains("Send [2] times"));
        return result;
    }

    private static Map<String, Object> missingBrokerAddressRetry() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-missing-broker-address-fixture");
        producer.setSendMsgTimeout(3_000);
        producer.setRetryTimesWhenSendFailed(1);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-missing-broker-address-fixture");

        Field brokerAddressTable = MQClientInstance.class.getDeclaredField("brokerAddrTable");
        brokerAddressTable.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String, HashMap<Long, String>> addresses =
            (ConcurrentMap<String, HashMap<Long, String>>) brokerAddressTable.get(client);
        addresses.clear();

        MQClientException failure = null;
        try {
            implementation.send(new Message("orders", new byte[] {1}));
        } catch (MQClientException expected) {
            failure = expected;
        } finally {
            shutdownUnstartedClientExecutors(client);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        result.put("outcome", failure == null ? "success" : failure.getClass().getSimpleName());
        result.put("responseCode", failure == null ? -1 : failure.getResponseCode());
        result.put("messageContainsAttemptCount",
            failure != null && failure.getMessage().contains("Send [2] times"));
        result.put("messageContainsBothBrokers", failure != null
            && failure.getMessage().contains("broker-a")
            && failure.getMessage().contains("broker-b"));
        result.put("causeClass", failure != null && failure.getCause() != null
            ? failure.getCause().getClass().getSimpleName() : "none");
        result.put("causeMessageContainsBrokerFailure", failure != null && failure.getCause() != null
            && failure.getCause().getMessage().contains("The broker[")
            && failure.getCause().getMessage().contains(" not exist"));
        return result;
    }

    private static Map<String, Object> missingBrokerAddressExplicit() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-missing-broker-explicit-fixture");
        producer.setSendMsgTimeout(3_000);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-missing-broker-explicit-fixture");

        Field brokerAddressTable = MQClientInstance.class.getDeclaredField("brokerAddrTable");
        brokerAddressTable.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String, HashMap<Long, String>> addresses =
            (ConcurrentMap<String, HashMap<Long, String>>) brokerAddressTable.get(client);
        addresses.clear();

        MQClientException failure = null;
        try {
            implementation.send(new Message("orders", new byte[] {1}),
                new MessageQueue("orders", "broker-a", 0), 3_000L);
        } catch (MQClientException expected) {
            failure = expected;
        } finally {
            shutdownUnstartedClientExecutors(client);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("outcome", failure == null ? "success" : failure.getClass().getSimpleName());
        result.put("responseCode", failure == null ? -1 : failure.getResponseCode());
        result.put("messageContainsMissingBroker", failure != null
            && failure.getMessage().contains("The broker[broker-a] not exist"));
        result.put("causeClass", failure != null && failure.getCause() != null
            ? failure.getCause().getClass().getSimpleName() : "none");
        return result;
    }

    private static Map<String, Object> checkForbiddenDefaultAsync() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-check-forbidden-async-fixture");
        producer.setSendMsgTimeout(3_000);
        producer.setRetryTimesWhenSendAsyncFailed(3);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-check-forbidden-async-fixture");

        AtomicInteger checkCalls = new AtomicInteger();
        AtomicInteger sendBeforeCalls = new AtomicInteger();
        AtomicInteger sendAfterCalls = new AtomicInteger();
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "check-forbidden-default-async-fixture";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
                checkCalls.incrementAndGet();
                throw new MQClientException("fixture forbidden", null);
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "send-hook-after-forbidden-default-async-fixture";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendBeforeCalls.incrementAndGet();
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendAfterCalls.incrementAndGet();
            }
        });

        Throwable failure = null;
        try {
            Method sendDefaultImpl = DefaultMQProducerImpl.class.getDeclaredMethod(
                "sendDefaultImpl", Message.class, CommunicationMode.class,
                org.apache.rocketmq.client.producer.SendCallback.class, long.class);
            sendDefaultImpl.setAccessible(true);
            sendDefaultImpl.invoke(implementation, new Message("orders", new byte[] {1}),
                CommunicationMode.ASYNC, null, 3_000L);
        } catch (InvocationTargetException expected) {
            failure = expected.getCause();
        } finally {
            shutdownUnstartedClientExecutors(client);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("retryTimesWhenSendAsyncFailed", producer.getRetryTimesWhenSendAsyncFailed());
        result.put("checkCalls", checkCalls.get());
        result.put("sendBeforeCalls", sendBeforeCalls.get());
        result.put("sendAfterCalls", sendAfterCalls.get());
        result.put("outcome", failure == null ? "success" : failure.getClass().getSimpleName());
        result.put("responseCode", failure instanceof MQClientException
            ? ((MQClientException) failure).getResponseCode() : -1);
        result.put("messageContainsAttemptCount",
            failure != null && failure.getMessage().contains("Send [1] times"));
        result.put("causeClass", failure != null && failure.getCause() != null
            ? failure.getCause().getClass().getSimpleName() : "none");
        return result;
    }

    private static Map<String, Object> checkForbiddenExplicitAsync() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-check-forbidden-explicit-async-fixture");
        producer.setSendMsgTimeout(3_000);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-check-forbidden-explicit-async-fixture");

        AtomicInteger checkCalls = new AtomicInteger();
        AtomicInteger sendBeforeCalls = new AtomicInteger();
        AtomicInteger sendAfterCalls = new AtomicInteger();
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "check-forbidden-explicit-async-fixture";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
                checkCalls.incrementAndGet();
                throw new MQClientException("fixture forbidden", null);
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "send-hook-after-forbidden-explicit-async-fixture";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendBeforeCalls.incrementAndGet();
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendAfterCalls.incrementAndGet();
            }
        });

        Throwable failure = null;
        try {
            Method sendKernelImpl = DefaultMQProducerImpl.class.getDeclaredMethod(
                "sendKernelImpl", Message.class, MessageQueue.class, CommunicationMode.class,
                org.apache.rocketmq.client.producer.SendCallback.class, TopicPublishInfo.class, long.class);
            sendKernelImpl.setAccessible(true);
            sendKernelImpl.invoke(implementation, new Message("orders", new byte[] {1}),
                new MessageQueue("orders", "broker-a", 0), CommunicationMode.ASYNC, null, null, 3_000L);
        } catch (InvocationTargetException expected) {
            failure = expected.getCause();
        } finally {
            shutdownUnstartedClientExecutors(client);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("checkCalls", checkCalls.get());
        result.put("sendBeforeCalls", sendBeforeCalls.get());
        result.put("sendAfterCalls", sendAfterCalls.get());
        result.put("outcome", failure == null ? "success" : failure.getClass().getSimpleName());
        result.put("responseCode", failure instanceof MQClientException
            ? ((MQClientException) failure).getResponseCode() : -1);
        result.put("messageContainsAttemptCount",
            failure != null && failure.getMessage().contains("Send [1] times"));
        result.put("causeClass", failure != null && failure.getCause() != null
            ? failure.getCause().getClass().getSimpleName() : "none");
        return result;
    }

    private static Map<String, Object> checkForbiddenDeadline() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-check-forbidden-deadline-fixture");
        producer.setSendMsgTimeout(20);
        producer.setRetryTimesWhenSendFailed(1);
        DefaultMQProducerImpl implementation = producer.getDefaultMQProducerImpl();
        MQClientInstance client = installProducerSendFixtureRoute(
            producer, implementation, "orders", "producer-check-forbidden-deadline-fixture");

        AtomicInteger checkCalls = new AtomicInteger();
        AtomicInteger sendBeforeCalls = new AtomicInteger();
        AtomicInteger sendAfterCalls = new AtomicInteger();
        implementation.registerCheckForbiddenHook(new CheckForbiddenHook() {
            @Override
            public String hookName() {
                return "check-forbidden-deadline-fixture";
            }

            @Override
            public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
                checkCalls.incrementAndGet();
                try {
                    Thread.sleep(80);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                throw new MQClientException("fixture forbidden", null);
            }
        });
        implementation.registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "send-hook-after-forbidden-deadline-fixture";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendBeforeCalls.incrementAndGet();
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
                sendAfterCalls.incrementAndGet();
            }
        });

        String outcome;
        try {
            implementation.send(new Message("orders", new byte[] {1}));
            outcome = "success";
        } catch (Exception expected) {
            outcome = expected.getClass().getSimpleName();
        } finally {
            shutdownUnstartedClientExecutors(client);
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("sendMsgTimeout", producer.getSendMsgTimeout());
        result.put("retryTimesWhenSendFailed", producer.getRetryTimesWhenSendFailed());
        result.put("checkCalls", checkCalls.get());
        result.put("sendBeforeCalls", sendBeforeCalls.get());
        result.put("sendAfterCalls", sendAfterCalls.get());
        result.put("outcome", outcome);
        return result;
    }

    private static Map<String, Object> checkForbiddenContext() {
        CheckForbiddenContext nullGroup = new CheckForbiddenContext();
        nullGroup.setGroup(null);
        CheckForbiddenContext context = new CheckForbiddenContext();
        context.setNameSrvAddr("namesrv.example:9876");
        context.setGroup("producer-group");
        context.setMessage(new Message("orders", "body".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        context.setMq(new MessageQueue("orders", "broker-a", 7));
        context.setBrokerAddr("broker.example:10911");
        context.setCommunicationMode(CommunicationMode.ASYNC);
        context.setSendResult(null);
        context.setException(null);
        context.setArg("opaque-argument");
        context.setUnitMode(true);

        Map<String, Object> result = new TreeMap<>();
        result.put("nullGroupIsNull", nullGroup.getGroup() == null);
        result.put("nameSrvAddr", context.getNameSrvAddr());
        result.put("group", context.getGroup());
        result.put("messageTopic", context.getMessage().getTopic());
        result.put("messageBody", new String(context.getMessage().getBody(), java.nio.charset.StandardCharsets.UTF_8));
        result.put("queueTopic", context.getMq().getTopic());
        result.put("queueBroker", context.getMq().getBrokerName());
        result.put("queueId", context.getMq().getQueueId());
        result.put("brokerAddr", context.getBrokerAddr());
        result.put("communicationMode", context.getCommunicationMode().name());
        result.put("sendResultIsNull", context.getSendResult() == null);
        result.put("exceptionIsNull", context.getException() == null);
        result.put("arg", context.getArg());
        result.put("unitMode", context.isUnitMode());
        result.put("communicationModes", Arrays.stream(CommunicationMode.values())
            .map(Enum::name).collect(java.util.stream.Collectors.toList()));
        return result;
    }

    private static Map<String, Object> sendMessageContext() {
        SendMessageContext defaults = new SendMessageContext();
        Map<String, Object> defaultValues = new TreeMap<>();
        defaultValues.put("producerGroupIsNull", defaults.getProducerGroup() == null);
        defaultValues.put("messageIsNull", defaults.getMessage() == null);
        defaultValues.put("queueIsNull", defaults.getMq() == null);
        defaultValues.put("brokerAddrIsNull", defaults.getBrokerAddr() == null);
        defaultValues.put("bornHostIsNull", defaults.getBornHost() == null);
        defaultValues.put("communicationModeIsNull", defaults.getCommunicationMode() == null);
        defaultValues.put("sendResultIsNull", defaults.getSendResult() == null);
        defaultValues.put("exceptionIsNull", defaults.getException() == null);
        defaultValues.put("traceContextIsNull", defaults.getMqTraceContext() == null);
        defaultValues.put("propsIsNull", defaults.getProps() == null);
        defaultValues.put("producerIsNull", defaults.getProducer() == null);
        defaultValues.put("messageType", defaults.getMsgType().name());
        defaultValues.put("namespaceIsNull", defaults.getNamespace() == null);

        SendMessageContext context = new SendMessageContext();
        context.setProducerGroup("producer-group");
        context.setMessage(new Message("orders", "body".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        context.setMq(new MessageQueue("orders", "broker-a", 7));
        context.setBrokerAddr("broker.example:10911");
        context.setBornHost("192.0.2.1");
        context.setProducer(new DefaultMQProducer().getDefaultMQProducerImpl());
        context.setCommunicationMode(CommunicationMode.ASYNC);
        context.setException(new IllegalStateException("hook failure"));
        context.setMqTraceContext("trace-context");
        context.setProps(Map.of("source", "hook"));
        context.setMsgType(MessageType.Delay_Msg);
        context.setNamespace("tenant");
        Map<String, Object> values = new TreeMap<>();
        values.put("producerGroup", context.getProducerGroup());
        values.put("messageTopic", context.getMessage().getTopic());
        values.put("messageBody", new String(context.getMessage().getBody(), java.nio.charset.StandardCharsets.UTF_8));
        values.put("queueTopic", context.getMq().getTopic());
        values.put("queueBroker", context.getMq().getBrokerName());
        values.put("queueId", context.getMq().getQueueId());
        values.put("brokerAddr", context.getBrokerAddr());
        values.put("bornHost", context.getBornHost());
        values.put("communicationMode", context.getCommunicationMode().name());
        values.put("producerIsNull", context.getProducer() == null);
        values.put("exceptionClass", context.getException().getClass().getSimpleName());
        values.put("exceptionMessage", context.getException().getMessage());
        values.put("traceContext", context.getMqTraceContext());
        values.put("props", context.getProps());
        values.put("messageType", context.getMsgType().name());
        values.put("namespace", context.getNamespace());

        Map<String, Object> result = new TreeMap<>();
        result.put("defaults", defaultValues);
        result.put("values", values);
        result.put("communicationModes", Arrays.stream(CommunicationMode.values())
            .map(Enum::name).collect(java.util.stream.Collectors.toList()));
        return result;
    }

    private static List<Long> longs(long[] values) {
        List<Long> result = new ArrayList<>(values.length);
        for (long value : values) result.add(value);
        return result;
    }

    private static List<Map<String, Object>> compressionCases(DefaultMQProducer producer, int threshold) {
        try {
            Field implementationField = DefaultMQProducer.class.getDeclaredField("defaultMQProducerImpl");
            implementationField.setAccessible(true);
            Object implementation = implementationField.get(producer);
            Method compress = implementation.getClass().getDeclaredMethod("tryToCompressMessage", Message.class);
            compress.setAccessible(true);
            List<Map<String, Object>> cases = new ArrayList<>();
            for (int size : new int[] {threshold - 1, threshold, threshold + 1}) {
                Message message = new Message("orders", new byte[size]);
                Map<String, Object> row = new TreeMap<>();
                row.put("bodyLength", size);
                row.put("compressed", compress.invoke(implementation, message));
                cases.add(row);
            }
            return cases;
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("cannot invoke pinned producer compression method", error);
        }
    }

    private static Map<String, Object> runtimeCompressionSettings() {
        DefaultMQProducer producer = new DefaultMQProducer("runtime-compression-settings");
        producer.setCompressMsgBodyOverHowmuch(8);
        Map<String, Object> result = new TreeMap<>();
        result.put("threshold", producer.getCompressMsgBodyOverHowmuch());
        result.put("thresholdCases", compressionCases(producer, 8));

        producer.setCompressMsgBodyOverHowmuch(1);
        result.put("thresholdAfterUpdate", producer.getCompressMsgBodyOverHowmuch());
        result.put("level1", compressionAtCurrentLevel(producer, 1));
        result.put("level9", compressionAtCurrentLevel(producer, 9));
        return result;
    }

    private static Map<String, Object> compressionAtCurrentLevel(DefaultMQProducer producer, int level) {
        try {
            Field implementationField = DefaultMQProducer.class.getDeclaredField("defaultMQProducerImpl");
            implementationField.setAccessible(true);
            Object implementation = implementationField.get(producer);
            Method compress = implementation.getClass().getDeclaredMethod("tryToCompressMessage", Message.class);
            compress.setAccessible(true);
            byte[] body = new byte[16_384];
            Arrays.fill(body, (byte) 'x');
            Message message = new Message("orders", body);
            producer.setCompressLevel(level);
            boolean compressed = (Boolean) compress.invoke(implementation, message);
            Map<String, Object> result = new TreeMap<>();
            result.put("level", producer.getCompressLevel());
            result.put("compressed", compressed);
            result.put("compressedBodyLength", message.getBody().length);
            return result;
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("cannot invoke pinned producer compression method", error);
        }
    }

    private static Map<String, Object> runtimeFaultStrategy() {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("runtime-fault-strategy");
            long[] latencyMax = {10, 20};
            long[] notAvailable = {100, 300};
            producer.setLatencyMax(latencyMax);
            producer.setNotAvailableDuration(notAvailable);
            producer.setSendLatencyFaultEnable(true);
            producer.setStartDetectorEnable(true);
            long[] returnedLatencyMax = producer.getLatencyMax();
            long[] returnedNotAvailable = producer.getNotAvailableDuration();
            latencyMax[1] = 16;
            returnedLatencyMax[0] = 12;
            notAvailable[0] = 200;
            returnedNotAvailable[1] = 400;

            Field implementationField = DefaultMQProducer.class.getDeclaredField("defaultMQProducerImpl");
            implementationField.setAccessible(true);
            Object implementation = implementationField.get(producer);
            Object fault = implementation.getClass().getMethod("getMqFaultStrategy").invoke(implementation);
            Method update = fault.getClass().getMethod("updateFaultItem",
                String.class, long.class, boolean.class, boolean.class);
            update.invoke(fault, "enabled-broker", 18L, false, true);
            Field toleranceField = fault.getClass().getDeclaredField("latencyFaultTolerance");
            toleranceField.setAccessible(true);
            Object tolerance = toleranceField.get(fault);
            Method isAvailable = tolerance.getClass().getMethod("isAvailable", String.class);

            Map<String, Object> result = new TreeMap<>();
            result.put("initialLatencyMaxMillis", longs(new long[] {10, 20}));
            result.put("initialNotAvailableDurationMillis", longs(new long[] {100, 300}));
            result.put("latencyInputMutation", indexedValue(1, 16));
            result.put("latencyGetterMutation", indexedValue(0, 12));
            result.put("durationInputMutation", indexedValue(0, 200));
            result.put("durationGetterMutation", indexedValue(1, 400));
            result.put("latencyMaxMillis", longs(producer.getLatencyMax()));
            result.put("notAvailableDurationMillis", longs(producer.getNotAvailableDuration()));
            result.put("latencyArrayIdentityRetained", producer.getLatencyMax() == latencyMax);
            result.put("durationArrayIdentityRetained", producer.getNotAvailableDuration() == notAvailable);
            result.put("latencyGetterIdentityRetained", producer.getLatencyMax() == returnedLatencyMax);
            result.put("durationGetterIdentityRetained", producer.getNotAvailableDuration() == returnedNotAvailable);
            result.put("sendLatencyFaultEnableAfterToggle", producer.isSendLatencyFaultEnable());
            result.put("enabledBrokerAvailable", isAvailable.invoke(tolerance, "enabled-broker"));

            producer.setSendLatencyFaultEnable(false);
            update.invoke(fault, "disabled-broker", 15L, false, true);
            result.put("sendLatencyFaultEnableAfterDisable", producer.isSendLatencyFaultEnable());
            result.put("disabledBrokerAvailable", isAvailable.invoke(tolerance, "disabled-broker"));
            producer.setSendLatencyFaultEnable(true);
            long[] priorLatencyMax = producer.getLatencyMax();
            long[] replacementLatencyMax = {20};
            producer.setLatencyMax(replacementLatencyMax);
            priorLatencyMax[0] = 0;
            long[] priorNotAvailable = producer.getNotAvailableDuration();
            long[] replacementNotAvailable = {0};
            producer.setNotAvailableDuration(replacementNotAvailable);
            priorNotAvailable[0] = 999;
            update.invoke(fault, "updated-table-broker", 15L, false, true);
            result.put("sendLatencyFaultEnableAfterReenable", producer.isSendLatencyFaultEnable());
            result.put("latencyReplacementValues", longs(producer.getLatencyMax()));
            result.put("durationReplacementValues", longs(producer.getNotAvailableDuration()));
            result.put("latencyReplacementIdentityRetained", producer.getLatencyMax() == replacementLatencyMax);
            result.put("durationReplacementIdentityRetained", producer.getNotAvailableDuration() == replacementNotAvailable);
            result.put("updatedTableBrokerAvailable", isAvailable.invoke(tolerance, "updated-table-broker"));
            result.put("startDetectorEnableAfterToggle", producer.isStartDetectorEnable());
            producer.setStartDetectorEnable(false);
            result.put("startDetectorEnableAfterDisable", producer.isStartDetectorEnable());
            return result;
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("cannot invoke pinned producer fault strategy", error);
        }
    }

    private static Map<String, Object> runtimeFaultStrategyNullArrays() {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("runtime-fault-null-arrays");
            producer.setSendLatencyFaultEnable(true);
            Field implementationField = DefaultMQProducer.class.getDeclaredField("defaultMQProducerImpl");
            implementationField.setAccessible(true);
            Object implementation = implementationField.get(producer);
            Object fault = implementation.getClass().getMethod("getMqFaultStrategy").invoke(implementation);
            Method update = fault.getClass().getMethod("updateFaultItem",
                String.class, long.class, boolean.class, boolean.class);

            Map<String, Object> result = new TreeMap<>();
            producer.setLatencyMax(null);
            result.put("latencyMaxGetterIsNull", producer.getLatencyMax() == null);
            result.put("nullLatencyMaxUpdateFailure", invokeFaultUpdate(update, fault, 10L));

            producer.setLatencyMax(new long[] {10L});
            producer.setNotAvailableDuration(null);
            result.put("notAvailableDurationGetterIsNull", producer.getNotAvailableDuration() == null);
            result.put("unmatchedLatencyWithNullDurationFailure", invokeFaultUpdate(update, fault, 9L));
            result.put("matchedLatencyWithNullDurationFailure", invokeFaultUpdate(update, fault, 10L));

            producer.setNotAvailableDuration(new long[] {100L});
            producer.setLatencyMax(new long[] {10L, 20L});
            result.put("shortDurationTableFailure", invokeFaultUpdate(update, fault, 20L));

            producer.setLatencyMax(new long[] {20L});
            producer.setNotAvailableDuration(new long[] {100L, 200L});
            result.put("extraDurationRowsFailure", invokeFaultUpdate(update, fault, 20L));
            return result;
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("cannot invoke pinned producer null fault-table cases", error);
        }
    }

    private static String invokeFaultUpdate(Method update, Object fault, long latency) {
        try {
            update.invoke(fault, "fault-table-input", latency, false, true);
            return "none";
        } catch (InvocationTargetException error) {
            return error.getCause().getClass().getName();
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("cannot invoke pinned producer fault-table update", error);
        }
    }

    private static Map<String, Object> indexedValue(int index, long value) {
        Map<String, Object> result = new TreeMap<>();
        result.put("index", index);
        result.put("value", value);
        return result;
    }

    private static Map<String, Object> compressionProperties() {
        Map<String, Object> result = new TreeMap<>();
        String previousType = System.getProperty(MixAll.MESSAGE_COMPRESS_TYPE);
        String previousLevel = System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL);
        try {
            System.setProperty(MixAll.MESSAGE_COMPRESS_TYPE, " lz4 ");
            System.setProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "7");
            DefaultMQProducer configured = new DefaultMQProducer();
            result.put("inputType", " lz4 ");
            result.put("inputLevel", "7");
            result.put("parsedType", configured.getCompressType().name());
            result.put("parsedLevel", configured.getCompressLevel());

            System.setProperty(MixAll.MESSAGE_COMPRESS_TYPE, "unsupported");
            result.put("invalidTypeAccepted", accepts(() -> new DefaultMQProducer()));
            System.setProperty(MixAll.MESSAGE_COMPRESS_TYPE, "ZLIB");
            System.setProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "invalid");
            result.put("invalidLevelAccepted", accepts(() -> new DefaultMQProducer()));
        } finally {
            restoreProperty(MixAll.MESSAGE_COMPRESS_TYPE, previousType);
            restoreProperty(MixAll.MESSAGE_COMPRESS_LEVEL, previousLevel);
        }
        return result;
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }

    private static List<Map<String, Object>> batchCases() {
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(row("empty", accepts(() -> MessageBatch.generateFromList(List.of()))));
        cases.add(row("mixed-topics", accepts(() -> MessageBatch.generateFromList(List.of(
            new Message("orders", new byte[] {1}), new Message("payments", new byte[] {2}))))));
        cases.add(row("wait-store-mismatch", accepts(() -> {
            Message first = new Message("orders", new byte[] {1});
            Message second = new Message("orders", new byte[] {2});
            second.setWaitStoreMsgOK(false);
            MessageBatch.generateFromList(List.of(first, second));
        })));
        cases.add(row("delayed", accepts(() -> {
            Message message = new Message("orders", new byte[] {1});
            message.setDelayTimeLevel(1);
            MessageBatch.generateFromList(List.of(message));
        })));
        cases.add(row("retry-topic", accepts(() -> MessageBatch.generateFromList(List.of(
            new Message("%RETRY%workers", new byte[] {1}))))));
        cases.add(row("different-tags-allowed", accepts(() -> {
            Message first = new Message("orders", new byte[] {1});
            Message second = new Message("orders", new byte[] {2});
            first.setTags("one");
            second.setTags("two");
            MessageBatch.generateFromList(List.of(first, second));
        })));
        return cases;
    }

    private static Map<String, Object> batchEncoding() {
        MessageBatch batch = MessageBatch.generateFromList(List.of(
            new Message("orders", "first".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
            new Message("orders", "second".getBytes(java.nio.charset.StandardCharsets.UTF_8))));
        byte[] body = batch.encode();
        Map<String, Object> result = new TreeMap<>();
        result.put("bodyHex", toHex(body));
        result.put("length", body.length);
        return result;
    }

    private static String toHex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) result.append(String.format("%02X", value & 0xFF));
        return result.toString();
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1
            && (Boolean.parseBoolean(args[1]) || "--verify".equals(args[1]));
        String json = JSON.toJSONString(produce()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java producer fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }
}
