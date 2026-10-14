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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
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
import org.apache.rocketmq.remoting.protocol.RequestCode;

/** Offline Java 5.5.1 producer defaults and pre-I/O validation oracle. */
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

        result.put("batchValidation", batchCases());
        result.put("batchEncoding", batchEncoding());
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

    private static Map<String, Object> checkForbiddenContext() {
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
