package compat;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.utils.NetworkUtil;

/** Deterministic Java 5.5.1 message-record bytes and decoded outcomes. */
public final class MessageDecodeFixtures {
    private static final int MAGIC_V2 = -626843477;

    private MessageDecodeFixtures() { }

    public static void main(String[] args) throws Exception {
        Path output;
        boolean verify;
        if (args.length == 2) {
            output = Path.of(args[0]);
            verify = Boolean.parseBoolean(args[1]);
        } else if (args.length == 3 && "--output".equals(args[0])) {
            output = Path.of(args[1]);
            verify = Boolean.parseBoolean(args[2]);
        } else {
            throw new IllegalArgumentException("usage: MessageDecodeFixtures OUTPUT true|false");
        }
        run(output, verify);
    }

    public static void run(Path output, boolean verify) throws Exception {
        Map<String, Object> root = new TreeMap<>();
        root.put("provenance", Map.of(
            "client", "org.apache.rocketmq:rocketmq-client:5.5.1",
            "sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9",
            "serverContacted", false,
            "generator", "compat.MessageDecodeFixtures"
        ));
        root.put("messageExtHostApi", messageExtHostApi());
        root.put("messageExtSystemFlagsApi", messageExtSystemFlagsApi());
        root.put("messagePublicApi", messagePublicApi());
        List<Map<String, Object>> cases = new ArrayList<>();
        cases.add(record("ipv4-v1", message("orders", "hello-v1", 17, 41,
            InetAddress.getByName("192.0.2.10"), 10911,
            InetAddress.getByName("198.51.100.20"), 10911, 0,
            Map.of("TAGS", "green", "UNIQ_KEY", "producer-unique-1")), null));
        cases.add(record("no-tags-v1", message("orders", "without-tags", 26, 50,
            InetAddress.getByName("192.0.2.18"), 10928,
            InetAddress.getByName("198.51.100.27"), 10929, 0, Map.of()), null));
        cases.add(record("born-ipv6-v1", message("orders", "hello-ipv6", 18, 42,
            InetAddress.getByName("2001:db8::10"), 10912,
            InetAddress.getByName("198.51.100.21"), 10913,
            MessageSysFlag.BORNHOST_V6_FLAG,
            Map.of("TAGS", "blue")), null));
        String longTopic = "long-" + "t".repeat(130);
        cases.add(record("store-ipv6-v2", message("long-v2", "hello-v2", 19, 43,
            InetAddress.getByName("192.0.2.12"), 10914,
            InetAddress.getByName("2001:db8::20"), 10915,
            MessageSysFlag.STOREHOSTADDRESS_V6_FLAG,
            Map.of("TAGS", "violet", "TRAN_MSG", "true", "UNIQ_KEY", "producer-unique-2")), longTopic));
        cases.add(record("born-and-store-ipv6-v1", message("dual-ipv6", "both-hosts-ipv6", 21, 45,
            InetAddress.getByName("2001:db8::30"), 10918,
            InetAddress.getByName("2001:db8::40"), 10919,
            MessageSysFlag.BORNHOST_V6_FLAG | MessageSysFlag.STOREHOSTADDRESS_V6_FLAG,
            Map.of("TAGS", "dual")), null));
        byte[] compressedBody = "zstd-body-".repeat(96).getBytes(StandardCharsets.UTF_8);
        cases.add(record("compressed-zstd-v1", message("compressed", compressedBody, 20, 44,
            InetAddress.getByName("192.0.2.13"), 10916,
            InetAddress.getByName("198.51.100.22"), 10917,
            MessageSysFlag.COMPRESSED_FLAG | MessageSysFlag.COMPRESSION_ZSTD_TYPE,
            Map.of("TAGS", "compressed")), null));
        cases.add(record("compressed-lz4-v1", message("compressed-lz4", compressedBody, 22, 46,
            InetAddress.getByName("192.0.2.14"), 10920,
            InetAddress.getByName("198.51.100.23"), 10921,
            MessageSysFlag.COMPRESSED_FLAG | MessageSysFlag.COMPRESSION_LZ4_TYPE,
            Map.of("TAGS", "compressed-lz4")), null));
        cases.add(record("compressed-zlib-v1", message("compressed-zlib", compressedBody, 23, 47,
            InetAddress.getByName("192.0.2.15"), 10922,
            InetAddress.getByName("198.51.100.24"), 10923,
            MessageSysFlag.COMPRESSED_FLAG | MessageSysFlag.COMPRESSION_ZLIB_TYPE,
            Map.of("TAGS", "compressed-zlib")), null));
        Message firstInner = new Message("orders", "inner-one".getBytes(StandardCharsets.UTF_8));
        firstInner.setFlag(301);
        MessageAccessor.putProperty(firstInner, "TAGS", "green");
        MessageAccessor.putProperty(firstInner, "UNIQ_KEY", "inner-unique-1");
        Message secondInner = new Message("orders", "inner-two".getBytes(StandardCharsets.UTF_8));
        secondInner.setFlag(302);
        MessageAccessor.putProperty(secondInner, "TAGS", "blue");
        byte[] innerBatch = MessageDecoder.encodeMessages(List.of(firstInner, secondInner));
        cases.add(record("inner-batch", message("orders", innerBatch, 24, 48,
            InetAddress.getByName("192.0.2.16"), 10924,
            InetAddress.getByName("198.51.100.25"), 10925,
            MessageSysFlag.INNER_BATCH_FLAG | MessageSysFlag.NEED_UNWRAP_FLAG,
            Map.of("TAGS", "outer")), null));
        cases.add(record("inner-batch-malformed", message("orders", new byte[] {0, 0, 0, 8}, 25, 49,
            InetAddress.getByName("192.0.2.17"), 10926,
            InetAddress.getByName("198.51.100.26"), 10927,
            MessageSysFlag.INNER_BATCH_FLAG | MessageSysFlag.NEED_UNWRAP_FLAG,
            Map.of("TAGS", "outer")), null));

        root.put("cases", cases);
        String json = JSON.toJSONString(root, JSONWriter.Feature.MapSortField) + "\n";
        if (verify) {
            String checkedIn = Files.readString(output, StandardCharsets.UTF_8);
            if (!json.equals(checkedIn)) {
                throw new IllegalStateException("message decode fixture drift: " + output);
            }
            System.out.println("verified " + cases.size() + " Java message decode fixtures at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json, StandardCharsets.UTF_8);
            System.out.println("wrote " + cases.size() + " Java message decode fixtures to " + output);
        }
    }

    private static Map<String, Object> messagePublicApi() {
        Message message = new Message("orders", "green", "first second", 17,
            new byte[] {0, -1, 42}, false);
        Map<String, Object> expected = new TreeMap<>();
        Message defaultMessage = new Message();
        expected.put("defaultTopicIsNull", defaultMessage.getTopic() == null);
        expected.put("defaultBodyIsNull", defaultMessage.getBody() == null);
        expected.put("defaultPropertiesIsNull", defaultMessage.getProperties() == null);
        expected.put("defaultToString", defaultMessage.toString());
        defaultMessage.getProperty("missing");
        expected.put("propertyLookupInitializesProperties",
            defaultMessage.getProperties() != null && defaultMessage.getProperties().isEmpty());

        Message nullableConstructor = new Message(null, (byte[]) null);
        expected.put("nullableConstructorTopicIsNull", nullableConstructor.getTopic() == null);
        expected.put("nullableConstructorBodyIsNull", nullableConstructor.getBody() == null);
        expected.put("nullableConstructorWaitStore", nullableConstructor.isWaitStoreMsgOK());
        Message nullableFields = new Message(null, null, null, -3, null, false);
        expected.put("nullableFieldsTopicIsNull", nullableFields.getTopic() == null);
        expected.put("nullableFieldsBodyIsNull", nullableFields.getBody() == null);
        expected.put("nullableFieldsWaitStore", nullableFields.isWaitStoreMsgOK());

        Message nullableSetters = new Message("before", new byte[] {1});
        nullableSetters.setTopic(null);
        nullableSetters.setBody(null);
        expected.put("nullableSetterTopicIsNull", nullableSetters.getTopic() == null);
        expected.put("nullableSetterBodyIsNull", nullableSetters.getBody() == null);

        Message nullableProperties = new Message();
        nullableProperties.setTags(null);
        nullableProperties.setBuyerId(null);
        nullableProperties.setInstanceId(null);
        expected.put("nullTagsGetterIsNull", nullableProperties.getTags() == null);
        expected.put("nullTagsRemainsPresent", nullableProperties.hasProperty("TAGS"));
        expected.put("nullBuyerGetterIsNull", nullableProperties.getBuyerId() == null);
        expected.put("nullBuyerRemainsPresent", nullableProperties.hasProperty("BUYER_ID"));
        expected.put("nullInstanceGetterIsNull",
            nullableProperties.getProperty("INSTANCE_ID") == null);
        expected.put("nullInstanceRemainsPresent", nullableProperties.hasProperty("INSTANCE_ID"));
        Message nullableString = new Message();
        nullableString.setTags(null);
        expected.put("nullablePropertyString", nullableString.toString());
        expected.put("nullPropertyNameRejected",
            throwsIllegalArgument(() -> nullableProperties.putUserProperty(null, "value")));
        expected.put("nullPropertyValueRejected",
            throwsIllegalArgument(() -> nullableProperties.putUserProperty("key", null)));

        expected.put("topic", message.getTopic());
        expected.put("tags", message.getTags());
        expected.put("keys", message.getKeys());
        expected.put("flag", message.getFlag());
        expected.put("body", Base64.getEncoder().encodeToString(message.getBody()));
        expected.put("waitStore", message.isWaitStoreMsgOK());
        expected.put("propertyTags", message.getProperty("TAGS"));
        expected.put("userPropertyTags", message.getUserProperty("TAGS"));
        expected.put("missingProperty", message.getProperty("missing"));
        expected.put("hasTags", message.hasProperty("TAGS"));
        expected.put("hasMissing", message.hasProperty("missing"));

        message.setKeys(List.of("third", "fourth"));
        expected.put("collectionKeys", message.getKeys());
        message.setBuyerId("buyer-1");
        message.setInstanceId("instance-1");
        expected.put("buyerId", message.getBuyerId());
        expected.put("instanceIdProperty", message.getProperty("INSTANCE_ID"));

        message.setDelayTimeLevel(-7);
        message.setDelayTimeSec(Long.MIN_VALUE);
        message.setDelayTimeMs(9007199254740993L);
        message.setDeliverTimeMs(Long.MAX_VALUE);
        expected.put("delayTimeLevel", message.getDelayTimeLevel());
        expected.put("delayTimeSec", message.getDelayTimeSec());
        expected.put("delayTimeMs", message.getDelayTimeMs());
        expected.put("deliverTimeMs", message.getDeliverTimeMs());

        Message absent = new Message();
        expected.put("absentDelayTimeLevel", absent.getDelayTimeLevel());
        expected.put("absentDelayTimeSec", absent.getDelayTimeSec());
        expected.put("absentDelayTimeMs", absent.getDelayTimeMs());
        expected.put("absentDeliverTimeMs", absent.getDeliverTimeMs());
        MessageAccessor.putProperty(absent, "DELAY", "\u0661\u0662");
        MessageAccessor.putProperty(absent, "TIMER_DELAY_SEC", "+\u0663\u0664");
        expected.put("unicodeDelayTimeLevel", absent.getDelayTimeLevel());
        expected.put("unicodeDelayTimeSec", absent.getDelayTimeSec());
        MessageAccessor.putProperty(absent, "DELAY", "2147483648");
        expected.put("overflowDelayTimeLevelThrows", throwsNumberFormat(absent, "DELAY"));
        MessageAccessor.putProperty(absent, "TIMER_DELAY_MS", "9223372036854775808");
        expected.put("overflowDelayTimeMsThrows", throwsNumberFormat(absent, "TIMER_DELAY_MS"));

        List<Map<String, Object>> numericInputs = new ArrayList<>();
        for (String value : List.of("", " ", "+", "++1", "+1", "-1", "2147483647",
            "-2147483648", "2147483648", "-2147483649", "\u0661\u0662")) {
            numericInputs.add(numericPropertyInput("DELAY", value));
        }
        for (String value : List.of("", " ", "+", "++1", "+1", "-1", "9223372036854775807",
            "-9223372036854775808", "9223372036854775808", "-9223372036854775809", "+\u0663\u0664")) {
            numericInputs.add(numericPropertyInput("TIMER_DELAY_SEC", value));
            numericInputs.add(numericPropertyInput("TIMER_DELAY_MS", value));
            numericInputs.add(numericPropertyInput("TIMER_DELIVER_MS", value));
        }
        expected.put("numericPropertyInputs", numericInputs);

        Message diagnostic = new Message("invoice", new byte[] {0, -1, 42});
        expected.put("messageToString", diagnostic.toString());
        diagnostic.getProperties().put("DIRECT", "through-map");
        expected.put("directMapMutation", diagnostic.getProperty("DIRECT"));
        return expected;
    }

    private static Map<String, Object> numericPropertyInput(String property, String value) {
        Message message = new Message();
        MessageAccessor.putProperty(message, property, value);
        Map<String, Object> result = new TreeMap<>();
        result.put("property", property);
        result.put("input", value);
        try {
            long parsed = 0;
            switch (property) {
                case "DELAY":
                    parsed = message.getDelayTimeLevel();
                    break;
                case "TIMER_DELAY_SEC":
                    parsed = message.getDelayTimeSec();
                    break;
                case "TIMER_DELAY_MS":
                    parsed = message.getDelayTimeMs();
                    break;
                default:
                    parsed = message.getDeliverTimeMs();
            }
            result.put("accepted", true);
            result.put("value", parsed);
        } catch (NumberFormatException expected) {
            result.put("accepted", false);
        }
        return result;
    }

    private static boolean throwsNumberFormat(Message message, String property) {
        try {
            if ("DELAY".equals(property)) message.getDelayTimeLevel();
            else if ("TIMER_DELAY_SEC".equals(property)) message.getDelayTimeSec();
            else if ("TIMER_DELAY_MS".equals(property)) message.getDelayTimeMs();
            else message.getDeliverTimeMs();
            return false;
        } catch (NumberFormatException expected) {
            return true;
        }
    }

    private static boolean throwsIllegalArgument(Runnable action) {
        try {
            action.run();
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static Map<String, Object> messageExtHostApi() throws Exception {
        InetSocketAddress born = new InetSocketAddress(
            InetAddress.getByAddress(new byte[] {(byte) 192, 0, 2, 10}), 10911);
        InetSocketAddress unresolved = InetSocketAddress.createUnresolved("broker.invalid", 10912);
        MessageExt message = new MessageExt();
        message.setBornHost(born);
        message.setStoreHost(unresolved);

        Map<String, Object> expected = new TreeMap<>();
        expected.put("bornHostClass", message.getBornHost().getClass().getName());
        expected.put("bornHostToString", message.getBornHost().toString());
        expected.put("bornHostString", message.getBornHostString());
        expected.put("bornHostNameString", message.getBornHostNameString());
        expected.put("bornHostGetterRetainsObject", message.getBornHost() == born);
        expected.put("ipv4SocketAddressString", NetworkUtil.socketAddress2String(born));
        expected.put("bornHostBytes", byteBufferBase64(message.getBornHostBytes()));
        expected.put("bornHostBufferBytes", byteBufferBase64(message.getBornHostBytes(ByteBuffer.allocate(8))));
        expected.put("storeHostClass", message.getStoreHost().getClass().getName());
        expected.put("storeHostToString", message.getStoreHost().toString());
        expected.put("storeHostUnresolved", ((InetSocketAddress) message.getStoreHost()).isUnresolved());
        expected.put("storeHostGetterRetainsObject", message.getStoreHost() == unresolved);

        InetSocketAddress ipv6 = new InetSocketAddress(InetAddress.getByName("2001:db8::20"), 10915);
        MessageExt ipv6Message = new MessageExt();
        ipv6Message.setBornHost(ipv6);
        expected.put("ipv6HostToString", ipv6Message.getBornHost().toString());
        expected.put("ipv6HostString", ipv6Message.getBornHostString());
        expected.put("ipv6HostNameString", ipv6Message.getBornHostNameString());
        expected.put("ipv6SocketAddressString", NetworkUtil.socketAddress2String(ipv6));
        expected.put("ipv6HostBytes", byteBufferBase64(ipv6Message.getBornHostBytes()));
        expected.put("ipv6HostBufferBytes", byteBufferBase64(ipv6Message.getBornHostBytes(ByteBuffer.allocate(20))));
        ipv6Message.setStoreHost(ipv6);
        expected.put("ipv6StoreHostBytes", byteBufferBase64(ipv6Message.getStoreHostBytes()));

        boolean unresolvedBytesThrow = false;
        try {
            message.getStoreHostBytes();
        } catch (NullPointerException expectedFailure) {
            unresolvedBytesThrow = true;
        }
        expected.put("unresolvedHostBytesThrow", unresolvedBytesThrow);

        InetSocketAddress fromHost = new InetSocketAddress("127.0.0.1", 10913);
        MessageExt fromHostMessage = new MessageExt();
        fromHostMessage.setBornHost(fromHost);
        expected.put("stringHostToString", fromHostMessage.getBornHost().toString());
        expected.put("stringHostAddress", fromHostMessage.getBornHostString());
        expected.put("stringHostName", fromHostMessage.getBornHostNameString());

        MessageExt constructed = new MessageExt(7, 13L, born, 17L, unresolved, "message-id");
        expected.put("constructorQueueId", constructed.getQueueId());
        expected.put("constructorBornTimestamp", constructed.getBornTimestamp());
        expected.put("constructorBornHost", constructed.getBornHost().toString());
        expected.put("constructorStoreTimestamp", constructed.getStoreTimestamp());
        expected.put("constructorStoreHost", constructed.getStoreHost().toString());
        expected.put("constructorMessageId", constructed.getMsgId());
        return expected;
    }

    private static Map<String, Object> messageExtSystemFlagsApi() {
        MessageExt message = new MessageExt();
        Map<String, Object> expected = new TreeMap<>();
        expected.put("topicSysFlagAbsentIsNull", message.getTopicSysFlag() == null);
        expected.put("groupSysFlagAbsentIsNull", message.getGroupSysFlag() == null);
        message.setTopicSysFlag(73);
        message.setGroupSysFlag(-19);
        expected.put("topicSysFlagSet", message.getTopicSysFlag());
        expected.put("groupSysFlagSet", message.getGroupSysFlag());
        message.setTopicSysFlag(null);
        message.setGroupSysFlag(null);
        expected.put("topicSysFlagCleared", message.getTopicSysFlag() == null);
        expected.put("groupSysFlagCleared", message.getGroupSysFlag() == null);

        MessageAccessor.putProperty(message, org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG, "");
        MessageAccessor.putProperty(message, org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG, "");
        expected.put("topicSysFlagEmptyIsNull", message.getTopicSysFlag() == null);
        expected.put("groupSysFlagEmptyIsNull", message.getGroupSysFlag() == null);
        MessageAccessor.putProperty(message, org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG, "bad");
        MessageAccessor.putProperty(message, org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG, "bad");
        expected.put("topicSysFlagMalformedThrows", throwsNumberFormat(message, true));
        expected.put("groupSysFlagMalformedThrows", throwsNumberFormat(message, false));
        MessageAccessor.putProperty(message, org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG, "\u0661");
        expected.put("topicSysFlagUnicodeDigit", message.getTopicSysFlag());

        message.setSysFlag(0x40);
        message.setBornHostV6Flag();
        expected.put("bornV6FlagAfterSetter", message.getSysFlag());
        message.setStoreHostAddressV6Flag();
        expected.put("storeV6FlagAfterSetter", message.getSysFlag());
        expected.put("singleTagFilterType", MessageExt.parseTopicFilterType(0).name());
        expected.put("multiTagFilterType", MessageExt.parseTopicFilterType(0x42).name());

        MessageExt diagnostic = new MessageExt();
        diagnostic.setTopic("invoice");
        diagnostic.setBody(new byte[] {0, -1, 42});
        diagnostic.setFlag(5);
        MessageAccessor.putProperty(diagnostic, "KEY", "value");
        expected.put("messageExtToString", diagnostic.toString());
        return expected;
    }

    private static boolean throwsNumberFormat(MessageExt message, boolean topic) {
        try {
            if (topic) message.getTopicSysFlag(); else message.getGroupSysFlag();
            return false;
        } catch (NumberFormatException expected) {
            return true;
        }
    }

    private static String byteBufferBase64(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static MessageExt message(String topic, String body, int queueId, long queueOffset,
        InetAddress born, int bornPort, InetAddress store, int storePort, int sysFlag,
        Map<String, String> properties) {
        return message(topic, body.getBytes(StandardCharsets.UTF_8), queueId, queueOffset,
            born, bornPort, store, storePort, sysFlag, properties);
    }

    private static MessageExt message(String topic, byte[] body, int queueId, long queueOffset,
        InetAddress born, int bornPort, InetAddress store, int storePort, int sysFlag,
        Map<String, String> properties) {
        MessageExt message = new MessageExt();
        message.setTopic(topic);
        message.setBody(body);
        message.setQueueId(queueId);
        message.setQueueOffset(queueOffset);
        message.setCommitLogOffset(1_700_000_000_000L + queueOffset);
        message.setSysFlag(sysFlag);
        message.setFlag(queueId * 3);
        message.setBodyCRC(0);
        message.setBornTimestamp(1_700_000_100_000L + queueOffset);
        message.setStoreTimestamp(1_700_000_200_000L + queueOffset);
        message.setBornHost(new InetSocketAddress(born, bornPort));
        message.setStoreHost(new InetSocketAddress(store, storePort));
        message.setReconsumeTimes(2);
        message.setPreparedTransactionOffset(1_600_000_000_000L + queueOffset);
        properties.forEach((key, value) -> MessageAccessor.putProperty(message, key, value));
        return message;
    }

    private static Map<String, Object> record(String name, MessageExt source, String v2Topic) throws Exception {
        byte[] encoded = MessageDecoder.encode(source, true);
        if (v2Topic != null) {
            encoded = convertToV2(encoded, source.getSysFlag(), v2Topic);
        }
        int bornHostLength = (source.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storeHostLength = (source.getSysFlag() & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        int bodyLengthOffset = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornHostLength
            + 8 + storeHostLength + 4 + 8;
        int bodyLength = ByteBuffer.wrap(encoded).getInt(bodyLengthOffset);
        int bodyCrc = UtilAll.crc32(encoded, bodyLengthOffset + 4, bodyLength);
        ByteBuffer.wrap(encoded).putInt(8, bodyCrc);
        MessageExt decoded = MessageDecoder.decode(ByteBuffer.wrap(encoded), true, true, false, false, true);
        Map<String, Object> expected = new TreeMap<>();
        expected.put("topic", decoded.getTopic());
        expected.put("bodyBase64", Base64.getEncoder().encodeToString(decoded.getBody()));
        expected.put("flag", decoded.getFlag());
        expected.put("bodyCRC", decoded.getBodyCRC());
        expected.put("queueId", decoded.getQueueId());
        expected.put("queueOffset", decoded.getQueueOffset());
        expected.put("commitLogOffset", decoded.getCommitLogOffset());
        expected.put("sysFlag", decoded.getSysFlag());
        expected.put("bornTimestamp", decoded.getBornTimestamp());
        expected.put("storeTimestamp", decoded.getStoreTimestamp());
        expected.put("bornHost", decoded.getBornHost().toString());
        expected.put("storeHost", decoded.getStoreHost().toString());
        expected.put("reconsumeTimes", decoded.getReconsumeTimes());
        expected.put("preparedTransactionOffset", decoded.getPreparedTransactionOffset());
        expected.put("msgId", decoded.getMsgId());
        expected.put("properties", decoded.getProperties() == null
            ? Map.of() : new TreeMap<>(decoded.getProperties()));

        MessageExt propertiesStringDecoded = MessageDecoder.decode(
            ByteBuffer.wrap(encoded), true, true, false, true, true);
        expected.put("propertiesWithString", propertiesStringDecoded.getProperties() == null
            ? Map.of() : new TreeMap<>(propertiesStringDecoded.getProperties()));

        MessageClientExt clientDecoded = (MessageClientExt) MessageDecoder.decode(
            ByteBuffer.wrap(encoded), true, true, true, false, true);
        Map<String, Object> expectedClientIds = new TreeMap<>();
        expectedClientIds.put("msgId", clientDecoded.getMsgId());
        expectedClientIds.put("offsetMsgId", clientDecoded.getOffsetMsgId());

        List<Map<String, Object>> bodyOptions = new ArrayList<>();
        for (boolean readBody : new boolean[] {false, true}) {
            for (boolean decompressBody : new boolean[] {false, true}) {
                MessageExt optionDecoded = MessageDecoder.decode(
                    ByteBuffer.wrap(encoded), readBody, decompressBody, true, false, true);
                Map<String, Object> option = new TreeMap<>();
                option.put("readBody", readBody);
                option.put("decompressBody", decompressBody);
                option.put("bodyBase64", optionDecoded.getBody() == null ? null
                    : Base64.getEncoder().encodeToString(optionDecoded.getBody()));
                option.put("sysFlag", optionDecoded.getSysFlag());
                bodyOptions.add(option);
            }
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("name", name);
        result.put("recordBase64", Base64.getEncoder().encodeToString(encoded));
        result.put("sha256", hex(MessageDigest.getInstance("SHA-256").digest(encoded)));
        result.put("decoded", expected);
        result.put("clientDecoded", expectedClientIds);
        result.put("decodeOptions", bodyOptions);
        return result;
    }

    private static byte[] convertToV2(byte[] v1, int sysFlag, String topic) {
        ByteBuffer input = ByteBuffer.wrap(v1);
        int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        int bodyLengthOffset = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornHostLength
            + 8 + storeHostLength + 4 + 8;
        int bodyLength = input.getInt(bodyLengthOffset);
        int topicLengthOffset = bodyLengthOffset + 4 + bodyLength;
        int topicLength = Byte.toUnsignedInt(v1[topicLengthOffset]);
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int propertiesOffset = topicLengthOffset + 1 + topicLength;
        byte[] v2 = new byte[v1.length - 1 - topicLength + 2 + topicBytes.length];
        System.arraycopy(v1, 0, v2, 0, topicLengthOffset);
        ByteBuffer output = ByteBuffer.wrap(v2);
        output.putInt(0, v2.length);
        output.putInt(4, MAGIC_V2);
        output.position(topicLengthOffset);
        output.putShort((short) topicBytes.length);
        output.put(topicBytes);
        System.arraycopy(v1, propertiesOffset, v2, topicLengthOffset + 2 + topicBytes.length,
            v1.length - propertiesOffset);
        return v2;
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            value.append(String.format("%02X", b & 0xff));
        }
        return value.toString();
    }
}
