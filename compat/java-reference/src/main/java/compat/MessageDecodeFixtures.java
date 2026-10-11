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
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.UtilAll;

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

        Map<String, Object> result = new TreeMap<>();
        result.put("name", name);
        result.put("recordBase64", Base64.getEncoder().encodeToString(encoded));
        result.put("sha256", hex(MessageDigest.getInstance("SHA-256").digest(encoded)));
        result.put("decoded", expected);
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
