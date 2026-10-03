package compat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.FastCodesHeader;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.SerializeType;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;

public final class Main {
    private static final int MAX_FRAME_SIZE = 16 * 1024 * 1024;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || "self-test".equals(args[0])) {
            selfTest();
            return;
        }
        if ("fixtures".equals(args[0])) {
            Path output = option(args, "--output");
            generateFixtures(output);
            return;
        }
        if ("header-fixtures".equals(args[0])) {
            Path output = option(args, "--output");
            generateHeaderFixtures(output);
            return;
        }
        if ("decode".equals(args[0])) {
            Path input = option(args, "--input");
            System.out.println(toJson(decode(Files.readAllBytes(input))));
            return;
        }
        if ("verify-rust-fixtures".equals(args[0])) {
            verifyRustFixtures(option(args, "--dir"));
            return;
        }
        if ("scenario".equals(args[0])) {
            String id = option(args, "--id").toString();
            Path output = option(args, "--output");
            writeScenario(id, output);
            return;
        }
        throw new IllegalArgumentException("usage: self-test | fixtures --output DIR | header-fixtures --output DIR | decode --input FILE | scenario --id ID --output FILE");
    }

    private static Path option(String[] args, String name) {
        for (int i = 0; i + 1 < args.length; i++) {
            if (name.equals(args[i])) {
                return Path.of(args[i + 1]);
            }
        }
        throw new IllegalArgumentException("missing option " + name);
    }

    private static void selfTest() throws Exception {
        for (String id : new String[] {"ROUTE-001-get-existing-topic", "HEARTBEAT-001", "PROD-001-sync-send",
            "CONS-002-pull-found", "CONS-003-pull-no-new-msg", "CONS-001-query-offset",
            "CONS-004-update-offset", "ORDER-001-lock-unlock"}) {
            int code = id.startsWith("ROUTE") ? 105 : id.startsWith("HEARTBEAT") ? 34
                : id.startsWith("PROD") ? 10 : id.startsWith("CONS-001") ? 14
                : id.startsWith("CONS-004") ? 15 : id.startsWith("ORDER") ? 41 : 11;
            SerializeType type = id.startsWith("HEARTBEAT") || id.startsWith("CONS-002")
                || id.startsWith("CONS-003") || id.startsWith("CONS-004") ? SerializeType.ROCKETMQ : SerializeType.JSON;
            byte[] frame = bytes(command(code, type, id).encode());
            Map<String, Object> decoded = decode(frame);
            require(((Number) decoded.get("code")).intValue() == code, "fixture code round-trip");
            require(id.equals(decoded.get("remark")), "fixture semantic round-trip");
            require(Arrays.equals(frame, bytes(RemotingCommand.decode(Arrays.copyOfRange(frame, 4, frame.length)).encode())),
                "fixture deterministic re-encode");
        }
        for (SerializeType type : SerializeType.values()) {
            RemotingCommand command = command(34, type, "oracle-self-test");
            byte[] frame = bytes(command.encode());
            Map<String, Object> decoded = decode(frame);
            require(((Number) decoded.get("code")).intValue() == 34, "code round-trip");
            require(type.name().equals(decoded.get("serialization")), "serialization round-trip");
            require("oracle-self-test".equals(decoded.get("remark")), "remark round-trip");
            require(Arrays.equals(frame, bytes(RemotingCommand.decode(Arrays.copyOfRange(frame, 4, frame.length)).encode())), "deterministic re-encode");
        }
        boolean malformedRejected = false;
        try {
            decode(new byte[] {0, 0, 0, 99, 0, 0, 0, 0});
        } catch (IOException expected) {
            malformedRejected = true;
        }
        require(malformedRejected, "malformed frame rejected");
        System.out.println("Java reference self-test passed");
    }

    private static void generateFixtures(Path output) throws Exception {
        Files.createDirectories(output);
        String[] ids = {"ROUTE-001-get-existing-topic", "HEARTBEAT-001", "PROD-001-sync-send",
            "CONS-002-pull-found", "CONS-003-pull-no-new-msg", "CONS-001-query-offset",
            "CONS-004-update-offset", "ORDER-001-lock-unlock", "TX-001-end-transaction",
            "REQREP-001-send-reply", "PRIVATE-LARGE-CODE"};
        int[] codes = {105, 34, 10, 11, 11, 14, 15, 41, 37, 324, 987654};
        for (int i = 0; i < ids.length; i++) {
            writeFixture(output, ids[i], codes[i], i % 2 == 0 ? SerializeType.JSON : SerializeType.ROCKETMQ);
            for (SerializeType type : SerializeType.values()) {
                writeFixture(output, ids[i] + "-" + type.name(), codes[i], type);
            }
        }
        writeFixture(output, "HEADER-PROOF-001", 10, SerializeType.JSON);
        writeFixture(output, "PRIVATE-LARGE-CODE", 987654, SerializeType.ROCKETMQ);
        writeFastHeaderFixtures(output);
        selfTest();
    }

    private static void generateHeaderFixtures(Path output) throws Exception {
        Files.createDirectories(output);
        GetRouteInfoRequestHeader route = new GetRouteInfoRequestHeader();
        route.setTopic("orders");
        writeTypedHeaderFixture(output, "route-request", 105, route);

        SendMessageRequestHeader send = new SendMessageRequestHeader();
        send.setProducerGroup("producers");
        send.setTopic("orders");
        send.setDefaultTopic("TBW102");
        send.setDefaultTopicQueueNums(4);
        send.setQueueId(2);
        send.setSysFlag(1);
        send.setBornTimestamp(1712345678901L);
        send.setFlag(7);
        send.setProperties("TAGS\\u0001blue");
        send.setReconsumeTimes(0);
        send.setUnitMode(false);
        send.setMaxReconsumeTimes(16);
        send.setBatch(false);
        writeTypedHeaderFixture(output, "send-v1", 10, send);
        writeTypedHeaderFixture(output, "send-v2", 310,
            SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(send));

        PullMessageRequestHeader pull = new PullMessageRequestHeader();
        pull.setConsumerGroup("workers");
        pull.setTopic("orders");
        pull.setQueueId(1);
        pull.setQueueOffset(9L);
        pull.setMaxMsgNums(32);
        pull.setSysFlag(0);
        pull.setCommitOffset(8L);
        pull.setSuspendTimeoutMillis(15000L);
        pull.setSubVersion(1712345678901L);
        writeTypedHeaderFixture(output, "pull-request", 11, pull);

        PullMessageResponseHeader pullResponse = new PullMessageResponseHeader();
        pullResponse.setSuggestWhichBrokerId(0L);
        pullResponse.setNextBeginOffset(10L);
        pullResponse.setMinOffset(0L);
        pullResponse.setMaxOffset(100L);
        RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(0, pullResponse);
        writeTypedHeaderFixture(output, "pull-response", response);

        QueryConsumerOffsetRequestHeader queryOffset = new QueryConsumerOffsetRequestHeader();
        queryOffset.setConsumerGroup("workers");
        queryOffset.setTopic("orders");
        queryOffset.setQueueId(1);
        writeTypedHeaderFixture(output, "query-offset-request", 14, queryOffset);
        QueryConsumerOffsetResponseHeader queryResponse = new QueryConsumerOffsetResponseHeader();
        queryResponse.setOffset(9L);
        writeTypedHeaderFixture(output, "query-offset-response",
            RemotingCommand.createResponseCommandWithHeader(0, queryResponse));

        UpdateConsumerOffsetRequestHeader update = new UpdateConsumerOffsetRequestHeader();
        update.setConsumerGroup("workers");
        update.setTopic("orders");
        update.setQueueId(1);
        update.setCommitOffset(9L);
        writeTypedHeaderFixture(output, "update-offset-request", 15, update);

        SendMessageResponseHeader sendResponse = new SendMessageResponseHeader();
        sendResponse.setMsgId("msg-1");
        sendResponse.setQueueId(2);
        sendResponse.setQueueOffset(99L);
        writeTypedHeaderFixture(output, "send-response",
            RemotingCommand.createResponseCommandWithHeader(0, sendResponse));

        GetConsumerListByGroupRequestHeader consumerList = new GetConsumerListByGroupRequestHeader();
        consumerList.setConsumerGroup("workers");
        writeTypedHeaderFixture(output, "consumer-list-request", 38, consumerList);
        System.out.println("Generated typed Java header fixtures in " + output);
    }

    private static void writeTypedHeaderFixture(Path output, String id, int code,
            CommandCustomHeader header) throws Exception {
        RemotingCommand command = RemotingCommand.createRequestCommand(code, header);
        command.setLanguage(LanguageCode.JAVA);
        command.setVersion(0);
        command.setOpaque(101);
        writeTypedHeaderFixture(output, id, command);
    }

    private static void writeTypedHeaderFixture(Path output, String id, RemotingCommand command)
            throws Exception {
        command.setLanguage(LanguageCode.JAVA);
        command.setVersion(0);
        command.setOpaque(101);
        command.setSerializeTypeCurrentRPC(SerializeType.JSON);
        byte[] frame = bytes(command.encode());
        Map<String, Object> decoded = decode(frame);
        String stem = "header-" + id;
        Files.write(output.resolve(stem + ".bin"), frame);
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("java_client_version", "5.5.1");
        metadata.put("scenario_id", "P4-HEADER-" + id.toUpperCase());
        metadata.put("request_code", command.getCode());
        metadata.put("normalized_fields", decoded);
        Files.writeString(output.resolve(stem + ".json"), toJson(metadata) + "\n", StandardCharsets.UTF_8);
    }

    private static void verifyRustFixtures(Path directory) throws Exception {
        if (!Files.isDirectory(directory)) throw new IOException("missing Rust fixture directory: " + directory);
        int count = 0;
        Map<String, Map<String, Object>> decodedByName = new LinkedHashMap<>();
        try (var files = Files.list(directory)) {
            for (Path file : (Iterable<Path>) files.filter(path -> path.toString().endsWith(".bin"))::iterator) {
                decodedByName.put(file.getFileName().toString(), decode(Files.readAllBytes(file)));
                count++;
            }
        }
        if (count == 2) {
            Map<String, Object> json = decodedByName.get("json-route.bin");
            Map<String, Object> rocketmq = decodedByName.get("rocketmq-transaction.bin");
            require(json != null && ((Number) json.get("code")).intValue() == 105, "Rust JSON route fixture decoded");
            require(rocketmq != null && ((Number) rocketmq.get("code")).intValue() == 37, "Rust RocketMQ transaction fixture decoded");
            require(json.get("body_length").equals(4) && rocketmq.get("body_length").equals(4), "Rust fixture bodies decoded");
            require("RUST".equals(json.get("language")) && "RUST".equals(rocketmq.get("language")), "Rust language decoded");
            require("rust-phase-2".equals(((Map<?, ?>) json.get("ext_fields")).get("oracle")), "Rust JSON ext field decoded");
            require("rust-phase-2".equals(((Map<?, ?>) rocketmq.get("ext_fields")).get("oracle")), "Rust private ext field decoded");
        } else {
            require(count >= 6, "Rust typed header fixtures were supplied");
            Map<String, Map<String, Object>> byName = new LinkedHashMap<>();
            try (var files = Files.list(directory)) {
                for (Path file : (Iterable<Path>) files.filter(path -> path.toString().endsWith(".bin"))::iterator) {
                    String name = file.getFileName().toString();
                    byName.put(name, decode(Files.readAllBytes(file)));
                }
            }
            assertExtField(byName, "typed-route.bin", 105, "topic", "orders");
            assertExtField(byName, "typed-send-v1.bin", 10, "producerGroup", "producers");
            assertExtField(byName, "typed-send-v2.bin", 310, "a", "producers");
            assertExtField(byName, "typed-pull.bin", 11, "consumerGroup", "workers");
            assertExtField(byName, "typed-query-offset.bin", 14, "consumerGroup", "workers");
            assertExtField(byName, "typed-update-offset.bin", 15, "commitOffset", "9");
        }
        System.out.println("Java decoded " + count + " Rust-generated fixture(s)");
    }

    private static void assertExtField(Map<String, Map<String, Object>> decoded, String name,
            int code, String field, String expected) {
        Map<String, Object> command = decoded.get(name);
        require(command != null, "Rust typed fixture exists: " + name);
        require(((Number) command.get("code")).intValue() == code, "Rust typed fixture code: " + name);
        require(expected.equals(((Map<?, ?>) command.get("ext_fields")).get(field)),
            "Rust typed fixture field " + field + ": " + name);
    }

    private static void writeFixture(Path output, String id, int code, SerializeType type) throws Exception {
        RemotingCommand command = command(code, type, id);
        writeFixture(output, id, code, type, command, bytes(command.encode()));
    }

    private static void writeFastHeaderFixtures(Path output) throws Exception {
        RemotingCommand fast = command(10, SerializeType.ROCKETMQ, "HEADER-FAST-001");
        fast.setRemark("header-equivalent");
        fast.setExtFields(new java.util.HashMap<>(Map.of("oracle", "5.5.1")));
        fast.writeCustomHeader(new FixtureFastHeader("fixture-topic"));
        ByteBuf buffer = Unpooled.buffer();
        fast.fastEncodeHeader(buffer);
        buffer.writeBytes(fast.getBody() == null ? new byte[0] : fast.getBody());
        byte[] fastFrame = new byte[buffer.readableBytes()];
        buffer.readBytes(fastFrame);
        buffer.release();
        writeFixture(output, "HEADER-FAST-001", 10, SerializeType.ROCKETMQ, fast, fastFrame);

        RemotingCommand ordinary = command(10, SerializeType.ROCKETMQ, "header-equivalent");
        ordinary.addExtField("topic", "fixture-topic");
        writeFixture(output, "HEADER-ORDINARY-001", 10, SerializeType.ROCKETMQ, ordinary, bytes(ordinary.encode()));
    }

    private static void writeFixture(Path output, String id, int code, SerializeType type,
            RemotingCommand command, byte[] frame) throws Exception {
        Map<String, Object> semantic = decode(frame);
        String stem = id.toLowerCase().replaceAll("[^a-z0-9-]", "-");
        Path binary = output.resolve(stem + ".bin");
        Files.write(binary, frame);
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("java_client_version", "5.5.1");
        metadata.put("server_version", "5.3.4");
        metadata.put("scenario_id", id);
        metadata.put("serialization", type.name());
        metadata.put("request_code", code);
        metadata.put("normalized_fields", semantic);
        metadata.put("body_length", command.getBody() == null ? 0 : command.getBody().length);
        metadata.put("payload_sha256", sha256(frame));
        metadata.put("creation_command", "mvn -f compat/java-reference/pom.xml exec:java -Dexec.args=\"fixtures --output <DIR>\"");
        Files.writeString(output.resolve(stem + ".json"), toJson(metadata) + "\n", StandardCharsets.UTF_8);
    }

    private static void writeScenario(String id, Path output) throws Exception {
        int code;
        if (id.startsWith("ROUTE")) code = 105;
        else if (id.startsWith("HEARTBEAT")) code = 34;
        else if (id.startsWith("PROD")) code = 10;
        else if (id.startsWith("CONS")) code = 11;
        else if (id.startsWith("ORDER")) code = 41;
        else throw new IllegalArgumentException("unknown offline scenario: " + id);
        Files.createDirectories(output.toAbsolutePath().getParent());
        byte[] frame = bytes(command(code, SerializeType.JSON, id).encode());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("scenario_id", id);
        result.put("java_client_version", "5.5.1");
        result.put("server_version", "5.3.4");
        result.put("execution_phase", 19);
        result.put("live_status", "PENDING_PHASE_19");
        result.put("normalized_fields", decode(frame));
        Files.writeString(output, toJson(result) + "\n", StandardCharsets.UTF_8);
        System.out.println(toJson(result));
    }

    private static final class FixtureFastHeader implements CommandCustomHeader, FastCodesHeader {
        private final String topic;

        private FixtureFastHeader(String topic) {
            this.topic = topic;
        }

        @Override
        public void checkFields() {
        }

        @Override
        public void encode(ByteBuf out) {
            writeIfNotNull(out, "topic", topic);
        }

        @Override
        public void decode(java.util.HashMap<String, String> fields) {
            // This fixture exercises the Java encoder path only.
        }
    }

    private static RemotingCommand command(int code, SerializeType type, String remark) {
        RemotingCommand command = RemotingCommand.createRequestCommand(code, null);
        command.setLanguage(LanguageCode.RUST);
        command.setVersion(0);
        command.setOpaque(7);
        command.setRemark(remark);
        command.addExtField("oracle", "5.5.1");
        if (remark.startsWith("HEADER-PROOF")) command.addExtField("topic", "fixture-topic");
        command.setSerializeTypeCurrentRPC(type);
        return command;
    }

    private static Map<String, Object> decode(byte[] frame) throws Exception {
        if (frame.length < 8 || frame.length > MAX_FRAME_SIZE) throw new IOException("invalid frame size");
        ByteBuffer input = ByteBuffer.wrap(frame);
        int declared = input.getInt();
        if (declared != frame.length - 4) throw new IOException("frame length mismatch");
        int markedLength = input.getInt();
        int headerLength = RemotingCommand.getHeaderLength(markedLength);
        if (headerLength > input.remaining()) throw new IOException("header length exceeds frame");
        RemotingCommand command = RemotingCommand.decode(Arrays.copyOfRange(frame, 4, frame.length));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", command.getCode());
        result.put("language", command.getLanguage() == null ? null : command.getLanguage().name());
        result.put("version", command.getVersion());
        result.put("opaque", command.getOpaque());
        result.put("flag", command.getFlag());
        result.put("response", command.isResponseType());
        result.put("remark", command.getRemark());
        result.put("ext_fields", command.getExtFields());
        result.put("body_length", command.getBody() == null ? 0 : command.getBody().length);
        result.put("body_sha256", command.getBody() == null ? sha256(new byte[0]) : sha256(command.getBody()));
        result.put("serialization", command.getSerializeTypeCurrentRPC().name());
        result.put("frame_sha256", sha256(frame));
        return result;
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static String sha256(byte[] bytes) throws IOException {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder result = new StringBuilder();
            for (byte value : digest) result.append(String.format("%02x", value));
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 unavailable", e);
        }
    }

    private static String toJson(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return "\"" + escape((String) value) + "\"";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?>) {
            StringBuilder result = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!first) result.append(',');
                first = false;
                result.append(toJson(entry.getKey().toString())).append(':').append(toJson(entry.getValue()));
            }
            return result.append('}').toString();
        }
        if (value instanceof Iterable<?>) {
            StringBuilder result = new StringBuilder("[");
            boolean first = true;
            for (Object item : (Iterable<?>) value) {
                if (!first) result.append(',');
                first = false;
                result.append(toJson(item));
            }
            return result.append(']').toString();
        }
        return toJson(value.toString());
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static void require(boolean condition, String label) {
        if (!condition) throw new IllegalStateException("self-test failed: " + label);
    }
}
