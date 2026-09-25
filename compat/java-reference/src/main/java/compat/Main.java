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
        throw new IllegalArgumentException("usage: self-test | fixtures --output DIR | decode --input FILE | scenario --id ID --output FILE");
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
        Map<String, Object> json = decodedByName.get("json-route.bin");
        Map<String, Object> rocketmq = decodedByName.get("rocketmq-transaction.bin");
        require(json != null && ((Number) json.get("code")).intValue() == 105, "Rust JSON route fixture decoded");
        require(rocketmq != null && ((Number) rocketmq.get("code")).intValue() == 37, "Rust RocketMQ transaction fixture decoded");
        require(json.get("body_length").equals(4) && rocketmq.get("body_length").equals(4), "Rust fixture bodies decoded");
        require("RUST".equals(json.get("language")) && "RUST".equals(rocketmq.get("language")), "Rust language decoded");
        require("rust-phase-2".equals(((Map<?, ?>) json.get("ext_fields")).get("oracle")), "Rust JSON ext field decoded");
        require("rust-phase-2".equals(((Map<?, ?>) rocketmq.get("ext_fields")).get("oracle")), "Rust private ext field decoded");
        require(count == 2, "exactly two Rust-generated fixtures decoded");
        System.out.println("Java decoded " + count + " Rust-generated fixture(s)");
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
