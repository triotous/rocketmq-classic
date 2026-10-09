package compat;

import com.alibaba.fastjson2.JSON;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;

/** Fresh-process oracle for ClientConfig fields initialized from system properties. */
public final class ClientConfigPropertiesProbe {
    private static final Map<String, String> VALUES = Map.of(
        "rocketmq.client.name", "fixture-client",
        "com.rocketmq.read.body", "TrUe",
        "com.rocketmq.decompress.body", " true ",
        "com.rocketmq.sendMessageWithVIPChannel", "TRUE",
        "com.rocketmq.heartbeat.v2", "true",
        "com.rocketmq.socks.proxy.config", "",
        "com.rocketmq.sendLatencyEnable", "true",
        "com.rocketmq.startDetectorEnable", " true "
    );

    private ClientConfigPropertiesProbe() { }

    public static void main(String[] args) throws Exception {
        for (String key : VALUES.keySet()) {
            System.clearProperty(key);
        }
        ClientConfig defaults = new ClientConfig();
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("systemProperties", new TreeMap<>(VALUES));
        fixture.put("defaults", snapshot(defaults));
        for (Map.Entry<String, String> entry : VALUES.entrySet()) {
            System.setProperty(entry.getKey(), entry.getValue());
        }
        fixture.put("configured", snapshot(new ClientConfig()));

        Path file = Path.of(args[0]);
        String json = JSON.toJSONString(fixture) + "\n";
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) {
                throw new IllegalStateException("fixture drift: " + file);
            }
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("ClientConfig system-property inputs verified");
    }

    private static Map<String, Object> snapshot(ClientConfig config) {
        Map<String, Object> result = new TreeMap<>();
        result.put("instanceName", config.getInstanceName());
        result.put("decodeReadBody", config.isDecodeReadBody());
        result.put("decodeDecompressBody", config.isDecodeDecompressBody());
        result.put("vipChannelEnabled", config.isVipChannelEnabled());
        result.put("useHeartbeatV2", config.isUseHeartbeatV2());
        result.put("socksProxyConfig", config.getSocksProxyConfig());
        result.put("sendLatencyEnable", config.isSendLatencyEnable());
        result.put("startDetectorEnable", config.isStartDetectorEnable());
        return result;
    }
}
