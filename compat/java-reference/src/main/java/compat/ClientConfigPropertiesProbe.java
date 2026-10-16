package compat;

import com.alibaba.fastjson2.JSON;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;

/** Fresh-process oracle for ClientConfig fields initialized from system properties. */
public final class ClientConfigPropertiesProbe {
    private static final Map<String, String> VALUES = Map.ofEntries(
        Map.entry("rocketmq.client.name", "fixture-client"),
        Map.entry("com.rocketmq.read.body", "TrUe"),
        Map.entry("com.rocketmq.decompress.body", " true "),
        Map.entry("com.rocketmq.sendMessageWithVIPChannel", "TRUE"),
        Map.entry("com.rocketmq.heartbeat.v2", "true"),
        Map.entry("com.rocketmq.socks.proxy.config", ""),
        Map.entry("com.rocketmq.sendLatencyEnable", "true"),
        Map.entry("com.rocketmq.startDetectorEnable", " true "),
        Map.entry("com.rocketmq.remoting.clientAsyncSemaphoreValue", "-17"),
        Map.entry("com.rocketmq.remoting.clientOnewaySemaphoreValue", "2147483647"),
        Map.entry("com.rocketmq.remoting.client.connect.timeout", "-13"),
        Map.entry("com.rocketmq.remoting.client.channel.maxIdleTimeSeconds", "-7"),
        Map.entry("com.rocketmq.remoting.client.closeSocketIfTimeout", "false")
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
        fixture.put("remotingSemaphoreValues", remotingSemaphoreValues());
        fixture.put("remotingTimeoutSettings", remotingTimeoutSettings());

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

    private static Map<String, Object> remotingSemaphoreValues() {
        org.apache.rocketmq.remoting.netty.NettyClientConfig config =
            new org.apache.rocketmq.remoting.netty.NettyClientConfig();
        Map<String, Object> result = new TreeMap<>();
        result.put("async", config.getClientAsyncSemaphoreValue());
        result.put("oneway", config.getClientOnewaySemaphoreValue());
        return result;
    }

    private static Map<String, Object> remotingTimeoutSettings() {
        org.apache.rocketmq.remoting.netty.NettyClientConfig config =
            new org.apache.rocketmq.remoting.netty.NettyClientConfig();
        Map<String, Object> result = new TreeMap<>();
        result.put("connectTimeoutMillis", config.getConnectTimeoutMillis());
        result.put("clientChannelMaxIdleTimeSeconds", config.getClientChannelMaxIdleTimeSeconds());
        result.put("closeSocketIfTimeout", config.isClientCloseSocketIfTimeout());
        return result;
    }
}
