package compat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.rpchook.NamespaceRpcHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.rpchook.StreamTypeRPCHook;
import org.apache.rocketmq.remoting.rpchook.DynamicalExtFieldRPCHook;

/** Invokes actual pinned hooks in MQClientAPIImpl constructor order. No transport. */
public final class RuntimeHookFixtures {
    public static void main(String[] args) throws Exception {
        List<Object> cases = new ArrayList<>();
        String oldZone = System.getProperty("rocketmq.zone");
        String oldMode = System.getProperty("rocketmq.zone.mode");
        try {
            for (String namespace : new String[] {null, "", "v2"}) {
                for (boolean stream : new boolean[] {false, true}) {
                    for (String zone : new String[] {"", " ", "zone-a", "\u00a0", "\u2003", "\u0085"}) {
                        System.setProperty("rocketmq.zone", zone);
                        System.setProperty("rocketmq.zone.mode", zone);
                        ClientConfig config = new ClientConfig(); config.setNamespaceV2(namespace);
                        RemotingCommand command = RemotingCommand.createRequestCommand(34, null);
                        command.addExtField("ns", "seed-ns"); command.addExtField("nsd", "false");
                        command.addExtField("ReqT", "seed-type");
                        command.addExtField("__ZONE_NAME", "seed-zone"); command.addExtField("__ZONE_MODE", "seed-mode");
                        new NamespaceRpcHook(config).doBeforeRequest("peer", command);
                        if (stream) new StreamTypeRPCHook().doBeforeRequest("peer", command);
                        Map<String, Object> item = new TreeMap<>();
                        item.put("namespaceV2", namespace); item.put("stream", stream); item.put("zone", zone);
                        item.put("beforeUser", new TreeMap<>(command.getExtFields()));
                        // Explicit user override/signing boundary between stream and dynamic.
                        command.addExtField("ns", "signed-ns"); command.addExtField("nsd", "signed-flag");
                        command.addExtField("ReqT", "signature-type");
                        command.addExtField("__ZONE_NAME", "user-zone"); command.addExtField("__ZONE_MODE", "user-mode");
                        new DynamicalExtFieldRPCHook().doBeforeRequest("peer", command);
                        item.put("afterDynamic", new TreeMap<>(command.getExtFields()));
                        cases.add(item);
                    }
                }
            }
        } finally {
            if (oldZone == null) System.clearProperty("rocketmq.zone"); else System.setProperty("rocketmq.zone", oldZone);
            if (oldMode == null) System.clearProperty("rocketmq.zone.mode"); else System.setProperty("rocketmq.zone.mode", oldMode);
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "NamespaceRpcHook/StreamTypeRPCHook/DynamicalExtFieldRPCHook.doBeforeRequest; MQClientAPIImpl constructor order");
        fixture.put("serverBaselineContacted", false); fixture.put("cases", cases);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture, com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else { Files.createDirectories(file.getParent()); Files.writeString(file, json); }
        System.out.println("Runtime hook invoked cases verified: " + cases.size());
    }
}
