package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.HeartbeatV2Result;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/** Persisted invocation evidence: only RemotingClient is substituted. */
public final class ExtensionMoreInvokedFixtures {
    private ExtensionMoreInvokedFixtures() { }

    private static final class Capture {
        RemotingCommand request;
        RemotingCommand response;
        String mode;
        RemotingClient proxy() {
            return (RemotingClient) Proxy.newProxyInstance(RemotingClient.class.getClassLoader(),
                new Class<?>[] {RemotingClient.class}, (proxy, method, args) -> {
                    if (method.getName().startsWith("invoke")) {
                        mode = method.getName();
                        request = (RemotingCommand) args[1];
                        return "invokeSync".equals(mode) ? response : null;
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        }
    }

    private static void check(boolean ok, String label) {
        if (!ok) throw new IllegalStateException(label);
    }

    private static Map<String, Object> command(RemotingCommand cmd) {
        cmd.makeCustomHeaderToNet();
        Map<String, Object> result = new TreeMap<>();
        result.put("code", cmd.getCode());
        result.put("ext", cmd.getExtFields() == null ? Collections.emptyMap() : new TreeMap<>(cmd.getExtFields()));
        result.put("bodyBase64", cmd.getBody() == null ? null : Base64.getEncoder().encodeToString(cmd.getBody()));
        result.put("version", cmd.getVersion());
        return result;
    }

    private static void record(Path dir, boolean verify, Capture c, String name, int code, String outcome)
        throws Exception {
        check("invokeSync".equals(c.mode) && c.request != null && c.request.getCode() == code,
            name + " dispatch");
        Map<String, Object> fixture = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", "MQClientAPIImpl");
        provenance.put("transport", "mock RemotingClient");
        provenance.put("serverBaselineContacted", false);
        fixture.put("provenance", provenance);
        fixture.put("case", name);
        fixture.put("mode", "SYNC");
        int responseVersion = c.response.getVersion();
        fixture.put("request", command(c.request));
        Map<String, Object> response = command(c.response);
        response.put("version", responseVersion);
        fixture.put("response", response);
        fixture.put("outcome", outcome);
        Path file = dir.resolve(name + ".json");
        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) check(Files.exists(file) && json.equals(Files.readString(file)), "fixture drift: " + file);
        else {
            Files.createDirectories(dir);
            Files.writeString(file, json);
        }
    }

    public static void main(String[] args) throws Exception {
        check(args.length > 0, "expected output directory");
        Path dir = Path.of(args[0]);
        boolean verify = Arrays.asList(args).contains("--verify");
        Capture c = new Capture();
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, config,
            null, nettyConfig -> c.proxy());
        HeartbeatData heartbeat = new HeartbeatData();
        heartbeat.setClientID("client");
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        c.response.setVersion(551);
        check(api.sendHeartbeat("broker:10911", heartbeat, 10000) == 551, "heartbeat version");
        record(dir, verify, c, "heartbeat-success", RequestCode.HEART_BEAT, "version=551");
        for (int code : new int[] {1, 987654}) {
            c.response = RemotingCommand.createResponseCommand(code, "offline");
            try {
                api.sendHeartbeat("broker:10911", heartbeat, 10000);
                throw new IllegalStateException("heartbeat accepted error");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "heartbeat error code");
            }
            record(dir, verify, c, code == 1 ? "heartbeat-error" : "heartbeat-unknown",
                RequestCode.HEART_BEAT, "MQBrokerException;code=" + code);
        }
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        c.response.setVersion(551);
        c.response.addExtField(MixAll.IS_SUB_CHANGE, "true");
        c.response.addExtField(MixAll.IS_SUPPORT_HEART_BEAT_V2, "true");
        HeartbeatV2Result v2 = api.sendHeartbeatV2("broker:10911", heartbeat, 10000);
        check(v2.getVersion() == 551 && v2.isSubChange() && v2.isSupportV2(), "v2 fields");
        record(dir, verify, c, "heartbeat-v2-success", RequestCode.HEART_BEAT, "version=551;sub=true;support=true");
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        v2 = api.sendHeartbeatV2("broker:10911", heartbeat, 10000);
        check(!v2.isSubChange() && !v2.isSupportV2(), "v2 defaults");
        record(dir, verify, c, "heartbeat-v2-default", RequestCode.HEART_BEAT, "version=0;sub=false;support=false");
        for (int code : new int[] {1, 987654}) {
            c.response = RemotingCommand.createResponseCommand(code, "offline");
            try {
                api.sendHeartbeatV2("broker:10911", heartbeat, 10000);
                throw new IllegalStateException("v2 accepted error");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "v2 error code");
            }
            record(dir, verify, c, code == 1 ? "heartbeat-v2-error" : "heartbeat-v2-unknown",
                RequestCode.HEART_BEAT, "MQBrokerException;code=" + code);
        }
        for (int code : new int[] {0, 1, 987654}) {
            c.response = RemotingCommand.createResponseCommand(code, "offline");
            if (code == 0) api.unregisterClient("broker:10911", "client", "producer", "consumer", 10000);
            else try {
                api.unregisterClient("broker:10911", "client", "producer", "consumer", 10000);
                throw new IllegalStateException("unregister accepted error");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "unregister error code");
            }
            record(dir, verify, c, code == 0 ? "unregister-success" : code == 1 ? "unregister-error" : "unregister-unknown",
                RequestCode.UNREGISTER_CLIENT, code == 0 ? "ok" : "MQBrokerException;code=" + code);
        }
        QueryAssignmentResponseBody assignments = new QueryAssignmentResponseBody();
        assignments.setMessageQueueAssignments(Collections.emptySet());
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        c.response.setBody(assignments.encode());
        check(api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
            MessageModel.CLUSTERING, 10000).isEmpty(), "assignment empty");
        record(dir, verify, c, "assignment-empty", RequestCode.QUERY_ASSIGNMENT, "assignments=0");
        MessageQueueAssignment assigned = new MessageQueueAssignment();
        assigned.setMessageQueue(new MessageQueue("oracle-topic", "broker", 2));
        assigned.setMode(MessageRequestMode.POP);
        assigned.setAttachments(Collections.singletonMap("source", "oracle"));
        assignments.setMessageQueueAssignments(Collections.singleton(assigned));
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        c.response.setBody(assignments.encode());
        check(api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
            MessageModel.CLUSTERING, 10000).contains(assigned), "assignment populated");
        record(dir, verify, c, "assignment-populated", RequestCode.QUERY_ASSIGNMENT,
            "assignments=1;topic=oracle-topic;broker=broker;queue=2;mode=POP;source=oracle");
        for (int code : new int[] {1, 987654}) {
            c.response = RemotingCommand.createResponseCommand(code, "offline");
            try {
                api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
                    MessageModel.CLUSTERING, 10000);
                throw new IllegalStateException("assignment accepted error");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "assignment error code");
            }
            record(dir, verify, c, code == 1 ? "assignment-error" : "assignment-unknown",
                RequestCode.QUERY_ASSIGNMENT, "MQBrokerException;code=" + code);
        }
        c.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "no body");
        try {
            api.queryAssignment("broker:10911", "oracle-topic", "consumer", "client", "AVG",
                MessageModel.CLUSTERING, 10000);
            throw new IllegalStateException("assignment accepted absent body");
        } catch (NullPointerException expected) {
            record(dir, verify, c, "assignment-no-body", RequestCode.QUERY_ASSIGNMENT,
                "NullPointerException;malformed");
        }
        for (int code : new int[] {0, 1, 987654}) {
            c.response = RemotingCommand.createResponseCommand(code, "offline");
            if (code == 0) api.setMessageRequestMode("broker:10911", "oracle-topic", "consumer",
                MessageRequestMode.POP, 2, 10000);
            else try {
                api.setMessageRequestMode("broker:10911", "oracle-topic", "consumer",
                    MessageRequestMode.POP, 2, 10000);
                throw new IllegalStateException("request mode accepted error");
            } catch (MQClientException expected) {
                check(expected.getResponseCode() == code, "request mode error code");
            }
            record(dir, verify, c, code == 0 ? "request-mode-success" : code == 1 ? "request-mode-error" : "request-mode-unknown",
                RequestCode.SET_MESSAGE_REQUEST_MODE, code == 0 ? "ok" : "MQClientException;code=" + code);
        }
    }
}
