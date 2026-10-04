package compat;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.OffsetNotFoundException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/** Persisted semantic evidence from real client invocations with only the transport mocked. */
public final class InvokedRpcFixtures {
    private InvokedRpcFixtures() { }

    private static final class Capture {
        String mode;
        RemotingCommand request;
        RemotingCommand response;

        RemotingClient proxy() {
            return (RemotingClient) Proxy.newProxyInstance(RemotingClient.class.getClassLoader(),
                new Class<?>[] {RemotingClient.class}, (proxy, method, args) -> {
                    if (method.getName().startsWith("invoke")) {
                        mode = method.getName();
                        request = (RemotingCommand) args[1];
                        return method.getName().equals("invokeSync") ? response : null;
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        }
    }

    private static void check(boolean ok, String label) {
        if (!ok) throw new IllegalStateException(label);
    }

    private static String quote(String value) {
        if (value == null) return "null";
        StringBuilder out = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            if (c == '"' || c == '\\') out.append('\\');
            if (c < 32) {
                out.append(String.format("\\u%04x", (int) c));
            } else {
                out.append(c);
            }
        }
        return out.append('"').toString();
    }

    private static String fields(Map<String, String> values) {
        StringBuilder out = new StringBuilder("{");
        if (values != null) {
            for (Map.Entry<String, String> entry : new TreeMap<>(values).entrySet()) {
                if (out.length() > 1) out.append(',');
                out.append(quote(entry.getKey())).append(':').append(quote(entry.getValue()));
            }
        }
        return out.append('}').toString();
    }

    private static String command(RemotingCommand cmd) {
        cmd.makeCustomHeaderToNet();
        return "{\"code\":" + cmd.getCode() + ",\"ext\":" + fields(cmd.getExtFields())
            + ",\"bodyBase64\":" + quote(cmd.getBody() == null ? null
                : Base64.getEncoder().encodeToString(cmd.getBody())) + "}";
    }

    private static String fixture(String name, Capture capture, int code, String outcome) {
        check(capture.request != null && capture.request.getCode() == code
            && "invokeSync".equals(capture.mode), name + " dispatch");
        return "{\"provenance\":{\"client\":\"org.apache.rocketmq:rocketmq-client:5.5.1\","
            + "\"entrypoint\":\"MQClientAPIImpl\",\"transport\":\"mock RemotingClient\","
            + "\"serverBaselineContacted\":false},\"case\":" + quote(name)
            + ",\"mode\":\"SYNC\",\"request\":" + command(capture.request)
            + ",\"response\":" + command(capture.response)
            + ",\"outcome\":" + quote(outcome) + "}\n";
    }

    public static void run(Path output, boolean verify) throws Exception {
        Capture capture = new Capture();
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, config,
            null, nettyConfig -> capture.proxy());
        List<String[]> cases = new ArrayList<>();

        TopicRouteData route = new TopicRouteData();
        route.setOrderTopicConf("oracle-order");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(route.encode());
        check("oracle-order".equals(api.getTopicRouteInfoFromNameServer("oracle-topic", 10000)
            .getOrderTopicConf()), "route outcome");
        cases.add(new String[] {"route-success.json", fixture("route-success", capture,
            RequestCode.GET_ROUTEINFO_BY_TOPIC, "orderTopicConf=oracle-order")});

        SendMessageRequestHeader send = new SendMessageRequestHeader();
        send.setProducerGroup("producer");
        send.setTopic("oracle-topic");
        send.setDefaultTopic("TBW102");
        send.setDefaultTopicQueueNums(4);
        send.setQueueId(2);
        send.setSysFlag(0);
        send.setBornTimestamp(1L);
        send.setFlag(0);
        send.setProperties("");
        send.setReconsumeTimes(0);
        SendMessageResponseHeader sent = new SendMessageResponseHeader();
        sent.setMsgId("broker-message");
        sent.setQueueId(2);
        sent.setQueueOffset(17L);
        capture.response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, sent);
        capture.response.makeCustomHeaderToNet();
        SendResult result = api.sendMessage("broker:10911", "broker",
            new Message("oracle-topic", "payload".getBytes(StandardCharsets.UTF_8)), send, 10000,
            CommunicationMode.SYNC, null, null);
        check(result.getQueueOffset() == 17L && "broker-message".equals(result.getOffsetMsgId()),
            "send outcome");
        cases.add(new String[] {"send-success.json", fixture("send-success", capture,
            RequestCode.SEND_MESSAGE_V2, "SEND_OK;queueOffset=17;offsetMsgId=broker-message")});

        QueryConsumerOffsetRequestHeader offset = new QueryConsumerOffsetRequestHeader();
        offset.setConsumerGroup("consumer");
        offset.setTopic("oracle-topic");
        offset.setQueueId(2);
        QueryConsumerOffsetResponseHeader offsetReply = new QueryConsumerOffsetResponseHeader();
        offsetReply.setOffset(42L);
        capture.response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, offsetReply);
        capture.response.makeCustomHeaderToNet();
        check(api.queryConsumerOffset("broker:10911", offset, 10000) == 42L, "offset outcome");
        cases.add(new String[] {"offset-success.json", fixture("offset-success", capture,
            RequestCode.QUERY_CONSUMER_OFFSET, "offset=42")});

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.QUERY_NOT_FOUND, "offline");
        try {
            api.queryConsumerOffset("broker:10911", offset, 10000);
            throw new IllegalStateException("missing offset accepted");
        } catch (OffsetNotFoundException expected) {
            check(expected.getResponseCode() == ResponseCode.QUERY_NOT_FOUND, "missing offset outcome");
        }
        cases.add(new String[] {"offset-not-found.json", fixture("offset-not-found", capture,
            RequestCode.QUERY_CONSUMER_OFFSET, "OffsetNotFoundException;code=22")});

        for (String[] item : cases) {
            Path file = output.resolve(item[0]);
            if (verify) {
                check(Files.exists(file) && item[1].equals(Files.readString(file)), "fixture drift: " + file);
            } else {
                Files.createDirectories(output);
                Files.writeString(file, item[1], StandardCharsets.UTF_8);
            }
        }
    }
}