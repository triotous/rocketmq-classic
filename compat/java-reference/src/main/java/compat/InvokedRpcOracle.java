package compat;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;

/** Offline invocation checks: only the transport is substituted, not MQClientAPIImpl. */
public final class InvokedRpcOracle {
    private InvokedRpcOracle() { }

    private static final class Capture {
        String mode;
        String address;
        RemotingCommand request;
        RemotingCommand response;

        RemotingClient proxy() {
            return (RemotingClient) Proxy.newProxyInstance(RemotingClient.class.getClassLoader(),
                new Class<?>[] {RemotingClient.class}, (proxy, method, args) -> {
                    if (method.getName().startsWith("invoke")) {
                        mode = method.getName();
                        address = (String) args[0];
                        request = (RemotingCommand) args[1];
                        return method.getName().equals("invokeSync") ? response : null;
                    }
                    if (method.getReturnType() == boolean.class) return false;
                    return null;
                });
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    public static void run() throws Exception {
        Capture capture = new Capture();
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, config,
            null, nettyConfig -> capture.proxy());

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
        api.sendMessage("broker:10911", "broker", new Message("oracle-topic", "payload".getBytes(StandardCharsets.UTF_8)),
            send, 10000, CommunicationMode.ONEWAY, null, null);
        capture.request.makeCustomHeaderToNet();
        check("invokeOneway".equals(capture.mode), "send mode");
        check("broker:10911".equals(capture.address), "send address");
        check(capture.request.getCode() == RequestCode.SEND_MESSAGE_V2, "send code");
        check("oracle-topic".equals(capture.request.getExtFields().get("b")), "send V2 topic");
        check(new String(capture.request.getBody(), StandardCharsets.UTF_8).equals("payload"), "send body");

        QueryConsumerOffsetRequestHeader offset = new QueryConsumerOffsetRequestHeader();
        offset.setConsumerGroup("consumer");
        offset.setTopic("oracle-topic");
        offset.setQueueId(2);
        QueryConsumerOffsetResponseHeader offsetReply = new QueryConsumerOffsetResponseHeader();
        offsetReply.setOffset(42L);
        capture.response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, offsetReply);
        capture.response.makeCustomHeaderToNet();
        check(api.queryConsumerOffset("broker:10911", offset, 10000) == 42L, "offset result");
        capture.request.makeCustomHeaderToNet();
        check("invokeSync".equals(capture.mode), "offset mode");
        check(capture.request.getCode() == RequestCode.QUERY_CONSUMER_OFFSET, "offset code");
        check("consumer".equals(capture.request.getExtFields().get("consumerGroup")), "offset group");

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST, "offline");
        try {
            api.getTopicRouteInfoFromNameServer("oracle-topic", 10000);
            throw new IllegalStateException("route error response accepted");
        } catch (org.apache.rocketmq.client.exception.MQClientException expected) {
            check(expected.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST, "route error code");
        }
        capture.request.makeCustomHeaderToNet();
        check("invokeSync".equals(capture.mode) && capture.address == null, "route transport");
        check(capture.request.getCode() == RequestCode.GET_ROUTEINFO_BY_TOPIC, "route code");
        check("oracle-topic".equals(capture.request.getExtFields().get("topic")), "route topic");
        System.out.println("Invoked Java 5.5.1 MQClientAPIImpl: send ONEWAY, offset SYNC success, route SYNC error (mock transport)");
    }
}