package compat;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;

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

        Message message = new Message("oracle-topic", "payload".getBytes(StandardCharsets.UTF_8));
        SendMessageResponseHeader sendReply = new SendMessageResponseHeader();
        sendReply.setMsgId("broker-message");
        sendReply.setQueueId(2);
        sendReply.setQueueOffset(17L);
        for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.FLUSH_DISK_TIMEOUT,
            ResponseCode.FLUSH_SLAVE_TIMEOUT, ResponseCode.SLAVE_NOT_AVAILABLE}) {
            capture.response = RemotingCommand.createResponseCommandWithHeader(code, sendReply);
            capture.response.makeCustomHeaderToNet();
            SendResult result = api.sendMessage("broker:10911", "broker", message, send, 10000,
                CommunicationMode.SYNC, null, null);
            check("invokeSync".equals(capture.mode) && "broker:10911".equals(capture.address), "sync send transport");
            check(capture.request.getCode() == RequestCode.SEND_MESSAGE_V2, "sync send code");
            check(result.getSendStatus() == (code == ResponseCode.SUCCESS ? SendStatus.SEND_OK
                : code == ResponseCode.FLUSH_DISK_TIMEOUT ? SendStatus.FLUSH_DISK_TIMEOUT
                : code == ResponseCode.FLUSH_SLAVE_TIMEOUT ? SendStatus.FLUSH_SLAVE_TIMEOUT
                : SendStatus.SLAVE_NOT_AVAILABLE), "sync send status " + code);
            check("broker-message".equals(result.getOffsetMsgId()) && result.getQueueOffset() == 17L,
                "sync send result");
        }
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.sendMessage("broker:10911", "broker", message, send, 10000, CommunicationMode.SYNC, null, null);
            throw new IllegalStateException("send error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "send error code");
        }
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.SEND_MESSAGE_V2,
            "send error transport");

        check(api.sendMessage("broker:10911", "broker", message, send, 10000,
            CommunicationMode.ASYNC, null, null) == null, "async send dispatch result");
        check("invokeAsync".equals(capture.mode) && "broker:10911".equals(capture.address),
            "async send transport");
        check(capture.request.getCode() == RequestCode.SEND_MESSAGE_V2, "async send code");

        PullMessageRequestHeader pull = new PullMessageRequestHeader();
        pull.setConsumerGroup("consumer");
        pull.setTopic("oracle-topic");
        pull.setQueueId(2);
        pull.setQueueOffset(10L);
        pull.setMaxMsgNums(1);
        pull.setSysFlag(0);
        pull.setCommitOffset(0L);
        pull.setSuspendTimeoutMillis(0L);
        pull.setSubVersion(1L);
        PullMessageResponseHeader pullReply = new PullMessageResponseHeader();
        pullReply.setNextBeginOffset(11L);
        pullReply.setMinOffset(0L);
        pullReply.setMaxOffset(20L);
        pullReply.setSuggestWhichBrokerId(0L);
        for (int code : new int[] {ResponseCode.SUCCESS, ResponseCode.PULL_NOT_FOUND,
            ResponseCode.PULL_RETRY_IMMEDIATELY, ResponseCode.PULL_OFFSET_MOVED}) {
            capture.response = RemotingCommand.createResponseCommandWithHeader(code, pullReply);
            capture.response.makeCustomHeaderToNet();
            if (code == ResponseCode.SUCCESS) capture.response.setBody("raw".getBytes(StandardCharsets.UTF_8));
            PullResult result = api.pullMessage("broker:10911", pull, 10000, CommunicationMode.SYNC, null);
            check("invokeSync".equals(capture.mode) && "broker:10911".equals(capture.address), "pull transport");
            check(capture.request.getCode() == RequestCode.PULL_MESSAGE, "pull code");
            check(result.getPullStatus() == (code == ResponseCode.SUCCESS ? PullStatus.FOUND
                : code == ResponseCode.PULL_NOT_FOUND ? PullStatus.NO_NEW_MSG
                : code == ResponseCode.PULL_RETRY_IMMEDIATELY ? PullStatus.NO_MATCHED_MSG
                : PullStatus.OFFSET_ILLEGAL), "pull status " + code);
            check(result.getNextBeginOffset() == 11L, "pull next offset");
        }
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.pullMessage("broker:10911", pull, 10000, CommunicationMode.SYNC, null);
            throw new IllegalStateException("pull error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "pull error code");
        }
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.PULL_MESSAGE,
            "pull error transport");

        check(api.pullMessage("broker:10911", pull, 10000, CommunicationMode.ASYNC, null) == null,
            "async pull dispatch result");
        check("invokeAsync".equals(capture.mode) && "broker:10911".equals(capture.address),
            "async pull transport");
        check(capture.request.getCode() == RequestCode.PULL_MESSAGE, "async pull code");

        UpdateConsumerOffsetRequestHeader update = new UpdateConsumerOffsetRequestHeader();
        update.setConsumerGroup("consumer");
        update.setTopic("oracle-topic");
        update.setQueueId(2);
        update.setCommitOffset(42L);
        api.updateConsumerOffsetOneway("broker:10911", update, 10000);
        capture.request.makeCustomHeaderToNet();
        check("invokeOneway".equals(capture.mode) && "broker:10911".equals(capture.address), "update transport");
        check(capture.request.getCode() == RequestCode.UPDATE_CONSUMER_OFFSET, "update code");
        check("42".equals(capture.request.getExtFields().get("commitOffset")), "update offset");

        HeartbeatData heartbeat = new HeartbeatData();
        heartbeat.setClientID("client");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setVersion(551);
        check(api.sendHeartbeat("broker:10911", heartbeat, 10000) == 551, "heartbeat version");
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.HEART_BEAT,
            "heartbeat transport");
        check("client".equals(HeartbeatData.decode(capture.request.getBody(), HeartbeatData.class).getClientID()),
            "heartbeat body");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.sendHeartbeat("broker:10911", heartbeat, 10000);
            throw new IllegalStateException("heartbeat error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "heartbeat error code");
        }
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.HEART_BEAT,
            "heartbeat error transport");

        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        api.unregisterClient("broker:10911", "client", "producer", "consumer", 10000);
        capture.request.makeCustomHeaderToNet();
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.UNREGISTER_CLIENT,
            "unregister transport");
        check("client".equals(capture.request.getExtFields().get("clientID")), "unregister client");
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "offline");
        try {
            api.unregisterClient("broker:10911", "client", "producer", "consumer", 10000);
            throw new IllegalStateException("unregister error accepted");
        } catch (MQBrokerException expected) {
            check(expected.getResponseCode() == ResponseCode.SYSTEM_ERROR, "unregister error code");
        }
        check("invokeSync".equals(capture.mode) && capture.request.getCode() == RequestCode.UNREGISTER_CLIENT,
            "unregister error transport");

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
        System.out.println("Invoked Java 5.5.1 MQClientAPIImpl: send ONEWAY/SYNC/ASYNC dispatch, pull SYNC/ASYNC dispatch, offset SYNC/ONEWAY, heartbeat and unregister SYNC, route SYNC error (mock transport)");
    }
}