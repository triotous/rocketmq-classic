package compat;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;

/** Captures invoked consumer requests and observed Java results without a broker. */
public final class ConsumerMoreInvokedFixtures {
    private ConsumerMoreInvokedFixtures() { }

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

    private static void check(boolean condition, String label) {
        if (!condition) throw new IllegalStateException(label);
    }

    private static Map<String, Object> command(RemotingCommand value) {
        value.makeCustomHeaderToNet();
        Map<String, Object> result = new TreeMap<>();
        result.put("code", value.getCode());
        result.put("ext", value.getExtFields() == null ? Collections.emptyMap()
            : new TreeMap<>(value.getExtFields()));
        result.put("bodyBase64", value.getBody() == null ? null
            : Base64.getEncoder().encodeToString(value.getBody()));
        result.put("remark", value.getRemark());
        return result;
    }

    private static void record(Path output, boolean verify, Capture capture, String name,
        int code, String mode, String outcome) throws Exception {
        check(("invoke" + mode).equals(capture.mode) && capture.request != null
            && capture.request.getCode() == code, name + " dispatch");
        Map<String, Object> fixture = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("entrypoint", "MQClientAPIImpl");
        provenance.put("transport", "mock RemotingClient");
        provenance.put("serverBaselineContacted", false);
        fixture.put("provenance", provenance);
        fixture.put("case", name);
        fixture.put("mode", mode.toUpperCase());
        fixture.put("request", command(capture.request));
        fixture.put("response", command(capture.response));
        fixture.put("outcome", outcome);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
        Path file = output.resolve(name + ".json");
        if (verify) check(Files.exists(file) && json.equals(Files.readString(file)), "fixture drift: " + file);
        else {
            Files.createDirectories(output);
            Files.writeString(file, json);
        }
    }

    public static void main(String[] args) throws Exception {
        check(args.length >= 1, "expected output directory");
        Path output = Path.of(args[0]);
        boolean verify = Arrays.asList(args).contains("--verify");
        Capture capture = new Capture();
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIImpl api = new MQClientAPIImpl(new NettyClientConfig(), null, null, config,
            null, nettyConfig -> capture.proxy());

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
            String name = code == ResponseCode.SUCCESS ? "pull-found" : code == ResponseCode.PULL_NOT_FOUND
                ? "pull-no-new" : code == ResponseCode.PULL_RETRY_IMMEDIATELY ? "pull-no-matched"
                : "pull-offset-illegal";
            capture.response = RemotingCommand.createResponseCommandWithHeader(code, pullReply);
            capture.response.makeCustomHeaderToNet();
            if (code == ResponseCode.SUCCESS) capture.response.setBody(new byte[] {0, (byte) 255, 42});
            PullResult result = api.pullMessage("broker:10911", pull, 10000, CommunicationMode.SYNC, null);
            check(result.getNextBeginOffset() == 11L && result.getMinOffset() == 0L
                && result.getMaxOffset() == 20L, name + " offsets");
            String status = code == ResponseCode.SUCCESS ? "Found" : code == ResponseCode.PULL_NOT_FOUND
                ? "NoNewMessage" : code == ResponseCode.PULL_RETRY_IMMEDIATELY ? "NoMatchedMessage"
                : "OffsetIllegal";
            check(result.getPullStatus().name().equals(code == ResponseCode.SUCCESS ? "FOUND"
                : code == ResponseCode.PULL_NOT_FOUND ? "NO_NEW_MSG"
                : code == ResponseCode.PULL_RETRY_IMMEDIATELY ? "NO_MATCHED_MSG" : "OFFSET_ILLEGAL"), name);
            record(output, verify, capture, name, RequestCode.PULL_MESSAGE, "Sync", "status=" + status
                + ";next=11;min=0;max=20;broker=0;raw=" + (code == ResponseCode.SUCCESS ? "AP8q" : "null"));
        }
        for (int code : new int[] {ResponseCode.SYSTEM_ERROR, 987654}) {
            capture.response = RemotingCommand.createResponseCommand(code, "offline");
            try {
                api.pullMessage("broker:10911", pull, 10000, CommunicationMode.SYNC, null);
                throw new IllegalStateException("pull error accepted");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "pull exception code");
            }
            record(output, verify, capture, code == ResponseCode.SYSTEM_ERROR ? "pull-error" : "pull-unknown",
                RequestCode.PULL_MESSAGE, "Sync", "MQBrokerException;code=" + code);
        }

        for (String shape : new String[] {"no-header", "invalid-header"}) {
            capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "offline");
            if (shape.equals("invalid-header")) {
                capture.response.addExtField("nextBeginOffset", "bad");
                capture.response.addExtField("minOffset", "0");
                capture.response.addExtField("maxOffset", "20");
                capture.response.addExtField("suggestWhichBrokerId", "0");
            }
            try {
                api.pullMessage("broker:10911", pull, 10000, CommunicationMode.SYNC, null);
                throw new IllegalStateException("malformed pull accepted");
            } catch (NullPointerException | NumberFormatException expected) {
                record(output, verify, capture, "pull-" + shape, RequestCode.PULL_MESSAGE,
                    "Sync", "malformed");
            }
        }

        MessageQueue queue = new MessageQueue("oracle-topic", "broker", 2);
        LockBatchRequestBody lock = new LockBatchRequestBody();
        lock.setConsumerGroup("consumer");
        lock.setClientId("client");
        lock.setMqSet(Collections.singleton(queue));
        LockBatchResponseBody locked = new LockBatchResponseBody();
        locked.setLockOKMQSet(Collections.singleton(queue));
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        capture.response.setBody(locked.encode());
        check(api.lockBatchMQ("broker:10911", lock, 10000).equals(Set.of(queue)), "lock success");
        record(output, verify, capture, "lock-success", RequestCode.LOCK_BATCH_MQ, "Sync", "queues=oracle-topic:broker:2");
        for (int code : new int[] {ResponseCode.SYSTEM_ERROR, 987654}) {
            capture.response = RemotingCommand.createResponseCommand(code, "offline");
            try {
                api.lockBatchMQ("broker:10911", lock, 10000);
                throw new IllegalStateException("lock error accepted");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "lock exception code");
            }
            record(output, verify, capture, code == ResponseCode.SYSTEM_ERROR ? "lock-error" : "lock-unknown",
                RequestCode.LOCK_BATCH_MQ, "Sync", "MQBrokerException;code=" + code);
        }

        for (String shape : new String[] {"no-body", "invalid-json"}) {
            capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "offline");
            if (shape.equals("invalid-json")) capture.response.setBody(new byte[] {'{'});
            try {
                api.lockBatchMQ("broker:10911", lock, 10000);
                throw new IllegalStateException("malformed lock accepted");
            } catch (NullPointerException | com.alibaba.fastjson2.JSONException expected) {
                record(output, verify, capture, "lock-" + shape, RequestCode.LOCK_BATCH_MQ,
                    "Sync", "malformed");
            }
        }

        UnlockBatchRequestBody unlock = new UnlockBatchRequestBody();
        unlock.setConsumerGroup("consumer");
        unlock.setClientId("client");
        unlock.setMqSet(Collections.singleton(queue));
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        api.unlockBatchMQ("broker:10911", unlock, 10000, false);
        record(output, verify, capture, "unlock-sync-success", RequestCode.UNLOCK_BATCH_MQ, "Sync", "ok");
        for (int code : new int[] {ResponseCode.SYSTEM_ERROR, 987654}) {
            capture.response = RemotingCommand.createResponseCommand(code, "offline");
            try {
                api.unlockBatchMQ("broker:10911", unlock, 10000, false);
                throw new IllegalStateException("unlock error accepted");
            } catch (MQBrokerException expected) {
                check(expected.getResponseCode() == code, "unlock exception code");
            }
            record(output, verify, capture, code == ResponseCode.SYSTEM_ERROR ? "unlock-sync-error"
                : "unlock-sync-unknown", RequestCode.UNLOCK_BATCH_MQ, "Sync", "MQBrokerException;code=" + code);
        }
        capture.response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "ignored by oneway");
        api.unlockBatchMQ("broker:10911", unlock, 10000, true);
        record(output, verify, capture, "unlock-oneway", RequestCode.UNLOCK_BATCH_MQ, "Oneway", "ok");
    }
}
