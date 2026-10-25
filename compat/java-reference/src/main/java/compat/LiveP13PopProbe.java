package compat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;

/** Exercises live POP receipt lifecycle behavior against the pinned Broker. */
public final class LiveP13PopProbe {
    private static final String BROKER_NAME = "broker-a";

    private LiveP13PopProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4 || !("seed".equals(args[0]) || "consume".equals(args[0]))) {
            throw new IllegalArgumentException("usage: LiveP13PopProbe seed|consume <broker-address> <topic> <language>");
        }
        if ("seed".equals(args[0])) {
            seed(args[1], args[2], args[3]);
        } else {
            consume(args[1], args[2], args[3]);
        }
    }

    private static void seed(String brokerAddress, String topic, String language) throws Exception {
        MQClientAPIImpl api = producerApi();
        api.start();
        try {
            for (int queueId = 0; queueId < 2; queueId++) {
                String body = "p13-" + language + "-q" + queueId;
                Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
                SendMessageRequestHeader header = new SendMessageRequestHeader();
                header.setProducerGroup("compat_p13_" + language);
                header.setTopic(topic);
                header.setDefaultTopic("TBW102");
                header.setDefaultTopicQueueNums(4);
                header.setQueueId(queueId);
                header.setSysFlag(0);
                header.setBornTimestamp(System.currentTimeMillis());
                header.setFlag(message.getFlag());
                header.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
                header.setReconsumeTimes(0);
                header.setUnitMode(false);
                header.setBatch(false);
                header.setBrokerName(BROKER_NAME);
                api.sendMessage(brokerAddress, BROKER_NAME, message, header, 10_000,
                    CommunicationMode.SYNC, null, null);
                System.out.printf("P13\tseed\t%s\t%d%n", body, queueId);
            }
        } finally {
            api.shutdown();
        }
    }

    private static void consume(String brokerAddress, String topic, String language) throws Exception {
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        MQClientAPIExt api = new MQClientAPIExt(config, new NettyClientConfig(), null, null);
        api.start();
        try {
            String prefix = "compat_p13_pop_" + language;
            String queueZeroBody = "p13-" + language + "-q0";
            String queueOneBody = "p13-" + language + "-q1";

            MessageExt first = pop(api, brokerAddress, topic, prefix + "_expiry", 0, queueZeroBody, 1_000);
            String firstReceipt = receipt(first);
            TimeUnit.MILLISECONDS.sleep(1_400);
            MessageExt redelivered = pop(api, brokerAddress, topic, prefix + "_expiry", 0, queueZeroBody, 10_000);
            String redeliveryReceipt = receipt(redelivered);
            if (firstReceipt.equals(redeliveryReceipt)) {
                throw new IllegalStateException("expiry redelivery reused the expired POP receipt");
            }
            requireAck(api.ackMessageAsync(brokerAddress, ackHeader(redelivered, topic,
                prefix + "_expiry", redeliveryReceipt), 10_000).get(15, TimeUnit.SECONDS), "expiry-redelivery");
            System.out.printf("P13\texpiry-redelivery\tOK\t%d\n", redelivered.getQueueId());

            MessageExt changedSource = pop(api, brokerAddress, topic, prefix + "_change", 0, queueZeroBody, 60_000);
            String oldReceipt = receipt(changedSource);
            AckResult changed = api.changeInvisibleTimeAsync(brokerAddress, BROKER_NAME,
                changeHeader(changedSource, topic, prefix + "_change", oldReceipt, 120_000), 10_000)
                .get(15, TimeUnit.SECONDS);
            if (changed.getStatus() != AckStatus.OK || changed.getExtraInfo() == null
                || oldReceipt.equals(changed.getExtraInfo())) {
                throw new IllegalStateException("change-invisibility did not return a replacement receipt: " + changed);
            }
            requireAck(api.ackMessageAsync(brokerAddress, ackHeader(changedSource, topic,
                prefix + "_change", changed.getExtraInfo()), 10_000).get(15, TimeUnit.SECONDS), "replacement-receipt");
            System.out.println("P13\tchange-invisibility\tOK");
            System.out.println("P13\treplacement-receipt\tOK\tACKED");

            MessageExt queueZero = pop(api, brokerAddress, topic, prefix + "_mixed", 0, queueZeroBody, 60_000);
            MessageExt queueOne = pop(api, brokerAddress, topic, prefix + "_mixed", 1, queueOneBody, 60_000);
            int receiptZeroQueue = receiptQueueId(receipt(queueZero));
            int receiptOneQueue = receiptQueueId(receipt(queueOne));
            if (queueZero.getQueueId() != 0 || queueOne.getQueueId() != 1
                || receiptZeroQueue != 0 || receiptOneQueue != 1) {
                throw new IllegalStateException("POP receipt queue mapping did not preserve queue 0/1");
            }
            requireAck(api.ackMessageAsync(brokerAddress, ackHeader(queueZero, topic,
                prefix + "_mixed", receipt(queueZero)), 10_000).get(15, TimeUnit.SECONDS), "mixed-queue-0");
            requireAck(api.ackMessageAsync(brokerAddress, ackHeader(queueOne, topic,
                prefix + "_mixed", receipt(queueOne)), 10_000).get(15, TimeUnit.SECONDS), "mixed-queue-1");
            System.out.println("P13\tmixed-queue-mapping\tOK\t0,1");

            boolean malformedRejected = false;
            try {
                ExtraInfoUtil.getQueueOffset(ExtraInfoUtil.split("17 123 456 2 1 broker-a 4 nope"));
            } catch (RuntimeException expected) {
                malformedRejected = true;
            }
            if (!malformedRejected) {
                throw new IllegalStateException("Java accepted malformed POP extra-info");
            }
            System.out.println("P13\tmalformed-extra-info-rejected\tOK");
            System.out.println("P13\tpop-delivery\tOK\t5");
            System.out.println("P13\tack\tOK\t4");
        } finally {
            api.shutdown();
        }
    }

    private static MQClientAPIImpl producerApi() {
        ClientConfig config = new ClientConfig();
        config.setVipChannelEnabled(false);
        return new MQClientAPIImpl(new NettyClientConfig(), null, null, config);
    }

    private static MessageExt pop(MQClientAPIExt api, String brokerAddress, String topic,
        String group, int queueId, String expectedBody, long invisibleTime) throws Exception {
        PopMessageRequestHeader header = new PopMessageRequestHeader();
        header.setConsumerGroup(group);
        header.setTopic(topic);
        header.setQueueId(queueId);
        header.setMaxMsgNums(1);
        header.setInvisibleTime(invisibleTime);
        header.setPollTime(0);
        header.setBornTime(System.currentTimeMillis());
        header.setExpType("TAG");
        header.setExp("*");
        PopResult result = api.popMessageAsync(brokerAddress, BROKER_NAME, header, 10_000)
            .get(15, TimeUnit.SECONDS);
        if (result.getPopStatus() == null || !"FOUND".equals(result.getPopStatus().name())
            || result.getMsgFoundList() == null || result.getMsgFoundList().isEmpty()) {
            throw new IllegalStateException("POP did not find " + expectedBody + ": " + result);
        }
        MessageExt message = result.getMsgFoundList().get(0);
        if (!expectedBody.equals(new String(message.getBody(), StandardCharsets.UTF_8))) {
            throw new IllegalStateException("POP returned an unexpected body: " + Arrays.toString(message.getBody()));
        }
        return message;
    }

    private static String receipt(MessageExt message) {
        String value = message.getProperty(MessageConst.PROPERTY_POP_CK);
        if (value == null || value.isEmpty()) throw new IllegalStateException("POP result omitted its receipt");
        return value;
    }

    private static int receiptQueueId(String raw) {
        return ExtraInfoUtil.getQueueId(ExtraInfoUtil.split(raw));
    }

    private static AckMessageRequestHeader ackHeader(MessageExt message, String topic, String group, String token) {
        AckMessageRequestHeader header = new AckMessageRequestHeader();
        header.setConsumerGroup(group);
        header.setTopic(topic);
        header.setQueueId(message.getQueueId());
        header.setOffset(message.getQueueOffset());
        header.setExtraInfo(token);
        return header;
    }

    private static ChangeInvisibleTimeRequestHeader changeHeader(MessageExt message, String topic,
        String group, String token, long invisibleTime) {
        ChangeInvisibleTimeRequestHeader header = new ChangeInvisibleTimeRequestHeader();
        header.setConsumerGroup(group);
        header.setTopic(topic);
        header.setQueueId(message.getQueueId());
        header.setOffset(message.getQueueOffset());
        header.setExtraInfo(token);
        header.setInvisibleTime(invisibleTime);
        return header;
    }

    private static void requireAck(AckResult result, String operation) {
        if (result.getStatus() != AckStatus.OK) {
            throw new IllegalStateException(operation + " ACK returned " + result.getStatus());
        }
    }
}
