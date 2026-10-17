package compat;

import java.nio.charset.StandardCharsets;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;

/** Exercises synchronous send and physical-offset readback through Java 5.5.1. */
public final class LiveProducerProbe {
    private LiveProducerProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 4 && "send".equals(args[0])) {
            send(args[1], args[2], args[3]);
            return;
        }
        if (args.length == 4 && "view".equals(args[0])) {
            view(args[1], args[2], args[3]);
            return;
        }
        throw new IllegalArgumentException(
            "usage: LiveProducerProbe send <broker-address> <topic> <body> | view <broker-address> <topic> <offset-message-id>");
    }

    private static MQClientAPIImpl newApi() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setVipChannelEnabled(false);
        return new MQClientAPIImpl(new NettyClientConfig(), null, null, clientConfig);
    }

    private static void send(String brokerAddress, String topic, String body) throws Exception {
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setProducerGroup("compat_phase0");
            header.setTopic(topic);
            header.setDefaultTopic("TBW102");
            header.setDefaultTopicQueueNums(4);
            header.setQueueId(0);
            header.setSysFlag(0);
            header.setBornTimestamp(System.currentTimeMillis());
            header.setFlag(message.getFlag());
            header.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
            header.setReconsumeTimes(0);
            header.setUnitMode(false);
            header.setBatch(false);
            header.setBrokerName("broker-a");
            SendResult result = api.sendMessage(brokerAddress, "broker-a", message, header,
                10_000, CommunicationMode.SYNC, null, null);
            long physicalOffset = MessageDecoder.decodeMessageId(result.getOffsetMsgId()).getOffset();
            System.out.printf("SEND\t%s\t%s\t%d\t%d\t%s\t%d%n",
                result.getSendStatus().name(), result.getMessageQueue().getTopic(),
                result.getMessageQueue().getQueueId(), result.getQueueOffset(),
                result.getOffsetMsgId(), physicalOffset);
        } finally {
            api.shutdown();
        }
    }

    private static void view(String brokerAddress, String topic, String offsetMessageId)
        throws Exception {
        long physicalOffset = MessageDecoder.decodeMessageId(offsetMessageId).getOffset();
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            MessageExt message = api.viewMessage(brokerAddress, topic, physicalOffset, 10_000);
            System.out.printf("MESSAGE\t%s\t%d\t%d\t%s%n", message.getTopic(),
                message.getQueueId(), message.getQueueOffset(), toHex(message.getBody()));
        } finally {
            api.shutdown();
        }
    }

    private static String toHex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] encoded = new char[bytes.length * 2];
        for (int index = 0; index < bytes.length; index++) {
            int value = bytes[index] & 0xff;
            encoded[index * 2] = digits[value >>> 4];
            encoded[index * 2 + 1] = digits[value & 0x0f];
        }
        return new String(encoded);
    }
}
