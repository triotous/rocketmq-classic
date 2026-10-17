package compat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
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
        if (args.length == 4 && "async-send".equals(args[0])) {
            asyncSend(args[1], args[2], args[3]);
            return;
        }
        if (args.length == 6 && "oneway-send".equals(args[0])) {
            onewaySend(args[1], args[2], args[3], args[4], Long.parseLong(args[5]));
            return;
        }
        if (args.length == 4 && "view".equals(args[0])) {
            view(args[1], args[2], args[3]);
            return;
        }
        throw new IllegalArgumentException(
            "usage: LiveProducerProbe send|async-send <broker-address> <topic> <body> | oneway-send <broker-address> <topic> <body> <key> <minimum-max-offset> | view <broker-address> <topic> <offset-message-id>");
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

    private static void asyncSend(String brokerAddress, String topic, String body) throws Exception {
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

            CountDownLatch completed = new CountDownLatch(1);
            AtomicInteger callbackCount = new AtomicInteger();
            AtomicReference<SendResult> sendResult = new AtomicReference<>();
            AtomicReference<Throwable> sendFailure = new AtomicReference<>();
            SendCallback callback = new SendCallback() {
                @Override
                public void onSuccess(SendResult result) {
                    callbackCount.incrementAndGet();
                    sendResult.set(result);
                    completed.countDown();
                }

                @Override
                public void onException(Throwable error) {
                    callbackCount.incrementAndGet();
                    sendFailure.set(error);
                    completed.countDown();
                }
            };
            // The direct API callback updates producer fault state. Construct its
            // implementation without starting NameServer discovery or routing.
            DefaultMQProducerImpl producer = new DefaultMQProducerImpl(
                new DefaultMQProducer("compat_phase0"));
            api.sendMessage(brokerAddress, "broker-a", message, header, 10_000,
                CommunicationMode.ASYNC, callback, null, null, 0, null, producer);

            if (!completed.await(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException("asynchronous send callback timed out");
            }
            if (callbackCount.get() != 1) {
                throw new IllegalStateException("asynchronous send callback count was " + callbackCount.get());
            }
            if (sendFailure.get() != null) {
                throw new Exception("asynchronous send failed", sendFailure.get());
            }
            SendResult result = sendResult.get();
            if (result == null) {
                throw new IllegalStateException("asynchronous send callback returned no result");
            }
            long physicalOffset = MessageDecoder.decodeMessageId(result.getOffsetMsgId()).getOffset();
            System.out.printf("ASYNC_CALLBACK\t%d\t%s\t%s\t%d\t%d\t%s\t%d%n",
                callbackCount.get(), result.getSendStatus().name(),
                result.getMessageQueue().getTopic(), result.getMessageQueue().getQueueId(),
                result.getQueueOffset(), result.getOffsetMsgId(), physicalOffset);
        } finally {
            api.shutdown();
        }
    }

    private static void onewaySend(
        String brokerAddress, String topic, String body, String key, long minimumMaxOffset)
        throws Exception {
        MQClientAPIImpl api = newApi();
        api.start();
        try {
            Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            message.setKeys(key);
            MessageAccessor.putProperty(
                message, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, key + "_uniq");
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
                10_000, CommunicationMode.ONEWAY, null, null);
            if (result != null) {
                throw new IllegalStateException("oneway send unexpectedly returned a Broker result");
            }
            System.out.printf("ONEWAY_RETURN\tNO_RESPONSE\t%s\t%s%n", topic, key);
            long maxOffset = waitForQueueMaxOffset(api, brokerAddress, topic, minimumMaxOffset);
            System.out.printf("QUEUE_MAX_OFFSET\t%d%n", maxOffset);
        } finally {
            api.shutdown();
        }
    }

    private static long waitForQueueMaxOffset(
        MQClientAPIImpl api, String brokerAddress, String topic, long minimumMaxOffset)
        throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            long maxOffset = api.getMaxOffset(
                brokerAddress, new MessageQueue(topic, "broker-a", 0), 5_000);
            if (maxOffset >= minimumMaxOffset) {
                return maxOffset;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException(
            "oneway message did not reach the Broker queue before the deadline");
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
