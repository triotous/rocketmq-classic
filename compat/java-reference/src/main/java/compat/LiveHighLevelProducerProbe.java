package compat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

/** Exercises the high-level Java producer send overloads against a live Broker. */
public final class LiveHighLevelProducerProbe {
    private LiveHighLevelProducerProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: <nameserver-address> <topic>");
        }
        run(args[0], args[1]);
    }

    private static void run(String namesrvAddress, String topic) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("compat_p7_java");
        producer.setNamesrvAddr(namesrvAddress);
        producer.setSendMessageWithVIPChannel(false);
        producer.setSendMsgTimeout(10_000);
        producer.setDefaultTopicQueueNums(4);
        producer.start();
        try {
            SendResult sync = producer.send(message(topic, "p7-sync"));
            print("SYNC", sync);

            CountDownLatch completed = new CountDownLatch(1);
            AtomicInteger callbackCount = new AtomicInteger();
            AtomicReference<SendResult> asyncResult = new AtomicReference<>();
            AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
            producer.send(message(topic, "p7-async"), new SendCallback() {
                @Override
                public void onSuccess(SendResult result) {
                    callbackCount.incrementAndGet();
                    asyncResult.set(result);
                    completed.countDown();
                }

                @Override
                public void onException(Throwable error) {
                    callbackCount.incrementAndGet();
                    asyncFailure.set(error);
                    completed.countDown();
                }
            });
            if (!completed.await(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException("asynchronous send callback timed out");
            }
            if (callbackCount.get() != 1 || asyncFailure.get() != null || asyncResult.get() == null) {
                throw new IllegalStateException("asynchronous send callback did not complete once",
                    asyncFailure.get());
            }
            System.out.printf("P7\tASYNC\t%d\t%s\t%d%n", callbackCount.get(),
                asyncResult.get().getSendStatus().name(), asyncResult.get().getMessageQueue().getQueueId());

            producer.sendOneway(message(topic, "p7-oneway"));
            System.out.println("P7\tONEWAY\tNO_RESPONSE");

            SendResult explicit = producer.send(message(topic, "p7-explicit"),
                new MessageQueue(topic, "broker-a", 1));
            print("EXPLICIT_QUEUE", explicit);

            SendResult selected = producer.send(message(topic, "p7-selector"), (queues, msg, arg) ->
                queues.stream().filter(queue -> queue.getQueueId() == 2).findFirst()
                    .orElseThrow(() -> new IllegalStateException("queue 2 is missing from the route")), null);
            print("SELECTOR", selected);

            java.util.List<Message> batchMessages = Arrays.asList(
                message(topic, "p7-batch-0"), message(topic, "p7-batch-1"));
            SendResult batch = producer.send(batchMessages);
            System.out.printf("P7\tVALID_BATCH\t%s\t%d\t%d%n", batch.getSendStatus().name(),
                batch.getMessageQueue().getQueueId(), batchMessages.size());

            try {
                producer.send(Arrays.asList(
                    message(topic, "p7-invalid-batch-0"),
                    message(topic + "-different", "p7-invalid-batch-1")));
                throw new IllegalStateException("batch with mixed topics was accepted");
            } catch (MQClientException expected) {
                System.out.println("P7\tINVALID_BATCH\tREJECTED");
            }
        } finally {
            producer.shutdown();
        }
    }

    private static Message message(String topic, String body) {
        return new Message(topic, body.getBytes(StandardCharsets.UTF_8));
    }

    private static void print(String method, SendResult result) {
        System.out.printf("P7\t%s\t%s\t%d%n", method, result.getSendStatus().name(),
            result.getMessageQueue().getQueueId());
    }
}
