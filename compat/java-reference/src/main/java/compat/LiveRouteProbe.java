package compat;

import java.util.Comparator;
import java.util.List;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageQueue;

/** Minimal live route probe using the pinned RocketMQ Java client dependency. */
public final class LiveRouteProbe {
    private LiveRouteProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: LiveRouteProbe <namesrv-address> <topic>");
        }

        DefaultMQProducer producer = new DefaultMQProducer("p19RouteJava");
        producer.setNamesrvAddr(args[0]);
        try {
            producer.start();
            List<MessageQueue> queues = producer.fetchPublishMessageQueues(args[1]);
            queues.sort(Comparator.comparing(MessageQueue::getBrokerName)
                .thenComparingInt(MessageQueue::getQueueId));
            for (MessageQueue queue : queues) {
                System.out.printf("QUEUE\t%s\t%s\t%d%n", queue.getTopic(),
                    queue.getBrokerName(), queue.getQueueId());
            }
        } finally {
            producer.shutdown();
        }
    }
}
