package compat;

import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/** Sends a committed or rolled-back transaction to the local 5.3.4 Broker. */
public final class LiveTransactionProbe {
    private LiveTransactionProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                "usage: LiveTransactionProbe <namesrv> <topic> <producer-group> <commit|rollback> <key> <body>");
        }
        LocalTransactionState decision;
        if ("commit".equals(args[3])) {
            decision = LocalTransactionState.COMMIT_MESSAGE;
        } else if ("rollback".equals(args[3])) {
            decision = LocalTransactionState.ROLLBACK_MESSAGE;
        } else {
            throw new IllegalArgumentException("transaction decision must be commit or rollback");
        }

        TransactionMQProducer producer = new TransactionMQProducer(args[2]);
        producer.setNamesrvAddr(args[0]);
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object argument) {
                return decision;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt message) {
                return LocalTransactionState.UNKNOW;
            }
        });
        try {
            producer.start();
            Message message = new Message(args[1], args[5].getBytes(StandardCharsets.UTF_8));
            message.setKeys(args[4]);
            TransactionSendResult result = producer.sendMessageInTransaction(message, args[3]);
            System.out.printf("TX\t%s\t%s\t%s\t%s%n",
                result.getSendStatus().name(), result.getLocalTransactionState().name(),
                args[4], result.getMsgId());
        } finally {
            producer.shutdown();
        }
    }
}
