use std::{
    any::Any, collections::HashSet, env, error::Error, fmt::Write as _, path::Path, sync::Arc,
    time::Duration,
};

use rocketmq_classic::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, LocalTransactionState, MessageSelector,
    Producer, ProducerConfig, ProducerError, PullConsumer, PullConsumerConfig, PushConsumer,
    PushConsumerConfig, PushMessageListener, TransactionCallbackError, TransactionListener,
    TransactionProducer, create_reply_message,
};
use rocketmq_classic_common::message::{
    DecodeOptions, Message, MessageExt, MessageQueue, decode_many_as_client_compatible,
    decode_message_id, encode_compact_messages, encode_properties,
};
use rocketmq_classic_common::sysflag::pull::{build_pull_sys_flag, build_pull_sys_flag_with_lite};
use rocketmq_classic_consumer::PopReceipt;
use rocketmq_classic_consumer::rebalance::{
    AllocateMessageQueueAveragely, AllocateMessageQueueStrategy,
};
use rocketmq_classic_core::api::{
    MQClientApi,
    producer::{SendKind, SendMode},
};
use rocketmq_classic_core::unique_id::UniqueIdGenerator;
use rocketmq_classic_protocol::{
    SharedValue,
    body::{
        LiteSubscriptionAction, LiteSubscriptionCtlRequestBody, LiteSubscriptionDto,
        LiteSubscriptionDtoSet, LockBatchRequestBody, MessageRequestMode,
        QueryAssignmentRequestBody, SetMessageRequestModeRequestBody, UnlockBatchRequestBody,
    },
    header::{
        ConsumerSendMsgBackRequestHeader, EndTransactionRequestHeader, GetMaxOffsetRequestHeader,
        PopLiteMessageRequestHeader, PopMessageRequestHeader, PullMessageRequestHeader,
        QueryConsumerOffsetRequestHeader, SendMessageRequestHeaderV1,
        UnregisterClientRequestHeader, UpdateConsumerOffsetRequestHeader, ViewMessageRequestHeader,
    },
    heartbeat::{
        ConsumeFromWhere, ConsumeType, ConsumerData, HeartbeatData, MessageModel, ProducerData,
        SubscriptionData,
    },
};
use rocketmq_classic_remoting::client::{ClientConfig, RemotingClient, TlsConfig, TransportMode};
use tokio::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.first().map(String::as_str) {
        Some("route") if arguments.len() == 3 => route(&arguments[1], &arguments[2]).await,
        Some("p3-raw-route") if arguments.len() == 3 => {
            p3_raw_route(&arguments[1], &arguments[2]).await
        }
        Some("p3-tls") if arguments.len() == 5 => {
            p3_tls(&arguments[1], &arguments[2], &arguments[3], &arguments[4]).await
        }
        Some("rebalance") if arguments.len() == 4 => {
            rebalance(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("p7-producer") if arguments.len() == 3 => {
            high_level_producer_modes(&arguments[1], &arguments[2]).await
        }
        Some("p5-producer") if arguments.len() == 4 => {
            p5_producer(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("p5-consumer") if arguments.len() == 5 => {
            p5_consumer(&arguments[1], &arguments[2], &arguments[3], &arguments[4]).await
        }
        Some("p5-extension-prepare") if arguments.len() == 4 => {
            p5_extensions(&arguments[1], &arguments[2], &arguments[3], true).await
        }
        Some("p5-extension-consume") if arguments.len() == 4 => {
            p5_extensions(&arguments[1], &arguments[2], &arguments[3], false).await
        }
        Some("p13-pop-consume") if arguments.len() == 4 => {
            p13_pop_consume(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("p13-pop-prepare") if arguments.len() == 4 => {
            p13_pop_prepare(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("p6-shared-runtime") if arguments.len() == 6 => {
            shared_runtime_probe(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
            )
            .await
        }
        Some("p9-pull") if arguments.len() == 4 => {
            pull_consumer_probe(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("p19-request") if arguments.len() == 5 => {
            request_reply_request(&arguments[1], &arguments[2], &arguments[3], &arguments[4]).await
        }
        Some("p19-respond") if arguments.len() == 6 => {
            request_reply_respond(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
            )
            .await
        }
        Some("transaction") if arguments.len() == 7 => {
            transaction(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
                &arguments[6],
            )
            .await
        }
        Some("max-offset") if arguments.len() == 4 => {
            max_offset(&arguments[1], &arguments[2], arguments[3].parse()?).await
        }
        Some("heartbeat") if arguments.len() == 3 => {
            heartbeat(&arguments[1], &arguments[2]).await
        }
        Some("send") if arguments.len() == 4 => {
            send(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("timeout-send") if arguments.len() == 7 => {
            timeout_send(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                arguments[5].parse()?,
                &arguments[6],
            )
            .await
        }
        Some("async-send") if arguments.len() == 4 => {
            async_send(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("oneway-send") if arguments.len() == 6 => {
            oneway_send(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                arguments[5].parse()?,
            )
            .await
        }
        Some("batch-send") if arguments.len() == 5 => {
            batch_send(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                arguments[4].parse()?,
            )
            .await
        }
        Some("view") if arguments.len() == 4 => {
            view(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("view-batch") if arguments.len() == 4 => {
            view_batch(&arguments[1], &arguments[2], &arguments[3]).await
        }
        Some("offset-query") if arguments.len() == 5 => {
            offset_query(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                arguments[4].parse()?,
            )
            .await
        }
        Some("offset-update") if arguments.len() == 6 => {
            offset_update(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                arguments[4].parse()?,
                arguments[5].parse()?,
            )
            .await
        }
        Some("pull") if arguments.len() == 6 => {
            pull(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                arguments[4].parse()?,
                arguments[5].parse()?,
            )
            .await
        }
        Some("lock") if arguments.len() == 7 => {
            lock_batch(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
                arguments[6].parse()?,
            )
            .await
        }
        Some("unlock") if arguments.len() == 7 => {
            unlock_batch(
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
                &arguments[5],
                arguments[6].parse()?,
            )
            .await
        }
        _ => Err("usage: live_client route <namesrv-address> <topic> | rebalance <namesrv-address> <topic> <comma-separated-consumer-ids> | p7-producer <namesrv-address> <topic> | p5-producer <broker-address> <topic> <producer-group> | p5-consumer <broker-address> <group> <topic> <seed-body> | p5-extension-prepare|p5-extension-consume <broker-address> <topic> <group-prefix> | p6-shared-runtime <namesrv-address> <topic> <producer-a> <producer-b> <consumer-group> | p9-pull <namesrv-address> <topic> <consumer-group> | p19-request <namesrv-address> <topic> <producer-group> <body> | p19-respond <namesrv-address> <topic> <consumer-group> <producer-group> <reply-body> | transaction <namesrv-address> <topic> <producer-group> <commit|rollback> <key> <body> | max-offset <broker-address> <topic> <queue-id> | timeout-send <proxy-address> <topic> <producer-group> <body> <timeout-ms> <go-file> | heartbeat <broker-address> <producer-group> | send|async-send <broker-address> <topic> <body> | oneway-send <broker-address> <topic> <body> <key> <minimum-max-offset> | batch-send <broker-address> <topic> <body-prefix> <message-count> | view <broker-address> <topic> <offset-message-id> | view-batch <broker-address> <topic> <comma-separated-offset-message-ids> | offset-query <broker-address> <group> <topic> <queue-id> | offset-update <broker-address> <group> <topic> <queue-id> <offset> | pull <broker-address> <group> <topic> <queue-id> <queue-offset> | lock|unlock <broker-address> <group> <client-id> <topic> <broker-name> <queue-id>".into()),
    }
}

async fn route(namesrv_address: &str, topic: &str) -> Result<(), Box<dyn Error>> {
    let mut config = ProducerConfig::new("p19RouteRust")?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.client_config_mut().set_mq_client_api_timeout(5_000);

    let producer = Arc::new(Producer::new(config));
    producer.start().await?;
    let result = producer.fetch_publish_message_queues(topic).await;
    let shutdown = producer.shutdown().await;
    shutdown?;
    match result {
        Ok(queues) if queues.is_empty() => println!("EMPTY_ROUTE\t{topic}"),
        Ok(mut queues) => {
            queues.sort_by_key(|queue| {
                (
                    queue.broker_name().unwrap_or_default().to_owned(),
                    queue.queue_id(),
                )
            });
            for queue in queues {
                println!(
                    "QUEUE\t{}\t{}\t{}",
                    queue.topic().unwrap_or_default(),
                    queue.broker_name().unwrap_or_default(),
                    queue.queue_id()
                );
            }
        }
        Err(ProducerError::NoRoute(_)) => println!("NO_ROUTE\t{topic}"),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

async fn p3_raw_route(namesrv_address: &str, topic: &str) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    remoting.update_name_server_address_list(vec![namesrv_address.to_owned()]);
    remoting.start()?;
    let api = MQClientApi::new(remoting.clone());
    let route = api
        .get_topic_route(topic, Instant::now() + Duration::from_secs(10))
        .await?;
    let brokers = route
        .as_ref()
        .map(|route| route.broker_datas_snapshot().unwrap_or_default().len())
        .unwrap_or_default();
    remoting.shutdown().await;
    let counts = remoting.lifecycle_counts();
    if brokers == 0 {
        return Err(format!("raw route invoke returned no route for {topic}").into());
    }
    if counts.pending_requests != 0 || counts.connections != 0 || counts.owned_tasks != 0 {
        return Err(format!("raw route shutdown retained resources: {counts:?}").into());
    }
    println!(
        "P3\tROUTE\tFOUND\t{brokers}\t{}\t{}\t{}",
        counts.pending_requests, counts.connections, counts.owned_tasks
    );
    Ok(())
}

async fn p3_tls(
    address: &str,
    trust_cert_path: &str,
    mode: &str,
    expected_outcome: &str,
) -> Result<(), Box<dyn Error>> {
    let ca_pem = std::fs::read(trust_cert_path)?;
    let mut tls = TlsConfig::default();
    tls.ca_pem = Some(ca_pem);
    tls.test_mode = false;
    tls.auth_server = true;
    let remoting = RemotingClient::with_config(ClientConfig {
        transport: TransportMode::Tls(tls),
        connect_timeout: 5_000,
        ..ClientConfig::default()
    })?;
    remoting.start()?;
    let api = MQClientApi::new(remoting.clone());
    let mut heartbeat = HeartbeatData {
        client_id: Some(format!("compat-p3-rust-{mode}")),
        ..HeartbeatData::default()
    };
    heartbeat.set_producer_data_set_owned(Some(vec![Some(ProducerData {
        group_name: Some("compat_p3_tls".to_owned()),
    })]));
    let result = api
        .send_heartbeat(
            address,
            &heartbeat,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    let accepted = result.is_ok();
    remoting.shutdown().await;
    let counts = remoting.lifecycle_counts();
    if counts.pending_requests != 0 || counts.connections != 0 || counts.owned_tasks != 0 {
        return Err(format!("TLS shutdown retained resources: {counts:?}").into());
    }
    let actual = if accepted { "accepted" } else { "rejected" };
    if actual != expected_outcome {
        return Err(format!("unexpected TLS outcome for {mode}: {actual}").into());
    }
    println!(
        "P3\tTLS\t{mode}\t{}\t{}\t{}\t{}",
        if accepted { "ACCEPTED" } else { "REJECTED" },
        counts.pending_requests,
        counts.connections,
        counts.owned_tasks,
    );
    Ok(())
}

async fn rebalance(
    namesrv_address: &str,
    topic: &str,
    comma_separated_consumer_ids: &str,
) -> Result<(), Box<dyn Error>> {
    let mut config = ProducerConfig::new("p19RebalanceRust")?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.client_config_mut().set_mq_client_api_timeout(5_000);

    let producer = Arc::new(Producer::new(config));
    producer.start().await?;
    let result = producer.fetch_publish_message_queues(topic).await;
    let shutdown = producer.shutdown().await;
    shutdown?;
    let mut queues = result?;
    queues.sort_by_key(|queue| {
        (
            queue.topic().unwrap_or_default().to_owned(),
            queue.broker_name().unwrap_or_default().to_owned(),
            queue.queue_id(),
        )
    });

    let mut consumer_ids = comma_separated_consumer_ids
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if consumer_ids.iter().any(String::is_empty) {
        return Err("consumer ID list contains an empty value".into());
    }
    consumer_ids.sort();
    let strategy = AllocateMessageQueueAveragely;
    for consumer_id in &consumer_ids {
        let assigned = strategy.allocate(
            "compat_phase0_rebalance_group",
            consumer_id,
            &queues,
            &consumer_ids,
        )?;
        let queue_keys = assigned
            .iter()
            .map(|queue| {
                format!(
                    "{}|{}|{}",
                    queue.topic().unwrap_or_default(),
                    queue.broker_name().unwrap_or_default(),
                    queue.queue_id()
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        println!("ASSIGN\t{consumer_id}\t{queue_keys}");
    }
    Ok(())
}

struct FixedTransactionListener {
    state: LocalTransactionState,
}

impl TransactionListener for FixedTransactionListener {
    fn execute_local_transaction(
        &self,
        _message: &mut Message,
        _argument: Option<&(dyn Any + Send + Sync)>,
    ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
        Ok(Some(self.state))
    }

    fn check_local_transaction(
        &self,
        _message: &mut MessageExt,
    ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
        Ok(Some(LocalTransactionState::Unknown))
    }
}

async fn transaction(
    namesrv_address: &str,
    topic: &str,
    producer_group: &str,
    decision: &str,
    key: &str,
    body: &str,
) -> Result<(), Box<dyn Error>> {
    let state = match decision {
        "commit" => LocalTransactionState::Commit,
        "rollback" => LocalTransactionState::Rollback,
        _ => return Err("transaction decision must be commit or rollback".into()),
    };
    let mut config = ProducerConfig::new(producer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    let producer = TransactionProducer::new(config);
    producer.set_transaction_listener(Some(Arc::new(FixedTransactionListener { state })));
    producer.start().await?;

    let mut message = Message::new(topic, body.as_bytes().to_vec());
    message.set_keys(key.to_owned())?;
    let sent = producer
        .send_message_in_transaction(&mut message, None)
        .await;
    let shutdown = producer.shutdown().await;
    shutdown?;
    let result = sent?;
    println!(
        "TX\t{}\t{}\t{}\t{}",
        result.send_result.status.name(),
        result.local_transaction_state.java_name(),
        key,
        result.send_result.msg_id
    );
    Ok(())
}

async fn heartbeat(broker_address: &str, producer_group: &str) -> Result<(), Box<dyn Error>> {
    let heartbeat = HeartbeatData {
        client_id: Some("p19-live-heartbeat".into()),
        ..HeartbeatData::default()
    };
    heartbeat
        .producer_data_set
        .as_ref()
        .expect("default heartbeat initializes producer set")
        .with_mut(|producers| {
            producers.insert(Some(SharedValue::from(ProducerData {
                group_name: Some(producer_group.to_owned()),
            })));
        });
    let body = heartbeat.java_encode()?;

    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let result = api
        .send_heartbeat(
            broker_address,
            &heartbeat,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let version = result?;
    println!("HEARTBEAT\t{version}\t{}", encode_hex(&body));
    Ok(())
}

async fn send(broker_address: &str, topic: &str, body: &str) -> Result<(), Box<dyn Error>> {
    send_with_mode(broker_address, topic, body, SendMode::Sync).await
}

async fn async_send(broker_address: &str, topic: &str, body: &str) -> Result<(), Box<dyn Error>> {
    send_with_mode(broker_address, topic, body, SendMode::Future).await
}

async fn high_level_producer_modes(
    namesrv_address: &str,
    topic: &str,
) -> Result<(), Box<dyn Error>> {
    let mut config = ProducerConfig::new("compat_p7_rust")?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.set_send_timeout_millis(10_000);
    let producer = Arc::new(Producer::new(config));
    producer.start().await?;

    let result = async {
        let mut sync_message = Message::new(topic, b"p7-sync".to_vec());
        let sync = producer.send(&mut sync_message).await?;
        println!("P7\tSYNC\t{}\t{}", sync.status.name(), sync.queue_id);

        let mut async_message = Message::new(topic, b"p7-async".to_vec());
        let async_result = producer.send_future(&mut async_message).await?;
        println!(
            "P7\tASYNC\t1\t{}\t{}",
            async_result.status.name(),
            async_result.queue_id
        );

        let mut oneway_message = Message::new(topic, b"p7-oneway".to_vec());
        producer.send_oneway(&mut oneway_message).await?;
        println!("P7\tONEWAY\tNO_RESPONSE");

        let explicit_queue = MessageQueue::new(topic, "broker-a", 1);
        let mut explicit_message = Message::new(topic, b"p7-explicit".to_vec());
        let explicit = producer
            .send_to_queue(&mut explicit_message, &explicit_queue)
            .await?;
        println!(
            "P7\tEXPLICIT_QUEUE\t{}\t{}",
            explicit.status.name(),
            explicit.queue_id
        );

        let mut selector_message = Message::new(topic, b"p7-selector".to_vec());
        let selected = producer
            .send_with_selector(&mut selector_message, |queues, _| {
                queues.iter().find(|queue| queue.queue_id() == 2).cloned()
            })
            .await?;
        println!(
            "P7\tSELECTOR\t{}\t{}",
            selected.status.name(),
            selected.queue_id
        );

        let mut batch = vec![
            Message::new(topic, b"p7-batch-0".to_vec()),
            Message::new(topic, b"p7-batch-1".to_vec()),
        ];
        let valid_batch = producer.send_batch(&mut batch).await?;
        println!(
            "P7\tVALID_BATCH\t{}\t{}\t{}",
            valid_batch.status.name(),
            valid_batch.queue_id,
            batch.len()
        );

        let mut invalid_batch = vec![
            Message::new(topic, b"p7-invalid-batch-0".to_vec()),
            Message::new(format!("{topic}-different"), b"p7-invalid-batch-1".to_vec()),
        ];
        if producer.send_batch(&mut invalid_batch).await.is_ok() {
            return Err("batch with mixed topics was accepted".into());
        }
        println!("P7\tINVALID_BATCH\tREJECTED");
        Ok::<(), Box<dyn Error>>(())
    }
    .await;

    let shutdown = producer.shutdown().await;
    shutdown?;
    result
}

async fn shared_runtime_probe(
    namesrv_address: &str,
    topic: &str,
    producer_group_a: &str,
    producer_group_b: &str,
    consumer_group: &str,
) -> Result<(), Box<dyn Error>> {
    let mut producer_config_a = ProducerConfig::new(producer_group_a)?;
    producer_config_a
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    producer_config_a.set_send_timeout_millis(10_000);
    let producer_a = Arc::new(Producer::new(producer_config_a));

    let mut producer_config_b = ProducerConfig::new(producer_group_b)?;
    producer_config_b
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    producer_config_b.set_send_timeout_millis(10_000);
    let producer_b = Arc::new(Producer::new(producer_config_b));

    let mut consumer_config = PushConsumerConfig::new(consumer_group)?;
    consumer_config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    let consumer = Arc::new(PushConsumer::new(consumer_config));
    let (observed_tx, mut observed_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    consumer
        .set_message_listener(PushMessageListener::concurrently(
            move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let observed_tx = observed_tx.clone();
                async move {
                    for message in messages {
                        let body = String::from_utf8_lossy(message.body().as_ref()).into_owned();
                        let _ = observed_tx.send(body);
                    }
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        ))
        .await?;
    consumer.subscribe(topic, "*").await?;

    let mut producer_a_started = false;
    let mut producer_b_started = false;
    let mut consumer_started = false;
    let mut producer_a_stopped = false;
    let result: Result<(), Box<dyn Error>> = async {
        producer_a.start().await?;
        producer_a_started = true;
        producer_b.start().await?;
        producer_b_started = true;
        consumer.start().await?;
        consumer_started = true;

        send_tagged(&producer_a, topic, "DROP", "p6-before-change").await?;
        expect_consumed(&mut observed_rx, "p6-before-change").await?;

        consumer.subscribe(topic, "KEEP").await?;
        println!("P6\tSUBSCRIPTION\t*->KEEP");
        send_tagged(&producer_a, topic, "DROP", "p6-filtered-after-change").await?;
        send_tagged(&producer_a, topic, "KEEP", "p6-keep-after-change").await?;
        expect_consumed(&mut observed_rx, "p6-keep-after-change").await?;

        producer_a.shutdown().await?;
        producer_a_stopped = true;
        println!("P6\tUNREGISTER\t{producer_group_a}");
        send_tagged(&producer_b, topic, "KEEP", "p6-survivor-after-unregister").await?;
        println!("P6\tSURVIVOR\tSEND_OK");
        expect_consumed(&mut observed_rx, "p6-survivor-after-unregister").await?;

        match tokio::time::timeout(Duration::from_millis(1_500), observed_rx.recv()).await {
            Err(_) => {}
            Ok(Some(unexpected)) => {
                return Err(std::io::Error::other(format!(
                    "unexpected post-change delivery: {unexpected}"
                ))
                .into());
            }
            Ok(None) => return Err("push-consumer observation channel closed".into()),
        }
        Ok(())
    }
    .await;

    let mut cleanup_failures = Vec::new();
    if consumer_started {
        if let Err(error) = consumer.shutdown().await {
            cleanup_failures.push(format!("consumer shutdown: {error}"));
        }
    }
    if producer_b_started {
        if let Err(error) = producer_b.shutdown().await {
            cleanup_failures.push(format!("producer B shutdown: {error}"));
        }
    }
    if producer_a_started && !producer_a_stopped {
        if let Err(error) = producer_a.shutdown().await {
            cleanup_failures.push(format!("producer A shutdown: {error}"));
        }
    }

    result?;
    if !cleanup_failures.is_empty() {
        return Err(cleanup_failures.join("; ").into());
    }
    Ok(())
}

async fn pull_consumer_probe(
    namesrv_address: &str,
    topic: &str,
    consumer_group: &str,
) -> Result<(), Box<dyn Error>> {
    let consumer = new_pull_probe_consumer(namesrv_address, consumer_group).await?;
    let session: Result<MessageQueue, Box<dyn Error>> = async {
        let queues = consumer.fetch_message_queues(topic).await?;
        if queues.len() != 1 {
            return Err(std::io::Error::other(format!(
                "expected one pull queue, got {}",
                queues.len()
            ))
            .into());
        }
        let queue = queues.into_iter().next().expect("checked one queue exists");
        if queue.queue_id() != 0 || queue.broker_name() != Some("broker-a") {
            return Err(std::io::Error::other(format!("unexpected pull queue: {queue:?}")).into());
        }

        let found = consumer.pull(&queue, Some("MATCH"), 0, 1).await?;
        assert_pull_status(&found.result, "FOUND")?;
        print_pull_result(&found.result);
        let messages = found
            .messages
            .as_ref()
            .ok_or_else(|| std::io::Error::other("FOUND pull omitted decoded messages"))?;
        let body = messages
            .first()
            .map(|message| String::from_utf8_lossy(message.body().as_ref()).into_owned())
            .ok_or_else(|| std::io::Error::other("FOUND pull returned no decoded message"))?;
        if body != "p9-seed-match" {
            return Err(std::io::Error::other(format!(
                "manual pull returned unexpected body: {body}"
            ))
            .into());
        }
        println!("P9\tBODY\t{body}");

        let unmatched = consumer
            .pull_with_selector(&queue, &MessageSelector::by_tag("MISSING"), 0, 1)
            .await?;
        assert_pull_status(&unmatched.result, "NO_MATCHED_MSG")?;
        print_pull_result(&unmatched.result);
        let empty = consumer.pull(&queue, Some("*"), 2, 1).await?;
        assert_pull_status(&empty.result, "NO_NEW_MSG")?;
        print_pull_result(&empty.result);
        let invalid = consumer.pull(&queue, Some("*"), 3, 1).await?;
        assert_pull_status(&invalid.result, "OFFSET_ILLEGAL")?;
        print_pull_result(&invalid.result);

        let min_offset = consumer.get_min_offset(&queue).await?;
        let max_offset = consumer.get_max_offset(&queue).await?;
        let timestamp_search = consumer.search_offset(&queue, 0).await?;
        println!("P9\tBOUNDARY\t{min_offset}\t{max_offset}\t{timestamp_search}");
        if min_offset != 0 || max_offset != 2 || timestamp_search != 0 {
            return Err(std::io::Error::other(
                "pull offset boundary RPCs returned unexpected values",
            )
            .into());
        }

        consumer.update_consume_offset(&queue, 1).await?;
        consumer
            .update_consume_offset_to_broker(&queue, 1, false)
            .await?;
        Ok(queue)
    }
    .await;
    let shutdown = consumer.shutdown().await;
    let queue = session?;
    shutdown?;

    let restarted = new_pull_probe_consumer(namesrv_address, consumer_group).await?;
    let resume = restarted.fetch_consume_offset(&queue, true).await;
    let shutdown = restarted.shutdown().await;
    let resume = resume?;
    shutdown?;
    if resume != 1 {
        return Err(
            std::io::Error::other(format!("pull restart expected offset 1, got {resume}")).into(),
        );
    }
    println!("P9\tRESUME\t{resume}");
    Ok(())
}

async fn request_reply_request(
    namesrv_address: &str,
    topic: &str,
    producer_group: &str,
    body: &str,
) -> Result<(), Box<dyn Error>> {
    let mut config = ProducerConfig::new(producer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.set_topics(Some(vec![topic.to_owned()].into()));
    config.set_send_timeout_millis(10_000);
    let producer = Arc::new(Producer::new(config));
    producer.set_send_message_with_vip_channel(false);
    producer.start().await?;

    let result: Result<(), Box<dyn Error>> = async {
        let mut request = Message::new(topic, body.as_bytes().to_vec());
        let reply = producer
            .request(&mut request, Duration::from_secs(15))
            .await?;
        print_request_reply_message("REQUEST", &request);
        print_request_reply_message("REPLY", &reply);
        if request.get_property("CORRELATION_ID") != reply.get_property("CORRELATION_ID") {
            return Err("reply correlation ID differs from request".into());
        }
        Ok(())
    }
    .await;

    let shutdown = producer.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}

async fn request_reply_respond(
    namesrv_address: &str,
    topic: &str,
    consumer_group: &str,
    producer_group: &str,
    reply_body: &str,
) -> Result<(), Box<dyn Error>> {
    use std::sync::atomic::{AtomicBool, Ordering};

    let mut producer_config = ProducerConfig::new(producer_group)?;
    producer_config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    producer_config.set_send_timeout_millis(10_000);
    let producer = Arc::new(Producer::new(producer_config));
    producer.set_send_message_with_vip_channel(false);

    let mut consumer_config = PushConsumerConfig::new(consumer_group)?;
    consumer_config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    let consumer = Arc::new(PushConsumer::new(consumer_config));
    let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let replied = Arc::new(AtomicBool::new(false));
    let listener_producer = producer.clone();
    let listener_replied = replied.clone();
    let listener_body = reply_body.as_bytes().to_vec();
    consumer
        .set_message_listener(PushMessageListener::concurrently(
            move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let producer = listener_producer.clone();
                let result_tx = result_tx.clone();
                let replied = listener_replied.clone();
                let reply_body = listener_body.clone();
                async move {
                    let mut status = ConsumeConcurrentlyStatus::ConsumeSuccess;
                    for request in messages {
                        if replied.swap(true, Ordering::AcqRel) {
                            continue;
                        }
                        let request_body = encode_hex(request.body().as_ref());
                        let request_correlation =
                            request.get_property("CORRELATION_ID").unwrap_or_default();
                        let request_reply_to =
                            request.get_property("REPLY_TO_CLIENT").unwrap_or_default();
                        println!(
                            "REQ\tRECEIVED\t{}\t{}\t{}\t{}",
                            request.topic(),
                            request_body,
                            request_correlation,
                            request_reply_to
                        );
                        let outcome = match create_reply_message(Some(&request), reply_body.clone())
                        {
                            Ok(mut reply) => match producer.send(&mut reply).await {
                                Ok(sent) if sent.status.name() == "SEND_OK" => {
                                    format!(
                                        "SEND_OK\t{}\t{}\t{}\t{}",
                                        reply.topic(),
                                        request_body,
                                        request_correlation,
                                        request_reply_to
                                    )
                                }
                                Ok(sent) => {
                                    status = ConsumeConcurrentlyStatus::ReconsumeLater;
                                    format!("ERROR\tBroker send status {}", sent.status)
                                }
                                Err(error) => {
                                    status = ConsumeConcurrentlyStatus::ReconsumeLater;
                                    format!("ERROR\t{error}")
                                }
                            },
                            Err(error) => {
                                status = ConsumeConcurrentlyStatus::ReconsumeLater;
                                format!("ERROR\t{error}")
                            }
                        };
                        let _ = result_tx.send(outcome);
                    }
                    (Some(status), context)
                }
            },
        ))
        .await?;
    consumer.subscribe(topic, "*").await?;

    let mut producer_started = false;
    let mut consumer_started = false;
    let result: Result<(), Box<dyn Error>> = async {
        producer.start().await?;
        producer_started = true;
        consumer.start().await?;
        consumer_started = true;
        println!("REQ\tREADY");
        let outcome = tokio::time::timeout(Duration::from_secs(30), result_rx.recv())
            .await?
            .ok_or_else(|| std::io::Error::other("request observation channel closed"))?;
        if outcome.starts_with("ERROR\t") {
            return Err(outcome.into());
        }
        if !outcome.starts_with("SEND_OK\t") {
            return Err(format!("unexpected responder outcome: {outcome}").into());
        }
        println!("REQ\tRESPONDER\t{outcome}");
        Ok(())
    }
    .await;

    let mut cleanup_failures = Vec::new();
    if consumer_started {
        if let Err(error) = consumer.shutdown().await {
            cleanup_failures.push(format!("consumer shutdown: {error}"));
        }
    }
    if producer_started {
        if let Err(error) = producer.shutdown().await {
            cleanup_failures.push(format!("producer shutdown: {error}"));
        }
    }
    result?;
    if !cleanup_failures.is_empty() {
        return Err(cleanup_failures.join("; ").into());
    }
    Ok(())
}

fn print_request_reply_message(kind: &str, message: &Message) {
    let property = |name: &str| message.get_property(name).unwrap_or_else(|| "-".to_owned());
    println!(
        "REQ\t{kind}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        message.topic(),
        encode_hex(message.body().as_ref()),
        property("CORRELATION_ID"),
        property("REPLY_TO_CLIENT"),
        property("TTL"),
        property("CLUSTER"),
        property("MSG_TYPE"),
    );
}

async fn new_pull_probe_consumer(
    namesrv_address: &str,
    consumer_group: &str,
) -> Result<Arc<PullConsumer>, Box<dyn Error>> {
    let mut config = PullConsumerConfig::new(consumer_group)?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(namesrv_address.to_owned()));
    config.set_enable_rebalance(false);
    let consumer = Arc::new(PullConsumer::new(config));
    consumer.start().await?;
    Ok(consumer)
}

fn assert_pull_status(
    result: &rocketmq_classic_core::api::consumer::PullResultExt,
    expected: &str,
) -> Result<(), Box<dyn Error>> {
    if result.status.name() != expected {
        return Err(std::io::Error::other(format!(
            "expected pull status {expected} but got {}",
            result.status.name()
        ))
        .into());
    }
    Ok(())
}

fn print_pull_result(result: &rocketmq_classic_core::api::consumer::PullResultExt) {
    println!(
        "P9\tPULL\t{}\t{}\t{}\t{}\t{}",
        result.status.name(),
        result.next_begin_offset,
        result.min_offset,
        result.max_offset,
        result.suggest_which_broker_id
    );
}

async fn send_tagged(
    producer: &Producer,
    topic: &str,
    tag: &str,
    body: &str,
) -> Result<(), Box<dyn Error>> {
    let mut message = Message::with_tags(
        Some(topic.to_owned()),
        Some(tag.to_owned()),
        Some(body.as_bytes().to_vec()),
    );
    let result = producer.send(&mut message).await?;
    if result.status.name() != "SEND_OK" {
        return Err(std::io::Error::other(format!(
            "send of {body} returned {}",
            result.status.name()
        ))
        .into());
    }
    Ok(())
}

async fn expect_consumed(
    received: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    expected: &str,
) -> Result<(), Box<dyn Error>> {
    let actual = tokio::time::timeout(Duration::from_secs(15), received.recv())
        .await?
        .ok_or_else(|| std::io::Error::other("push-consumer observation channel closed"))?;
    if actual != expected {
        return Err(std::io::Error::other(format!(
            "expected consumed body {expected} but received {actual}"
        ))
        .into());
    }
    println!("P6\tRECEIVED\t{actual}");
    Ok(())
}

async fn oneway_send(
    broker_address: &str,
    topic: &str,
    body: &str,
    key: &str,
    minimum_max_offset: i64,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let result =
        oneway_send_and_query(&api, broker_address, topic, body, key, minimum_max_offset).await;
    remoting.shutdown().await;
    result
}

async fn oneway_send_and_query(
    api: &MQClientApi,
    broker_address: &str,
    topic: &str,
    body: &str,
    key: &str,
    minimum_max_offset: i64,
) -> Result<(), Box<dyn Error>> {
    let mut message = Message::new(topic, body.as_bytes().to_vec());
    message.set_keys(key.to_owned())?;
    message.set_system_property("UNIQ_KEY", format!("{key}_uniq"))?;
    let born_timestamp = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let header = {
        let mut __phase18_header_0 = SendMessageRequestHeaderV1::default();
        __phase18_header_0.set_producer_group(Some("compat_phase0".into()));
        __phase18_header_0.set_topic(topic.to_owned());
        __phase18_header_0.set_default_topic("TBW102".into());
        __phase18_header_0.set_default_topic_queue_nums(4);
        __phase18_header_0.set_queue_id(0);
        __phase18_header_0.set_sys_flag(0);
        __phase18_header_0.set_born_timestamp(born_timestamp);
        __phase18_header_0.set_flag(0);
        __phase18_header_0.set_properties(Some(encode_properties(&message.properties())));
        __phase18_header_0.set_reconsume_times(Some(0));
        __phase18_header_0.set_unit_mode(Some(false));
        __phase18_header_0.set_batch(Some(false));
        __phase18_header_0.set_broker_name(Some("broker-a".into()));
        __phase18_header_0
    };
    let result = api
        .send_message(
            broker_address,
            "broker-a",
            &header,
            body.as_bytes().to_vec(),
            "p19-live-rust-oneway-message",
            SendKind::V2,
            SendMode::Oneway,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;
    if result.is_some() {
        return Err("oneway send unexpectedly returned a Broker result".into());
    }
    println!("ONEWAY_RETURN\tNO_RESPONSE\t{topic}\t{key}");

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut header = GetMaxOffsetRequestHeader::default();
        header.set_topic(Some(topic.to_owned()));
        header.set_queue_id(Some(0));
        header.set_broker_name(Some("broker-a".into()));
        let max_offset = api
            .get_max_offset(
                broker_address,
                &header,
                Instant::now() + Duration::from_secs(5),
            )
            .await?;
        if max_offset >= minimum_max_offset {
            println!("QUEUE_MAX_OFFSET\t{max_offset}");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("oneway message did not reach the Broker queue before the deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn batch_send(
    broker_address: &str,
    topic: &str,
    body_prefix: &str,
    message_count: usize,
) -> Result<(), Box<dyn Error>> {
    if message_count == 0 {
        return Err("batch message count must be positive".into());
    }
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let ids = UniqueIdGenerator::new(None);
    let mut messages = Vec::with_capacity(message_count);
    for index in 0..message_count {
        let mut message = Message::new(topic, format!("{body_prefix}-{index}").into_bytes());
        message.set_keys(format!("compat_phase0_batch_rust_{index}"))?;
        ids.set_if_missing(&mut message);
        messages.push(message);
    }
    let mut batch_message = Message::new(topic, Vec::new());
    let batch_id = ids.set_if_missing(&mut batch_message);
    let body = encode_compact_messages(&messages)?;
    let header = {
        let mut __phase18_header_1 = SendMessageRequestHeaderV1::default();
        __phase18_header_1.set_producer_group(Some("compat_phase0".into()));
        __phase18_header_1.set_topic(topic.to_owned());
        __phase18_header_1.set_default_topic("TBW102".into());
        __phase18_header_1.set_default_topic_queue_nums(4);
        __phase18_header_1.set_queue_id(0);
        __phase18_header_1.set_sys_flag(0);
        __phase18_header_1.set_born_timestamp(i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )?);
        __phase18_header_1.set_flag(0);
        __phase18_header_1.set_properties(Some(encode_properties(&batch_message.properties())));
        __phase18_header_1.set_reconsume_times(Some(0));
        __phase18_header_1.set_unit_mode(Some(false));
        __phase18_header_1.set_batch(Some(true));
        __phase18_header_1.set_broker_name(Some("broker-a".into()));
        __phase18_header_1
    };
    let result = api
        .send_message(
            broker_address,
            "broker-a",
            &header,
            body,
            &batch_id,
            SendKind::Batch,
            SendMode::Sync,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let result = result?.ok_or("batch send returned no Broker result")?;
    let offset_msg_id = result
        .offset_msg_id
        .as_deref()
        .ok_or("batch send returned no offset message ID")?;
    let first_offset_message_id = offset_msg_id
        .split(',')
        .next()
        .filter(|message_id| !message_id.is_empty())
        .ok_or("batch send returned no offset message ID")?;
    let physical_offset = decode_message_id(first_offset_message_id)?.offset();
    println!(
        "BATCH_SEND\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        result.status,
        result.topic,
        result.queue_id,
        result.queue_offset,
        offset_msg_id,
        first_offset_message_id,
        physical_offset,
        message_count
    );
    Ok(())
}

async fn send_with_mode(
    broker_address: &str,
    topic: &str,
    body: &str,
    mode: SendMode,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let message = Message::new(topic, body.as_bytes().to_vec());
    let born_timestamp = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let header = {
        let mut __phase18_header_2 = SendMessageRequestHeaderV1::default();
        __phase18_header_2.set_producer_group(Some("compat_phase0".into()));
        __phase18_header_2.set_topic(topic.to_owned());
        __phase18_header_2.set_default_topic("TBW102".into());
        __phase18_header_2.set_default_topic_queue_nums(4);
        __phase18_header_2.set_queue_id(0);
        __phase18_header_2.set_sys_flag(0);
        __phase18_header_2.set_born_timestamp(born_timestamp);
        __phase18_header_2.set_flag(0);
        __phase18_header_2.set_properties(Some(encode_properties(&message.properties())));
        __phase18_header_2.set_reconsume_times(Some(0));
        __phase18_header_2.set_unit_mode(Some(false));
        __phase18_header_2.set_batch(Some(false));
        __phase18_header_2.set_broker_name(Some("broker-a".into()));
        __phase18_header_2
    };
    let result = api
        .send_message(
            broker_address,
            "broker-a",
            &header,
            body.as_bytes().to_vec(),
            "p19-live-rust-message",
            SendKind::V2,
            mode,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let result = result?.ok_or("synchronous send returned no result")?;
    let offset_msg_id = result
        .offset_msg_id
        .as_deref()
        .ok_or("synchronous send returned no offset message ID")?;
    let physical_offset = decode_message_id(offset_msg_id)?.offset();
    if mode == SendMode::Future {
        println!(
            "ASYNC_FUTURE\t1\t{}\t{}\t{}\t{}\t{}\t{}",
            result.status,
            result.topic,
            result.queue_id,
            result.queue_offset,
            offset_msg_id,
            physical_offset
        );
    } else {
        println!(
            "SEND\t{}\t{}\t{}\t{}\t{}\t{}",
            result.status,
            result.topic,
            result.queue_id,
            result.queue_offset,
            offset_msg_id,
            physical_offset
        );
    }
    Ok(())
}

async fn p5_producer(
    broker_address: &str,
    topic: &str,
    producer_group: &str,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let operation = async {
        let mut max_header = GetMaxOffsetRequestHeader::default();
        max_header.set_topic(Some(topic.to_owned()));
        max_header.set_queue_id(Some(0));
        max_header.set_broker_name(Some("broker-a".into()));
        let base_offset = api
            .get_max_offset(
                broker_address,
                &max_header,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
        println!("P5\tBASE\t{base_offset}");

        let ids = UniqueIdGenerator::new(None);
        for (method, kind, reply, batch) in [
            ("send-v1", SendKind::V1, false, false),
            ("send-v2", SendKind::V2, false, false),
            ("batch", SendKind::Batch, false, true),
        ] {
            let mut message = Message::new(topic, format!("p5-rust-{method}").into_bytes());
            message.set_keys(format!("compat_p5_rust_{method}"))?;
            if reply {
                message.set_system_property("MSG_TYPE", "reply")?;
                message.set_system_property(
                    "CORRELATION_ID",
                    format!("compat-p5-correlation-{method}"),
                )?;
                message.set_system_property("REPLY_TO_CLIENT", "compat-p5-client")?;
                message.set_system_property("TTL", "60000")?;
            }
            let message_id = ids.set_if_missing(&mut message);

            let (body, request_id, header_message) = if batch {
                let encoded = encode_compact_messages(std::slice::from_ref(&message))?;
                let mut envelope = Message::new(topic, Vec::new());
                let batch_id = ids.set_if_missing(&mut envelope);
                (encoded, batch_id, envelope)
            } else {
                let body = message.body().to_vec();
                (body, message_id, message)
            };
            let born_timestamp = i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis(),
            )?;
            let mut header = SendMessageRequestHeaderV1::default();
            header.set_producer_group(Some(producer_group.to_owned()));
            header.set_topic(topic.to_owned());
            header.set_default_topic("TBW102".into());
            header.set_default_topic_queue_nums(4);
            header.set_queue_id(0);
            header.set_sys_flag(0);
            header.set_born_timestamp(born_timestamp);
            header.set_flag(header_message.flag());
            header.set_properties(Some(encode_properties(&header_message.properties())));
            header.set_reconsume_times(Some(0));
            header.set_unit_mode(Some(false));
            header.set_batch(Some(batch));
            header.set_broker_name(Some("broker-a".into()));
            let result = api
                .send_message(
                    broker_address,
                    "broker-a",
                    &header,
                    body,
                    &request_id,
                    kind,
                    SendMode::Sync,
                    Instant::now() + Duration::from_secs(10),
                )
                .await?
                .ok_or("synchronous producer RPC returned no result")?;
            if result.offset_msg_id.as_deref().is_none_or(str::is_empty) {
                return Err(format!("{method} returned an incomplete send result").into());
            }
            println!(
                "P5\tRESULT\t{method}\t{}\t{}\t{}\t{}",
                result.status.name(),
                result.topic,
                result.queue_id,
                result.queue_offset
            );
        }

        let heartbeat = HeartbeatData {
            client_id: Some("compat-p5-client".into()),
            ..HeartbeatData::default()
        };
        heartbeat
            .producer_data_set
            .as_ref()
            .expect("default heartbeat initializes producer set")
            .with_mut(|producers| {
                producers.insert(Some(SharedValue::from(ProducerData {
                    group_name: Some(producer_group.to_owned()),
                })));
            });
        let consumer = ConsumerData {
            group_name: Some("compat_p5_reply_group".into()),
            consume_type: Some(ConsumeType::ConsumePassively),
            message_model: Some(MessageModel::Clustering),
            consume_from_where: Some(ConsumeFromWhere::ConsumeFromFirstOffset),
            ..ConsumerData::default()
        };
        heartbeat
            .consumer_data_set
            .as_ref()
            .expect("default heartbeat initializes consumer set")
            .with_mut(|consumers| {
                consumers.insert(Some(SharedValue::from(consumer)));
            });
        api.send_heartbeat(
            broker_address,
            &heartbeat,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;

        for (method, kind) in [
            ("reply-v1", SendKind::ReplyV1),
            ("reply-v2", SendKind::ReplyV2),
        ] {
            let mut message = Message::new(topic, format!("p5-rust-{method}").into_bytes());
            message.set_keys(format!("compat_p5_rust_{method}"))?;
            message.set_system_property("MSG_TYPE", "reply")?;
            message
                .set_system_property("CORRELATION_ID", format!("compat-p5-correlation-{method}"))?;
            message.set_system_property("REPLY_TO_CLIENT", "compat-p5-client")?;
            message.set_system_property("TTL", "60000")?;
            let message_id = ids.set_if_missing(&mut message);
            let born_timestamp = i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis(),
            )?;
            let mut header = SendMessageRequestHeaderV1::default();
            header.set_producer_group(Some(producer_group.to_owned()));
            header.set_topic(topic.to_owned());
            header.set_default_topic("TBW102".into());
            header.set_default_topic_queue_nums(4);
            header.set_queue_id(0);
            header.set_sys_flag(0);
            header.set_born_timestamp(born_timestamp);
            header.set_flag(message.flag());
            header.set_properties(Some(encode_properties(&message.properties())));
            header.set_reconsume_times(Some(0));
            header.set_unit_mode(Some(false));
            header.set_batch(Some(false));
            header.set_broker_name(Some("broker-a".into()));
            let result = api
                .send_message(
                    broker_address,
                    "broker-a",
                    &header,
                    message.body().to_vec(),
                    &message_id,
                    kind,
                    SendMode::Oneway,
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            if result.is_some() {
                return Err(
                    format!("{method} one-way reply unexpectedly returned a result").into(),
                );
            }
            println!("P5\tRESULT\t{method}\tONEWAY");
        }

        let mut end = EndTransactionRequestHeader::default();
        end.set_topic(Some(topic.to_owned()));
        end.set_producer_group(Some(producer_group.to_owned()));
        end.set_tran_state_table_offset(Some(-1));
        end.set_commit_log_offset(Some(-1));
        end.set_commit_or_rollback(Some(8));
        end.set_from_transaction_check(Some(false));
        end.set_msg_id(Some("compat-p5-end-transaction".into()));
        end.set_transaction_id(Some("compat-p5-transaction".into()));
        api.end_transaction_oneway(
            broker_address,
            &end,
            Some("compat-p5-end".into()),
            Instant::now() + Duration::from_secs(10),
        )
        .await?;
        println!("P5\tRESULT\tend-transaction\tONEWAY");

        let mut final_header = GetMaxOffsetRequestHeader::default();
        final_header.set_topic(Some(topic.to_owned()));
        final_header.set_queue_id(Some(0));
        final_header.set_broker_name(Some("broker-a".into()));
        let final_offset = api
            .get_max_offset(
                broker_address,
                &final_header,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
        println!("P5\tMAX\t{final_offset}");
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    operation
}

async fn p5_consumer(
    broker_address: &str,
    group: &str,
    topic: &str,
    seed_body: &str,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let operation = async {
        let client_id = "compat-p5-rust-consumer";
        let heartbeat = HeartbeatData {
            client_id: Some(client_id.to_owned()),
            ..HeartbeatData::default()
        };
        let mut subscription = SubscriptionData::new(Some(topic.to_owned()), Some("*".to_owned()));
        subscription.set_expression_type(Some("TAG".to_owned()));
        let consumer = ConsumerData {
            group_name: Some(group.to_owned()),
            consume_type: Some(ConsumeType::ConsumeActively),
            message_model: Some(MessageModel::Clustering),
            consume_from_where: Some(ConsumeFromWhere::ConsumeFromFirstOffset),
            subscription_data_set: Some(SharedValue::from(HashSet::from([Some(
                SharedValue::from(subscription),
            )]))),
            ..ConsumerData::default()
        };
        heartbeat
            .consumer_data_set
            .as_ref()
            .expect("default heartbeat initializes consumer set")
            .with_mut(|consumers| {
                consumers.insert(Some(SharedValue::from(consumer)));
            });
        api.send_heartbeat(
            broker_address,
            &heartbeat,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;

        let ordinary = p5_pull(&api, broker_address, group, topic, false).await?;
        if ordinary.status.name() != "FOUND" {
            return Err(format!("ordinary pull returned {}", ordinary.status.name()).into());
        }
        let messages = decode_many_as_client_compatible(
            ordinary
                .raw_messages
                .as_deref()
                .ok_or("ordinary pull returned no message bytes")?,
            DecodeOptions::default(),
        );
        if !messages
            .iter()
            .any(|message| message.body().as_ref() == seed_body.as_bytes())
        {
            return Err("ordinary pull did not return this client's seed message".into());
        }
        println!(
            "P5C\tPULL\t{}\t{}\t{}\t{}",
            ordinary.status.name(),
            messages.len(),
            ordinary.min_offset,
            ordinary.max_offset
        );

        let lite = p5_pull(&api, broker_address, group, topic, true).await?;
        if lite.status.name() != "FOUND" {
            return Err(format!("lite pull returned {}", lite.status.name()).into());
        }
        let lite_messages = decode_many_as_client_compatible(
            lite.raw_messages
                .as_deref()
                .ok_or("lite pull returned no message bytes")?,
            DecodeOptions::default(),
        );
        if lite_messages.is_empty() {
            return Err("lite pull returned no decoded messages".into());
        }
        println!(
            "P5C\tLITE_PULL\t{}\t{}\t{}\t{}",
            lite.status.name(),
            lite_messages.len(),
            lite.min_offset,
            lite.max_offset
        );

        let mut update = UpdateConsumerOffsetRequestHeader::default();
        update.set_consumer_group(Some(group.to_owned()));
        update.set_topic(Some(topic.to_owned()));
        update.set_queue_id(Some(0));
        update.set_commit_offset(Some(1));
        update.set_broker_name(Some("broker-a".to_owned()));
        api.update_consumer_offset(
            broker_address,
            &update,
            false,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;
        println!("P5C\tUPDATE_OFFSET\t1");

        let mut query = QueryConsumerOffsetRequestHeader::default();
        query.set_consumer_group(Some(group.to_owned()));
        query.set_topic(Some(topic.to_owned()));
        query.set_queue_id(Some(0));
        let queried = api
            .query_consumer_offset(
                broker_address,
                &query,
                Instant::now() + Duration::from_secs(10),
            )
            .await?
            .ok_or("query offset returned no stored offset")?;
        if queried != 1 {
            return Err(format!("query offset returned {queried} after update").into());
        }
        println!("P5C\tQUERY_OFFSET\t{queried}");

        let consumer_ids = api
            .get_consumer_list_by_group(
                broker_address,
                group,
                Instant::now() + Duration::from_secs(10),
            )
            .await?
            .ok_or("consumer-list returned no ID list")?;
        if !consumer_ids
            .iter()
            .any(|id| id.as_deref() == Some(client_id))
        {
            return Err("consumer-list omitted this client's registered identity".into());
        }
        println!("P5C\tCONSUMER_LIST\tPRESENT");

        let selected = messages
            .iter()
            .find(|message| message.body().as_ref() == seed_body.as_bytes())
            .ok_or("seed message disappeared from the decoded pull result")?;
        let mut send_back = ConsumerSendMsgBackRequestHeader::default();
        send_back.set_group(group.to_owned());
        send_back.set_broker_name(Some("broker-a".to_owned()));
        send_back.set_origin_topic(topic.to_owned());
        send_back.set_offset(selected.commit_log_offset());
        send_back.set_delay_level(3);
        send_back.set_origin_msg_id(selected.message_id().map(str::to_owned));
        send_back.set_max_reconsume_times(Some(16));
        send_back.set_unit_mode(false);
        api.consumer_send_message_back(
            broker_address,
            &send_back,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;
        println!("P5C\tSEND_BACK\tOK");

        let queue = MessageQueue::new(topic, "broker-a", 0);
        let mut lock = LockBatchRequestBody::default();
        lock.set_consumer_group(Some(group.to_owned()));
        lock.set_client_id(Some(client_id.to_owned()));
        lock.set_only_this_broker(true);
        lock.set_mq_set(Some(SharedValue::from(HashSet::from([Some(
            SharedValue::from(queue.clone()),
        )]))));
        let locked = api
            .lock_batch_mq(
                broker_address,
                &lock,
                Instant::now() + Duration::from_secs(10),
            )
            .await?
            .is_some_and(|queues| queues.contains(&Some(queue.clone())));
        if !locked {
            return Err("lock response omitted the requested queue".into());
        }
        println!("P5C\tLOCK\ttrue");

        let mut unlock = UnlockBatchRequestBody::default();
        unlock.set_consumer_group(Some(group.to_owned()));
        unlock.set_client_id(Some(client_id.to_owned()));
        unlock.set_only_this_broker(true);
        unlock.set_mq_set(Some(SharedValue::from(HashSet::from([Some(
            SharedValue::from(queue),
        )]))));
        api.unlock_batch_mq(
            broker_address,
            &unlock,
            false,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;
        println!("P5C\tUNLOCK\tOK");
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    operation
}

async fn p13_pop_prepare(
    broker_address: &str,
    topic: &str,
    language: &str,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    remoting.start()?;
    let api = MQClientApi::new(remoting.clone());
    let operation = async {
        for suffix in ["expiry", "change", "mixed"] {
            let group = format!("compat_p13_pop_{language}_{suffix}");
            let mut mode = SetMessageRequestModeRequestBody::default();
            mode.set_topic(Some(topic.to_owned()));
            mode.set_consumer_group(Some(group.clone()));
            mode.set_mode(Some(MessageRequestMode::Pop));
            mode.set_pop_share_queue_num(1);
            api.set_message_request_mode(
                broker_address,
                &mode,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
            println!("P13\tprepare\t{group}\tPOP");
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    operation
}

async fn p13_pop_consume(
    broker_address: &str,
    topic: &str,
    language: &str,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    remoting.start()?;
    let api = MQClientApi::new(remoting.clone());
    let operation = async {
        let prefix = format!("compat_p13_pop_{language}");
        let queue_zero_body = "p13-java-q0";
        let queue_one_body = "p13-java-q1";

        let (_first, first_receipt) = p13_pop_one(
            &api,
            broker_address,
            topic,
            &format!("{prefix}_expiry"),
            0,
            queue_zero_body,
            1_000,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(1_400)).await;
        let (redelivered, redelivery_receipt) = p13_pop_one(
            &api,
            broker_address,
            topic,
            &format!("{prefix}_expiry"),
            0,
            queue_zero_body,
            10_000,
        )
        .await?;
        if first_receipt.raw() == redelivery_receipt.raw() {
            return Err("expiry redelivery reused the expired POP receipt".into());
        }
        let ack = api
            .ack_message(
                broker_address,
                &redelivery_receipt.ack_header(topic, &format!("{prefix}_expiry")),
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
        if ack.name() != "OK" {
            return Err(format!("expiry redelivery ACK returned {}", ack.name()).into());
        }
        println!("P13\texpiry-redelivery\tOK\t{}", redelivered.queue_id());

        let (_changed_source, old_receipt) = p13_pop_one(
            &api,
            broker_address,
            topic,
            &format!("{prefix}_change"),
            0,
            queue_zero_body,
            60_000,
        )
        .await?;
        let change_header = old_receipt.change_invisible_header(topic, &format!("{prefix}_change"), 120_000);
        let changed = api
            .change_invisible_time(
                broker_address,
                old_receipt.broker_name(),
                &change_header,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
        if changed.status.name() != "OK" {
            return Err(format!("change-invisibility returned {}", changed.status.name()).into());
        }
        let replacement_raw = changed
            .extra_info
            .ok_or("change-invisibility omitted the replacement receipt")?;
        if replacement_raw == old_receipt.raw() {
            return Err("change-invisibility reused the old receipt".into());
        }
        let replacement = PopReceipt::parse(replacement_raw)?;
        let ack = api
            .ack_message(
                broker_address,
                &replacement.ack_header(topic, &format!("{prefix}_change")),
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
        if ack.name() != "OK" {
            return Err(format!("replacement receipt ACK returned {}", ack.name()).into());
        }
        println!("P13\tchange-invisibility\tOK");
        println!("P13\treplacement-receipt\tOK\tACKED");

        let (queue_zero, receipt_zero) = p13_pop_one(
            &api,
            broker_address,
            topic,
            &format!("{prefix}_mixed"),
            0,
            queue_zero_body,
            60_000,
        )
        .await?;
        let (queue_one, receipt_one) = p13_pop_one(
            &api,
            broker_address,
            topic,
            &format!("{prefix}_mixed"),
            1,
            queue_one_body,
            60_000,
        )
        .await?;
        if queue_zero.queue_id() != 0 || queue_one.queue_id() != 1
            || receipt_zero.queue_id() != 0 || receipt_one.queue_id() != 1
        {
            return Err("POP message and receipt queue mapping did not preserve queue 0/1".into());
        }
        for receipt in [&receipt_zero, &receipt_one] {
            let ack = api
                .ack_message(
                    broker_address,
                    &receipt.ack_header(topic, &format!("{prefix}_mixed")),
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            if ack.name() != "OK" {
                return Err(format!("mixed queue ACK returned {}", ack.name()).into());
            }
        }
        println!("P13\tmixed-queue-mapping\tOK\t0,1");

        if PopReceipt::parse("17 123 456 2 1 broker-a 4 nope").is_ok() {
            return Err("Rust accepted malformed POP extra-info".into());
        }
        println!("P13\tmalformed-extra-info-rejected\tOK");
        println!("P13\tpop-delivery\tOK\t5");
        println!("P13\tack\tOK\t4");
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    operation
}

async fn p13_pop_one(
    api: &MQClientApi,
    broker_address: &str,
    topic: &str,
    group: &str,
    queue_id: i32,
    expected_body: &str,
    invisible_time: i64,
) -> Result<(MessageExt, PopReceipt), Box<dyn Error>> {
    let mut mode = SetMessageRequestModeRequestBody::default();
    mode.set_topic(Some(topic.to_owned()));
    mode.set_consumer_group(Some(group.to_owned()));
    mode.set_mode(Some(MessageRequestMode::Pop));
    mode.set_pop_share_queue_num(1);
    api.set_message_request_mode(
        broker_address,
        &mode,
        Instant::now() + Duration::from_secs(10),
    )
    .await?;
    let born_time = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let mut header = PopMessageRequestHeader::default();
    header.set_consumer_group(Some(group.to_owned()));
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(queue_id);
    header.set_max_msg_nums(1);
    header.set_invisible_time(invisible_time);
    header.set_poll_time(0);
    header.set_born_time(born_time);
    header.set_exp_type(Some("TAG".to_owned()));
    header.set_exp(Some("*".to_owned()));
    let reply = api
        .pop_message(
            broker_address,
            &header,
            Instant::now() + Duration::from_secs(15),
        )
        .await?;
    if reply.status.name() != "FOUND" {
        return Err(format!("POP returned {} for {expected_body}", reply.status.name()).into());
    }
    let raw = reply.raw_messages.ok_or("successful POP omitted message bytes")?;
    let mut messages = decode_many_as_client_compatible(&raw, DecodeOptions::default());
    if messages.len() != 1 || messages[0].body().as_ref() != expected_body.as_bytes() {
        return Err(format!("POP returned an unexpected message for {expected_body}").into());
    }
    let message = messages.remove(0);
    let raw_receipt = message
        .properties()
        .get("POP_CK")
        .ok_or("POP result omitted POP_CK")?
        .clone();
    let receipt = PopReceipt::parse(raw_receipt)?;
    if message.queue_id() != queue_id || receipt.queue_id() != queue_id {
        return Err(format!("POP queue {queue_id} mapped to message/receipt {}/{}", message.queue_id(), receipt.queue_id()).into());
    }
    Ok((message, receipt))
}

async fn p5_extensions(
    broker_address: &str,
    topic: &str,
    group_prefix: &str,
    prepare_only: bool,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let client_id = format!("compat-p5-rust-{group_prefix}");
    let base_group = format!("{group_prefix}_base");
    let lite_group = format!("{group_prefix}_lite");
    let operation = async {
        if prepare_only {
            let heartbeat = HeartbeatData {
                client_id: Some(client_id.clone()),
                ..HeartbeatData::default()
            };
            let mut subscription =
                SubscriptionData::new(Some(topic.to_owned()), Some("*".to_owned()));
            subscription.set_expression_type(Some("TAG".to_owned()));
            let consumer = ConsumerData {
                group_name: Some(base_group.clone()),
                consume_type: Some(ConsumeType::ConsumeActively),
                message_model: Some(MessageModel::Clustering),
                consume_from_where: Some(ConsumeFromWhere::ConsumeFromFirstOffset),
                subscription_data_set: Some(SharedValue::from(HashSet::from([Some(
                    SharedValue::from(subscription),
                )]))),
                ..ConsumerData::default()
            };
            heartbeat
                .consumer_data_set
                .as_ref()
                .expect("default heartbeat initializes consumer set")
                .with_mut(|consumers| {
                    consumers.insert(Some(SharedValue::from(consumer)));
                });

            let heartbeat_version = api
                .send_heartbeat(
                    broker_address,
                    &heartbeat,
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            println!("P5E\theartbeat\tOK\t{heartbeat_version}");
            let heartbeat_v2 = api
                .send_heartbeat_v2(
                    broker_address,
                    &heartbeat,
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            println!(
                "P5E\theartbeat-v2\tOK\t{}\t{}\t{}",
                heartbeat_v2.version,
                heartbeat_v2.is_sub_change,
                heartbeat_v2.is_support_heartbeat_v2
            );

            let mut assignment_request = QueryAssignmentRequestBody::default();
            assignment_request.set_topic(Some(topic.to_owned()));
            assignment_request.set_consumer_group(Some(base_group.clone()));
            assignment_request.set_client_id(Some(client_id.clone()));
            assignment_request.set_strategy_name(Some("AVG".to_owned()));
            assignment_request.set_message_model(Some(MessageModel::Clustering));
            let assignments = api
                .query_assignment(
                    broker_address,
                    &assignment_request,
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            let assignment_count = assignments
                .get_message_queue_assignments()
                .map_or(0, |items| items.snapshot().len());
            println!("P5E\tassignment\tOK\t{assignment_count}");

            let mut request_mode = SetMessageRequestModeRequestBody::default();
            request_mode.set_topic(Some(topic.to_owned()));
            request_mode.set_consumer_group(Some(base_group.clone()));
            request_mode.set_mode(Some(MessageRequestMode::Pop));
            request_mode.set_pop_share_queue_num(1);
            api.set_message_request_mode(
                broker_address,
                &request_mode,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
            println!("P5E\trequest-mode\tOK");

            let lite_topic = "entries";
            let lite_subscription = LiteSubscriptionDto {
                action: Some(LiteSubscriptionAction::CompleteAdd),
                client_id: Some(client_id.clone()),
                group: Some(lite_group.clone()),
                topic: Some(topic.to_owned()),
                lite_topic_set: Some(SharedValue::from(std::collections::BTreeSet::from([Some(
                    lite_topic.to_owned(),
                )]))),
                version: 0,
                ..LiteSubscriptionDto::default()
            };
            let body = LiteSubscriptionCtlRequestBody {
                subscription_set: Some(SharedValue::from(LiteSubscriptionDtoSet::from([Some(
                    SharedValue::from(lite_subscription),
                )]))),
            };
            api.lite_subscription_ctl(
                broker_address,
                &body,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
            println!("P5E\tlite-subscription\tOK");

            let mut unregister = UnregisterClientRequestHeader::default();
            unregister.set_client_id(client_id);
            unregister.set_producer_group(None);
            unregister.set_consumer_group(Some(base_group));
            api.unregister_client(
                broker_address,
                &unregister,
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
            println!("P5E\tunregister\tOK");
        } else {
            let lite_topic = format!("%LMQ%{topic}%entries");
            let pop_group = format!("{group_prefix}_pop");
            let pop = p5_extension_pop(&api, broker_address, topic, &pop_group, &client_id, false)
                .await?;
            require_pop_body(&pop.0, "p5-extension-seed", "POP")?;
            println!("P5E\tpop\tFOUND\t{}", encode_hex(pop.0[0].body().as_ref()));

            let lite = p5_extension_pop(
                &api,
                broker_address,
                &lite_topic,
                &lite_group,
                &client_id,
                true,
            )
            .await?;
            require_pop_body(&lite.0, "p5-extension-lite", "POP-LITE")?;
            println!(
                "P5E\tpop-lite\tFOUND\t{}",
                encode_hex(lite.0[0].body().as_ref())
            );

            let ack_group = format!("{group_prefix}_ack");
            let acked =
                p5_extension_pop(&api, broker_address, topic, &ack_group, &client_id, false)
                    .await?;
            require_pop_body(&acked.0, "p5-extension-seed", "ACK POP")?;
            let ack_receipt = pop_receipt(&acked.0[0])?;
            let ack = api
                .ack_message(
                    broker_address,
                    &ack_receipt.ack_header(topic, &ack_group),
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            println!("P5E\tack\t{}", ack.name());

            let batch_group = format!("{group_prefix}_batch_ack");
            let batch =
                p5_extension_pop(&api, broker_address, topic, &batch_group, &client_id, false)
                    .await?;
            require_pop_body(&batch.0, "p5-extension-seed", "BATCH ACK POP")?;
            let batch_receipt = pop_receipt(&batch.0[0])?.raw().to_owned();
            let batch_status = rocketmq_classic_consumer::pop::batch_ack_message_from_extra_info(
                &api,
                broker_address,
                Some(topic),
                Some(&batch_group),
                Some(&[Some(batch_receipt)]),
                Instant::now() + Duration::from_secs(10),
            )
            .await?;
            println!("P5E\tbatch-ack\t{}", batch_status.name());

            let change_group = format!("{group_prefix}_change");
            let changed = p5_extension_pop(
                &api,
                broker_address,
                topic,
                &change_group,
                &client_id,
                false,
            )
            .await?;
            require_pop_body(&changed.0, "p5-extension-seed", "CHANGE INVISIBLE POP")?;
            let receipt = pop_receipt(&changed.0[0])?;
            let result = api
                .change_invisible_time(
                    broker_address,
                    receipt.broker_name(),
                    &receipt.change_invisible_header(topic, &change_group, 120_000),
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            if result.pop_time.is_none()
                || result.invisible_time.is_none()
                || result.revive_qid.is_none()
            {
                return Err("change-invisible-time omitted successful response metadata".into());
            }
            println!(
                "P5E\tchange-invisible-time\t{}\tMETADATA",
                result.status.name()
            );
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    operation
}

async fn p5_extension_pop(
    api: &MQClientApi,
    broker_address: &str,
    topic: &str,
    group: &str,
    client_id: &str,
    lite: bool,
) -> Result<(Vec<MessageExt>,), Box<dyn Error>> {
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let raw = if lite {
        let mut header = PopLiteMessageRequestHeader::default();
        header.set_client_id(Some(client_id.to_owned()));
        header.set_consumer_group(Some(group.to_owned()));
        header.set_topic(Some(topic.to_owned()));
        header.set_max_msg_num(1);
        header.set_invisible_time(60_000);
        header.set_poll_time(0);
        header.set_born_time(now);
        let reply = api
            .pop_lite_message(broker_address, &header, deadline)
            .await?;
        if reply.status.name() != "FOUND" {
            return Err(format!("lite POP returned {}", reply.status.name()).into());
        }
        reply.raw_messages
    } else {
        let mut header = PopMessageRequestHeader::default();
        header.set_consumer_group(Some(group.to_owned()));
        header.set_topic(Some(topic.to_owned()));
        header.set_queue_id(0);
        header.set_max_msg_nums(1);
        header.set_invisible_time(60_000);
        header.set_poll_time(0);
        header.set_born_time(now);
        header.set_exp_type(Some("TAG".to_owned()));
        header.set_exp(Some("*".to_owned()));
        let reply = api.pop_message(broker_address, &header, deadline).await?;
        if reply.status.name() != "FOUND" {
            return Err(format!("POP returned {}", reply.status.name()).into());
        }
        reply.raw_messages
    }
    .ok_or("successful POP omitted message bytes")?;
    let messages = decode_many_as_client_compatible(&raw, DecodeOptions::default());
    if messages.is_empty() {
        return Err("successful POP returned no decoded message".into());
    }
    Ok((messages,))
}

fn require_pop_body(
    messages: &[MessageExt],
    expected: &str,
    operation: &str,
) -> Result<(), Box<dyn Error>> {
    if messages
        .iter()
        .any(|message| message.body().as_ref() == expected.as_bytes())
    {
        Ok(())
    } else {
        Err(format!("{operation} returned messages without expected body {expected:?}").into())
    }
}

fn pop_receipt(message: &MessageExt) -> Result<PopReceipt, Box<dyn Error>> {
    let properties = message.properties();
    let raw = properties
        .get("POP_CK")
        .ok_or("POP message omitted POP_CK receipt")?;
    Ok(PopReceipt::parse(raw.clone())?)
}

async fn p5_pull(
    api: &MQClientApi,
    broker_address: &str,
    group: &str,
    topic: &str,
    lite: bool,
) -> Result<rocketmq_classic_core::api::consumer::PullResultExt, Box<dyn Error>> {
    let mut header = PullMessageRequestHeader::default();
    header.set_consumer_group(Some(group.to_owned()));
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(Some(0));
    header.set_queue_offset(Some(0));
    header.set_max_msg_nums(Some(32));
    header.set_sys_flag(Some(build_pull_sys_flag_with_lite(
        false, false, true, false, lite,
    )));
    header.set_commit_offset(Some(0));
    header.set_suspend_timeout_millis(Some(0));
    header.set_subscription(Some("*".to_owned()));
    header.set_sub_version(Some(0));
    header.set_expression_type(Some("TAG".to_owned()));
    header.set_broker_name(Some("broker-a".to_owned()));
    Ok(api
        .pull_message(
            broker_address,
            &header,
            Instant::now() + Duration::from_secs(10),
        )
        .await?)
}

async fn view(
    broker_address: &str,
    topic: &str,
    offset_message_id: &str,
) -> Result<(), Box<dyn Error>> {
    let physical_offset = decode_message_id(offset_message_id)?.offset();
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let result = api
        .view_message(
            broker_address,
            &{
                let mut __phase18_header_0 = ViewMessageRequestHeader::default();
                __phase18_header_0.set_topic(Some(topic.to_owned()));
                __phase18_header_0.set_offset(Some(physical_offset));
                __phase18_header_0
            },
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let message = result?;
    println!(
        "MESSAGE\t{}\t{}\t{}\t{}",
        message.topic(),
        message.queue_id(),
        message.queue_offset(),
        encode_hex(&message.body())
    );
    Ok(())
}

async fn view_batch(
    broker_address: &str,
    topic: &str,
    offset_message_ids: &str,
) -> Result<(), Box<dyn Error>> {
    let physical_offsets = decode_batch_offsets(offset_message_ids)?;
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let result = async {
        for physical_offset in physical_offsets {
            let message = api
                .view_message(
                    broker_address,
                    &{
                        let mut __phase18_header_1 = ViewMessageRequestHeader::default();
                        __phase18_header_1.set_topic(Some(topic.to_owned()));
                        __phase18_header_1.set_offset(Some(physical_offset));
                        __phase18_header_1
                    },
                    Instant::now() + Duration::from_secs(10),
                )
                .await?;
            println!(
                "MESSAGE\t{}\t{}\t{}\t{}",
                message.topic(),
                message.queue_id(),
                message.queue_offset(),
                encode_hex(&message.body())
            );
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    remoting.shutdown().await;
    result
}

async fn offset_query(
    broker_address: &str,
    group: &str,
    topic: &str,
    queue_id: i32,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut header = QueryConsumerOffsetRequestHeader::default();
    header.set_consumer_group(Some(group.to_owned()));
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(Some(queue_id));
    header.set_broker_name(Some("broker-a".to_owned()));
    let result = api
        .query_consumer_offset(
            broker_address,
            &header,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let offset = result?.ok_or("Broker reported that the consumer offset was not found")?;
    println!("OFFSET\t{offset}");
    Ok(())
}

async fn max_offset(
    broker_address: &str,
    topic: &str,
    queue_id: i32,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut header = GetMaxOffsetRequestHeader::default();
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(Some(queue_id));
    header.set_broker_name(Some("broker-a".to_owned()));
    let result = api
        .get_max_offset(
            broker_address,
            &header,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    println!("MAX_OFFSET\t{}", result?);
    Ok(())
}

async fn timeout_send(
    broker_address: &str,
    topic: &str,
    producer_group: &str,
    body: &str,
    timeout_ms: u64,
    go_file: &str,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut max_header = GetMaxOffsetRequestHeader::default();
    max_header.set_topic(Some(topic.to_owned()));
    max_header.set_queue_id(Some(0));
    max_header.set_broker_name(Some("broker-a".to_owned()));
    let initial_offset = api
        .get_max_offset(
            broker_address,
            &max_header,
            Instant::now() + Duration::from_secs(5),
        )
        .await?;
    println!("TIMEOUT\tREADY\trust\t{initial_offset}");

    let gate = Path::new(go_file);
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !gate.exists() {
        if tokio::time::Instant::now() >= ready_deadline {
            remoting.shutdown().await;
            return Err("timed out waiting for the delayed-response gate".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let message = Message::new(topic, body.as_bytes().to_vec());
    let born_timestamp = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let mut header = SendMessageRequestHeaderV1::default();
    header.set_producer_group(Some(producer_group.to_owned()));
    header.set_topic(topic.to_owned());
    header.set_default_topic("TBW102".to_owned());
    header.set_default_topic_queue_nums(4);
    header.set_queue_id(0);
    header.set_sys_flag(0);
    header.set_born_timestamp(born_timestamp);
    header.set_flag(0);
    header.set_properties(Some(encode_properties(&message.properties())));
    header.set_reconsume_times(Some(0));
    header.set_unit_mode(Some(false));
    header.set_batch(Some(false));
    header.set_broker_name(Some("broker-a".to_owned()));

    let started = std::time::Instant::now();
    let result = api
        .send_message(
            broker_address,
            "broker-a",
            &header,
            body.as_bytes().to_vec(),
            &format!("{body}-unique"),
            SendKind::V2,
            SendMode::Sync,
            Instant::now() + Duration::from_millis(timeout_ms),
        )
        .await;
    let elapsed_ms = started.elapsed().as_millis();
    remoting.shutdown().await;

    match result {
        Err(error)
            if error
                .to_string()
                .to_ascii_lowercase()
                .contains("deadline expired") =>
        {
            println!("TIMEOUT\tRESULT\trust\tTIMEOUT\tRPC\t{elapsed_ms}\t{initial_offset}");
            Ok(())
        }
        Err(error) => Err(error.into()),
        Ok(_) => Err("Broker response did not exceed the send deadline".into()),
    }
}

async fn offset_update(
    broker_address: &str,
    group: &str,
    topic: &str,
    queue_id: i32,
    offset: i64,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut header = UpdateConsumerOffsetRequestHeader::default();
    header.set_consumer_group(Some(group.to_owned()));
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(Some(queue_id));
    header.set_commit_offset(Some(offset));
    header.set_broker_name(Some("broker-a".to_owned()));
    let result = api
        .update_consumer_offset(
            broker_address,
            &header,
            false,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    result?;
    println!("OFFSET_UPDATED\t{offset}");
    Ok(())
}

async fn pull(
    broker_address: &str,
    group: &str,
    topic: &str,
    queue_id: i32,
    queue_offset: i64,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut header = PullMessageRequestHeader::default();
    header.set_consumer_group(Some(group.to_owned()));
    header.set_topic(Some(topic.to_owned()));
    header.set_queue_id(Some(queue_id));
    header.set_queue_offset(Some(queue_offset));
    header.set_max_msg_nums(Some(32));
    header.set_sys_flag(Some(build_pull_sys_flag(false, false, true, false)));
    header.set_commit_offset(Some(0));
    header.set_suspend_timeout_millis(Some(0));
    header.set_subscription(Some("*".to_owned()));
    header.set_sub_version(Some(0));
    header.set_expression_type(Some("TAG".to_owned()));
    header.set_broker_name(Some("broker-a".to_owned()));
    let result = api
        .pull_message(
            broker_address,
            &header,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let result = result?;
    let hex = result
        .raw_messages
        .as_deref()
        .map(encode_hex)
        .unwrap_or_default();
    println!(
        "PULL\t{}\t{}\t{}\t{}\t{}\t{}",
        result.status.name(),
        result.next_begin_offset,
        result.min_offset,
        result.max_offset,
        result.suggest_which_broker_id,
        hex
    );
    Ok(())
}

async fn lock_batch(
    broker_address: &str,
    group: &str,
    client_id: &str,
    topic: &str,
    broker_name: &str,
    queue_id: i32,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let queue = MessageQueue::new(topic, broker_name, queue_id);
    let mut body = LockBatchRequestBody::default();
    body.set_consumer_group(Some(group.to_owned()));
    body.set_client_id(Some(client_id.to_owned()));
    body.set_only_this_broker(true);
    body.set_mq_set(Some(SharedValue::new(HashSet::from([Some(
        SharedValue::new(queue.clone()),
    )]))));
    let result = api
        .lock_batch_mq(
            broker_address,
            &body,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    let acquired = result?.is_some_and(|queues| queues.contains(&Some(queue.clone())));
    println!(
        "LOCK\t{}\t{}\t{}\t{}",
        acquired, topic, broker_name, queue_id
    );
    Ok(())
}

async fn unlock_batch(
    broker_address: &str,
    group: &str,
    client_id: &str,
    topic: &str,
    broker_name: &str,
    queue_id: i32,
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let mut body = UnlockBatchRequestBody::default();
    body.set_consumer_group(Some(group.to_owned()));
    body.set_client_id(Some(client_id.to_owned()));
    body.set_only_this_broker(true);
    body.set_mq_set(Some(SharedValue::new(HashSet::from([Some(
        SharedValue::new(MessageQueue::new(topic, broker_name, queue_id)),
    )]))));
    let result = api
        .unlock_batch_mq(
            broker_address,
            &body,
            false,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    remoting.shutdown().await;
    result?;
    println!("UNLOCK\tOK");
    Ok(())
}

fn decode_batch_offsets(offset_message_ids: &str) -> Result<Vec<i64>, Box<dyn Error>> {
    let mut offsets = Vec::new();
    for message_id in offset_message_ids.split(',') {
        if message_id.is_empty() {
            return Err("batch readback requires every physical offset ID".into());
        }
        offsets.push(decode_message_id(message_id)?.offset());
    }
    Ok(offsets)
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::decode_batch_offsets;

    #[test]
    fn batch_readback_decodes_every_physical_offset_id() {
        let ids = "C0A86B0200002A9F0000000000000000,C0A86B0200002A9F00000000000000E1";

        assert_eq!(decode_batch_offsets(ids).unwrap(), [0, 225]);
    }

    #[test]
    fn batch_readback_rejects_missing_physical_offset_ids() {
        assert!(decode_batch_offsets("").is_err());
        assert!(decode_batch_offsets("C0A86B0200002A9F0000000000000000,").is_err());
    }
}
