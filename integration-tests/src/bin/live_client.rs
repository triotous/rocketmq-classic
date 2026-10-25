use std::{collections::HashSet, env, error::Error, fmt::Write as _, sync::Arc, time::Duration};

use rocketmq_classic::{Producer, ProducerConfig, ProducerError};
use rocketmq_classic_common::message::{
    Message, MessageQueue, decode_message_id, encode_compact_messages, encode_properties,
};
use rocketmq_classic_common::sysflag::pull::build_pull_sys_flag;
use rocketmq_classic_core::api::{
    MQClientApi,
    producer::{SendKind, SendMode},
};
use rocketmq_classic_core::unique_id::UniqueIdGenerator;
use rocketmq_classic_protocol::{
    SharedValue,
    body::{LockBatchRequestBody, UnlockBatchRequestBody},
    header::{
        GetMaxOffsetRequestHeader, PullMessageRequestHeader, QueryConsumerOffsetRequestHeader,
        SendMessageRequestHeaderV1, UpdateConsumerOffsetRequestHeader, ViewMessageRequestHeader,
    },
    heartbeat::{HeartbeatData, ProducerData},
};
use rocketmq_classic_remoting::client::{RemotingClient, TransportMode};
use tokio::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.first().map(String::as_str) {
        Some("route") if arguments.len() == 3 => route(&arguments[1], &arguments[2]).await,
        Some("heartbeat") if arguments.len() == 3 => {
            heartbeat(&arguments[1], &arguments[2]).await
        }
        Some("send") if arguments.len() == 4 => {
            send(&arguments[1], &arguments[2], &arguments[3]).await
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
        _ => Err("usage: live_client route <namesrv-address> <topic> | heartbeat <broker-address> <producer-group> | send|async-send <broker-address> <topic> <body> | oneway-send <broker-address> <topic> <body> <key> <minimum-max-offset> | batch-send <broker-address> <topic> <body-prefix> <message-count> | view <broker-address> <topic> <offset-message-id> | view-batch <broker-address> <topic> <comma-separated-offset-message-ids> | offset-query <broker-address> <group> <topic> <queue-id> | offset-update <broker-address> <group> <topic> <queue-id> <offset> | pull <broker-address> <group> <topic> <queue-id> <queue-offset> | lock|unlock <broker-address> <group> <client-id> <topic> <broker-name> <queue-id>".into()),
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
