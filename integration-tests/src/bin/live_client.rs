use std::{env, error::Error, fmt::Write as _, sync::Arc, time::Duration};

use rocketmq_classic::{Producer, ProducerConfig, ProducerError};
use rocketmq_classic_common::message::{Message, decode_message_id, encode_properties};
use rocketmq_classic_core::api::{
    MQClientApi,
    producer::{SendKind, SendMode},
};
use rocketmq_classic_core::error::RpcError;
use rocketmq_classic_protocol::{
    SharedValue,
    codes::response,
    header::{QueryMessageRequestHeader, SendMessageRequestHeaderV1, ViewMessageRequestHeader},
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
        Some("oneway-send") if arguments.len() == 5 => {
            oneway_send(&arguments[1], &arguments[2], &arguments[3], &arguments[4]).await
        }
        Some("view") if arguments.len() == 4 => {
            view(&arguments[1], &arguments[2], &arguments[3]).await
        }
        _ => Err("usage: live_client route <namesrv-address> <topic> | heartbeat <broker-address> <producer-group> | send|async-send <broker-address> <topic> <body> | oneway-send <broker-address> <topic> <body> <key> | view <broker-address> <topic> <offset-message-id>".into()),
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
) -> Result<(), Box<dyn Error>> {
    let remoting = RemotingClient::new(TransportMode::Plain);
    if let Err(error) = remoting.start() {
        remoting.shutdown().await;
        return Err(error.into());
    }
    let api = MQClientApi::new(remoting.clone());
    let result = oneway_send_and_query(&api, broker_address, topic, body, key).await;
    remoting.shutdown().await;
    result
}

async fn oneway_send_and_query(
    api: &MQClientApi,
    broker_address: &str,
    topic: &str,
    body: &str,
    key: &str,
) -> Result<(), Box<dyn Error>> {
    let mut message = Message::new(topic, body.as_bytes().to_vec());
    message.set_keys(key.to_owned())?;
    let born_timestamp = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let header = SendMessageRequestHeaderV1 {
        producer_group: Some("compat_phase0".into()),
        topic: topic.to_owned(),
        default_topic: "TBW102".into(),
        default_topic_queue_nums: 4,
        queue_id: 0,
        sys_flag: 0,
        born_timestamp,
        flag: 0,
        properties: Some(encode_properties(&message.properties())),
        reconsume_times: Some(0),
        unit_mode: Some(false),
        batch: Some(false),
        broker_name: Some("broker-a".into()),
        ..SendMessageRequestHeaderV1::default()
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

    let now_millis = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let query_header = QueryMessageRequestHeader {
        topic: topic.to_owned(),
        key: key.to_owned(),
        max_num: 32,
        begin_timestamp: now_millis.saturating_sub(60_000),
        end_timestamp: now_millis.saturating_add(60_000),
        ..QueryMessageRequestHeader::default()
    };
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let result = api
            .query_message(
                broker_address,
                &query_header,
                false,
                Instant::now() + Duration::from_secs(5),
            )
            .await;
        let found = match result {
            Ok(result) => result.get_message_list().and_then(|messages| {
                messages
                    .snapshot()
                    .into_iter()
                    .flatten()
                    .map(|message| message.snapshot())
                    .find(|message| {
                        message
                            .keys()
                            .is_some_and(|keys| keys.split(' ').any(|candidate| candidate == key))
                    })
            }),
            Err(RpcError::Response {
                code: response::NO_MESSAGE,
                ..
            }) => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(message) = found {
            let body_hex = encode_hex(message.body().as_ref());
            println!(
                "MESSAGE\t{}\t{}\t{}\t{}\t{}\t{}",
                message.topic(),
                message.queue_id(),
                message.queue_offset(),
                message.commit_log_offset(),
                message.keys().unwrap_or_default(),
                body_hex
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("oneway message was not found by key before the deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
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
    let header = SendMessageRequestHeaderV1 {
        producer_group: Some("compat_phase0".into()),
        topic: topic.to_owned(),
        default_topic: "TBW102".into(),
        default_topic_queue_nums: 4,
        queue_id: 0,
        sys_flag: 0,
        born_timestamp,
        flag: 0,
        properties: Some(encode_properties(&message.properties())),
        reconsume_times: Some(0),
        unit_mode: Some(false),
        batch: Some(false),
        broker_name: Some("broker-a".into()),
        ..SendMessageRequestHeaderV1::default()
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
    let physical_offset = decode_message_id(&result.offset_msg_id)?.offset();
    if mode == SendMode::Future {
        println!(
            "ASYNC_FUTURE\t1\t{}\t{}\t{}\t{}\t{}\t{}",
            result.status,
            result.topic,
            result.queue_id,
            result.queue_offset,
            result.offset_msg_id,
            physical_offset
        );
    } else {
        println!(
            "SEND\t{}\t{}\t{}\t{}\t{}\t{}",
            result.status,
            result.topic,
            result.queue_id,
            result.queue_offset,
            result.offset_msg_id,
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
            &ViewMessageRequestHeader {
                topic: Some(topic.to_owned()),
                offset: physical_offset,
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

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}
