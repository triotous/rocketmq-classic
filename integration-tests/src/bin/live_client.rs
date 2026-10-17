use std::{env, error::Error, fmt::Write as _, sync::Arc, time::Duration};

use rocketmq_classic::{Producer, ProducerConfig, ProducerError};
use rocketmq_classic_core::api::MQClientApi;
use rocketmq_classic_protocol::{
    SharedValue,
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
        _ => Err("usage: live_client route <namesrv-address> <topic> | heartbeat <broker-address> <producer-group>".into()),
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

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}
