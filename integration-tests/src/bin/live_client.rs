use std::{env, error::Error, sync::Arc};

use rocketmq_classic::{Producer, ProducerConfig, ProducerError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() != 3 || arguments[0] != "route" {
        return Err("usage: live_client route <namesrv-address> <topic>".into());
    }

    let mut config = ProducerConfig::new("p19RouteRust")?;
    config
        .client_config_mut()
        .set_namesrv_addr(Some(arguments[1].clone()));
    config.client_config_mut().set_mq_client_api_timeout(5_000);

    let producer = Arc::new(Producer::new(config));
    producer.start().await?;
    let result = producer.fetch_publish_message_queues(&arguments[2]).await;
    let shutdown = producer.shutdown().await;
    shutdown?;
    match result {
        Ok(queues) if queues.is_empty() => println!("EMPTY_ROUTE\t{}", arguments[2]),
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
        Err(ProducerError::NoRoute(_)) => println!("NO_ROUTE\t{}", arguments[2]),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}
