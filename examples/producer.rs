use rocketmq_classic::{CompressionType, Producer, ProducerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = ProducerConfig::new("example-producer")?;
    config.set_compress_type(CompressionType::Zlib);
    let producer = std::sync::Arc::new(Producer::new(config));
    producer.start().await?;
    producer.shutdown().await?;
    Ok(())
}
