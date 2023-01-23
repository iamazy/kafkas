use std::time::Duration;

use futures::{pin_mut, StreamExt};
use kafkas::{
    client::{Kafka, KafkaOptions},
    consumer::{Consumer, ConsumerOptions},
    executor::TokioExecutor,
    Error,
};

#[tokio::main]
async fn main() -> Result<(), Box<Error>> {
    tracing_subscriber::fmt()
        // Configure formatting settings.
        .with_target(true)
        .with_level(true)
        .with_max_level(tracing::Level::INFO)
        .with_ansi(true)
        .with_file(true)
        .with_line_number(true)
        // Set the subscriber as the default.
        .init();

    let kafka_client = Kafka::new("127.0.0.1:9092", KafkaOptions::default(), TokioExecutor).await?;

    let consumer_options = ConsumerOptions::new("app");
    let mut consumer = Consumer::new(kafka_client, consumer_options).await?;
    consumer.subscribe(vec!["kafka"]).await?;

    let consume_stream = consumer.stream()?;
    pin_mut!(consume_stream);

    while let Some(Ok(record)) = consume_stream.next().await {
        if let Some(value) = record.value {
            println!(
                "{:?} - {}",
                String::from_utf8(value.to_vec())?,
                record.offset
            );
        }
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}
