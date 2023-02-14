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

    let mut consumer_options = ConsumerOptions::new("default");
    consumer_options.auto_commit_enabled = false;

    let mut consumer = Consumer::new(kafka_client, consumer_options).await?;

    // seek offset
    // consumer.seek(TopicPartition::new("kafka", 0), 100000).await;

    let consume_stream = consumer.subscribe(vec!["kafka"]).await?;
    pin_mut!(consume_stream);

    while let Some(records) = consume_stream.next().await {
        for record in records {
            if let Some(value) = record.value {
                println!(
                    "{:?} - {}",
                    String::from_utf8(value.to_vec())?,
                    record.offset
                );
            }
        }
        // needed only when `auto_commit_enabled` is false
        consumer.commit_async().await?;
    }

    Ok(())
}
