use std::time::Duration;

use bytes::Bytes;
use futures::{pin_mut, SinkExt, StreamExt};
use kafka_protocol::records::TimestampType;
use kafkas::{
    client::{Kafka, KafkaOptions, SerializeMessage},
    consumer::Consumer,
    executor::{Executor, TokioExecutor},
    producer::{Producer, ProducerOptions},
    topic_name, Error, Record, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    NO_SEQUENCE,
};
use tokio::time::Instant;
use tracing::{error, info};

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

    let mut options = KafkaOptions::new();
    options.client_id("app");
    let kafka_client = Kafka::new("127.0.0.1:9092", options, None, None, TokioExecutor).await?;

    // produce(kafka_client.clone()).await?;

    let mut consumer = Consumer::new(kafka_client, "app1").await?;
    consumer.subscribe(vec!["kafka"]).await?;

    let consume_stream = consumer.stream()?;
    pin_mut!(consume_stream);

    while let Some(Ok(record)) = consume_stream.next().await {
        if let Some(record) = record.value {
            println!("{:?}", String::from_utf8(record.to_vec())?);
        }
    }

    tokio::time::sleep(Duration::from_secs(100000000)).await;
    Ok(())
}

struct TestData {
    value: Option<Bytes>,
}

impl TestData {
    fn new(value: &str) -> Self {
        Self {
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
        }
    }
}

impl SerializeMessage for TestData {
    fn partition(&self) -> Option<i32> {
        None
    }

    fn key(&self) -> Option<&Bytes> {
        None
    }

    fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    fn serialize_message(input: Self) -> kafkas::Result<Record> {
        Ok(Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: -1,
            sequence: NO_SEQUENCE,
            timestamp: 0,
            key: None,
            value: input.value.map(|value| Bytes::from(value)),
            headers: indexmap::IndexMap::new(),
        })
    }
}

async fn produce<Exe: Executor>(client: Kafka<Exe>) -> Result<(), Box<Error>> {
    let producer = Producer::new(client, ProducerOptions::default()).await?;

    let (mut tx, mut rx) = futures::channel::mpsc::unbounded();
    tokio::task::spawn(Box::pin(async move {
        while let Some(fut) = rx.next().await {
            if let Err(e) = fut.await {
                error!("{e}");
            }
        }
    }));

    let now = Instant::now();
    let topic = topic_name("kafka");
    for _ in 0..10 {
        let record = TestData::new("hello kafka 123");
        let ret = producer.send(&topic, record).await?;
        let _ = tx.send(ret).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    info!("elapsed: {:?}", now.elapsed());

    Ok(())
}
