use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use kafkas::{
    topic_name, Error, Kafka, KafkaOptions, Producer, ProducerOptions, Record, SerializeMessage,
    TimestampType, TokioExecutor, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    NO_SEQUENCE,
};
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

    let kafka_client = Kafka::new("127.0.0.1:9092", KafkaOptions::default(), TokioExecutor).await?;

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
    let producer = Producer::new(kafka_client, ProducerOptions::default()).await?;
    for i in 0..100_000_000 {
        let record = TestData::new(&format!("hello - kafka {i}"));
        let ret = producer.send(&topic, record).await?;
        let _ = tx.send(ret).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    info!("elapsed: {:?}", now.elapsed());
    // wait till all cached records send to kafka
    tokio::time::sleep(Duration::from_secs(5)).await;
    drop(producer);
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
            value: input.value,
            headers: indexmap::IndexMap::new(),
        })
    }
}
