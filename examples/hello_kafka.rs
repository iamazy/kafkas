use std::time::Duration;

use bytes::Bytes;
use chrono::Local;
use futures::{SinkExt, StreamExt};
use indexmap::indexmap;
use kafka_protocol::{
    messages::{
        create_topics_request::CreatableTopic, metadata_request::MetadataRequestTopic,
        ApiVersionsRequest, CreateTopicsRequest, DescribeClusterRequest, DescribeConfigsRequest,
        DescribeGroupsRequest, FindCoordinatorRequest, GroupId, HeartbeatRequest, JoinGroupRequest,
        ListGroupsRequest, MetadataRequest, RequestKind, ResponseKind, TopicName,
    },
    protocol::StrBytes,
    records::TimestampType,
};
use kafkas::{
    client::{Kafka, KafkaOptions, SerializeMessage},
    connection_manager::ConnectionManager,
    consumer::{
        coordinator::{ConsumerCoordinator, CoordinatorType},
        fetcher::Fetcher,
    },
    executor::{AsyncStdExecutor, Executor, TokioExecutor},
    producer::{Producer, ProducerOptions},
    topic_name, Error, Record, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    NO_SEQUENCE,
};
use tokio::time::Instant;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<Error>> {
    tracing_subscriber::fmt::init();

    let mut options = KafkaOptions::new();
    options.client_id("app");
    let kafka_client = Kafka::new("127.0.0.1:9092", options, None, None, TokioExecutor).await?;

    // produce(kafka_client).await?;

    let mut coordinator = ConsumerCoordinator::new(kafka_client.clone(), "app").await?;
    coordinator.subscribe(topic_name("kafka")).await?;
    coordinator.join_group().await?;
    coordinator.sync_group().await?;
    // coordinator.offset_fetch(7).await?;
    // coordinator.offset_commit().await?;
    coordinator.list_offsets().await?;
    // coordinator.heartbeat().await?;
    // coordinator.leave_group().await?;

    // let mut fetcher = Fetcher::new(kafka_client.clone(), coordinator.subscriptions.clone());
    // fetcher.fetch().await?;
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
    for _ in 0..10000_0000 {
        let record = TestData::new("hello kafka");
        let ret = producer.send(&topic, record).await?;
        let _ = tx.send(ret).await;
    }
    info!("elapsed: {:?}", now.elapsed());
    tokio::time::sleep(Duration::from_secs(1)).await;

    Ok(())
}
