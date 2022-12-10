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
    consumer::coordinator::{ConsumerCoordinator, CoordinatorType},
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
    let kafka_client = Kafka::new("127.0.0.1:50088", options, None, None, TokioExecutor).await?;

    // produce(kafka_client).await?;

    let mut coordinator = ConsumerCoordinator::new(kafka_client, "app").await?;
    coordinator.subscribe(topic_name("kafka")).await?;
    coordinator.join_group().await?;
    coordinator.sync_group().await?;
    coordinator.offset_fetch(7).await?;
    coordinator.offset_commit().await?;
    coordinator.heartbeat().await?;
    coordinator.leave_group().await?;
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

async fn api_versions<Exe: Executor>(
    addr: &String,
    manager: &ConnectionManager<Exe>,
) -> Result<(), Box<Error>> {
    let request = RequestKind::ApiVersionsRequest(ApiVersionsRequest::default());
    let response = manager.invoke(&addr, request).await?;
    if let ResponseKind::ApiVersionsResponse(res) = response {
        println!("{:?}", res);
    }
    Ok(())
}

async fn heartbeat<Exe: Executor>(
    addr: &String,
    manager: &ConnectionManager<Exe>,
) -> Result<(), Box<Error>> {
    let mut request = HeartbeatRequest::default();
    request.group_id = GroupId(StrBytes::from_str("test_group_id"));
    request.member_id = StrBytes::from_str("test_member_id");
    let request = RequestKind::HeartbeatRequest(request);
    let response = manager.invoke(&addr, request).await?;
    if let ResponseKind::HeartbeatResponse(res) = response {
        println!("{:?}", res);
    }
    Ok(())
}

async fn describe_cluster<Exe: Executor>(
    addr: &String,
    manager: &ConnectionManager<Exe>,
) -> Result<(), Box<Error>> {
    let request = RequestKind::DescribeClusterRequest(DescribeClusterRequest::default());
    let response = manager.invoke(&addr, request).await?;
    if let ResponseKind::DescribeClusterResponse(res) = response {
        println!("{:?}", res);
    }
    Ok(())
}

async fn find_coordinator<Exe: Executor>(
    addr: &String,
    manager: &ConnectionManager<Exe>,
) -> Result<(), Box<Error>> {
    let mut request = FindCoordinatorRequest::default();
    request.key_type = CoordinatorType::Group.into();
    let request = RequestKind::FindCoordinatorRequest(request);
    let response = manager.invoke(&addr, request).await?;
    if let ResponseKind::FindCoordinatorResponse(res) = response {
        println!("{:?}", res);
    }
    Ok(())
}

async fn describe_groups<Exe: Executor>(
    addr: &String,
    manager: &ConnectionManager<Exe>,
) -> Result<(), Box<Error>> {
    let mut request = DescribeGroupsRequest::default();
    request.groups = vec![GroupId(StrBytes::from_str("test_group_id"))];

    let request = RequestKind::DescribeGroupsRequest(request);
    let response = manager.invoke(&addr, request).await?;
    if let ResponseKind::DescribeGroupsResponse(res) = response {
        println!("{:?}", res);
    }
    Ok(())
}

async fn join_group<Exe: Executor>(
    addr: &String,
    manager: &ConnectionManager<Exe>,
) -> Result<(), Box<Error>> {
    let mut request = JoinGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_str("test_group_id"));
    request.member_id = StrBytes::from_str("test_member_id");
    request.protocol_type = StrBytes::from_str("consumer");
    let request = RequestKind::JoinGroupRequest(request);
    let response = manager.invoke(&addr, request).await?;
    if let ResponseKind::JoinGroupResponse(res) = response {
        println!("{:?}", res);
    }
    Ok(())
}

async fn list_groups<Exe: Executor>(
    addr: &String,
    manager: &ConnectionManager<Exe>,
) -> Result<(), Box<Error>> {
    let request = ListGroupsRequest::default();
    let request = RequestKind::ListGroupsRequest(request);
    let response = manager.invoke(&addr, request).await?;
    if let ResponseKind::ListGroupsResponse(res) = response {
        println!("{:?}", res);
    }
    Ok(())
}

async fn describe_configs<Exe: Executor>(
    addr: &String,
    manager: &ConnectionManager<Exe>,
) -> Result<(), Box<Error>> {
    let request = DescribeConfigsRequest::default();
    let request = RequestKind::DescribeConfigsRequest(request);
    let response = manager.invoke(&addr, request).await?;
    if let ResponseKind::DescribeConfigsResponse(res) = response {
        println!("{:?}", res);
    }
    Ok(())
}

async fn create_topic<Exe: Executor>(
    addr: &String,
    manager: &ConnectionManager<Exe>,
) -> Result<(), Box<Error>> {
    let mut request = CreateTopicsRequest::default();
    let mut create_topic = CreatableTopic::default();
    create_topic.num_partitions = 4;
    create_topic.replication_factor = 1;
    let topic_name = TopicName(StrBytes::from_str("test_topic"));
    request.topics = indexmap! {
        topic_name => create_topic,
    };
    let request = RequestKind::CreateTopicsRequest(request);
    let response = manager.invoke(&addr, request).await?;
    if let ResponseKind::CreateTopicsResponse(res) = response {
        println!("{:?}", res);
    }
    Ok(())
}

async fn metadata<Exe: Executor>(
    addr: &String,
    manager: &ConnectionManager<Exe>,
) -> Result<(), Box<Error>> {
    let mut request = MetadataRequest::default();
    let topic_name = TopicName(StrBytes::from_str("test_topic"));
    let mut metadata_topic = MetadataRequestTopic::default();
    metadata_topic.name = Some(topic_name);
    request.topics = Some(vec![metadata_topic]);
    let request = RequestKind::MetadataRequest(request);
    let response = manager.invoke(&addr, request).await?;
    if let ResponseKind::MetadataResponse(res) = response {
        println!("{:?}", res);
    }
    Ok(())
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
