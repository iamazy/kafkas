use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use kafka_protocol::{
    messages::{
        metadata_request::MetadataRequestTopic, DescribeGroupsRequest, DescribeGroupsResponse,
        FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
        HeartbeatRequest, HeartbeatResponse, InitProducerIdRequest, JoinGroupRequest,
        JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, ListOffsetsRequest,
        ListOffsetsResponse, MetadataRequest, OffsetCommitRequest, OffsetCommitResponse,
        OffsetFetchRequest, OffsetFetchResponse, ProduceRequest, ProduceResponse, ProducerId,
        RequestKind, ResponseKind, SyncGroupRequest, SyncGroupResponse, TopicName,
    },
    records::Record,
};
use tracing::error;

use crate::{
    connection::Connection,
    connection_manager::{ConnectionManager, ConnectionRetryOptions, OperationRetryOptions},
    consumer::ConsumerRecord,
    error::{ConnectionError, Error, Result},
    executor::Executor,
    metadata::{Cluster, Node},
    PartitionRef,
};

/// Helper trait for consumer deserialization
pub trait DeserializeMessage {
    /// type produced from the message
    type Output: Sized;
    /// deserialize method that will be called by the consumer
    fn deserialize_message(record: &ConsumerRecord) -> Self::Output;
}

/// Helper trait for message serialization
pub trait SerializeMessage {
    fn partition(&self) -> Option<i32>;
    fn key(&self) -> Option<&Bytes>;
    fn value(&self) -> Option<&Bytes>;
    /// serialize method that will be called by the producer
    fn serialize_message(input: Self) -> Result<Record>;
}

#[derive(Clone)]
pub struct Kafka<Exe: Executor> {
    pub(crate) manager: Arc<ConnectionManager<Exe>>,
    pub(crate) operation_retry_options: OperationRetryOptions,
    pub(crate) executor: Arc<Exe>,
    pub(crate) options: KafkaOptions,
    pub cluster_meta: Arc<Cluster>,
}

#[derive(Debug, Clone)]
pub struct KafkaOptions {
    pub client_id: Option<String>,
    pub request_timeout_ms: i32,
}

impl Default for KafkaOptions {
    fn default() -> Self {
        Self {
            client_id: Some("default_client_id".into()),
            request_timeout_ms: 1,
        }
    }
}

impl KafkaOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn client_id<S: Into<String>>(&mut self, client_id: S) {
        self.client_id = Some(client_id.into());
    }
}

impl<Exe: Executor> Kafka<Exe> {
    pub async fn new<S: Into<String>>(
        url: S,
        options: KafkaOptions,
        connection_retry_options: Option<ConnectionRetryOptions>,
        operation_retry_options: Option<OperationRetryOptions>,
        executor: Exe,
    ) -> Result<Self> {
        let url: String = url.into();
        let executor = Arc::new(executor);
        let operation_retry_options = operation_retry_options.unwrap_or_default();

        let manager = ConnectionManager::new(
            url,
            options.clone(),
            connection_retry_options,
            operation_retry_options.clone(),
            executor.clone(),
        )
        .await?;
        let manager = Arc::new(manager);

        let weak_manager = Arc::downgrade(&manager);
        let mut interval = executor.interval(std::time::Duration::from_secs(60));
        let res = executor.spawn(Box::pin(async move {
            while let Some(()) = interval.next().await {
                if let Some(strong_manager) = weak_manager.upgrade() {
                    strong_manager.check_connections().await;
                } else {
                    // if all the strong references to the manager were dropped,
                    // we can stop the task.
                    break;
                }
            }
        }));

        if res.is_err() {
            error!("the executor could not spawn the check connection task");
            return Err(ConnectionError::Shutdown.into());
        }

        Ok(Kafka {
            manager,
            operation_retry_options,
            executor,
            options,
            cluster_meta: Arc::new(Cluster::empty()),
        })
    }

    pub fn partitions(&self, topic: &TopicName) -> Result<PartitionRef> {
        self.cluster_meta.partitions(topic)
    }
}

impl<Exe: Executor> Kafka<Exe> {
    pub async fn init_producer_id(&self) -> Result<()> {
        let request = InitProducerIdRequest {
            producer_id: ProducerId(-1),
            producer_epoch: -1,
            ..Default::default()
        };
        let request = RequestKind::InitProducerIdRequest(request);
        let response = self.manager.invoke(&self.manager.url, request).await?;
        if let ResponseKind::InitProducerIdResponse(init_producer_id) = response {
            println!("{init_producer_id:?}");
            Ok(())
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                "unexpected response".into(),
            )))
        }
    }

    pub async fn produce(&self, node: &Node, request: ProduceRequest) -> Result<ProduceResponse> {
        let request = RequestKind::ProduceRequest(request);
        let response = self.manager.invoke(node.address(), request).await?;
        if let ResponseKind::ProduceResponse(response) = response {
            Ok(response)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn find_coordinator(
        &self,
        request: FindCoordinatorRequest,
    ) -> Result<FindCoordinatorResponse> {
        let request = RequestKind::FindCoordinatorRequest(request);
        let response = self.manager.invoke(&self.manager.url, request).await?;
        if let ResponseKind::FindCoordinatorResponse(response) = response {
            Ok(response)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn describe_groups(
        &self,
        node: &Node,
        request: DescribeGroupsRequest,
    ) -> Result<DescribeGroupsResponse> {
        let request = RequestKind::DescribeGroupsRequest(request);
        let response = self.manager.invoke(node.address(), request).await?;
        if let ResponseKind::DescribeGroupsResponse(response) = response {
            Ok(response)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn join_group(
        &self,
        node: &Node,
        request: JoinGroupRequest,
    ) -> Result<JoinGroupResponse> {
        let request = RequestKind::JoinGroupRequest(request);
        let response = self.manager.invoke(node.address(), request).await?;
        if let ResponseKind::JoinGroupResponse(response) = response {
            Ok(response)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn leave_group(
        &self,
        node: &Node,
        request: LeaveGroupRequest,
    ) -> Result<LeaveGroupResponse> {
        let request = RequestKind::LeaveGroupRequest(request);
        let response = self.manager.invoke(node.address(), request).await?;
        if let ResponseKind::LeaveGroupResponse(response) = response {
            Ok(response)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn sync_group(
        &self,
        node: &Node,
        request: SyncGroupRequest,
    ) -> Result<SyncGroupResponse> {
        let request = RequestKind::SyncGroupRequest(request);
        let response = self.manager.invoke(node.address(), request).await?;
        if let ResponseKind::SyncGroupResponse(response) = response {
            Ok(response)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn offset_fetch(
        &self,
        node: &Node,
        request: OffsetFetchRequest,
    ) -> Result<OffsetFetchResponse> {
        let request = RequestKind::OffsetFetchRequest(request);
        let response = self.manager.invoke(node.address(), request).await?;
        if let ResponseKind::OffsetFetchResponse(response) = response {
            Ok(response)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn offset_commit(
        &self,
        node: &Node,
        request: OffsetCommitRequest,
    ) -> Result<OffsetCommitResponse> {
        let request = RequestKind::OffsetCommitRequest(request);
        let response = self.manager.invoke(node.address(), request).await?;
        if let ResponseKind::OffsetCommitResponse(response) = response {
            Ok(response)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn list_offsets(
        &self,
        node: &Node,
        request: ListOffsetsRequest,
    ) -> Result<ListOffsetsResponse> {
        let request = RequestKind::ListOffsetsRequest(request);
        let response = self.manager.invoke(node.address(), request).await?;
        if let ResponseKind::ListOffsetsResponse(response) = response {
            Ok(response)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn heartbeat(
        &self,
        node: &Node,
        request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse> {
        let request = RequestKind::HeartbeatRequest(request);
        let response = self.manager.invoke(node.address(), request).await?;
        if let ResponseKind::HeartbeatResponse(response) = response {
            Ok(response)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn fetch(&self, node: &Node, request: FetchRequest) -> Result<FetchResponse> {
        let request = RequestKind::FetchRequest(request);
        let response = self.manager.invoke(node.address(), request).await?;
        if let ResponseKind::FetchResponse(response) = response {
            Ok(response)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn topics_metadata(&self, topics: Vec<TopicName>) -> Result<()> {
        let mut request = MetadataRequest::default();
        let mut metadata_topics = Vec::with_capacity(topics.len());
        for topic_name in topics {
            let metadata_topic = MetadataRequestTopic {
                name: Some(topic_name),
                ..Default::default()
            };
            metadata_topics.push(metadata_topic);
        }

        request.topics = Some(metadata_topics);
        let request = RequestKind::MetadataRequest(request);
        let response = self.manager.invoke(&self.manager.url, request).await?;
        if let ResponseKind::MetadataResponse(metadata) = response {
            self.cluster_meta.merge_meta(metadata)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }
}

#[derive(Clone)]
pub struct PartitionClient<'a, Exe: Executor> {
    pub topic: &'a String,
    pub partition: i32,
    pub connection: Arc<Connection<Exe>>,
}
