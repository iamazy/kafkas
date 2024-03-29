use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use kafka_protocol::{
    messages::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        metadata_request::MetadataRequestTopic, ApiKey, DescribeGroupsRequest,
        DescribeGroupsResponse, FetchRequest, FetchResponse, FindCoordinatorRequest,
        FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
        JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, ListOffsetsRequest,
        ListOffsetsResponse, MetadataRequest, OffsetCommitRequest, OffsetCommitResponse,
        OffsetFetchRequest, OffsetFetchResponse, ProduceRequest, ProduceResponse, RequestKind,
        ResponseKind, SyncGroupRequest, SyncGroupResponse, TopicName,
    },
    protocol::VersionRange,
    records::Record,
};
use uuid::Uuid;

use crate::{
    connection_manager::{ConnectionManager, OperationRetryOptions},
    error::{ConnectionError, Error, Result},
    executor::Executor,
    metadata::{Cluster, Node},
    PartitionRef, ToStrBytes,
};

/// Helper trait for consumer deserialization
pub trait DeserializeMessage {
    /// type produced from the message
    type Output: Sized;
    /// deserialize method that will be called by the consumer
    fn deserialize_message(record: Record) -> Self::Output;
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
    pub manager: Arc<ConnectionManager<Exe>>,
    pub operation_retry_options: OperationRetryOptions,
    pub executor: Arc<Exe>,
    pub cluster: Arc<Cluster>,
    supported_versions: Arc<DashMap<i16, VersionRange>>,
}

#[derive(Debug, Clone)]
pub struct KafkaOptions {
    pub client_id: Option<String>,
    pub request_timeout_ms: i32,
}

impl Default for KafkaOptions {
    fn default() -> Self {
        Self {
            client_id: Some("default".into()),
            request_timeout_ms: 1,
        }
    }
}

impl KafkaOptions {
    pub fn client_id<S: Into<String>>(&mut self, client_id: S) {
        self.client_id = Some(client_id.into());
    }
}

impl<Exe: Executor> Kafka<Exe> {
    pub async fn new<S: Into<String>>(
        url: S,
        options: KafkaOptions,
        executor: Exe,
    ) -> Result<Self> {
        let url: String = url.into();
        let executor = Arc::new(executor);
        let operation_retry_options = OperationRetryOptions::default();

        let manager = ConnectionManager::new(
            url,
            options,
            None,
            operation_retry_options.clone(),
            executor.clone(),
        )
        .await?;

        let api_versions_response = Self::api_version(&manager).await?;
        let supported_versions = DashMap::with_capacity(api_versions_response.api_keys.len());
        for (k, v) in api_versions_response.api_keys.iter() {
            supported_versions.insert(
                *k,
                VersionRange {
                    min: v.min_version,
                    max: v.max_version,
                },
            );
        }

        let manager = Arc::new(manager);

        let weak_manager = Arc::downgrade(&manager);
        let mut interval = executor.interval(std::time::Duration::from_secs(60));
        executor.spawn(Box::pin(async move {
            while let Some(()) = interval.next().await {
                match weak_manager.upgrade() {
                    Some(strong_manager) => strong_manager.check_connections().await,
                    None => break,
                }
            }
        }));

        Ok(Kafka {
            manager,
            operation_retry_options,
            executor,
            cluster: Arc::new(Cluster::default()),
            supported_versions: Arc::new(supported_versions),
        })
    }

    pub fn topic_id(&self, topic_name: &TopicName) -> Uuid {
        match self.cluster.topic_id(topic_name) {
            Some(topic_id) => topic_id,
            None => Uuid::nil(),
        }
    }

    pub fn partitions(&self, topic: &TopicName) -> Result<PartitionRef> {
        self.cluster.partitions(topic)
    }

    pub fn version_range(&self, key: ApiKey) -> Option<VersionRange> {
        self.supported_versions
            .get(&(key as i16))
            .map(|v| *v.value())
    }
}

macro_rules! invoke_request {
    ($manager:ident, $request:ident, $response:ident) => {
        match $manager.invoke(&$manager.url, $request).await? {
            ResponseKind::$response(response) => Ok(response),
            res => Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{res:?}"),
            ))),
        }
    };
    ($self:ident, $addr:ident, $request:ident, $response:ident) => {
        match $self.manager.invoke(&$addr, $request).await? {
            ResponseKind::$response(response) => Ok(response),
            res => Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{res:?}"),
            ))),
        }
    };
}

impl<Exe: Executor> Kafka<Exe> {
    async fn api_version(manager: &ConnectionManager<Exe>) -> Result<ApiVersionsResponse> {
        let request = RequestKind::ApiVersionsRequest(Self::api_version_builder()?);
        invoke_request!(manager, request, ApiVersionsResponse)
    }

    pub async fn produce(&self, node: &Node, request: ProduceRequest) -> Result<ProduceResponse> {
        let request = RequestKind::ProduceRequest(request);
        let addr = node.address();
        invoke_request!(self, addr, request, ProduceResponse)
    }

    pub async fn find_coordinator(
        &self,
        request: FindCoordinatorRequest,
    ) -> Result<FindCoordinatorResponse> {
        let request = RequestKind::FindCoordinatorRequest(request);
        let addr = &self.manager.url;
        invoke_request!(self, addr, request, FindCoordinatorResponse)
    }

    pub async fn describe_groups(
        &self,
        node: &Node,
        request: DescribeGroupsRequest,
    ) -> Result<DescribeGroupsResponse> {
        let request = RequestKind::DescribeGroupsRequest(request);
        let addr = node.address();
        invoke_request!(self, addr, request, DescribeGroupsResponse)
    }

    pub async fn join_group(
        &self,
        node: &Node,
        request: JoinGroupRequest,
    ) -> Result<JoinGroupResponse> {
        let request = RequestKind::JoinGroupRequest(request);
        let addr = node.address();
        invoke_request!(self, addr, request, JoinGroupResponse)
    }

    pub async fn leave_group(
        &self,
        node: &Node,
        request: LeaveGroupRequest,
    ) -> Result<LeaveGroupResponse> {
        let request = RequestKind::LeaveGroupRequest(request);
        let addr = node.address();
        invoke_request!(self, addr, request, LeaveGroupResponse)
    }

    pub async fn sync_group(
        &self,
        node: &Node,
        request: SyncGroupRequest,
    ) -> Result<SyncGroupResponse> {
        let request = RequestKind::SyncGroupRequest(request);
        let addr = node.address();
        invoke_request!(self, addr, request, SyncGroupResponse)
    }

    pub async fn offset_fetch(
        &self,
        node: &Node,
        request: OffsetFetchRequest,
    ) -> Result<OffsetFetchResponse> {
        let request = RequestKind::OffsetFetchRequest(request);
        let addr = node.address();
        invoke_request!(self, addr, request, OffsetFetchResponse)
    }

    pub async fn offset_commit(
        &self,
        node: &Node,
        request: OffsetCommitRequest,
    ) -> Result<OffsetCommitResponse> {
        let request = RequestKind::OffsetCommitRequest(request);
        let addr = node.address();
        invoke_request!(self, addr, request, OffsetCommitResponse)
    }

    pub async fn list_offsets(
        &self,
        node: &Node,
        request: ListOffsetsRequest,
    ) -> Result<ListOffsetsResponse> {
        let request = RequestKind::ListOffsetsRequest(request);
        let addr = node.address();
        invoke_request!(self, addr, request, ListOffsetsResponse)
    }

    pub async fn heartbeat(
        &self,
        node: &Node,
        request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse> {
        let request = RequestKind::HeartbeatRequest(request);
        let addr = node.address();
        invoke_request!(self, addr, request, HeartbeatResponse)
    }

    pub async fn fetch(&self, node: &Node, request: FetchRequest) -> Result<FetchResponse> {
        let request = RequestKind::FetchRequest(request);
        let addr = node.address();
        invoke_request!(self, addr, request, FetchResponse)
    }

    pub async fn update_metadata(&self, topics: Vec<TopicName>) -> Result<()> {
        let mut request = MetadataRequest::default();
        let mut metadata_topics = Vec::with_capacity(topics.len());
        for topic_name in topics {
            let mut metadata_topic = MetadataRequestTopic::default();
            metadata_topic.name = Some(topic_name);
            metadata_topics.push(metadata_topic);
        }

        request.topics = Some(metadata_topics);
        let request = RequestKind::MetadataRequest(request);
        let response = self.manager.invoke(&self.manager.url, request).await?;
        if let ResponseKind::MetadataResponse(metadata) = response {
            self.cluster.update_metadata(metadata)
        } else {
            Err(Error::Connection(ConnectionError::UnexpectedResponse(
                format!("{response:?}"),
            )))
        }
    }

    pub async fn update_full_metadata(&self) -> Result<()> {
        let mut topics = Vec::with_capacity(self.cluster.topics.len());
        for topic in self.cluster.topics.iter() {
            topics.push(topic.key().clone());
        }
        self.update_metadata(topics).await
    }
}

impl<Exe: Executor> Kafka<Exe> {
    const PKG_VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const PKG_NAME: &'static str = env!("CARGO_PKG_NAME");

    pub fn api_version_builder() -> Result<ApiVersionsRequest> {
        let mut request = ApiVersionsRequest::default();
        request.client_software_name = Self::PKG_NAME.to_string().to_str_bytes();
        request.client_software_version = Self::PKG_VERSION.to_string().to_str_bytes();
        Ok(request)
    }
}
