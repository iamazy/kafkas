use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    fmt::{Display, Formatter},
    sync::Arc,
};

use dashmap::DashMap;
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{
        fetch_request::{FetchPartition, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        ApiKey, BrokerId, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
        TopicName,
    },
    records::{Record, RecordBatchDecoder, NO_PARTITION_LEADER_EPOCH},
    ResponseError,
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    client::Kafka,
    consumer::{FetchPosition, IsolationLevel, OffsetResetStrategy, SubscriptionState},
    error::Result,
    executor::Executor,
    metadata::{Node, TopicPartition},
    Error, NodeId, PartitionId, UNKNOWN_EPOCH, UNKNOWN_OFFSET,
};

const INITIAL_EPOCH: i32 = 0;
const FINAL_EPOCH: i32 = -1;
const INVALID_SESSION_ID: i32 = 0;

pub struct Fetcher<Exe: Executor> {
    client: Kafka<Exe>,
    timestamp: i64,
    min_bytes: i32,
    max_bytes: i32,
    max_wait_ms: i32,
    fetch_size: i32,
    retry_backoff_ms: i64,
    request_timeout_ms: i64,
    max_poll_records: i32,
    check_crcs: bool,
    client_rack_id: String,
    subscription: Arc<RefCell<SubscriptionState>>,
    sessions: DashMap<i32, FetchSession>,
    completed_fetch: VecDeque<FetchedRecords>,
}

impl<Exe: Executor> Fetcher<Exe> {
    pub fn new(
        client: Kafka<Exe>,
        timestamp: i64,
        subscription: Arc<RefCell<SubscriptionState>>,
    ) -> Self {
        let sessions = DashMap::new();
        for node in client.cluster_meta.nodes.iter() {
            sessions.insert(node.id, FetchSession::new(node.id));
        }

        Self {
            client,
            timestamp,
            min_bytes: 1,
            max_bytes: 52428800, // 50 * 1024 * 1024,
            max_wait_ms: 500,
            fetch_size: 1048576, // 1 * 1024 * 1024
            retry_backoff_ms: 100,
            request_timeout_ms: 30000,
            max_poll_records: 500,
            check_crcs: false,
            client_rack_id: String::default(),
            subscription,
            sessions,
            completed_fetch: VecDeque::new(),
        }
    }

    pub async fn fetch(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::FetchKey) {
            let version = version_range.max;

            for mut entry in self.sessions.iter_mut() {
                let node_id = entry.node;
                let session = entry.value_mut();
                if let Some(node) = self.client.cluster_meta.nodes.get(&node_id) {
                    let fetch_response = self
                        .client
                        .fetch(node.value(), self.fetch_builder(node.id, session, version)?)
                        .await?;

                    for fetchable_topic in fetch_response.responses {
                        let topic = if let Some(topic_name) =
                            session.topic_names.get(&fetchable_topic.topic_id)
                        {
                            topic_name
                        } else {
                            &fetchable_topic.topic
                        };

                        let default_offset_strategy =
                            self.subscription.borrow().default_offset_strategy;
                        for partition in fetchable_topic.partitions {
                            if let Some(partition_state) =
                                self.subscription.borrow_mut().raw_assignment.get_mut(
                                    &TopicPartition::new(topic.clone(), partition.partition_index),
                                )
                            {
                                if partition.error_code.is_ok() {
                                    partition_state.last_stable_offset =
                                        partition.last_stable_offset;
                                    partition_state.log_start_offset = partition.log_start_offset;
                                    partition_state.high_water_mark = partition.high_watermark;

                                    debug!(
                                        "Fetch topic [{} - {}] success, last stable offset: {}, \
                                         log start offset: {}, high_water_mark: {}",
                                        topic.0,
                                        partition.partition_index,
                                        partition.last_stable_offset,
                                        partition.log_start_offset,
                                        partition.high_watermark
                                    );

                                    // decode record batch
                                    if let Some(mut records) = partition.records {
                                        let records = RecordBatchDecoder::decode(&mut records)?;
                                        if let Some(record) =
                                            records.iter().max_by_key(|record| record.offset)
                                        {
                                            partition_state.position.offset = record.offset;
                                            partition_state.position.current_leader.epoch =
                                                Some(record.partition_leader_epoch);
                                        }

                                        debug!(
                                            "Fetch topic [{} - {}] records success, records size: \
                                             {}",
                                            topic.0,
                                            partition.partition_index,
                                            records.len()
                                        );

                                        let fetched_records = FetchedRecords {
                                            partition: partition.partition_index,
                                            records,
                                        };
                                        self.completed_fetch.push_back(fetched_records);
                                    }
                                } else {
                                    error!(
                                        "Fetch topic [{} - {}] error: {}",
                                        topic.0,
                                        partition.partition_index,
                                        partition.error_code.err().unwrap()
                                    );
                                    partition_state.reset(default_offset_strategy)?;
                                }
                            }
                        }
                    }

                    if (7..=13).contains(&version) {
                        if fetch_response.error_code.is_ok() {
                            session.next_metadata.session_id = fetch_response.session_id;
                        } else {
                            error!(
                                "fetch records error: {}",
                                fetch_response.error_code.err().unwrap()
                            );
                            return Err(Error::Response {
                                error: fetch_response.error_code.err().unwrap(),
                                msg: None,
                            });
                        }
                    }
                }
            }
            Ok(())
        } else {
            Err(Error::InvalidApiRequest(ApiKey::FetchKey))
        }
    }

    pub async fn reset_offset(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::ListOffsetsKey) {
            let version = version_range.max;
            let partitions = self
                .subscription
                .borrow()
                .partitions_need_reset(self.timestamp);
            if partitions.is_empty() {
                return Ok(());
            }

            let mut offset_reset_timestamps = HashMap::new();
            for partition in partitions {
                if let Some(tp_state) = self.subscription.borrow().raw_assignment.get(&partition) {
                    let timestamp = tp_state.offset_strategy.strategy_timestamp();
                    if timestamp != 0 {
                        offset_reset_timestamps.insert(partition, timestamp);
                    }
                }
            }

            let requests =
                self.group_list_offsets_request(&mut offset_reset_timestamps, version)?;
            for (node, request) in requests {
                let response = self.client.list_offsets(&node, request).await?;
                let list_offset_result = self.handle_list_offsets_response(response)?;
                if !list_offset_result.partitions_to_retry.is_empty() {
                    self.subscription.borrow_mut().request_failed(
                        list_offset_result.partitions_to_retry,
                        self.timestamp + self.retry_backoff_ms,
                    );
                    // TODO: metadata request update
                }

                for (tp, list_offsets_data) in list_offset_result.fetched_offsets {
                    if let Some(timestamp) = offset_reset_timestamps.get(&tp) {
                        self.reset_offset_if_needed(
                            tp,
                            OffsetResetStrategy::from_timestamp(*timestamp),
                            list_offsets_data,
                        )?;
                    }
                }
            }

            Ok(())
        } else {
            Err(Error::InvalidApiRequest(ApiKey::ListOffsetsKey))
        }
    }
}

impl<Exe: Executor> Fetcher<Exe> {
    fn topic_id(&self, topic_name: &TopicName, session: &mut FetchSession) -> Uuid {
        if let Some(topic_id) = self.client.cluster_meta.topic_id(topic_name) {
            if !session.topic_names.contains_key(&topic_id) {
                session.add_topic(topic_id, topic_name.clone());
            }
            topic_id
        } else {
            Uuid::nil()
        }
    }

    fn reset_offset_if_needed(
        &mut self,
        partition: TopicPartition,
        offset_strategy: OffsetResetStrategy,
        offset_data: ListOffsetData,
    ) -> Result<()> {
        let position = FetchPosition {
            offset: offset_data.offset,
            offset_epoch: None,
            current_leader: self.client.cluster_meta.current_leader(&partition),
        };
        // TODO: metadata update last seen epoch if newer
        self.subscription
            .borrow_mut()
            .maybe_seek_unvalidated(partition, position, offset_strategy)
    }

    fn handle_list_offsets_response(
        &mut self,
        response: ListOffsetsResponse,
    ) -> Result<ListOffsetResult> {
        let mut fetched_offsets = HashMap::new();
        let mut partitions_to_retry = Vec::new();
        let mut unauthorized_topics = Vec::new();

        for topic in response.topics {
            for partition_response in topic.partitions {
                let partition = partition_response.partition_index;
                match partition_response.error_code.err() {
                    None => {
                        if !partition_response.old_style_offsets.is_empty() {
                            // Handle v0 response with offsets
                            let offset = if partition_response.old_style_offsets.len() > 1 {
                                let len = partition_response.old_style_offsets.len();
                                error!("Unexpected partition {partition} response of length {len}");
                                return Err(Error::Custom(format!(
                                    "Unexpected partition {partition} response of length {len}"
                                )));
                            } else {
                                partition_response.old_style_offsets[0]
                            };
                            debug!(
                                "Handing v0 ListOffsetResponse response for [{:?} - {}]. Fetched \
                                 offset {offset}",
                                &topic.name, partition
                            );
                            if offset != UNKNOWN_OFFSET {
                                fetched_offsets.insert(
                                    TopicPartition::new(topic.name, partition),
                                    ListOffsetData {
                                        offset,
                                        timestamp: None,
                                        leader_epoch: None,
                                    },
                                );
                            }
                        } else {
                            // Handle v1 and later response or v0 without offsets
                            debug!(
                                "Handling ListOffsetResponse response for [{} - {}], Fetched \
                                 offset {}, timestamp {}",
                                topic.name.0,
                                partition,
                                partition_response.offset,
                                partition_response.timestamp
                            );
                            if partition_response.offset != UNKNOWN_OFFSET {
                                let leader_epoch =
                                    if partition_response.leader_epoch == UNKNOWN_EPOCH {
                                        None
                                    } else {
                                        Some(partition_response.leader_epoch)
                                    };
                                fetched_offsets.insert(
                                    TopicPartition::new(topic.name, partition),
                                    ListOffsetData {
                                        offset: partition_response.offset,
                                        timestamp: Some(partition_response.timestamp),
                                        leader_epoch,
                                    },
                                );
                            }
                        }
                        break;
                    }
                    Some(ResponseError::UnsupportedForMessageFormat) => {
                        // The message format on the broker side is before 0.10.0, which means it
                        // does not support timestamps. We treat this case
                        // the same as if we weren't able to find an
                        // offset corresponding to the requested timestamp and leave it out of the
                        // result.
                        debug!(
                            "Cannot search by timestamp for [{} - {}] because the message format \
                             version is before 0.10.0",
                            topic.name.0, partition
                        );
                        break;
                    }
                    Some(
                        error @ ResponseError::NotLeaderOrFollower
                        | error @ ResponseError::ReplicaNotAvailable
                        | error @ ResponseError::OffsetNotAvailable
                        | error @ ResponseError::KafkaStorageError
                        | error @ ResponseError::LeaderNotAvailable
                        | error @ ResponseError::FencedLeaderEpoch
                        | error @ ResponseError::UnknownLeaderEpoch,
                    ) => {
                        debug!(
                            "Attempt to fetch offsets for [{} - {}] failed due to {}, retrying.",
                            topic.name.0, partition, error
                        );
                        partitions_to_retry
                            .push(TopicPartition::new(topic.name.clone(), partition));
                    }
                    Some(ResponseError::UnknownTopicOrPartition) => {
                        warn!(
                            "Received unknown topic or partition error in ListOffset request for \
                             partition [{} - {}]",
                            topic.name.0, partition
                        );
                        partitions_to_retry
                            .push(TopicPartition::new(topic.name.clone(), partition));
                    }
                    Some(ResponseError::TopicAuthorizationFailed) => {
                        unauthorized_topics.push(topic.name.clone());
                    }
                    Some(error) => {
                        warn!(
                            "Attempt to fetch offsets for [{} - {}] failed due to unexpected \
                             exception: {}, retrying.",
                            topic.name.0, partition, error
                        );
                        partitions_to_retry
                            .push(TopicPartition::new(topic.name.clone(), partition));
                    }
                }
            }
        }

        if !unauthorized_topics.is_empty() {
            Err(Error::TopicAuthorizationError {
                topics: unauthorized_topics,
            })
        } else {
            Ok(ListOffsetResult {
                fetched_offsets,
                partitions_to_retry,
            })
        }
    }

    fn group_list_offsets_request(
        &self,
        offset_reset_timestamps: &mut HashMap<TopicPartition, i64>,
        version: i16,
    ) -> Result<HashMap<Node, ListOffsetsRequest>> {
        let mut node_request = HashMap::new();
        for node_entry in self.client.cluster_meta.nodes.iter() {
            if let Ok(node_topology) = self.client.cluster_meta.drain_node(node_entry.value().id) {
                let partitions = node_topology.value();

                let mut topics = HashMap::new();
                for partition in partitions {
                    if let Some(timestamp) = offset_reset_timestamps.get_mut(partition) {
                        topics.insert(partition, *timestamp);
                    }
                }

                self.subscription.borrow_mut().set_next_allowed_retry(
                    topics.keys(),
                    self.timestamp + self.request_timeout_ms,
                );
                let request = self.list_offsets_builder(topics, version)?;
                node_request.insert(node_entry.value().clone(), request);
            }
        }
        Ok(node_request)
    }

    fn offset_reset_strategy_timestamp(&self, partition: &TopicPartition) -> Option<i64> {
        let reset_strategy = self
            .subscription
            .borrow()
            .raw_assignment
            .get(partition)
            .map(|tp_state| tp_state.offset_strategy);
        reset_strategy.map(|strategy| strategy.strategy_timestamp())
    }

    fn fetch_topic_builder(
        &self,
        node: NodeId,
        session: &mut FetchSession,
        version: i16,
    ) -> Result<Vec<FetchTopic>> {
        let mut topics: HashMap<TopicName, Vec<FetchPartition>> = HashMap::new();
        let tp_in_node = self.client.cluster_meta.drain_node(node)?;
        for tp in tp_in_node.iter() {
            if let Some(tp_state) = self.subscription.borrow().raw_assignment.get(tp) {
                let mut partition = FetchPartition::default();
                partition.partition = tp_state.partition;
                partition.fetch_offset = tp_state.position.offset;
                partition.partition_max_bytes = self.max_bytes;

                if version >= 5 {
                    partition.log_start_offset = tp_state.last_stable_offset;
                }

                if version >= 9 {
                    partition.current_leader_epoch =
                        tp_state.position.current_leader.epoch.unwrap_or(-1);
                }

                if version >= 12 {
                    partition.last_fetched_epoch = tp_state.position.offset_epoch.unwrap_or(-1);
                }

                debug!(
                    "Fetch topic [{} - {}] records with offset: {}, log_start_offset: {}, current \
                     leader epoch: {}, last fetched epoch: {}",
                    tp.topic.0,
                    partition.partition,
                    partition.fetch_offset,
                    partition.log_start_offset,
                    partition.current_leader_epoch,
                    partition.last_fetched_epoch
                );

                if let Some(partitions) = topics.get_mut(&tp.topic) {
                    partitions.push(partition);
                } else {
                    let partitions = vec![partition];
                    topics.insert(tp.topic.clone(), partitions);
                }
            }
        }

        let mut fetch_topics = Vec::with_capacity(topics.len());
        for (topic, partitions) in topics {
            let topic_id = self.topic_id(&topic, session);
            let topic = FetchTopic {
                topic,
                topic_id,
                partitions,
                ..Default::default()
            };
            fetch_topics.push(topic);
        }
        Ok(fetch_topics)
    }

    fn fetch_builder(
        &self,
        node: NodeId,
        session: &mut FetchSession,
        version: i16,
    ) -> Result<FetchRequest> {
        let mut request = FetchRequest::default();
        if version <= 13 {
            request.replica_id = BrokerId(-1);
            request.max_wait_ms = self.max_wait_ms;
            request.min_bytes = self.min_bytes;
            request.topics = self.fetch_topic_builder(node, session, version)?;

            if version >= 3 {
                request.max_bytes = self.max_bytes;
            }
            if version >= 4 {
                request.isolation_level = IsolationLevel::ReadUncommitted.into();
            }
            if version >= 7 {
                request.session_id = session.next_metadata.session_id;
                request.session_epoch = session.next_metadata.epoch;
                request.forgotten_topics_data = Default::default();
            }
            if version >= 11 {
                request.rack_id = Default::default();
            }
            if version >= 12 {
                request.cluster_id = None;
            }
        }
        Ok(request)
    }

    pub fn list_offsets_builder(
        &self,
        assignments: HashMap<&TopicPartition, i64>,
        version: i16,
    ) -> Result<ListOffsetsRequest> {
        let mut topics: HashMap<TopicName, Vec<ListOffsetsPartition>> = HashMap::new();
        for (partition, timestamp) in assignments {
            let list_offsets_partition = ListOffsetsPartition {
                partition_index: partition.partition,
                current_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                timestamp,
                ..Default::default()
            };

            if let Some(partitions) = topics.get_mut(&partition.topic) {
                partitions.push(list_offsets_partition);
            } else {
                let partitions = vec![list_offsets_partition];
                topics.insert(partition.topic.clone(), partitions);
            }
        }

        let mut list_offsets_topics = Vec::with_capacity(topics.len());
        for (name, partitions) in topics {
            list_offsets_topics.push(ListOffsetsTopic {
                name,
                partitions,
                ..Default::default()
            });
        }
        let mut request = ListOffsetsRequest {
            replica_id: BrokerId(-1),
            topics: list_offsets_topics,
            ..Default::default()
        };

        if version >= 2 {
            request.isolation_level = IsolationLevel::ReadUncommitted.into();
        }
        Ok(request)
    }
}

#[derive(Debug, Clone, Copy, Hash)]
pub struct FetchMetadata {
    session_id: i32,
    epoch: i32,
}

impl Display for FetchMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.session_id == INVALID_SESSION_ID {
            write!(f, "(session_id=INVALID, ")?;
        } else {
            write!(f, "(session_id={}, ", self.session_id)?;
        }

        if self.epoch == INITIAL_EPOCH {
            write!(f, "epoch=INITIAL)")
        } else if self.epoch == FINAL_EPOCH {
            write!(f, "epoch=FINAL)")
        } else {
            write!(f, "epoch={})", self.epoch)
        }
    }
}

impl FetchMetadata {
    pub fn new(session_id: i32, epoch: i32) -> Self {
        Self { session_id, epoch }
    }

    pub fn initial() -> Self {
        Self::new(INVALID_SESSION_ID, INITIAL_EPOCH)
    }

    pub fn is_full(&self) -> bool {
        self.epoch == INITIAL_EPOCH || self.epoch == FINAL_EPOCH
    }

    pub fn new_incremental(session_id: i32) -> Self {
        Self::new(session_id, next_epoch(INITIAL_EPOCH))
    }

    pub fn next_incremental(self) -> Self {
        Self::new(self.session_id, next_epoch(self.epoch))
    }

    pub fn next_close_existing(self) -> Self {
        Self::new(self.session_id, INITIAL_EPOCH)
    }
}

fn next_epoch(prev_epoch: i32) -> i32 {
    if prev_epoch < 0 {
        FINAL_EPOCH
    } else if prev_epoch == i32::MAX {
        1
    } else {
        prev_epoch + 1
    }
}

#[derive(Debug, Clone)]
pub struct FetchSession {
    node: NodeId,
    next_metadata: FetchMetadata,
    topic_names: HashMap<Uuid, TopicName>,
}

impl FetchSession {
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            next_metadata: FetchMetadata {
                session_id: INVALID_SESSION_ID,
                epoch: INITIAL_EPOCH,
            },
            topic_names: HashMap::new(),
        }
    }

    pub fn add_topic(&mut self, topic_id: Uuid, topic: TopicName) {
        if topic_id != Uuid::nil() && !topic.is_empty() {
            self.topic_names.insert(topic_id, topic);
        }
    }

    fn handle_fetch_response(&mut self, response: FetchResponse, version: u16) -> bool {
        if response.error_code.is_err() {
            let error = response.error_code.err().unwrap();
            info!(
                "Node {} was unable to process the fetch request with {}:{}.",
                self.node, self.next_metadata, error
            );
            if error == ResponseError::FetchSessionIdNotFound {
                self.next_metadata = FetchMetadata::initial();
            } else {
                self.next_metadata = self.next_metadata.next_close_existing();
            }
            return false;
        }

        false
    }
}

#[derive(Debug, Clone)]
pub struct FetchedRecords {
    partition: PartitionId,
    records: Vec<Record>,
}

struct ListOffsetResult {
    fetched_offsets: HashMap<TopicPartition, ListOffsetData>,
    partitions_to_retry: Vec<TopicPartition>,
}

/// Represents data about an offset returned by a broker.
struct ListOffsetData {
    offset: i64,
    // None if the broker does not support returning timestamps
    timestamp: Option<i64>,
    leader_epoch: Option<i32>,
}
