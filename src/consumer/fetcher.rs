use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use dashmap::DashMap;
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{
        fetch_request::{FetchPartition, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        ApiKey, BrokerId, FetchRequest, ListOffsetsRequest, TopicName,
    },
    records::{Record, RecordBatchDecoder, NO_PARTITION_LEADER_EPOCH},
};
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    client::Kafka,
    consumer::{IsolationLevel, SubscriptionState},
    error::Result,
    executor::Executor,
    Error, NodeId, PartitionId,
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

                        if let Some(partition_states) =
                            self.subscription.borrow_mut().assignments.get_mut(topic)
                        {
                            for partition in fetchable_topic.partitions {
                                if partition.error_code.is_ok() {
                                    if let Some(partition_state) = partition_states
                                        .iter_mut()
                                        .find(|p| p.partition == partition.partition_index)
                                    {
                                        partition_state.last_stable_offset =
                                            partition.last_stable_offset;
                                        partition_state.log_start_offset =
                                            partition.log_start_offset;
                                        partition_state.high_water_mark = partition.high_watermark;

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
                                                "fetch records of topic [{}] in partition {} \
                                                 success, records size: {}",
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
                                    }
                                } else {
                                    error!(
                                        "fetch partition {} error: {}",
                                        partition.partition_index,
                                        partition.error_code.err().unwrap()
                                    );
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

    // pub async fn list_offsets(&self) -> Result<()> {
    //     if let Some(version_range) = self.client.version_range(ApiKey::ListOffsetsKey) {
    //     } else {
    //         Err(Error::InvalidApiRequest(ApiKey::ListOffsetsKey))
    //     }
    // }
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

    async fn reset_offset_if_needed(&self, version: u16) -> Result<()> {
        let topics = self
            .subscription
            .borrow()
            .partitions_need_reset(self.timestamp);
        if topics.is_empty() {
            return Ok(());
        }

        let mut offset_reset_timestamps = HashMap::new();
        for (topic, partitions) in topics {
            let timestamps = self.offset_reset_strategy_timestamp(&topic, partitions);
            offset_reset_timestamps.insert(topic, timestamps);
        }

        let requests = self.group_list_offsets_request(offset_reset_timestamps, version)?;
        for (node, request) in requests {
            if let Some(node) = self.client.cluster_meta.nodes.get(&node) {
                let _response = self.client.list_offsets(node.value(), request).await?;
            }
        }

        Ok(())
    }

    fn group_list_offsets_request(
        &self,
        mut offset_reset_timestamps: HashMap<TopicName, Vec<(PartitionId, i64)>>,
        version: u16,
    ) -> Result<HashMap<NodeId, ListOffsetsRequest>> {
        let mut node_request = HashMap::new();
        for node_entry in self.client.cluster_meta.nodes.iter() {
            if let Ok(node_topology) = self.client.cluster_meta.drain_node(node_entry.value().id) {
                let node_topology = node_topology.value();

                let mut topics = HashMap::new();
                for (topic, partitions) in node_topology {
                    if let Some(partition_timestamps) = offset_reset_timestamps.get_mut(topic) {
                        let timestamps = partition_timestamps
                            .iter()
                            .filter(|(p, _)| partitions.contains(p))
                            .copied()
                            .collect::<Vec<(PartitionId, i64)>>();

                        topics.insert(topic, timestamps);
                    }
                }

                self.subscription
                    .borrow_mut()
                    .set_next_allowed_retry(&topics, self.timestamp + self.request_timeout_ms);
                let request = self.list_offsets_builder(topics, version)?;
                node_request.insert(*node_entry.key(), request);
            }
        }
        Ok(node_request)
    }

    fn offset_reset_strategy_timestamp(
        &self,
        topic: &TopicName,
        partitions: Vec<PartitionId>,
    ) -> Vec<(PartitionId, i64)> {
        let reset_strategies = self
            .subscription
            .borrow()
            .offset_reset_strategy(topic, partitions);
        let mut timestamps = Vec::with_capacity(reset_strategies.len());
        for (partition, strategy) in reset_strategies {
            let timestamp = strategy.strategy_timestamp();
            if timestamp != 0 {
                timestamps.push((partition, timestamp));
            }
        }
        timestamps
    }

    fn fetch_topic_builder(
        &self,
        node: NodeId,
        session: &mut FetchSession,
        version: i16,
    ) -> Result<Vec<FetchTopic>> {
        let mut topics = Vec::new();
        let node_ref = self.client.cluster_meta.drain_node(node)?;
        for (topic_name, partition_states) in self.subscription.borrow().assignments.iter() {
            let mut partitions = Vec::new();
            if let Some((_, all_partitions_in_node)) = node_ref
                .value()
                .iter()
                .find(|(topic, _)| topic == topic_name)
            {
                for partition_state in partition_states
                    .iter()
                    .filter(|p| all_partitions_in_node.contains(&p.partition))
                {
                    let mut partition = FetchPartition::default();
                    partition.partition = partition_state.partition;
                    partition.fetch_offset = partition_state.position.offset;
                    partition.partition_max_bytes = self.max_bytes;

                    if version >= 5 {
                        partition.log_start_offset = partition_state.last_stable_offset;
                    }

                    if version >= 9 {
                        partition.current_leader_epoch =
                            partition_state.position.current_leader.epoch.unwrap_or(-1);
                    }

                    if version >= 12 {
                        partition.last_fetched_epoch =
                            partition_state.position.offset_epoch.unwrap_or(-1);
                    }

                    debug!(
                        "fetch topic [{}] partition {} for records with offset: {}, \
                         log_start_offset: {}",
                        topic_name.0,
                        partition.partition,
                        partition.fetch_offset,
                        partition.log_start_offset
                    );
                    partitions.push(partition);
                }

                let topic = FetchTopic {
                    topic: topic_name.clone(),
                    topic_id: self.topic_id(topic_name, session),
                    partitions,
                    ..Default::default()
                };
                topics.push(topic);
            }
        }
        Ok(topics)
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
        assignments: HashMap<&TopicName, Vec<(PartitionId, i64)>>,
        version: u16,
    ) -> Result<ListOffsetsRequest> {
        let mut topics = Vec::new();
        for (topic_name, partition_list) in assignments {
            let mut partitions = Vec::new();
            for (partition, timestamp) in partition_list {
                let partition = ListOffsetsPartition {
                    partition_index: partition,
                    current_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                    timestamp,
                    ..Default::default()
                };
                partitions.push(partition);
            }
            let topic = ListOffsetsTopic {
                name: topic_name.clone(),
                partitions,
                ..Default::default()
            };
            topics.push(topic);
        }
        let mut request = ListOffsetsRequest {
            replica_id: BrokerId(-1),
            topics,
            ..Default::default()
        };

        if version >= 2 {
            request.isolation_level = IsolationLevel::ReadUncommitted.into();
        }
        Ok(request)
    }
}

#[derive(Debug, Clone, Hash)]
pub struct FetchMetadata {
    session_id: i32,
    epoch: i32,
}

impl FetchMetadata {
    pub fn new(session_id: i32, epoch: i32) -> FetchMetadata {
        Self { session_id, epoch }
    }

    pub fn is_full(&self) -> bool {
        self.epoch == INITIAL_EPOCH || self.epoch == FINAL_EPOCH
    }

    pub fn new_incremental(session_id: i32) -> Self {
        FetchMetadata {
            session_id,
            epoch: next_epoch(INITIAL_EPOCH),
        }
    }

    pub fn next_incremental(self) -> Self {
        FetchMetadata {
            session_id: self.session_id,
            epoch: next_epoch(self.epoch),
        }
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
    node: i32,
    next_metadata: FetchMetadata,
    topic_names: HashMap<Uuid, TopicName>,
}

impl FetchSession {
    pub fn new(node: i32) -> Self {
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
}

#[derive(Debug, Clone)]
pub struct FetchedRecords {
    partition: PartitionId,
    records: Vec<Record>,
}
