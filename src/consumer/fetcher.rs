use std::{cell::RefCell, collections::VecDeque, sync::Arc};

use dashmap::DashMap;
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{
        fetch_request::{FetchPartition, FetchRequestBuilder, FetchTopic},
        BrokerId,
    },
    records::{Record, RecordBatchDecoder},
};
use tracing::error;

use crate::{
    client::Kafka,
    consumer::{IsolationLevel, SubscriptionState},
    error::Result,
    executor::Executor,
    NodeId, PartitionId,
};

const INITIAL_EPOCH: i32 = 0;
const FINAL_EPOCH: i32 = -1;
const INVALID_SESSION_ID: i32 = 0;

pub struct Fetcher<Exe: Executor> {
    client: Kafka<Exe>,
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
    pub fn new(client: Kafka<Exe>, subscription: Arc<RefCell<SubscriptionState>>) -> Self {
        let sessions = DashMap::new();
        for node in client.cluster_meta.nodes.iter() {
            sessions.insert(node.id, FetchSession::new(node.id));
        }

        Self {
            client,
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
        for mut entry in self.sessions.iter_mut() {
            let node_id = entry.node;
            let session = entry.value_mut();
            if let Some(node) = self.client.cluster_meta.nodes.get(&node_id) {
                let fetch_response = self
                    .client
                    .fetch(node.value(), self.fetch_builder(node.id, session)?.build()?)
                    .await?;
                if fetch_response.error_code.is_ok() {
                    session.next_metadata.session_id = fetch_response.session_id;
                    for fetchable_topic in fetch_response.responses {
                        if let Some(partition_states) = self
                            .subscription
                            .borrow_mut()
                            .assignments
                            .get_mut(&fetchable_topic.topic)
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
                                        if let Some(mut records) = partition.records {
                                            let records = RecordBatchDecoder::decode(&mut records)?;
                                            if let Some(record) =
                                                records.iter().max_by_key(|record| record.offset)
                                            {
                                                partition_state.position.offset = record.offset;
                                                partition_state.position.current_leader.epoch =
                                                    Some(record.partition_leader_epoch);
                                            }

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
                }
            }
        }
        Ok(())
    }
}

impl<Exe: Executor> Fetcher<Exe> {
    fn fetch_builder(&self, node: NodeId, session: &FetchSession) -> Result<FetchRequestBuilder> {
        let mut builder = FetchRequestBuilder::default();
        builder
            .cluster_id(None)
            .session_id(session.next_metadata.session_id)
            .session_epoch(session.next_metadata.epoch)
            .replica_id(BrokerId(node))
            .max_wait_ms(self.max_wait_ms)
            .min_bytes(self.min_bytes)
            .max_bytes(self.max_bytes)
            .isolation_level(IsolationLevel::ReadCommitted.into())
            .rack_id(Default::default())
            .forgotten_topics_data(Default::default())
            .unknown_tagged_fields(Default::default());

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
                    let partition = FetchPartition {
                        partition: partition_state.partition,
                        current_leader_epoch: partition_state
                            .position
                            .current_leader
                            .epoch
                            .unwrap_or(-1),
                        fetch_offset: partition_state.position.offset,
                        log_start_offset: partition_state.log_start_offset,
                        last_fetched_epoch: partition_state.position.offset_epoch.unwrap_or(-1),
                        partition_max_bytes: self.max_bytes,
                        ..Default::default()
                    };
                    partitions.push(partition);
                }

                let topic = FetchTopic {
                    topic: topic_name.clone(),
                    partitions,
                    ..Default::default()
                };
                topics.push(topic);
                break;
            }
        }

        builder.topics(topics);
        Ok(builder)
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
}

impl FetchSession {
    pub fn new(node: i32) -> Self {
        Self {
            node,
            next_metadata: FetchMetadata {
                session_id: INVALID_SESSION_ID,
                epoch: INITIAL_EPOCH,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchedRecords {
    partition: PartitionId,
    records: Vec<Record>,
}
