pub mod coordinator;
pub mod fetcher;
pub mod partition_assignor;

use std::collections::{BTreeMap, HashMap, HashSet};

use kafka_protocol::{
    messages::{GroupId, TopicName},
    protocol::StrBytes,
};

use crate::{metadata::Node, PartitionId};

/// High-level consumer record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: BTreeMap<String, Vec<u8>>,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub enum OffsetStrategy {
    Latest,
    Earliest,
    At(u64),
}

impl Default for OffsetStrategy {
    fn default() -> Self {
        OffsetStrategy::Earliest
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerOptions {}

#[derive(Debug, Clone)]
pub struct RebalanceOptions {
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub heartbeat_interval_ms: i32,
    pub retry_backoff_ms: i64,
    pub leave_group_on_close: bool,
}

impl Default for RebalanceOptions {
    fn default() -> Self {
        Self {
            session_timeout_ms: 30_000,
            // max.poll.interval.ms
            rebalance_timeout_ms: 300_000,
            heartbeat_interval_ms: 3000,
            retry_backoff_ms: 100,
            leave_group_on_close: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SubscriptionState {
    subscription_type: SubscriptionType,
    topics: HashSet<TopicName>,
    offset_strategy: OffsetStrategy,
    assignments: HashMap<TopicName, Vec<TopicPartitionState>>,
}

impl SubscriptionState {
    pub fn partitions(&self) -> HashMap<TopicName, Vec<PartitionId>> {
        let mut partitions = HashMap::with_capacity(self.assignments.len());
        for (topic, partition_states) in self.assignments.iter() {
            partitions.insert(
                topic.clone(),
                partition_states.iter().map(|p| p.partition).collect(),
            );
        }
        partitions
    }
}

#[derive(Debug, Clone, Copy)]
enum SubscriptionType {
    None,
    AutoTopics,
    AutoPattern,
    UserAssigned,
}

impl Default for SubscriptionType {
    fn default() -> Self {
        SubscriptionType::None
    }
}

#[derive(Debug, Clone, Default)]
pub struct OffsetMetadata {
    committed_offset: i64,
    committed_leader_epoch: i32,
    metadata: Option<StrBytes>,
}

#[derive(Debug, Clone, Default)]
pub struct TopicPartitionState {
    partition: PartitionId,
    fetch_state: FetchState,
    position: FetchPosition,
    high_water_mark: i64,
    log_start_offset: i64,
    last_stable_offset: i64,
    paused: bool,
    offset_strategy: OffsetStrategy,
    next_allowed_retry_time_ms: i64,
}

impl TopicPartitionState {
    pub fn new(partition: PartitionId) -> Self {
        Self {
            partition,
            ..Default::default()
        }
    }

    pub fn partition(&self) -> PartitionId {
        self.partition
    }
}

#[derive(Debug, Clone)]
pub enum FetchState {
    Initializing,
    Fetching,
    AwaitReset,
    AwaitValidation,
}

impl Default for FetchState {
    fn default() -> Self {
        Self::Initializing
    }
}

#[derive(Debug, Clone, Default)]
pub struct FetchPosition {
    offset: i64,
    offset_epoch: Option<i32>,
    current_leader: LeaderAndEpoch,
}

impl FetchPosition {
    pub fn new(offset: i64, epoch: Option<i32>) -> Self {
        Self {
            offset,
            offset_epoch: epoch,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct LeaderAndEpoch {
    leader: Option<Node>,
    epoch: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupMetadata {
    group_id: GroupId,
    generation_id: i32,
    member_id: StrBytes,
    leader: StrBytes,
    group_instance_id: Option<StrBytes>,
    protocol_name: Option<StrBytes>,
    protocol_type: Option<StrBytes>,
}

impl ConsumerGroupMetadata {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            generation_id: -1,
            member_id: StrBytes::default(),
            leader: StrBytes::default(),
            group_instance_id: None,
            protocol_name: None,
            protocol_type: None,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

impl From<IsolationLevel> for i8 {
    fn from(value: IsolationLevel) -> Self {
        match value {
            IsolationLevel::ReadCommitted => 1,
            IsolationLevel::ReadUncommitted => 0,
        }
    }
}
