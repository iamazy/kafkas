pub mod coordinator;
pub mod partition_assignor;

use std::collections::{BTreeMap, HashMap};

use dashmap::{DashMap, DashSet};
use kafka_protocol::{
    messages::{GroupId, TopicName},
    protocol::StrBytes,
};

use crate::metadata::TopicPartition;

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
pub struct RebalanceConfig {
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub heartbeat_interval_ms: i32,
    pub retry_backoff_ms: i64,
    pub leave_group_on_close: bool,
}

impl Default for RebalanceConfig {
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
    topics: DashSet<TopicName>,
    offset_strategy: OffsetStrategy,
    offset_metadata: DashMap<TopicPartition, OffsetMetadata>,
    assignment: HashMap<TopicName, Vec<i32>>,
}

impl SubscriptionState {
    pub fn partitions(&self) -> HashMap<TopicName, Vec<i32>> {
        self.assignment.clone()
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
    fetch_state: FetchState,
    position: i64,
    high_water_mark: i64,
    log_start_offset: i64,
    last_stable_offset: i64,
    paused: bool,
    offset_strategy: OffsetStrategy,
    next_allowed_retry_time_ms: i64,
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
