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

#[derive(Debug, Clone, Copy)]
pub enum OffsetResetStrategy {
    Latest,
    Earliest,
    None,
}

impl OffsetResetStrategy {
    pub fn strategy_timestamp(&self) -> i64 {
        match self {
            Self::Earliest => -2,
            Self::Latest => -1,
            _ => 0,
        }
    }
}

impl Default for OffsetResetStrategy {
    fn default() -> Self {
        OffsetResetStrategy::Earliest
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
    default_offset_strategy: OffsetResetStrategy,
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

    fn partitions_need_reset(&self, now: i64) -> HashMap<TopicName, Vec<PartitionId>> {
        let mut topics = HashMap::new();
        for (topic, partition_states) in self.assignments.iter() {
            let mut partitions = Vec::new();
            for partition_state in partition_states {
                if matches!(partition_state.fetch_state, FetchState::AwaitReset)
                    && !partition_state.awaiting_retry_backoff(now)
                {
                    partitions.push(partition_state.partition);
                }
            }
            if !partitions.is_empty() {
                topics.insert(topic.clone(), partitions);
            }
        }
        topics
    }

    pub fn offset_reset_strategy(
        &self,
        topic: &TopicName,
        partitions: Vec<PartitionId>,
    ) -> Vec<(PartitionId, OffsetResetStrategy)> {
        let mut strategies = Vec::new();
        if let Some((_, partition_states)) = self
            .assignments
            .iter()
            .find(|(topic_name, _)| *topic_name == topic)
        {
            for partition_state in partition_states {
                if partitions.contains(&partition_state.partition) {
                    strategies.push((partition_state.partition, partition_state.offset_strategy));
                }
            }
        }
        strategies
    }

    pub fn set_next_allowed_retry(
        &mut self,
        topic: &HashMap<&TopicName, Vec<(PartitionId, i64)>>,
        next_allowed_reset_ms: i64,
    ) {
        for (topic, partitions) in topic {
            if let Some(partition_states) = self.assignments.get_mut(*topic) {
                for partition_state in partition_states.iter_mut() {
                    for (p, _) in partitions {
                        if *p == partition_state.partition {
                            partition_state.next_retry_time_ms = Some(next_allowed_reset_ms);
                            break;
                        }
                    }
                }
            }
        }
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
    next_retry_time_ms: Option<i64>,
    offset_strategy: OffsetResetStrategy,
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

    fn awaiting_retry_backoff(&self, now: i64) -> bool {
        if let Some(next_retry_time) = self.next_retry_time_ms {
            return now < next_retry_time;
        }
        false
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
