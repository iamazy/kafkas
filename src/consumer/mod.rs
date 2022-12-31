pub mod fetcher;
pub mod partition_assignor;

use std::collections::{BTreeMap, HashMap, HashSet};

use kafka_protocol::{
    messages::{GroupId, TopicName},
    protocol::StrBytes,
};
use tracing::{debug, info};

use crate::{Error, NodeId, PartitionId, Result};

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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OffsetResetStrategy {
    Latest,
    Earliest,
    None,
}

impl OffsetResetStrategy {
    pub fn from_timestamp(timestamp: i64) -> Self {
        match timestamp {
            -2 => Self::Earliest,
            -1 => Self::Latest,
            _ => Self::None,
        }
    }

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
    pub topics: HashSet<TopicName>,
    default_offset_strategy: OffsetResetStrategy,
    pub assignments: HashMap<TopicName, Vec<TopicPartitionState>>,
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
        self.collection_partitions(|state| {
            matches!(state.fetch_state, FetchState::AwaitReset)
                && !state.awaiting_retry_backoff(now)
        })
    }

    fn partitions_need_validation(&self, now: i64) -> HashMap<TopicName, Vec<PartitionId>> {
        self.collection_partitions(|state| {
            matches!(state.fetch_state, FetchState::AwaitValidation)
                && !state.awaiting_retry_backoff(now)
        })
    }

    fn collection_partitions<F>(&self, func: F) -> HashMap<TopicName, Vec<PartitionId>>
    where
        F: Fn(&TopicPartitionState) -> bool,
    {
        let mut topics = HashMap::new();
        for (topic, partition_states) in self.assignments.iter() {
            let mut partitions = Vec::new();
            for partition_state in partition_states {
                if func(partition_state) {
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

    pub fn request_failed(
        &mut self,
        topics: HashMap<TopicName, Vec<PartitionId>>,
        next_retry_time_ms: i64,
    ) {
        for (topic, partitions) in topics {
            if let Some(partition_states) = self.assignments.get_mut(&topic) {
                for partition_state in partition_states {
                    if partitions.contains(&partition_state.partition) {
                        partition_state.request_failed(next_retry_time_ms);
                    }
                }
            }
        }
    }

    fn assigned_state_or_none(
        &mut self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Option<&mut TopicPartitionState> {
        if let Some(partition_states) = self.assignments.get_mut(topic) {
            if let Some(partition_state) = partition_states
                .iter_mut()
                .find(|state| state.partition == partition)
            {
                return Some(partition_state);
            }
        }
        None
    }

    fn maybe_seek_unvalidated(
        &mut self,
        topic: &TopicName,
        partition: PartitionId,
        position: FetchPosition,
        offset_strategy: OffsetResetStrategy,
    ) -> Result<()> {
        if let Some(partition_state) = self.assigned_state_or_none(topic, partition) {
            if !matches!(partition_state.fetch_state, FetchState::AwaitReset) {
                debug!("Skipping reset of [{topic:?} - {partition}] since it is no longer needed");
            } else if partition_state.offset_strategy != offset_strategy {
                debug!(
                    "Skipping reset of topic [{topic:?} - {partition}] since an alternative reset \
                     has been requested"
                );
            } else {
                info!(
                    "Resetting offset for topic [{} - {partition}] to position {}.",
                    topic.0, position.offset
                );
                partition_state.seek_unvalidated(position)?;
            }
        } else {
            debug!("Skipping reset of [{topic:?} - {partition}] since it is no longer assigned");
        }

        Ok(())
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
    pub partition: PartitionId,
    pub fetch_state: FetchState,
    pub position: FetchPosition,
    pub high_water_mark: i64,
    pub log_start_offset: i64,
    pub last_stable_offset: i64,
    pub paused: bool,
    pub next_retry_time_ms: Option<i64>,
    pub offset_strategy: OffsetResetStrategy,
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

    fn request_failed(&mut self, next_allowed_retry_time_ms: i64) {
        self.next_retry_time_ms = Some(next_allowed_retry_time_ms);
    }

    fn seek_unvalidated(&mut self, position: FetchPosition) -> Result<()> {
        self.seek_validated(position)?;
        self.validate_position(position)
    }

    fn seek_validated(&mut self, position: FetchPosition) -> Result<()> {
        self.transition_state(FetchState::Fetching, |state| {
            state.position = position;
            state.offset_strategy = OffsetResetStrategy::None;
            state.next_retry_time_ms = None;
        })
    }

    fn reset(&mut self, strategy: OffsetResetStrategy) -> Result<()> {
        self.transition_state(FetchState::AwaitReset, |state| {
            state.offset_strategy = strategy;
            state.next_retry_time_ms = None;
        })
    }

    fn validate_position(&mut self, position: FetchPosition) -> Result<()> {
        if position.offset_epoch.is_some() && position.current_leader.epoch.is_some() {
            self.transition_state(FetchState::AwaitValidation, |state| {
                state.position = position;
                state.next_retry_time_ms = None;
            })
        } else {
            // If we have no epoch information for the current position, then we can skip validation
            self.transition_state(FetchState::Fetching, |state| {
                state.position = position;
                state.next_retry_time_ms = None;
            })
        }
    }

    fn transition_state<F>(&mut self, new_state: FetchState, mut func: F) -> Result<()>
    where
        F: FnMut(&mut Self),
    {
        let next_state = self.fetch_state.transition_to(new_state);
        if next_state == new_state {
            self.fetch_state = next_state;
            func(self);
            if self.position.is_nil() && next_state.requires_position() {
                return Err(Error::Custom(format!(
                    "Transitioned subscription state to {next_state:?}, but position is nil"
                )));
            } else if !next_state.requires_position() {
                self.position.clear();
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FetchState {
    Initializing,
    Fetching,
    AwaitReset,
    AwaitValidation,
}

impl FetchState {
    fn transition_to(self, new_state: FetchState) -> Self {
        if self.valid_transitions().contains(&new_state) {
            new_state
        } else {
            self
        }
    }

    #[inline]
    fn requires_position(&self) -> bool {
        match self {
            FetchState::Initializing => false,
            FetchState::Fetching => true,
            FetchState::AwaitReset => false,
            FetchState::AwaitValidation => true,
        }
    }

    #[inline]
    fn has_valid_position(&self) -> bool {
        match self {
            FetchState::Initializing => false,
            FetchState::Fetching => true,
            FetchState::AwaitReset => false,
            FetchState::AwaitValidation => false,
        }
    }

    #[inline]
    fn valid_transitions(&self) -> Vec<FetchState> {
        match self {
            FetchState::Initializing => vec![
                FetchState::Fetching,
                FetchState::AwaitReset,
                FetchState::AwaitValidation,
            ],
            FetchState::Fetching => vec![
                FetchState::Fetching,
                FetchState::AwaitReset,
                FetchState::AwaitValidation,
            ],
            FetchState::AwaitReset => vec![FetchState::Fetching, FetchState::AwaitReset],
            FetchState::AwaitValidation => vec![
                FetchState::Fetching,
                FetchState::AwaitReset,
                FetchState::AwaitValidation,
            ],
        }
    }
}

impl Default for FetchState {
    fn default() -> Self {
        Self::Initializing
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FetchPosition {
    pub offset: i64,
    pub offset_epoch: Option<i32>,
    pub current_leader: LeaderAndEpoch,
}

impl FetchPosition {
    pub fn new(offset: i64, epoch: Option<i32>) -> Self {
        Self {
            offset,
            offset_epoch: epoch,
            ..Default::default()
        }
    }

    pub fn clear(&mut self) {
        self.offset = i64::MIN;
        self.offset_epoch = None;
        self.current_leader.leader = None;
        self.current_leader.epoch = None;
    }

    pub fn is_nil(&self) -> bool {
        self.offset == i64::MIN
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LeaderAndEpoch {
    pub leader: Option<NodeId>,
    pub epoch: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupMetadata {
    pub group_id: GroupId,
    pub generation_id: i32,
    pub member_id: StrBytes,
    pub leader: StrBytes,
    pub group_instance_id: Option<StrBytes>,
    pub protocol_name: Option<StrBytes>,
    pub protocol_type: Option<StrBytes>,
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
