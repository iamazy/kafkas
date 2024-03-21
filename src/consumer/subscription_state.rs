use std::collections::{HashMap, HashSet};

use kafka_protocol::{messages::TopicName, protocol::StrBytes};
use tracing::{debug, info};

use crate::{
    consumer::{LeaderAndEpoch, OffsetResetStrategy},
    error::Result,
    metadata::TopicPartition,
    Error, PartitionId,
};

#[derive(Debug, Clone, Default)]
pub struct SubscriptionState {
    pub subscription_type: SubscriptionType,
    pub topics: HashSet<TopicName>,
    pub assignments: HashMap<TopicPartition, TopicPartitionState>,
    pub seek_offsets: HashMap<TopicPartition, i64>,
}

impl SubscriptionState {
    pub fn unsubscribe(&mut self) {
        self.subscription_type = SubscriptionType::None;
        self.topics.clear();
        self.assignments.clear();
        self.seek_offsets.clear();
    }

    pub fn has_auto_assigned_partitions(&self) -> bool {
        self.subscription_type == SubscriptionType::AutoTopics
            || self.subscription_type == SubscriptionType::AutoPattern
    }

    pub fn all_consumed(&self) -> HashMap<TopicPartition, OffsetMetadata> {
        let mut all_consumed = HashMap::new();
        for (tp, tp_state) in self.assignments.iter() {
            if tp_state.fetch_state.has_valid_position() {
                all_consumed.insert(
                    tp.clone(),
                    OffsetMetadata {
                        committed_offset: tp_state.position.offset,
                        committed_leader_epoch: tp_state.position.current_leader.epoch,
                        metadata: Some(StrBytes::default()),
                    },
                );
            }
        }

        all_consumed
    }

    pub fn partitions(&self) -> HashMap<TopicName, Vec<PartitionId>> {
        let mut topics: HashMap<TopicName, Vec<PartitionId>> = HashMap::new();
        for (tp, _tp_state) in self.assignments.iter() {
            if let Some(partitions) = topics.get_mut(&tp.topic) {
                partitions.push(tp.partition);
            } else {
                let partitions = vec![tp.partition];
                topics.insert(tp.topic.clone(), partitions);
            }
        }
        topics
    }

    pub fn position(&self, tp: &TopicPartition) -> Option<&FetchPosition> {
        match self.assignments.get(tp) {
            Some(tp_state) => Some(&tp_state.position),
            None => None,
        }
    }

    pub(crate) fn fetchable_partitions<F>(&self, func: F) -> Vec<TopicPartition>
    where
        F: Fn(&TopicPartition) -> bool,
    {
        let mut partitions = Vec::new();
        for (tp, tp_state) in self.assignments.iter() {
            if tp_state.is_fetchable() && func(tp) {
                partitions.push(tp.clone());
            }
        }
        partitions
    }

    pub(crate) fn partitions_need_reset(&self, _now: i64) -> Vec<TopicPartition> {
        self.collection_partitions(|state| {
            matches!(state.fetch_state, FetchState::AwaitReset)
            // && !state.awaiting_retry_backoff(now)
        })
    }

    fn partitions_need_validation(&self, _now: i64) -> Vec<TopicPartition> {
        self.collection_partitions(|state| {
            matches!(state.fetch_state, FetchState::AwaitValidation)
            // && !state.awaiting_retry_backoff(now)
        })
    }

    fn collection_partitions<F>(&self, func: F) -> Vec<TopicPartition>
    where
        F: Fn(&TopicPartitionState) -> bool,
    {
        let mut partitions = Vec::new();
        for (partition, partition_state) in self.assignments.iter() {
            if func(partition_state) {
                partitions.push(partition.clone());
            }
        }
        partitions
    }

    pub fn set_next_allowed_retry(
        &mut self,
        assignments: Vec<TopicPartition>,
        next_allowed_reset_ms: i64,
    ) {
        for topic in assignments.iter() {
            if let Some(tp_state) = self.assignments.get_mut(topic) {
                tp_state.next_retry_time_ms = Some(next_allowed_reset_ms);
            }
        }
    }

    pub fn request_failed(&mut self, partitions: Vec<TopicPartition>, next_retry_time_ms: i64) {
        for tp in partitions {
            if let Some(tp_state) = self.assignments.get_mut(&tp) {
                tp_state.request_failed(next_retry_time_ms);
            }
        }
    }

    pub(crate) fn seek_unvalidated(
        &mut self,
        partition: TopicPartition,
        position: FetchPosition,
    ) -> Result<()> {
        if let Some(tp_state) = self.assignments.get_mut(&partition) {
            tp_state.seek_validated(position)?;
        }
        Ok(())
    }

    pub(crate) fn maybe_seek_unvalidated(
        &mut self,
        partition: TopicPartition,
        position: FetchPosition,
        offset_strategy: OffsetResetStrategy,
    ) -> Result<()> {
        if let Some(partition_state) = self.assignments.get_mut(&partition) {
            if !matches!(partition_state.fetch_state, FetchState::AwaitReset) {
                debug!(
                    "Skipping reset of [{} - {}] since it is no longer needed",
                    partition.topic.as_str(),
                    partition.partition
                );
            } else if partition_state.offset_strategy != offset_strategy {
                debug!(
                    "Skipping reset of topic [{} - {}] since an alternative reset has been \
                     requested",
                    partition.topic.as_str(),
                    partition.partition
                );
            } else {
                info!(
                    "Resetting offset for topic [{} - {}] to position {}.",
                    partition.topic.as_str(),
                    partition.partition,
                    position.offset
                );
                partition_state.seek_unvalidated(position)?;
            }
        } else {
            debug!(
                "Skipping reset of [{} - {}] since it is no longer assigned",
                partition.topic.as_str(),
                partition.partition
            );
        }

        Ok(())
    }

    pub fn maybe_validate_position_for_current_leader(
        &mut self,
        tp: &TopicPartition,
        leader_epoch: LeaderAndEpoch,
    ) -> Result<bool> {
        if let Some(tp_state) = self.assignments.get_mut(tp) {
            // TODO：validate position
            // return tp_state.maybe_validate_position(leader_epoch);
            tp_state.update_position_leader_no_validation(leader_epoch)?;
            return Ok(true);
        }
        Ok(false)
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum SubscriptionType {
    #[default]
    None,
    AutoTopics,
    AutoPattern,
    UserAssigned,
}

#[derive(Debug, Clone, Default)]
pub struct OffsetMetadata {
    pub(crate) committed_offset: i64,
    pub(crate) committed_leader_epoch: Option<i32>,
    pub(crate) metadata: Option<StrBytes>,
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

    pub(crate) fn reset(&mut self, strategy: OffsetResetStrategy) -> Result<()> {
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

    fn update_position_leader_no_validation(
        &mut self,
        current_leader: LeaderAndEpoch,
    ) -> Result<()> {
        if !self.position.is_nil() {
            let mut position = self.position;
            self.transition_state(FetchState::Fetching, |state| {
                position.current_leader = current_leader;
                state.position = position;
                state.next_retry_time_ms = None;
            })?;
        }
        Ok(())
    }

    fn maybe_validate_position(&mut self, current_leader: LeaderAndEpoch) -> Result<bool> {
        if matches!(self.fetch_state, FetchState::AwaitReset) {
            return Ok(false);
        }
        if current_leader.leader.is_none() {
            return Ok(false);
        }
        if !self.position.is_nil() && self.position.current_leader == current_leader {
            let position = FetchPosition {
                offset: self.position.offset,
                offset_epoch: self.position.offset_epoch,
                current_leader,
            };
            self.validate_position(position)?;
        }
        let matched = matches!(self.fetch_state, FetchState::AwaitValidation);
        Ok(matched)
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

    fn is_fetchable(&self) -> bool {
        !self.paused && self.fetch_state.has_valid_position()
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
