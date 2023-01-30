pub mod fetch_session;
pub mod fetcher;
pub mod partition_assignor;

use std::{
    collections::{hash_map::Keys, BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_stream::try_stream;
use chrono::Local;
use dashmap::DashSet;
use futures::{
    channel::mpsc,
    future::{select, Either},
    lock::Mutex,
    pin_mut, Stream, StreamExt,
};
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{GroupId, TopicName},
    protocol::StrBytes,
    records::{Record, RecordBatchDecoder},
    ResponseError,
};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::{
    client::Kafka,
    consumer::fetcher::{CompletedFetch, Fetcher},
    coordinator::ConsumerCoordinator,
    executor::{Executor, Interval},
    metadata::TopicPartition,
    Error, NodeId, PartitionId, Result, ToStrBytes, DEFAULT_GENERATION_ID,
};

const INITIAL_EPOCH: i32 = 0;
const FINAL_EPOCH: i32 = -1;
const INVALID_SESSION_ID: i32 = 0;

/// High-level consumer record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: BTreeMap<String, Vec<u8>>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum OffsetResetStrategy {
    #[default]
    Earliest,
    Latest,
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

#[derive(Debug, Clone)]
pub struct ConsumerOptions {
    pub max_poll_records: i32,
    pub max_poll_interval_ms: i32,
    pub auto_commit_interval_ms: i32,
    pub auto_commit_enabled: bool,
    pub request_timeout_ms: i32,
    pub check_crcs: bool,
    pub fetch_min_bytes: i32,
    pub fetch_max_bytes: i32,
    pub fetch_max_wait_ms: i32,
    pub max_partition_fetch_bytes: i32,
    pub reconnect_backoff_ms: i64,
    pub reconnect_backoff_max_ms: i64,
    pub retry_backoff_ms: i64,
    pub client_rack: String,
    pub allow_auto_create_topics: bool,
    pub partition_assignment_strategy: String,
    pub isolation_level: IsolationLevel,
    pub auto_offset_reset: OffsetResetStrategy,
    pub group_id: String,
    pub rebalance_options: RebalanceOptions,
}

impl ConsumerOptions {
    pub fn new<S: AsRef<str>>(group: S) -> Self {
        Self {
            group_id: group.as_ref().to_string(),
            ..Default::default()
        }
    }
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            max_poll_records: 500,
            max_poll_interval_ms: 300000,
            auto_commit_interval_ms: 5000,
            auto_commit_enabled: true,
            request_timeout_ms: 30000,
            check_crcs: false,
            fetch_min_bytes: 1,
            fetch_max_bytes: 52428800, // 50 * 1024 * 1024,
            fetch_max_wait_ms: 500,
            max_partition_fetch_bytes: 1048576, // 1 * 1024 * 1024
            reconnect_backoff_ms: 50,
            reconnect_backoff_max_ms: 1000,
            retry_backoff_ms: 100,
            client_rack: String::default(),
            allow_auto_create_topics: false,
            partition_assignment_strategy: "range".into(),
            isolation_level: IsolationLevel::ReadUncommitted,
            auto_offset_reset: OffsetResetStrategy::Earliest,
            rebalance_options: RebalanceOptions::default(),
            group_id: Default::default(),
        }
    }
}

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
    pub assignments: HashMap<TopicPartition, TopicPartitionState>,
}

impl SubscriptionState {
    pub fn all_consumed(&self) -> HashMap<TopicPartition, OffsetMetadata> {
        let mut all_consumed = HashMap::new();
        for (tp, tp_state) in self.assignments.iter() {
            if tp_state.fetch_state.has_valid_position() {
                all_consumed.insert(
                    tp.clone(),
                    OffsetMetadata {
                        committed_offset: tp_state.position.offset,
                        committed_leader_epoch: tp_state.position.current_leader.epoch,
                        metadata: Some(StrBytes::from_str("")),
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

    fn fetchable_partitions<F>(&self, func: F) -> Vec<TopicPartition>
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

    fn partitions_need_reset(&self, now: i64) -> Vec<TopicPartition> {
        self.collection_partitions(|state| {
            matches!(state.fetch_state, FetchState::AwaitReset)
                && !state.awaiting_retry_backoff(now)
        })
    }

    fn partitions_need_validation(&self, now: i64) -> Vec<TopicPartition> {
        self.collection_partitions(|state| {
            matches!(state.fetch_state, FetchState::AwaitValidation)
                && !state.awaiting_retry_backoff(now)
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
        assignments: Keys<&TopicPartition, i64>,
        next_allowed_reset_ms: i64,
    ) {
        for topic in assignments {
            if let Some(tp_state) = self.assignments.get_mut(*topic) {
                tp_state.next_retry_time_ms = Some(next_allowed_reset_ms);
            }
        }
    }

    pub fn request_failed(&mut self, partitions: Vec<TopicPartition>, next_retry_time_ms: i64) {
        for tp in partitions {
            if let Some(partition_state) = self.assignments.get_mut(&tp) {
                partition_state.request_failed(next_retry_time_ms);
            }
        }
    }

    fn maybe_seek_unvalidated(
        &mut self,
        partition: TopicPartition,
        position: FetchPosition,
        offset_strategy: OffsetResetStrategy,
    ) -> Result<()> {
        if let Some(partition_state) = self.assignments.get_mut(&partition) {
            if !matches!(partition_state.fetch_state, FetchState::AwaitReset) {
                debug!(
                    "Skipping reset of [{} - {}] since it is no longer needed",
                    partition.topic.0, partition.partition
                );
            } else if partition_state.offset_strategy != offset_strategy {
                debug!(
                    "Skipping reset of topic [{} - {}] since an alternative reset has been \
                     requested",
                    partition.topic.0, partition.partition
                );
            } else {
                info!(
                    "Resetting offset for topic [{} - {}] to position {}.",
                    partition.topic.0, partition.partition, position.offset
                );
                partition_state.seek_unvalidated(position)?;
            }
        } else {
            debug!(
                "Skipping reset of [{} - {}] since it is no longer assigned",
                partition.topic.0, partition.partition
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

#[derive(Debug, Clone, Copy, Default)]
enum SubscriptionType {
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

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
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
            generation_id: DEFAULT_GENERATION_ID,
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

pub struct Consumer<Exe: Executor> {
    client: Kafka<Exe>,
    coordinator: ConsumerCoordinator<Exe>,
    fetcher: Fetcher<Exe>,
    options: Arc<ConsumerOptions>,
    notify_shutdown: broadcast::Sender<()>,
    fetches_rx: Option<mpsc::UnboundedReceiver<CompletedFetch>>,
}

impl<Exe: Executor> Consumer<Exe> {
    pub async fn new(client: Kafka<Exe>, options: ConsumerOptions) -> Result<Self> {
        let options = Arc::new(options);

        let (notify_shutdown, _) = broadcast::channel(1);
        let coordinator =
            ConsumerCoordinator::new(client.clone(), options.clone(), notify_shutdown.clone())
                .await?;

        let (tx, rx) = mpsc::unbounded();

        let fetcher = Fetcher::new(
            client.clone(),
            Local::now().timestamp(),
            coordinator.subscriptions().await,
            options.clone(),
            tx,
        );

        Ok(Self {
            client,
            coordinator,
            fetcher,
            options,
            notify_shutdown,
            fetches_rx: Some(rx),
        })
    }

    pub async fn commit_ack(&mut self) -> Result<()> {
        self.coordinator.offset_commit().await
    }

    pub async fn subscribe<S: AsRef<str>>(
        &mut self,
        topics: Vec<S>,
    ) -> Result<impl Stream<Item = Result<Record>> + '_> {
        self.coordinator
            .subscribe(
                topics
                    .iter()
                    .map(|topic| topic.as_ref().to_string().to_str_bytes().into())
                    .collect(),
            )
            .await?;
        self.coordinator.prepare_fetch().await?;

        let fetch_interval = self.client.executor.interval(Duration::from_millis(100));

        let spawn_ret = self.client.executor.spawn(Box::pin(do_fetch(
            self.fetcher.clone(),
            fetch_interval,
            self.notify_shutdown.subscribe(),
        )));

        match spawn_ret {
            Err(_) => Err(Error::Custom(
                "The executor could not spawn the fetch task".to_string(),
            )),
            Ok(_) => Ok(fetch_stream(
                self.options.clone(),
                self.fetches_rx.take().unwrap(),
                self.coordinator.subscriptions().await,
                self.fetcher.completed_partitions.clone(),
                self.notify_shutdown.subscribe(),
            )),
        }
    }
}

impl<Exe: Executor> Drop for Consumer<Exe> {
    fn drop(&mut self) {
        let _ = self.notify_shutdown.send(());
    }
}

async fn do_fetch<Exe: Executor>(
    mut fetcher: Fetcher<Exe>,
    mut interval: Interval,
    mut rx: broadcast::Receiver<()>,
) {
    while interval.next().await.is_some() {
        let fetcher_fut = fetcher.fetch();
        let shutdown = rx.recv();
        pin_mut!(fetcher_fut);
        pin_mut!(shutdown);

        match select(fetcher_fut, shutdown).await {
            Either::Left((Err(err), _)) => {
                error!("Fetch error: {err}");
            }
            Either::Left((_, _)) => {}
            Either::Right(_) => break,
        }
    }
    info!("Fetch task finished.");
}

fn fetch_stream(
    options: Arc<ConsumerOptions>,
    mut completed_fetches_rx: mpsc::UnboundedReceiver<CompletedFetch>,
    subscription: Arc<Mutex<SubscriptionState>>,
    completed_partitions: Arc<DashSet<TopicPartition>>,
    mut rx: broadcast::Receiver<()>,
) -> impl Stream<Item = Result<Record>> {
    try_stream! {
        while let Some(completed_fetch) = completed_fetches_rx.next().await {
            if let Some(partition_state) = subscription
                .lock()
                .await
                .assignments
                .get_mut(&completed_fetch.partition)
            {
                let records_fut = handle_partition_response(
                    completed_fetch,
                    &options,
                    partition_state,
                    &completed_partitions,
                );
                let shutdown = rx.recv();

                pin_mut!(records_fut);
                pin_mut!(shutdown);

                match select(records_fut, shutdown).await {
                    Either::Left((Ok(Some(records)), _)) => {
                        for record in records {
                            yield record;
                        }
                    }
                    Either::Left((Ok(None), _)) => {},
                    Either::Left((Err(err), _)) => error!("Fetch error: {}", err),
                    Either::Right(_) => {
                        info!("Fetch task is shutting down");
                        break
                    },
                }
            }
        }
    }
}

async fn handle_partition_response(
    completed_fetch: CompletedFetch,
    options: &Arc<ConsumerOptions>,
    partition_state: &mut TopicPartitionState,
    completed_partitions: &Arc<DashSet<TopicPartition>>,
) -> Result<Option<Vec<Record>>> {
    let mut partition = completed_fetch.partition_data;
    match partition.error_code.err() {
        Some(
            error @ ResponseError::NotLeaderOrFollower
            | error @ ResponseError::ReplicaNotAvailable
            | error @ ResponseError::KafkaStorageError
            | error @ ResponseError::FencedLeaderEpoch
            | error @ ResponseError::OffsetNotAvailable,
        ) => {
            debug!("Error in fetch for {}: {error}", completed_fetch.partition);
            // TODO: update metadata
        }
        Some(ResponseError::UnknownTopicOrPartition) => {
            warn!(
                "Received unknown topic or partition error in fetch for {}",
                completed_fetch.partition
            );
            // TODO: update metadata
        }
        Some(ResponseError::UnknownTopicId) => {
            warn!(
                "Received unknown topic ID error in fetch for {}",
                completed_fetch.partition
            );
            // TODO: update metadata
        }
        Some(ResponseError::InconsistentTopicId) => {
            warn!(
                "Received inconsistent topic ID error in fetch for {}",
                completed_fetch.partition
            );
            // TODO: update metadata
        }
        Some(error @ ResponseError::OffsetOutOfRange) => {
            let error_msg = format!(
                "Fetch position {} is out of range for {}",
                partition_state.position.offset, completed_fetch.partition
            );
            let strategy = options.auto_offset_reset;
            if !matches!(strategy, OffsetResetStrategy::None) {
                info!("{error_msg}, resetting offset");
                partition_state.reset(strategy)?;
                completed_partitions.remove(&completed_fetch.partition);
            } else {
                info!(
                    "{error_msg}, raising error to the application since no reset policy is \
                     configured."
                );
                return Err(error.into());
            }
        }
        Some(error @ ResponseError::TopicAuthorizationFailed) => {
            warn!("Not authorized to read from {}", completed_fetch.partition);
            return Err(error.into());
        }
        Some(ResponseError::UnknownLeaderEpoch) => {
            debug!(
                "Received unknown leader epoch error in fetch for {}",
                completed_fetch.partition
            );
        }
        Some(ResponseError::UnknownServerError) => {
            warn!(
                "Unknown server error while fetching offset for {}",
                completed_fetch.partition
            );
        }
        Some(error @ ResponseError::CorruptMessage) => {
            error!(
                "Encountered corrupt message when fetching offset for {}",
                completed_fetch.partition
            );
            return Err(error.into());
        }
        None => {
            if partition.last_stable_offset >= 0 {
                partition_state.last_stable_offset = partition.last_stable_offset;
            }
            if partition.log_start_offset >= 0 {
                partition_state.log_start_offset = partition.log_start_offset;
            }
            if partition.high_watermark >= 0 {
                partition_state.high_water_mark = partition.high_watermark;
            }
            debug!(
                "Fetch {} success, last stable offset: {}, log start offset: {}, high_water_mark: \
                 {}",
                completed_fetch.partition,
                partition.last_stable_offset,
                partition.log_start_offset,
                partition.high_watermark
            );
            // decode record batch
            if let Some(ref mut records) = partition.records {
                let records = RecordBatchDecoder::decode(records)?;
                if let Some(record) = records.last() {
                    if record.offset <= partition_state.position.offset {
                        return Err(Error::Custom(format!(
                            "Invalid record offset: {}, expect greater than {}",
                            record.offset, partition_state.position.offset
                        )));
                    }
                    partition_state.position.offset = record.offset;
                    partition_state.position.current_leader.epoch =
                        Some(record.partition_leader_epoch);
                }
                debug!(
                    "Fetch {} records success, records size: {}",
                    completed_fetch.partition,
                    records.len()
                );
                completed_partitions.remove(&completed_fetch.partition);
                return Ok(Some(records));
            }
        }
        Some(error) => {
            error!(
                "Unexpected error code {error} while fetching offset from {}",
                completed_fetch.partition
            )
        }
    }
    Ok(None)
}
