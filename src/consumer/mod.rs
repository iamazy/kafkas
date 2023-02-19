mod fetch_session;
mod fetcher;
pub(crate) mod partition_assignor;
pub(crate) mod subscription_state;

use std::{sync::Arc, time::Duration};

use async_stream::stream;
use bytes::Bytes;
use chrono::Local;
use dashmap::DashSet;
use futures::{
    channel::{mpsc, oneshot},
    future::{select, Either},
    pin_mut, SinkExt, Stream, StreamExt,
};
use indexmap::IndexMap;
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::GroupId,
    protocol::StrBytes,
    records::{Record, RecordBatchDecoder},
    ResponseError,
};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::{
    client::{DeserializeMessage, Kafka},
    consumer::{
        fetcher::{CompletedFetch, Fetcher},
        subscription_state::FetchPosition,
    },
    coordinator::{ConsumerCoordinator, CoordinatorEvent},
    executor::Executor,
    metadata::TopicPartition,
    NodeId, PartitionId, Result, ToStrBytes, DEFAULT_GENERATION_ID,
};

/// High-level consumer record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecord {
    pub offset: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: IndexMap<StrBytes, Option<Bytes>>,
    pub timestamp: i64,
}

impl DeserializeMessage for ConsumerRecord {
    type Output = Self;

    fn deserialize_message(record: Record) -> Self::Output {
        ConsumerRecord {
            offset: record.offset,
            key: record.key,
            value: record.value,
            headers: record.headers,
            timestamp: record.timestamp,
        }
    }
}

/// High-level consumer record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecords<T: DeserializeMessage + Sized> {
    partition: TopicPartition,
    records: Vec<T>,
}

impl<T: DeserializeMessage + Sized> ConsumerRecords<T> {
    pub fn topic(&self) -> &StrBytes {
        self.partition.topic()
    }

    pub fn partition(&self) -> PartitionId {
        self.partition.partition()
    }
}

impl<T: DeserializeMessage + Sized> Iterator for ConsumerRecords<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.records.pop()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum OffsetResetStrategy {
    #[default]
    Earliest,
    Latest,
    None,
}

impl OffsetResetStrategy {
    fn from_timestamp(timestamp: i64) -> Self {
        match timestamp {
            -2 => Self::Earliest,
            -1 => Self::Latest,
            _ => Self::None,
        }
    }

    pub(crate) fn strategy_timestamp(&self) -> i64 {
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
            max_poll_interval_ms: 300_000,
            auto_commit_interval_ms: 5_000,
            auto_commit_enabled: true,
            request_timeout_ms: 30_000,
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
            heartbeat_interval_ms: 3_000,
            retry_backoff_ms: 100,
            leave_group_on_close: true,
        }
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
            coordinator.event_sender(),
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

    pub async fn seek(&mut self, partition: TopicPartition, offset: i64) -> Result<()> {
        self.coordinator.seek(partition, offset).await
    }

    pub fn notify_shutdown(&self) -> broadcast::Receiver<()> {
        self.notify_shutdown.subscribe()
    }

    pub async fn commit_async(&mut self) -> Result<()> {
        self.coordinator.commit_async().await
    }

    pub async fn subscribe<S, T>(
        &mut self,
        topics: Vec<S>,
    ) -> Result<impl Stream<Item = ConsumerRecords<T>>>
    where
        S: AsRef<str>,
        T: DeserializeMessage<Output = T> + Sized,
    {
        self.coordinator
            .subscribe(
                topics
                    .iter()
                    .map(|topic| topic.as_ref().to_string().to_str_bytes().into())
                    .collect(),
            )
            .await?;
        self.coordinator.prepare_fetch().await?;

        // fetch records task
        self.client.executor.spawn(Box::pin(do_fetch(
            self.fetcher.clone(),
            self.notify_shutdown.subscribe(),
        )));

        // reset offset task
        let (reset_offset_tx, reset_offset_rx) = mpsc::unbounded();
        self.client.executor.spawn(Box::pin(reset_offset(
            self.fetcher.clone(),
            reset_offset_rx,
            self.notify_shutdown.subscribe(),
        )));

        Ok(fetch_stream::<Exe, T>(
            self.client.clone(),
            self.options.clone(),
            self.coordinator.event_sender(),
            self.fetches_rx.take().unwrap(),
            self.fetcher.completed_partitions.clone(),
            reset_offset_tx,
            self.notify_shutdown.subscribe(),
        ))
    }

    pub async fn unsubscribe(&mut self) -> Result<()> {
        self.coordinator
            .maybe_leave_group(StrBytes::from_str(
                "the consumer unsubscribed from all topics",
            ))
            .await?;
        self.coordinator.unsubscribe().await?;
        // Stop heartbeat and offset commit task, not really shutdown the consumer.
        self.notify_shutdown.send(())?;
        info!("Unsubscribed all topics or patterns and assigned partitions");
        Ok(())
    }
}

impl<Exe: Executor> Drop for Consumer<Exe> {
    fn drop(&mut self) {
        let _ = self.notify_shutdown.send(());
    }
}

async fn do_fetch<Exe: Executor>(mut fetcher: Fetcher<Exe>, mut rx: broadcast::Receiver<()>) {
    let mut interval = fetcher.client.executor.interval(Duration::from_millis(100));
    while interval.next().await.is_some() {
        let fetcher_fut = fetcher.fetch();
        let shutdown = rx.recv();
        pin_mut!(fetcher_fut);
        pin_mut!(shutdown);

        match select(fetcher_fut, shutdown).await {
            Either::Left((Err(err), _)) => {
                error!("Fetch error: {err}");
            }
            Either::Left(_) => {}
            Either::Right(_) => break,
        }
    }
    info!("Fetch task finished.");
}

fn fetch_stream<Exe, T>(
    client: Kafka<Exe>,
    options: Arc<ConsumerOptions>,
    mut event_tx: mpsc::UnboundedSender<CoordinatorEvent>,
    mut completed_fetches_rx: mpsc::UnboundedReceiver<CompletedFetch>,
    completed_partitions: Arc<DashSet<TopicPartition>>,
    mut reset_offset_tx: mpsc::UnboundedSender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> impl Stream<Item = ConsumerRecords<T>>
where
    Exe: Executor,
    T: DeserializeMessage<Output = T> + Sized,
{
    stream! {
        while let Some(completed_fetch) = completed_fetches_rx.next().await {
            let records_fut = handle_partition_response(
                &client,
                &mut reset_offset_tx,
                completed_fetch,
                &options,
                &mut event_tx,
                &completed_partitions,
            );
            let shutdown = shutdown_rx.recv();

            pin_mut!(records_fut);
            pin_mut!(shutdown);

            match select(records_fut, shutdown).await {
                Either::Left((Ok(Some((tp, raw_records))), _)) => {
                    let mut records = Vec::with_capacity(raw_records.len());
                    for record in raw_records {
                        records.push(T::deserialize_message(record));
                    }
                    yield ConsumerRecords {
                        partition: tp,
                        records
                    };
                }
                Either::Left((Ok(None), _)) => {},
                Either::Left((Err(err), _)) => error!("Fetch error: {}", err),
                Either::Right(_) => {
                    info!("Fetch task is shutting down");
                    break
                }
            }
        }
    }
}

async fn handle_partition_response<Exe: Executor>(
    client: &Kafka<Exe>,
    reset_offset_tx: &mut mpsc::UnboundedSender<()>,
    completed_fetch: CompletedFetch,
    options: &Arc<ConsumerOptions>,
    event_tx: &mut mpsc::UnboundedSender<CoordinatorEvent>,
    completed_partitions: &Arc<DashSet<TopicPartition>>,
) -> Result<Option<(TopicPartition, Vec<Record>)>> {
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
            client.update_full_metadata().await?;
        }
        Some(ResponseError::UnknownTopicOrPartition) => {
            warn!(
                "Received unknown topic or partition error in fetch for {}",
                completed_fetch.partition
            );
            client.update_full_metadata().await?;
        }
        Some(ResponseError::UnknownTopicId) => {
            warn!(
                "Received unknown topic ID error in fetch for {}",
                completed_fetch.partition
            );
            client.update_full_metadata().await?;
        }
        Some(ResponseError::InconsistentTopicId) => {
            warn!(
                "Received inconsistent topic ID error in fetch for {}",
                completed_fetch.partition
            );
            client.update_full_metadata().await?;
        }
        Some(error @ ResponseError::OffsetOutOfRange) => {
            let error_msg = format!("{} is out of range.", completed_fetch.partition);
            let strategy = options.auto_offset_reset;
            if !matches!(strategy, OffsetResetStrategy::None) {
                info!("{error_msg} resetting offset.");
                event_tx.unbounded_send(CoordinatorEvent::ResetOffset {
                    partition: completed_fetch.partition.clone(),
                    strategy,
                })?;
                reset_offset_tx.send(()).await?;
                completed_partitions.remove(&completed_fetch.partition);
            } else {
                info!(
                    "{error_msg} raising error to the application since no reset policy is \
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
            debug!(
                "Fetch {} success, last stable offset: {}, log start offset: {}, high_water_mark: \
                 {}",
                completed_fetch.partition,
                partition.last_stable_offset,
                partition.log_start_offset,
                partition.high_watermark
            );

            let mut position = None;
            let (tx, rx) = oneshot::channel();
            // decode record batch
            match partition.records {
                Some(ref mut records) => {
                    let records = RecordBatchDecoder::decode(records)?;
                    if let Some(record) = records.last() {
                        let mut fetch_position = FetchPosition::new(record.offset, None);
                        fetch_position.current_leader.epoch = Some(record.partition_leader_epoch);
                        position = Some(fetch_position);
                    }
                    debug!(
                        "Fetch {} records success, records size: {}",
                        completed_fetch.partition,
                        records.len()
                    );
                    event_tx.unbounded_send(CoordinatorEvent::PartitionData {
                        partition: completed_fetch.partition.clone(),
                        position,
                        data: partition,
                        notify: tx,
                    })?;
                    rx.await?;
                    completed_partitions.remove(&completed_fetch.partition);
                    return Ok(Some((completed_fetch.partition, records)));
                }
                None => {
                    event_tx.unbounded_send(CoordinatorEvent::PartitionData {
                        partition: completed_fetch.partition,
                        position,
                        data: partition,
                        notify: tx,
                    })?;
                    rx.await?;
                }
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

async fn reset_offset<Exe: Executor>(
    mut fetcher: Fetcher<Exe>,
    mut reset_offset_rx: mpsc::UnboundedReceiver<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    info!("Start the reset offset task.");
    loop {
        let next_reset_offset = reset_offset_rx.next();
        let shutdown = shutdown_rx.recv();

        pin_mut!(next_reset_offset);
        pin_mut!(shutdown);

        match select(next_reset_offset, shutdown).await {
            Either::Left((Some(_), _)) => {
                if let Err(err) = fetcher.reset_offset().await {
                    error!("Reset offset failed, {}", err);
                }
            }
            Either::Left((None, _)) | Either::Right(_) => {
                break;
            }
        }
    }
    info!("Reset offset task finished.");
}
