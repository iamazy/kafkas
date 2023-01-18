use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
    vec::Drain,
};

use dashmap::DashMap;
use futures::{channel::mpsc, lock::Mutex};
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{
        fetch_request::{FetchPartition, FetchTopic, ForgottenTopic},
        fetch_response::PartitionData,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        ApiKey, BrokerId, FetchRequest, ListOffsetsRequest, ListOffsetsResponse, TopicName,
    },
    records::{Record, RecordBatchDecoder, NO_PARTITION_LEADER_EPOCH},
    ResponseError,
};
use parking_lot::Mutex as plMutex;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::{
    client::Kafka,
    consumer::{
        fetch_session::{
            FetchRequestData, FetchRequestDataBuilder, FetchRequestPartitionData, FetchSession,
        },
        FetchPosition, IsolationLevel, OffsetResetStrategy, SubscriptionState, FINAL_EPOCH,
        INITIAL_EPOCH, INVALID_SESSION_ID,
    },
    error::Result,
    executor::Executor,
    map_to_list,
    metadata::{Node, TopicIdPartition, TopicPartition},
    Error, NodeId, INVALID_LOG_START_OFFSET, UNKNOWN_EPOCH, UNKNOWN_OFFSET,
};

#[derive(Clone)]
pub struct Fetcher<Exe: Executor> {
    client: Kafka<Exe>,
    timestamp: i64,
    min_bytes: i32,
    max_bytes: i32,
    max_wait_ms: i32,
    fetch_size: i32,
    retry_backoff_ms: i64,
    request_timeout_ms: i64,
    pub(crate) max_poll_records: i32,
    check_crcs: bool,
    client_rack_id: String,
    isolation_level: IsolationLevel,
    pub(crate) default_offset_strategy: OffsetResetStrategy,
    pub(crate) subscription: Arc<Mutex<SubscriptionState>>,
    sessions: Arc<DashMap<NodeId, FetchSession>>,
    completed_fetches: Arc<plMutex<VecDeque<CompletedFetch>>>,
    records_tx: mpsc::UnboundedSender<CompletedFetch>,
    nodes_with_pending_fetch_requests: HashSet<i32>,
}

impl<Exe: Executor> Fetcher<Exe> {
    pub fn new(
        client: Kafka<Exe>,
        timestamp: i64,
        subscription: Arc<Mutex<SubscriptionState>>,
        records_tx: mpsc::UnboundedSender<CompletedFetch>,
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
            isolation_level: IsolationLevel::ReadUncommitted,
            default_offset_strategy: OffsetResetStrategy::default(),
            sessions: Arc::new(sessions),
            completed_fetches: Arc::new(plMutex::new(VecDeque::new())),
            records_tx,
            nodes_with_pending_fetch_requests: HashSet::new(),
        }
    }

    pub async fn collect_fetch(&mut self) -> Result<Fetch> {
        let mut fetch = Fetch::default();
        let mut records_remaining = self.max_poll_records;
        let mut need_reset_offset = false;
        while records_remaining > 0 {
            let completed_fetch = self.completed_fetches.lock().pop_front();
            if let Some(completed_fetch) = completed_fetch {
                let partition = completed_fetch.partition_data;
                let tp = completed_fetch.partition;
                if let Some(partition_state) =
                    self.subscription.lock().await.assignments.get_mut(&tp)
                {
                    match partition.error_code.err() {
                        Some(
                            error @ ResponseError::NotLeaderOrFollower
                            | error @ ResponseError::ReplicaNotAvailable
                            | error @ ResponseError::KafkaStorageError
                            | error @ ResponseError::FencedLeaderEpoch
                            | error @ ResponseError::OffsetNotAvailable,
                        ) => {
                            debug!("Error in fetch for {tp}: {error}");
                            // TODO: update metadata
                        }
                        Some(ResponseError::UnknownTopicOrPartition) => {
                            warn!("Received unknown topic or partition error in fetch for {tp}");
                            // TODO: update metadata
                        }
                        Some(ResponseError::UnknownTopicId) => {
                            warn!("Received unknown topic ID error in fetch for {tp}");
                            // TODO: update metadata
                        }
                        Some(ResponseError::InconsistentTopicId) => {
                            warn!("Received inconsistent topic ID error in fetch for {tp}");
                            // TODO: update metadata
                        }
                        Some(error @ ResponseError::OffsetOutOfRange) => {
                            let error_msg = format!(
                                "Fetch position {} is out of range for {tp}",
                                partition_state.position.offset
                            );
                            let strategy = self.default_offset_strategy;
                            if !matches!(strategy, OffsetResetStrategy::None) {
                                info!("{error_msg}, resetting offset");
                                partition_state.reset(strategy)?;
                                need_reset_offset = true;
                            } else {
                                info!(
                                    "{error_msg}, raising error to the application since no reset \
                                     policy is configured."
                                );
                                return Err(Error::Response { error, msg: None });
                            }
                        }
                        Some(error @ ResponseError::TopicAuthorizationFailed) => {
                            warn!("Not authorized to read from {tp}");
                            return Err(Error::Response { error, msg: None });
                        }
                        Some(ResponseError::UnknownLeaderEpoch) => {
                            debug!("Received unknown leader epoch error in fetch for {tp}");
                        }
                        Some(ResponseError::UnknownServerError) => {
                            warn!("Unknown server error while fetching offset for {tp}");
                        }
                        Some(error @ ResponseError::CorruptMessage) => {
                            error!("Encountered corrupt message when fetching offset for {tp}");
                            return Err(Error::Response { error, msg: None });
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
                                "Fetch {tp} success, last stable offset: {}, log start offset: \
                                 {}, high_water_mark: {}",
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
                                    "Fetch {tp} records success, records size: {}",
                                    records.len()
                                );
                                records_remaining -= records.len() as i32;
                                fetch.records.insert(tp, records);
                            }
                        }
                        Some(error) => {
                            error!("Unexpected error code {error} while fetching offset from {tp}")
                        }
                    }
                }
            } else {
                break;
            }
        }
        if need_reset_offset {
            self.reset_offset().await?;
        }
        fetch.num_records = self.max_poll_records - records_remaining;
        Ok(fetch)
    }

    pub async fn fetch(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::FetchKey) {
            let mut version = version_range.max;
            let fetch_requests = self.prepare_fetch_requests().await?;
            for (node, mut fetch_request_data) in fetch_requests {
                if !fetch_request_data.can_use_topic_ids {
                    version = 12;
                }
                let metadata = fetch_request_data.metadata;
                if let Some(node) = self.client.cluster_meta.nodes.get(&node) {
                    let fetch_response = self
                        .client
                        .fetch(
                            node.value(),
                            self.fetch_builder(&mut fetch_request_data, version).await?,
                        )
                        .await?;

                    match self.sessions.get_mut(node.key()) {
                        Some(mut session) => {
                            if !session
                                .value_mut()
                                .handle_fetch_response(&fetch_response, version)
                            {
                                if let Some(error) = fetch_response.error_code.err() {
                                    if error == ResponseError::FetchSessionTopicIdError {
                                        // TODO: update metadata
                                    }
                                    continue;
                                }
                            }

                            for fetchable_topic in fetch_response.responses {
                                let topic = if let Some(topic_name) =
                                    session.session_topic_names.get(&fetchable_topic.topic_id)
                                {
                                    topic_name
                                } else {
                                    &fetchable_topic.topic
                                };

                                for partition in fetchable_topic.partitions {
                                    let tp = TopicPartition::new(
                                        topic.clone(),
                                        partition.partition_index,
                                    );

                                    match fetch_request_data.session_partitions.get(&tp) {
                                        Some(data) => {
                                            debug!(
                                                "Fetch {:?} at offset {} for {tp}",
                                                self.isolation_level, data.fetch_offset
                                            );
                                            self.completed_fetches.lock().push_back(
                                                CompletedFetch {
                                                    partition: tp,
                                                    partition_data: partition,
                                                    next_fetch_offset: data.fetch_offset,
                                                    last_epoch: None,
                                                    is_consumed: false,
                                                    initialized: false,
                                                },
                                            );
                                        }
                                        None => {
                                            if metadata.is_full() {
                                                warn!(
                                                    "Response for missing full request {tp}; \
                                                     metadata={metadata}."
                                                );
                                            } else {
                                                warn!(
                                                    "Response for missing session request \
                                                     partition: {tp}; metadata={metadata}."
                                                );
                                            }
                                            continue;
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
                        None => {
                            error!(
                                "Unable to find FetchSessionHandler for node {}. Ignoring fetch \
                                 response.",
                                node.key()
                            );
                            continue;
                        }
                    }
                }
            }
            Ok(())
        } else {
            Err(Error::InvalidApiRequest(ApiKey::FetchKey))
        }
    }
}

impl<Exe: Executor> Fetcher<Exe> {
    fn topic_id(&self, topic_name: &TopicName) -> Uuid {
        if let Some(topic_id) = self.client.cluster_meta.topic_id(topic_name) {
            topic_id
        } else {
            Uuid::nil()
        }
    }

    async fn fetchable_partitions(&self) -> Vec<TopicPartition> {
        let mut exclude: HashSet<TopicPartition> = HashSet::new();
        for fetch in self.completed_fetches.lock().iter() {
            exclude.insert(fetch.partition.clone());
        }
        self.subscription
            .lock()
            .await
            .fetchable_partitions(|tp| !exclude.contains(tp))
    }

    async fn validate_position_on_metadata_change(&self) {
        let mut lock = self.subscription.lock().await;
        let mut tp_list = Vec::with_capacity(lock.assignments.len());
        for (tp, _) in lock.assignments.iter() {
            tp_list.push(tp.clone());
        }
        for tp in tp_list {
            let current_leader = self.client.cluster_meta.current_leader(&tp);
            let _ = lock.maybe_validate_position_for_current_leader(&tp, current_leader);
        }
    }

    async fn prepare_fetch_requests(&self) -> Result<HashMap<NodeId, FetchRequestData>> {
        let mut fetchable: HashMap<NodeId, FetchRequestDataBuilder> = HashMap::new();

        self.validate_position_on_metadata_change().await;

        for tp in self.fetchable_partitions().await.iter() {
            if let Some(position) = self.subscription.lock().await.position(tp) {
                if let Some(node) = position.current_leader.leader {
                    if self.nodes_with_pending_fetch_requests.contains(&node) {
                        trace!(
                            "Skipping fetch for {} because previous request to {} has not been \
                             processed",
                            tp,
                            node
                        );
                    } else {
                        let data = FetchRequestPartitionData {
                            topic_id: self.topic_id(&tp.topic),
                            fetch_offset: position.offset,
                            log_start_offset: INVALID_LOG_START_OFFSET,
                            max_bytes: self.fetch_size,
                            current_leader_epoch: position.offset_epoch,
                            last_fetched_epoch: None,
                        };
                        if let Some(builder) = fetchable.get_mut(&node) {
                            builder.add(tp.clone(), data);
                        } else {
                            if self.sessions.get_mut(&node).is_none() {
                                let session = FetchSession::new(node);
                                self.sessions.insert(node, session);
                            }

                            let mut builder = FetchRequestDataBuilder::new();
                            builder.add(tp.clone(), data);
                            fetchable.insert(node, builder);
                            debug!(
                                "Added {:?} fetch request for {} at position {} to node {}",
                                self.isolation_level, tp, position.offset, node
                            );
                        }
                    }
                } else {
                    debug!(
                        "Requesting metadata update for {} since the position {:?} is missing the \
                         current leader node",
                        tp, position
                    );
                    // TODO: metadata update
                    continue;
                }
            } else {
                return Err(Error::Custom(format!(
                    "Missing position for fetchable {tp}"
                )));
            }
        }

        let mut requests = HashMap::new();
        for (node, mut builder) in fetchable {
            if let Some(mut session) = self.sessions.get_mut(&node) {
                requests.insert(node, builder.build(session.value_mut()));
            }
        }
        Ok(requests)
    }

    async fn reset_offset(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::ListOffsetsKey) {
            let version = version_range.max;
            let partitions = self
                .subscription
                .lock()
                .await
                .partitions_need_reset(self.timestamp);
            if partitions.is_empty() {
                return Ok(());
            }

            let mut offset_reset_timestamps = HashMap::new();
            for partition in partitions {
                if let Some(tp_state) = self.subscription.lock().await.assignments.get(&partition) {
                    let timestamp = tp_state.offset_strategy.strategy_timestamp();
                    if timestamp != 0 {
                        offset_reset_timestamps.insert(partition, timestamp);
                    }
                }
            }

            let requests = self
                .group_list_offsets_request(&mut offset_reset_timestamps, version)
                .await?;
            for (node, request) in requests {
                let response = self.client.list_offsets(&node, request).await?;
                let list_offset_result = self.handle_list_offsets_response(response)?;
                if !list_offset_result.partitions_to_retry.is_empty() {
                    self.subscription.lock().await.request_failed(
                        list_offset_result.partitions_to_retry,
                        self.timestamp + self.retry_backoff_ms,
                    );
                    // TODO: update metadata
                }

                for (tp, list_offsets_data) in list_offset_result.fetched_offsets {
                    if let Some(timestamp) = offset_reset_timestamps.get(&tp) {
                        self.reset_offset_if_needed(
                            tp,
                            OffsetResetStrategy::from_timestamp(*timestamp),
                            list_offsets_data,
                        )
                            .await?;
                    }
                }
            }

            Ok(())
        } else {
            Err(Error::InvalidApiRequest(ApiKey::ListOffsetsKey))
        }
    }

    async fn reset_offset_if_needed(
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
            .lock()
            .await
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

    async fn group_list_offsets_request(
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

                self.subscription.lock().await.set_next_allowed_retry(
                    topics.keys(),
                    self.timestamp + self.request_timeout_ms,
                );
                let request = self.list_offsets_builder(topics, version)?;
                node_request.insert(node_entry.value().clone(), request);
            }
        }
        Ok(node_request)
    }

    async fn offset_reset_strategy_timestamp(&self, partition: &TopicPartition) -> Option<i64> {
        let reset_strategy = self
            .subscription
            .lock()
            .await
            .assignments
            .get(partition)
            .map(|tp_state| tp_state.offset_strategy);
        reset_strategy.map(|strategy| strategy.strategy_timestamp())
    }

    fn add_to_forgotten_topic_map(
        &self,
        to_forget: Drain<TopicIdPartition>,
        forgotten_topics: &mut HashMap<TopicName, ForgottenTopic>,
    ) {
        for tp in to_forget {
            match forgotten_topics.get_mut(&tp.partition.topic) {
                Some(topic) => topic.partitions.push(tp.partition.partition),
                None => {
                    let topic = ForgottenTopic {
                        topic_id: tp.topic_id,
                        topic: tp.partition.topic,
                        partitions: vec![tp.partition.partition],
                        unknown_tagged_fields: Default::default(),
                    };
                    forgotten_topics.insert(topic.topic.clone(), topic);
                }
            }
        }
    }

    async fn fetch_builder(
        &self,
        data: &mut FetchRequestData,
        version: i16,
    ) -> Result<FetchRequest> {
        let mut request = FetchRequest::default();

        let mut forgotten_topics = HashMap::new();
        self.add_to_forgotten_topic_map(data.to_forget.drain(..), &mut forgotten_topics);

        // If a version older than v13 is used, topic-partition which were replaced
        // by a topic-partition with the same name but a different topic ID are not
        // sent out in the "forget" set in order to not remove the newly added
        // partition in the "fetch" set.
        if version >= 13 {
            self.add_to_forgotten_topic_map(data.to_replace.drain(..), &mut forgotten_topics);
        }
        request.forgotten_topics_data = map_to_list(forgotten_topics);

        let mut topics: HashMap<TopicName, FetchTopic> = HashMap::new();
        for (tp, data) in data.to_send.drain() {
            let partition = FetchPartition {
                partition: tp.partition,
                current_leader_epoch: data
                    .current_leader_epoch
                    .unwrap_or(NO_PARTITION_LEADER_EPOCH),
                last_fetched_epoch: data.last_fetched_epoch.unwrap_or(NO_PARTITION_LEADER_EPOCH),
                fetch_offset: data.fetch_offset + 1,
                log_start_offset: data.log_start_offset,
                partition_max_bytes: data.max_bytes,
                ..Default::default()
            };
            match topics.get_mut(&tp.topic) {
                Some(fetch_topic) => fetch_topic.partitions.push(partition),
                None => {
                    let topic = FetchTopic {
                        topic: tp.topic.clone(),
                        topic_id: data.topic_id,
                        partitions: vec![partition],
                        ..Default::default()
                    };
                    topics.insert(tp.topic, topic);
                }
            }
        }

        if version <= 13 {
            request.replica_id = BrokerId(-1);
            request.max_wait_ms = self.max_wait_ms;
            request.min_bytes = self.min_bytes;
            request.topics = map_to_list(topics);

            if version >= 3 {
                request.max_bytes = self.max_bytes;
            } else {
                request.max_bytes = i32::MAX;
            }
            if version >= 4 {
                request.isolation_level = IsolationLevel::ReadUncommitted.into();
            }
            if version >= 7 {
                request.session_id = data.metadata.session_id;
                request.session_epoch = data.metadata.epoch;
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
    pub session_id: i32,
    pub epoch: i32,
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

    pub fn next_incremental(&mut self) {
        self.epoch = next_epoch(self.epoch);
    }

    pub fn next_close_existing(&mut self) {
        self.epoch = INITIAL_EPOCH;
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

#[derive(Debug, Clone, Default)]
pub struct CompletedFetch {
    pub(crate) partition: TopicPartition,
    pub(crate) partition_data: PartitionData,
    next_fetch_offset: i64,
    last_epoch: Option<i32>,
    is_consumed: bool,
    initialized: bool,
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

#[derive(Default)]
pub struct Fetch {
    pub(crate) records: HashMap<TopicPartition, Vec<Record>>,
    position_advanced: bool,
    pub(crate) num_records: i32,
}

impl Fetch {
    pub(crate) fn records(self) -> Vec<Record> {
        let mut records = Vec::with_capacity(self.num_records as usize);
        for (_, records_per_tp) in self.records {
            records.extend(records_per_tp);
        }
        records
    }
}
