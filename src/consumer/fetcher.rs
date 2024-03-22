use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    vec::Drain,
};

use dashmap::{DashMap, DashSet};
use futures::channel::{mpsc, oneshot};
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{
        fetch_request::{FetchPartition, FetchTopic, ForgottenTopic},
        fetch_response::PartitionData,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        ApiKey, BrokerId, FetchRequest, ListOffsetsRequest, ListOffsetsResponse, TopicName,
    },
    records::NO_PARTITION_LEADER_EPOCH,
    ResponseError,
};
use tracing::{debug, error, trace, warn};

use crate::{
    client::Kafka,
    consumer::{
        fetch_session::{
            FetchRequestData, FetchRequestDataBuilder, FetchRequestPartitionData, FetchSession,
        },
        subscription_state::FetchPosition,
        ConsumerOptions, IsolationLevel, OffsetResetStrategy,
    },
    coordinator::CoordinatorEvent,
    error::Result,
    executor::Executor,
    map_to_list,
    metadata::{Node, TopicIdPartition, TopicPartition},
    Error, NodeId, INVALID_LOG_START_OFFSET, UNKNOWN_EPOCH, UNKNOWN_OFFSET,
};

#[derive(Clone)]
pub struct Fetcher<Exe: Executor> {
    pub client: Kafka<Exe>,
    timestamp: i64,
    options: Arc<ConsumerOptions>,
    pub event_tx: mpsc::UnboundedSender<CoordinatorEvent>,
    sessions: Arc<DashMap<NodeId, FetchSession>>,
    completed_fetches_tx: mpsc::UnboundedSender<CompletedFetch>,
    pub completed_partitions: Arc<DashSet<TopicPartition>>,
    nodes_with_pending_fetch_requests: HashSet<i32>,
    paused: Arc<AtomicBool>,
}

impl<Exe: Executor> Fetcher<Exe> {
    pub fn new(
        client: Kafka<Exe>,
        timestamp: i64,
        event_tx: mpsc::UnboundedSender<CoordinatorEvent>,
        options: Arc<ConsumerOptions>,
        completed_fetches_tx: mpsc::UnboundedSender<CompletedFetch>,
    ) -> Self {
        let sessions = DashMap::new();
        for node in client.cluster.nodes.iter() {
            sessions.insert(node.id, FetchSession::new(node.id));
        }

        Self {
            client,
            timestamp,
            event_tx,
            sessions: Arc::new(sessions),
            completed_fetches_tx,
            completed_partitions: Arc::new(DashSet::with_capacity(100)),
            options,
            nodes_with_pending_fetch_requests: HashSet::new(),
            paused: Arc::new(AtomicBool::new(true)),
        }
    }

    pub(crate) fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    pub(crate) fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    pub(crate) fn resume(&self) {
        self.paused.store(false, Ordering::Release);
    }

    pub async fn fetch(&mut self) -> Result<()> {
        match self.client.version_range(ApiKey::FetchKey) {
            Some(version_range) => {
                let mut version = version_range.max;
                let fetch_requests = self.prepare_fetch_requests().await?;
                for (node, mut fetch_request_data) in fetch_requests {
                    if !fetch_request_data.can_use_topic_ids {
                        version = 12;
                    }
                    let metadata = fetch_request_data.metadata;
                    if let Some(node) = self.client.cluster.nodes.get(&node) {
                        let fetch_request =
                            self.fetch_builder(&mut fetch_request_data, version).await?;
                        trace!("Send fetch request: {:?}", fetch_request);
                        // We add the node to the set of nodes with pending fetch requests before
                        // adding the listener because the future may have
                        // been fulfilled on another thread (e.g. during a
                        // disconnection being handled by the heartbeat
                        // thread) which will mean the listener
                        // will be invoked synchronously.
                        self.nodes_with_pending_fetch_requests.insert(node.id);
                        let fetch_response = self.client.fetch(node.value(), fetch_request).await?;
                        trace!("Receive fetch response: {:?}", fetch_response);
                        match self.sessions.get_mut(node.key()) {
                            Some(mut session) => {
                                if !session
                                    .value_mut()
                                    .handle_fetch_response(&fetch_response, version)
                                {
                                    if let Some(error) = fetch_response.error_code.err() {
                                        if error == ResponseError::FetchSessionTopicIdError {
                                            self.client.update_full_metadata().await?;
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
                                        let tp = TopicPartition {
                                            topic: topic.clone(),
                                            partition: partition.partition_index,
                                        };

                                        match fetch_request_data.session_partitions.get(&tp) {
                                            Some(data) => {
                                                debug!(
                                                    "Fetch {:?} at offset {} for {tp}",
                                                    self.options.isolation_level, data.fetch_offset
                                                );
                                                self.completed_partitions.insert(tp.clone());
                                                if let Err(err) = self
                                                    .completed_fetches_tx
                                                    .unbounded_send(CompletedFetch {
                                                        partition: tp,
                                                        partition_data: partition,
                                                        next_fetch_offset: data.fetch_offset,
                                                        last_epoch: None,
                                                        is_consumed: false,
                                                        initialized: false,
                                                    })
                                                {
                                                    error!("send error: {}", err);
                                                }
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
                                                         {tp}; metadata={metadata}."
                                                    );
                                                }
                                                continue;
                                            }
                                        }
                                    }
                                }

                                if version >= 7 {
                                    match fetch_response.error_code.err() {
                                        None => {
                                            session.next_metadata.session_id =
                                                fetch_response.session_id
                                        }
                                        Some(error) => return Err(error.into()),
                                    }
                                }
                            }
                            None => {
                                error!(
                                    "Unable to find FetchSession for node {}. Ignoring fetch \
                                     response.",
                                    node.key()
                                );
                                continue;
                            }
                        }
                        // TODO: How to ensure that `node.id` will be removed
                        self.nodes_with_pending_fetch_requests.remove(&node.id);
                    }
                }
                Ok(())
            }
            None => Err(Error::InvalidApiRequest(ApiKey::FetchKey)),
        }
    }
}

impl<Exe: Executor> Fetcher<Exe> {
    async fn fetchable_partitions(&self) -> Result<Vec<TopicPartition>> {
        let mut exclude: HashSet<TopicPartition> = HashSet::new();
        for fetch in self.completed_partitions.iter() {
            exclude.insert(fetch.key().clone());
        }
        let (tx, rx) = oneshot::channel();
        self.event_tx
            .unbounded_send(CoordinatorEvent::FetchablePartitions {
                exclude,
                partitions_tx: tx,
            })?;
        match rx.await {
            Ok(partitions) => Ok(partitions),
            Err(err) => {
                error!("Failed to get fetchable partitions, error: Canceled");
                Err(err.into())
            }
        }
    }

    async fn validate_position_on_metadata_change(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.event_tx
            .unbounded_send(CoordinatorEvent::Assignments { partitions_tx: tx })?;
        match rx.await {
            Ok(partitions) => {
                for tp in partitions {
                    let current_leader = self.client.cluster.current_leader(&tp);
                    self.event_tx.unbounded_send(
                        CoordinatorEvent::MaybeValidatePositionForCurrentLeader {
                            partition: tp,
                            current_leader,
                        },
                    )?;
                }
                Ok(())
            }
            Err(err) => {
                error!("Failed to validate position on metadata change, error: Canceled");
                Err(err.into())
            }
        }
    }

    async fn fetch_position(&self, partition: TopicPartition) -> Result<Option<FetchPosition>> {
        let (tx, rx) = oneshot::channel();
        self.event_tx
            .unbounded_send(CoordinatorEvent::FetchPosition {
                partition,
                position_tx: tx,
            })?;
        match rx.await {
            Ok(position) => Ok(position),
            Err(err) => {
                error!("Failed to get fetch position, error: Canceled");
                Err(err.into())
            }
        }
    }

    async fn prepare_fetch_requests(&self) -> Result<HashMap<NodeId, FetchRequestData>> {
        let mut fetchable: HashMap<NodeId, FetchRequestDataBuilder> = HashMap::new();

        self.validate_position_on_metadata_change().await?;

        let fetchable_partitions = self.fetchable_partitions().await?;
        for tp in fetchable_partitions {
            match self.fetch_position(tp.clone()).await? {
                Some(position) => match position.current_leader.leader {
                    Some(node) => {
                        if self.nodes_with_pending_fetch_requests.contains(&node) {
                            trace!(
                                "Skipping fetch for {tp} because previous request to {node} has \
                                 not been processed"
                            );
                        } else {
                            let data = FetchRequestPartitionData {
                                topic_id: self.client.topic_id(&tp.topic),
                                fetch_offset: position.offset,
                                log_start_offset: INVALID_LOG_START_OFFSET,
                                max_bytes: self.options.max_partition_fetch_bytes,
                                current_leader_epoch: position.offset_epoch,
                                last_fetched_epoch: None,
                            };
                            match fetchable.get_mut(&node) {
                                Some(builder) => builder.add(tp.clone(), data),
                                None => {
                                    if self.sessions.get_mut(&node).is_none() {
                                        let session = FetchSession::new(node);
                                        self.sessions.insert(node, session);
                                    }

                                    let mut builder = FetchRequestDataBuilder::new();
                                    builder.add(tp.clone(), data);
                                    fetchable.insert(node, builder);
                                    debug!(
                                        "Added {:?} fetch request for {tp} at position {} to node \
                                         {node}",
                                        self.options.isolation_level, position.offset
                                    );
                                }
                            }
                        }
                    }
                    None => {
                        debug!(
                            "Requesting metadata update for {} since the position {:?} is missing \
                             the current leader node",
                            tp, position
                        );
                        self.client.update_full_metadata().await?;
                        continue;
                    }
                },
                None => {
                    return Err(Error::Custom(format!(
                        "Missing position for fetchable {tp}"
                    )));
                }
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

    async fn strategy_timestamp(&self, partition: TopicPartition) -> Result<Option<i64>> {
        let (tx, rx) = oneshot::channel();
        self.event_tx
            .unbounded_send(CoordinatorEvent::StrategyTimestamp {
                partition,
                timestamp_tx: tx,
            })?;
        match rx.await {
            Ok(timestamp) => Ok(timestamp),
            Err(err) => {
                error!("Failed to get strategy timestamp, error: Canceled");
                Err(err.into())
            }
        }
    }

    async fn partitions_need_reset(&self, timestamp: i64) -> Result<Vec<TopicPartition>> {
        let (tx, rx) = oneshot::channel();
        self.event_tx
            .unbounded_send(CoordinatorEvent::PartitionsNeedReset {
                timestamp,
                partition_tx: tx,
            })?;
        match rx.await {
            Ok(partitions) => Ok(partitions),
            Err(err) => {
                error!("Failed to partitions which need reset, error: Canceled");
                Err(err.into())
            }
        }
    }

    pub(crate) async fn reset_offset(&mut self) -> Result<()> {
        let partitions = self.partitions_need_reset(self.timestamp).await?;
        if partitions.is_empty() {
            return Ok(());
        }

        let mut offset_reset_timestamps = HashMap::new();
        for partition in partitions {
            if let Some(timestamp) = self.strategy_timestamp(partition.clone()).await? {
                if timestamp != 0 {
                    offset_reset_timestamps.insert(partition, timestamp);
                }
            }
        }

        let requests = self
            .group_list_offsets_request(&mut offset_reset_timestamps)
            .await?;
        for (node, request) in requests {
            let response = self.client.list_offsets(&node, request).await?;
            let list_offset_result = self.handle_list_offsets_response(response)?;
            if !list_offset_result.partitions_to_retry.is_empty() {
                self.event_tx
                    .unbounded_send(CoordinatorEvent::RequestFailed {
                        partitions: list_offset_result.partitions_to_retry,
                        next_retry_time_ms: self.timestamp + self.options.retry_backoff_ms,
                    })?;
                self.client.update_full_metadata().await?;
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
    }

    async fn reset_offset_if_needed(
        &mut self,
        partition: TopicPartition,
        offset_strategy: OffsetResetStrategy,
        offset_data: ListOffsetData,
    ) -> Result<()> {
        let position = FetchPosition {
            offset: offset_data.offset - 1,
            offset_epoch: None,
            current_leader: self.client.cluster.current_leader(&partition),
        };
        // TODO: metadata update last seen epoch if newer
        self.event_tx
            .unbounded_send(CoordinatorEvent::MaybeSeekUnvalidated {
                partition,
                position,
                offset_strategy,
            })?;
        Ok(())
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
                                "Handing v0 ListOffsetResponse response for [{} - {}]. Fetched \
                                 offset {offset}",
                                topic.name.as_str(),
                                partition
                            );
                            if offset != UNKNOWN_OFFSET {
                                let tp = TopicPartition {
                                    topic: topic.name,
                                    partition,
                                };
                                fetched_offsets.insert(
                                    tp,
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
                                topic.name.as_str(),
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
                                let tp = TopicPartition {
                                    topic: topic.name,
                                    partition,
                                };
                                fetched_offsets.insert(
                                    tp,
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
                            topic.name.as_str(),
                            partition
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
                            topic.name.as_str(),
                            partition,
                            error
                        );
                        let tp = TopicPartition {
                            topic: topic.name.clone(),
                            partition,
                        };
                        partitions_to_retry.push(tp);
                    }
                    Some(ResponseError::UnknownTopicOrPartition) => {
                        warn!(
                            "Received unknown topic or partition error in ListOffset request for \
                             partition [{} - {}]",
                            topic.name.as_str(),
                            partition
                        );
                        let tp = TopicPartition {
                            topic: topic.name.clone(),
                            partition,
                        };
                        partitions_to_retry.push(tp);
                    }
                    Some(ResponseError::TopicAuthorizationFailed) => {
                        unauthorized_topics.push(topic.name.clone());
                    }
                    Some(error) => {
                        warn!(
                            "Attempt to fetch offsets for [{} - {}] failed due to unexpected \
                             exception: {}, retrying.",
                            topic.name.as_str(),
                            partition,
                            error
                        );
                        let tp = TopicPartition {
                            topic: topic.name.clone(),
                            partition,
                        };
                        partitions_to_retry.push(tp);
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
    ) -> Result<HashMap<Node, ListOffsetsRequest>> {
        let mut node_request = HashMap::new();
        for node_entry in self.client.cluster.nodes.iter() {
            if let Ok(node_topology) = self.client.cluster.drain_node(node_entry.value().id) {
                let partitions = node_topology.value();

                let mut topics = HashMap::new();
                for partition in partitions {
                    if let Some(timestamp) = offset_reset_timestamps.get_mut(partition) {
                        topics.insert(partition.clone(), *timestamp);
                    }
                }

                self.event_tx
                    .unbounded_send(CoordinatorEvent::SetNextAllowedRetry {
                        assignments: topics.keys().cloned().collect(),
                        next_allowed_reset_ms: self.timestamp
                            + self.options.request_timeout_ms as i64,
                    })?;
                let request = self.list_offsets_builder(topics)?;
                node_request.insert(node_entry.value().clone(), request);
            }
        }
        Ok(node_request)
    }

    fn add_to_forgotten_topic_map(
        &self,
        to_forget: Drain<TopicIdPartition>,
        forgotten_topics: &mut HashMap<TopicName, ForgottenTopic>,
    ) -> Result<()> {
        for tp in to_forget {
            match forgotten_topics.get_mut(&tp.partition.topic) {
                Some(topic) => topic.partitions.push(tp.partition.partition),
                None => {
                    let mut topic = ForgottenTopic::default();
                    topic.topic_id = tp.topic_id;
                    topic.topic = tp.partition.topic;
                    topic.partitions = vec![tp.partition.partition];
                    forgotten_topics.insert(topic.topic.clone(), topic);
                }
            }
        }
        Ok(())
    }

    async fn fetch_builder(
        &self,
        data: &mut FetchRequestData,
        version: i16,
    ) -> Result<FetchRequest> {
        let mut request = FetchRequest::default();

        let mut forgotten_topics = HashMap::new();
        self.add_to_forgotten_topic_map(data.to_forget.drain(..), &mut forgotten_topics)?;

        // If a version older than v13 is used, topic-partition which were replaced
        // by a topic-partition with the same name but a different topic ID are not
        // sent out in the "forget" set in order to not remove the newly added
        // partition in the "fetch" set.
        if version >= 13 {
            self.add_to_forgotten_topic_map(data.to_replace.drain(..), &mut forgotten_topics)?;
        }
        request.forgotten_topics_data = map_to_list(forgotten_topics);

        let mut topics: HashMap<TopicName, FetchTopic> = HashMap::new();
        for (tp, data) in data.to_send.drain(..) {
            let mut partition = FetchPartition::default();
            partition.partition = tp.partition;
            partition.current_leader_epoch = data
                .current_leader_epoch
                .unwrap_or(NO_PARTITION_LEADER_EPOCH);
            partition.last_fetched_epoch =
                data.last_fetched_epoch.unwrap_or(NO_PARTITION_LEADER_EPOCH);
            partition.fetch_offset = data.fetch_offset + 1;
            partition.log_start_offset = data.log_start_offset;
            partition.partition_max_bytes = data.max_bytes;

            match topics.get_mut(&tp.topic) {
                Some(fetch_topic) => fetch_topic.partitions.push(partition),
                None => {
                    let mut topic = FetchTopic::default();
                    topic.topic = tp.topic.clone();
                    topic.topic_id = data.topic_id;
                    topic.partitions = vec![partition];
                    topics.insert(tp.topic, topic);
                }
            }
        }

        request.replica_id = BrokerId(-1);
        request.max_wait_ms = self.options.fetch_max_wait_ms;
        request.min_bytes = self.options.fetch_min_bytes;
        request.topics = map_to_list(topics);

        if version >= 3 {
            request.max_bytes = self.options.max_partition_fetch_bytes;
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
        Ok(request)
    }

    pub fn list_offsets_builder(
        &self,
        assignments: HashMap<TopicPartition, i64>,
    ) -> Result<ListOffsetsRequest> {
        let mut topics: HashMap<TopicName, Vec<ListOffsetsPartition>> = HashMap::new();
        for (partition, timestamp) in assignments {
            let mut list_offsets_partition = ListOffsetsPartition::default();
            list_offsets_partition.partition_index = partition.partition;
            list_offsets_partition.current_leader_epoch = NO_PARTITION_LEADER_EPOCH;
            list_offsets_partition.timestamp = timestamp;

            match topics.get_mut(&partition.topic) {
                Some(partitions) => partitions.push(list_offsets_partition),
                None => {
                    let partitions = vec![list_offsets_partition];
                    topics.insert(partition.topic.clone(), partitions);
                }
            }
        }

        let mut list_offsets_topics = Vec::with_capacity(topics.len());
        for (name, partitions) in topics {
            let mut topic = ListOffsetsTopic::default();
            topic.name = name;
            topic.partitions = partitions;
            list_offsets_topics.push(topic);
        }

        let mut request = ListOffsetsRequest::default();
        request.replica_id = BrokerId(-1);
        request.topics = list_offsets_topics;
        request.isolation_level = IsolationLevel::ReadUncommitted.into();

        Ok(request)
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
