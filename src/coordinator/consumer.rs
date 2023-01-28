use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use futures::{
    future::{select, Either},
    lock::Mutex,
    pin_mut, StreamExt,
};
use indexmap::IndexMap;
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{
        consumer_protocol_assignment::TopicPartition as CpaTopicPartition,
        join_group_request::JoinGroupRequestProtocol,
        join_group_response::JoinGroupResponseMember,
        offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
        offset_fetch_request::{
            OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
        },
        sync_group_request::SyncGroupRequestAssignment,
        ApiKey, ConsumerProtocolAssignment, DescribeGroupsRequest, HeartbeatRequest,
        JoinGroupRequest, LeaveGroupRequest, OffsetCommitRequest, OffsetFetchRequest,
        SyncGroupRequest, TopicName,
    },
    protocol::{Message, StrBytes},
    ResponseError,
};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::{
    client::Kafka,
    consumer::{
        partition_assignor::{
            Assignment, GroupSubscription, PartitionAssigner, PartitionAssignor, Subscription,
            SUPPORTED_PARTITION_ASSIGNORS,
        },
        ConsumerGroupMetadata, ConsumerOptions, SubscriptionState, TopicPartitionState,
    },
    coordinator::{find_coordinator, CoordinatorType},
    error::{ConsumeError, Result},
    executor::Executor,
    metadata::{Node, TopicPartition},
    to_version_prefixed_bytes, Error, MemberId, ToStrBytes, DEFAULT_GENERATION_ID,
};

const CONSUMER_PROTOCOL_TYPE: &str = "consumer";

macro_rules! offset_fetch_block {
    ($self:ident, $source:ident) => {
        for topic in $source.topics {
            for partition in topic.partitions {
                let tp = TopicPartition::new(topic.name.clone(), partition.partition_index);
                if partition.error_code.is_ok() {
                    if let Some(partition_state) =
                        $self.subscriptions.lock().await.assignments.get_mut(&tp)
                    {
                        partition_state.position.offset = partition.committed_offset;
                        partition_state.position.offset_epoch =
                            Some(partition.committed_leader_epoch);
                        debug!(
                            "Fetch {tp} offset success, offset: {}",
                            partition.committed_offset
                        );
                    }
                } else {
                    error!(
                        "Fetch {tp} offset error, {}",
                        partition.error_code.err().unwrap()
                    );
                }
            }
        }
    };
}

macro_rules! coordinator_task {
    ($self:ident, $interval_ms:ident, $task:ident) => {
        let mut interval = $self
            .client
            .executor
            .interval(Duration::from_millis($interval_ms as u64));

        let weak_inner = Arc::downgrade(&$self.inner);

        let mut shutdown = $self.notify_shutdown.subscribe();
        let res = $self.client.executor.spawn(Box::pin(async move {
            while interval.next().await.is_some() {
                match weak_inner.upgrade() {
                    Some(strong_inner) => {
                        let mut lock = strong_inner.lock().await;
                        let task = lock.$task();
                        let shutdown = shutdown.recv();

                        pin_mut!(task);
                        pin_mut!(shutdown);

                        match select(task, shutdown).await {
                            Either::Left((Err(err), _)) => {
                                error!("{} failed, {}", stringify!($task), err);
                            }
                            Either::Left((Ok(_), _)) => {}
                            Either::Right(_) => break,
                        }
                    }
                    None => break,
                }
            }
            info!("{} task finished.", stringify!(task));
        }));

        if res.is_err() {
            return Err(Error::Custom(format!(
                "The executor could not spawn the {} task",
                stringify!(task)
            )));
        }
    };
}
#[derive(Debug, Copy, Clone)]
enum MemberState {
    // the client is not part of a group
    UnJoined,
    // the client has sent the join group request, but have not received response
    PreparingRebalance,
    // the client has received join group response, but have not received assignment
    CompletingRebalance,
    // the client has joined and is sending heartbeats
    Stable,
}

pub struct ConsumerCoordinator<Exe: Executor> {
    client: Kafka<Exe>,
    inner: Arc<Mutex<Inner<Exe>>>,
    options: Arc<ConsumerOptions>,
    notify_shutdown: broadcast::Sender<()>,
}

impl<Exe: Executor> ConsumerCoordinator<Exe> {
    pub async fn new(
        client: Kafka<Exe>,
        options: Arc<ConsumerOptions>,
        notify: broadcast::Sender<()>,
    ) -> Result<ConsumerCoordinator<Exe>> {
        let inner = Inner::new(client.clone(), options.clone()).await?;
        let inner = Arc::new(Mutex::new(inner));

        Ok(Self {
            inner,
            client,
            options,
            notify_shutdown: notify,
        })
    }

    pub async fn subscribe(&self, topics: Vec<TopicName>) -> Result<()> {
        self.inner.lock().await.subscribe(topics).await
    }

    pub async fn subscriptions(&self) -> Arc<Mutex<SubscriptionState>> {
        self.inner.lock().await.subscriptions.clone()
    }

    pub async fn current_generation(&self) -> i32 {
        self.inner.lock().await.group_meta.generation_id
    }

    pub async fn join_group(&self) -> Result<()> {
        self.inner.lock().await.join_group().await
    }

    pub async fn sync_group(&self) -> Result<()> {
        self.inner.lock().await.sync_group().await
    }

    pub async fn offset_fetch(&self) -> Result<()> {
        self.inner.lock().await.offset_fetch().await
    }

    pub async fn offset_commit(&mut self) -> Result<()> {
        if !self.options.auto_commit_enabled {
            self.inner.lock().await.offset_commit().await?;
        }
        Ok(())
    }

    pub async fn prepare_fetch(&self) -> Result<()> {
        self.join_group().await?;
        self.sync_group().await?;
        self.offset_fetch().await?;

        if self.options.auto_commit_enabled {
            // auto commit offset
            let interval_ms = self.options.auto_commit_interval_ms;
            coordinator_task!(self, interval_ms, offset_commit);
        }
        // heartbeat
        let interval_ms = self.options.rebalance_options.heartbeat_interval_ms;
        coordinator_task!(self, interval_ms, heartbeat);
        Ok(())
    }
}

struct Inner<Exe: Executor> {
    client: Kafka<Exe>,
    node: Node,
    pub group_meta: ConsumerGroupMetadata,
    group_subscription: GroupSubscription,
    consumer_options: Arc<ConsumerOptions>,
    pub subscriptions: Arc<Mutex<SubscriptionState>>,
    assignors: Vec<PartitionAssignor>,
    state: MemberState,
}

impl<Exe: Executor> Inner<Exe> {
    pub async fn new(client: Kafka<Exe>, options: Arc<ConsumerOptions>) -> Result<Inner<Exe>> {
        let group_id = options.group_id.clone().to_str_bytes();

        let node = find_coordinator(&client, group_id.clone(), CoordinatorType::Group).await?;

        info!(
            "Find coordinator success, group {:?}, node: {:?}",
            group_id, node
        );

        // TODO: check consumer options
        let assignors: Vec<_> = options.partition_assignment_strategy.split(',').collect();
        let assignors: Vec<PartitionAssignor> = SUPPORTED_PARTITION_ASSIGNORS
            .iter()
            .filter(|assignor| assignors.contains(&assignor.name()))
            .cloned()
            .collect();

        Ok(Self {
            client,
            node,
            subscriptions: Arc::new(Mutex::new(SubscriptionState::default())),
            group_meta: ConsumerGroupMetadata::new(group_id.into()),
            group_subscription: GroupSubscription::default(),
            consumer_options: options,
            assignors,
            state: MemberState::UnJoined,
        })
    }

    pub async fn subscribe(&self, topics: Vec<TopicName>) -> Result<()> {
        self.subscriptions
            .lock()
            .await
            .topics
            .extend(topics.clone());
        // TODO: remove it
        self.client.update_metadata(topics).await?;
        Ok(())
    }

    async fn rejoin_group(&mut self) -> Result<()> {
        self.join_group().await?;
        self.sync_group().await?;
        self.offset_fetch().await
    }

    fn reset_state(&mut self, should_reset_member_id: bool) {
        self.state = MemberState::UnJoined;
        if should_reset_member_id {
            self.group_meta.generation_id = DEFAULT_GENERATION_ID;
            self.group_meta.member_id = StrBytes::default();
            self.group_meta.protocol_name = None;
        } else {
            self.group_meta.generation_id = DEFAULT_GENERATION_ID;
            self.group_meta.protocol_name = None;
        }
    }

    pub async fn describe_groups(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::JoinGroupKey) {
            let describe_groups_response = self
                .client
                .describe_groups(&self.node, self.describe_groups_builder(version_range.max)?)
                .await?;
            for group in describe_groups_response.groups {
                if group.error_code.is_err() {
                    error!("Describe group {:?} failed", group.group_id);
                }
            }
            Ok(())
        } else {
            Err(Error::InvalidApiRequest(ApiKey::JoinGroupKey))
        }
    }

    #[async_recursion::async_recursion]
    pub async fn join_group(&mut self) -> Result<()> {
        self.state = MemberState::PreparingRebalance;
        if let Some(version_range) = self.client.version_range(ApiKey::JoinGroupKey) {
            let join_group_response = self
                .client
                .join_group(
                    &self.node,
                    self.join_group_builder(version_range.max).await?,
                )
                .await?;

            match join_group_response.error_code.err() {
                Some(ResponseError::MemberIdRequired) => {
                    self.group_meta.member_id = join_group_response.member_id;
                    warn!(
                        "Join group with unknown member id, will rejoin group [{}] with member \
                         id: {}",
                        self.group_meta.group_id.0, self.group_meta.member_id
                    );
                    self.join_group().await
                }
                Some(error) => Err(error.into()),
                None => {
                    self.group_meta.member_id = join_group_response.member_id;
                    self.group_meta.generation_id = join_group_response.generation_id;
                    self.group_meta.leader = join_group_response.leader;
                    self.group_meta.protocol_name = join_group_response.protocol_name;
                    self.group_meta.protocol_type = join_group_response.protocol_type;

                    let group_subscription = deserialize_member(join_group_response.members)?;
                    self.group_subscription = group_subscription;
                    self.state = MemberState::CompletingRebalance;

                    info!(
                        "Join group [{}] success, leader = {}, member_id = {}, generation_id = {}",
                        self.group_meta.group_id.0,
                        self.group_meta.leader,
                        self.group_meta.member_id,
                        self.group_meta.generation_id
                    );
                    Ok(())
                }
            }
        } else {
            Err(Error::InvalidApiRequest(ApiKey::JoinGroupKey))
        }
    }

    pub async fn leave_group(&mut self) -> Result<()> {
        let leave_group_response = self
            .client
            .leave_group(&self.node, self.leave_group_builder()?)
            .await?;

        match leave_group_response.error_code.err() {
            None => {
                for member in leave_group_response.members {
                    if member.error_code.is_ok() {
                        debug!(
                            "Member {} leave group {:?} success.",
                            member.member_id, self.group_meta.group_id
                        );
                    } else {
                        error!(
                            "Member {} leave group {:?} failed.",
                            member.member_id, self.group_meta.group_id
                        );
                    }
                }
                info!(
                    "Leave group {:?} success, member: {}",
                    self.group_meta.group_id, self.group_meta.member_id
                );
                Ok(())
            }
            Some(error) => Err(error.into()),
        }
    }

    fn has_generation_reset(&self) -> bool {
        self.group_meta.generation_id == DEFAULT_GENERATION_ID
            && self.group_meta.protocol_name.is_none()
    }

    fn is_protocol_type_inconsistent(&self, protocol_type: &Option<StrBytes>) -> bool {
        protocol_type.is_some() && protocol_type != &self.group_meta.protocol_type
    }

    pub async fn sync_group(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::SyncGroupKey) {
            let mut sync_group_response = self
                .client
                .sync_group(&self.node, self.sync_group_builder(version_range.max)?)
                .await?;
            match sync_group_response.error_code.err() {
                None => {
                    if self.is_protocol_type_inconsistent(&sync_group_response.protocol_type) {
                        error!(
                            "JoinGroup failed: Inconsistent Protocol Type, received {:?} but \
                             expected {:?}",
                            sync_group_response.protocol_type, self.group_meta.protocol_type
                        );
                        return Err(ResponseError::InconsistentGroupProtocol.into());
                    }

                    if self.group_meta.protocol_name.is_none() {
                        self.group_meta.protocol_name = sync_group_response.protocol_name;
                    }
                    if self.group_meta.protocol_type.is_none() {
                        self.group_meta.protocol_type = sync_group_response.protocol_type;
                    }
                    let assignment =
                        Assignment::deserialize_from_bytes(&mut sync_group_response.assignment)?;
                    self.subscriptions.lock().await.assignments.clear();
                    for (topic, partitions) in assignment.partitions {
                        for partition in partitions.iter() {
                            let tp = TopicPartition::new(topic.clone(), *partition);
                            let mut tp_state = TopicPartitionState::new(*partition);
                            tp_state.position.current_leader =
                                self.client.cluster_meta.current_leader(&tp);
                            self.subscriptions
                                .lock()
                                .await
                                .assignments
                                .insert(tp, tp_state);
                        }
                    }
                    self.state = MemberState::Stable;
                    info!(
                        "Sync group [{}] success, leader = {}, member_id = {}, generation_id = \
                         {}, protocol_type = {:?}, protocol_name = {:?}",
                        self.group_meta.group_id.0,
                        self.group_meta.leader,
                        self.group_meta.member_id,
                        self.group_meta.generation_id,
                        self.group_meta.protocol_type.as_ref(),
                        self.group_meta.protocol_name.as_ref(),
                    );
                    Ok(())
                }
                Some(error) => Err(error.into()),
            }
        } else {
            Err(Error::InvalidApiRequest(ApiKey::SyncGroupKey))
        }
    }

    pub async fn offset_fetch(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::OffsetFetchKey) {
            let mut offset_fetch_response = self
                .client
                .offset_fetch(
                    &self.node,
                    self.offset_fetch_builder(version_range.max).await?,
                )
                .await?;
            match offset_fetch_response.error_code.err() {
                None => {
                    if let Some(group) = offset_fetch_response.groups.pop() {
                        offset_fetch_block!(self, group);
                    } else {
                        offset_fetch_block!(self, offset_fetch_response);
                    }
                    Ok(())
                }
                Some(error) => Err(error.into()),
            }
        } else {
            Err(Error::InvalidApiRequest(ApiKey::OffsetFetchKey))
        }
    }

    pub async fn offset_commit(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::OffsetCommitKey) {
            let offset_commit_response = self
                .client
                .offset_commit(
                    &self.node,
                    self.offset_commit_builder(version_range.max).await?,
                )
                .await?;
            for topic in offset_commit_response.topics {
                for partition in topic.partitions {
                    if !partition.error_code.is_ok() {
                        error!(
                            "Failed to commit offset for partition {}",
                            partition.partition_index
                        );
                    }
                }
            }
            Ok(())
        } else {
            Err(Error::InvalidApiRequest(ApiKey::OffsetCommitKey))
        }
    }

    pub async fn heartbeat(&mut self) -> Result<()> {
        let sent_generation = self.group_meta.generation_id;
        if let Some(version_range) = self.client.version_range(ApiKey::HeartbeatKey) {
            let heartbeat_response = self
                .client
                .heartbeat(&self.node, self.heartbeat_builder(version_range.max)?)
                .await?;

            match heartbeat_response.error_code.err() {
                None => {
                    debug!(
                        "Heartbeat success, group: {}, member: {}",
                        self.group_meta.group_id.0, self.group_meta.member_id
                    );
                    Ok(())
                }
                Some(
                    error @ ResponseError::CoordinatorNotAvailable
                    | error @ ResponseError::NotCoordinator,
                ) => {
                    info!(
                        "Attempt to heartbeat failed since coordinator {} is either not started \
                         or not valid",
                        self.node.id
                    );
                    Err(error.into())
                }
                Some(error @ ResponseError::RebalanceInProgress) => {
                    if matches!(self.state, MemberState::Stable) {
                        warn!("Request joining group due to: group is already rebalancing");
                        self.rejoin_group().await?;
                        Err(error.into())
                    } else {
                        debug!(
                            "Ignoring heartbeat response with error {error} during {:?} state",
                            self.state
                        );
                        Ok(())
                    }
                }
                Some(
                    error @ ResponseError::IllegalGeneration
                    | error @ ResponseError::UnknownMemberId
                    | error @ ResponseError::FencedInstanceId,
                ) => {
                    if self.group_meta.generation_id == sent_generation {
                        info!(
                            "Attempt to heartbeat with generation {} and group instance id {:?} \
                             failed due to {error}, resetting generation",
                            self.group_meta.generation_id, self.group_meta.group_instance_id
                        );
                        self.reset_state(matches!(error, ResponseError::IllegalGeneration));
                        self.rejoin_group().await?;
                        Err(error.into())
                    } else {
                        info!(
                            "Attempt to heartbeat with stale generation {} and group instance id \
                             {:?} failed due to {error}, ignoring the error",
                            sent_generation, self.group_meta.group_instance_id
                        );
                        Ok(())
                    }
                }
                Some(error @ ResponseError::GroupAuthorizationFailed) => Err(error.into()),
                Some(error) => Err(error.into()),
            }
        } else {
            Err(Error::InvalidApiRequest(ApiKey::HeartbeatKey))
        }
    }
}

/// for request builder and response handler
impl<Exe: Executor> Inner<Exe> {
    async fn join_group_protocol(&self) -> Result<IndexMap<StrBytes, JoinGroupRequestProtocol>> {
        let topics = self
            .subscriptions
            .lock()
            .await
            .topics
            .iter()
            .cloned()
            .collect::<HashSet<TopicName>>();
        let mut protocols = IndexMap::with_capacity(topics.len());
        for assignor in self.assignors.iter() {
            let subscription = Subscription::new(
                topics.clone(),
                assignor.subscription_user_data(&topics)?,
                self.subscriptions.lock().await.partitions(),
            );
            let metadata = subscription.serialize_to_bytes()?;
            protocols.insert(
                assignor.name().to_string().to_str_bytes(),
                JoinGroupRequestProtocol {
                    metadata,
                    ..Default::default()
                },
            );
        }
        Ok(protocols)
    }

    pub fn look_up_assignor<S: AsRef<str>>(&self, name: S) -> Result<&PartitionAssignor> {
        for assigner in self.assignors.iter() {
            if assigner.name() == name.as_ref() {
                return Ok(assigner);
            }
        }
        Err(ConsumeError::PartitionAssignorNotAvailable(name.as_ref().to_string()).into())
    }

    fn describe_groups_builder(&self, version: i16) -> Result<DescribeGroupsRequest> {
        let mut request = DescribeGroupsRequest::default();
        if version <= 5 {
            request.groups = vec![self.group_meta.group_id.clone()];

            if version >= 3 {
                request.include_authorized_operations = true;
            }
        }
        Ok(request)
    }

    async fn join_group_builder(&self, version: i16) -> Result<JoinGroupRequest> {
        let mut request = JoinGroupRequest::default();
        if version <= 9 {
            request.group_id = self.group_meta.group_id.clone();
            request.member_id = self.group_meta.member_id.clone();
            request.protocol_type = StrBytes::from_str(CONSUMER_PROTOCOL_TYPE);
            request.protocols = self.join_group_protocol().await?;
            request.session_timeout_ms = self.consumer_options.rebalance_options.session_timeout_ms;
            if version >= 1 {
                request.rebalance_timeout_ms =
                    self.consumer_options.rebalance_options.rebalance_timeout_ms;
            }
            if version >= 5 {
                request.group_instance_id = self.group_meta.group_instance_id.clone();
            }
        }
        Ok(request)
    }

    fn leave_group_builder(&self) -> Result<LeaveGroupRequest> {
        let request = LeaveGroupRequest {
            group_id: self.group_meta.group_id.clone(),
            member_id: self.group_meta.member_id.clone(),
            ..Default::default()
        };

        // let mut members = Vec::new();
        // let member = MemberIdentity {
        //     member_id: self.group_meta.member_id.clone(),
        //     group_instance_id: self.group_meta.group_instance_id.clone(),
        //     ..Default::default()
        // };
        // members.push(member);
        // builder.members(members);

        Ok(request)
    }

    fn sync_group_builder(&self, version: i16) -> Result<SyncGroupRequest> {
        let mut request = SyncGroupRequest::default();
        if version <= 5 {
            request.group_id = self.group_meta.group_id.clone();
            request.member_id = self.group_meta.member_id.clone();
            request.generation_id = self.group_meta.generation_id;

            if self.group_meta.member_id == self.group_meta.leader {
                match self.group_meta.protocol_name {
                    Some(ref protocol) => {
                        let assignor = self.look_up_assignor(&protocol.to_string())?;
                        let cluster = self.client.cluster_meta.clone();
                        request.assignments = serialize_assignments(
                            assignor.assign(cluster, &self.group_subscription)?,
                        )?;
                    }
                    None => {
                        return Err(Error::Custom(format!(
                            "Group leader {} has no partition assignor protocol",
                            self.group_meta.leader
                        )));
                    }
                }
            }

            if version >= 3 {
                request.group_instance_id = self.group_meta.group_instance_id.clone();
            }

            if version == 5 {
                request.protocol_name = self.group_meta.protocol_name.clone();
                request.protocol_type = self.group_meta.protocol_type.clone();
            }
        }
        Ok(request)
    }

    pub async fn offset_fetch_builder(&self, version: i16) -> Result<OffsetFetchRequest> {
        let mut request = OffsetFetchRequest::default();
        if version <= 7 {
            let mut topics = Vec::with_capacity(self.subscriptions.lock().await.topics.len());
            for assign in self.subscriptions.lock().await.topics.iter() {
                let partitions = self.client.cluster_meta.partitions(assign)?;
                let topic = OffsetFetchRequestTopic {
                    name: assign.clone(),
                    partition_indexes: partitions.value().clone(),
                    ..Default::default()
                };
                topics.push(topic);
            }
            request.group_id = self.group_meta.group_id.clone();
            request.topics = Some(topics);
        } else {
            let mut topics = Vec::with_capacity(self.subscriptions.lock().await.topics.len());
            for assign in self.subscriptions.lock().await.topics.iter() {
                let partitions = self.client.cluster_meta.partitions(assign)?;
                let topic = OffsetFetchRequestTopics {
                    name: assign.clone(),
                    partition_indexes: partitions.value().clone(),
                    ..Default::default()
                };
                topics.push(topic);
            }

            let group = OffsetFetchRequestGroup {
                group_id: self.group_meta.group_id.clone(),
                topics: Some(topics),
                ..Default::default()
            };
            request.groups = vec![group];
        }

        Ok(request)
    }

    pub async fn offset_commit_builder(&self, version: i16) -> Result<OffsetCommitRequest> {
        let mut request = OffsetCommitRequest::default();
        if version <= 8 {
            request.group_id = self.group_meta.group_id.clone();

            let mut topics: HashMap<TopicName, Vec<OffsetCommitRequestPartition>> =
                HashMap::with_capacity(self.subscriptions.lock().await.topics.len());
            let all_consumed = self.subscriptions.lock().await.all_consumed();
            for (tp, meta) in all_consumed.iter() {
                let partition = OffsetCommitRequestPartition {
                    partition_index: tp.partition,
                    committed_offset: meta.committed_offset,
                    committed_leader_epoch: meta.committed_leader_epoch.unwrap_or(-1),
                    commit_timestamp: -1,
                    committed_metadata: meta.metadata.clone(),
                    ..Default::default()
                };

                if let Some(partitions) = topics.get_mut(&tp.topic) {
                    partitions.push(partition);
                } else {
                    let partitions = vec![partition];
                    topics.insert(tp.topic.clone(), partitions);
                }
            }

            let mut offset_commit_topics = Vec::with_capacity(topics.len());
            for (topic, partitions) in topics {
                offset_commit_topics.push(OffsetCommitRequestTopic {
                    name: topic,
                    partitions,
                    ..Default::default()
                })
            }

            request.topics = offset_commit_topics;

            if version >= 1 {
                request.generation_id = self.group_meta.generation_id;
                request.member_id = self.group_meta.member_id.clone();
            }

            if version >= 7 {
                request.group_instance_id = self.group_meta.group_instance_id.clone();
            }

            if (2..=4).contains(&version) {
                request.retention_time_ms = -1;
            }
        }

        Ok(request)
    }

    pub fn heartbeat_builder(&self, version: i16) -> Result<HeartbeatRequest> {
        let mut request = HeartbeatRequest::default();
        if version <= 4 {
            request.group_id = self.group_meta.group_id.clone();
            request.member_id = self.group_meta.member_id.clone();
            request.generation_id = self.group_meta.generation_id;

            if version >= 3 {
                request.group_instance_id = self.group_meta.group_instance_id.clone();
            }
        }
        Ok(request)
    }
}

fn deserialize_member(members: Vec<JoinGroupResponseMember>) -> Result<GroupSubscription> {
    let mut subscriptions = HashMap::new();
    for mut member in members {
        let mut subscription = Subscription::deserialize_from_bytes(&mut member.metadata)?;
        subscription.group_instance_id(member.group_instance_id);
        subscriptions.insert(member.member_id, subscription);
    }

    Ok(GroupSubscription::new(subscriptions))
}

fn serialize_assignments(
    assignments: HashMap<MemberId, Assignment>,
) -> Result<Vec<SyncGroupRequestAssignment>> {
    let version = ConsumerProtocolAssignment::VERSIONS.max;
    let mut sync_group_assignments = Vec::with_capacity(assignments.len());
    for (member_id, assignment) in assignments {
        let mut assigned_partitions = IndexMap::with_capacity(assignment.partitions.len());
        for (topic, partitions) in assignment.partitions {
            assigned_partitions.insert(topic, CpaTopicPartition { partitions });
        }
        let assignment = to_version_prefixed_bytes(
            version,
            ConsumerProtocolAssignment {
                assigned_partitions,
                user_data: assignment.user_data,
            },
        )?;

        sync_group_assignments.push(SyncGroupRequestAssignment {
            member_id,
            assignment,
            ..Default::default()
        });
    }
    Ok(sync_group_assignments)
}
