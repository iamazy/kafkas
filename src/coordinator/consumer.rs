use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_lock::RwLock;
use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    future::{select, Either},
    pin_mut, SinkExt, StreamExt,
};
use indexmap::IndexMap;
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{
        consumer_protocol_assignment::TopicPartition as CpaTopicPartition,
        join_group_request::JoinGroupRequestProtocol,
        join_group_response::JoinGroupResponseMember,
        leave_group_request::MemberIdentity,
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
        subscription_state::{
            OffsetMetadata, SubscriptionState, SubscriptionType, TopicPartitionState,
        },
        ConsumerGroupMetadata, ConsumerOptions,
    },
    coordinator::{find_coordinator, CoordinatorEvent, CoordinatorType},
    error::{ConsumeError, Result},
    executor::{Executor, JoinHandle},
    metadata::{Node, TopicPartition},
    to_version_prefixed_bytes, Error, MemberId, ToStrBytes, DEFAULT_GENERATION_ID,
};

const CONSUMER_PROTOCOL_TYPE: &str = "consumer";

macro_rules! offset_fetch_block {
    ($self:ident, $source:ident) => {
        for topic in $source.topics {
            for partition in topic.partitions {
                let tp = TopicPartition::new0(topic.name.clone(), partition.partition_index);
                if partition.error_code.is_ok() {
                    if let Some(partition_state) =
                        $self.subscriptions.write().await.assignments.get_mut(&tp)
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

        let seek_offsets: HashMap<TopicPartition, i64> =
            HashMap::from_iter($self.subscriptions.write().await.seek_offsets.drain());
        for (tp, offset) in seek_offsets {
            if let Some(tp_state) = $self.subscriptions.write().await.assignments.get_mut(&tp) {
                tp_state.position.offset = offset;
                tp_state.position.offset_epoch = None;
                tp_state.position.current_leader = $self.client.cluster_meta.current_leader(&tp);
                info!("Seek {tp} with offset: {offset}",);
            }
        }
    };
}

macro_rules! coordinator_task {
    ($self:ident, $interval_ms:ident, $event:ident) => {
        let mut interval = $self
            .client
            .executor
            .interval(Duration::from_millis($interval_ms as u64));

        let mut shutdown = $self.notify_shutdown.subscribe();
        let mut event_tx = $self.event_tx.clone();

        $self.client.executor.spawn(Box::pin(async move {
            let task_name = stringify!($event);

            loop {
                let interval_fut = interval.next();
                let shutdown = shutdown.recv();

                pin_mut!(interval_fut);
                pin_mut!(shutdown);

                match select(interval_fut, shutdown).await {
                    Either::Left((Some(_), _)) => {
                        let _ = event_tx.send(CoordinatorEvent::$event).await;
                    }
                    Either::Left((None, _)) | Either::Right(_) => {
                        break;
                    }
                }
            }

            info!("{} task finished.", task_name);
        }));
    };
}
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
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
    inner: Option<Inner<Exe>>,
    inner_handle: Option<JoinHandle<Inner<Exe>>>,
    pub(crate) event_tx: UnboundedSender<CoordinatorEvent>,
    options: Arc<ConsumerOptions>,
    commit_offset_tx: Option<UnboundedSender<()>>,
    notify_shutdown: broadcast::Sender<()>,
}

impl<Exe: Executor> ConsumerCoordinator<Exe> {
    pub async fn new(
        client: Kafka<Exe>,
        options: Arc<ConsumerOptions>,
        notify: broadcast::Sender<()>,
    ) -> Result<ConsumerCoordinator<Exe>> {
        let (event_tx, event_rx) = unbounded();

        let inner = Inner::new(client.clone(), options.clone(), event_rx).await?;
        let handle = client.executor.spawn(Box::pin(coordinator_loop(inner)));

        Ok(Self {
            inner: None,
            inner_handle: Some(handle),
            event_tx,
            client,
            options,
            notify_shutdown: notify,
            commit_offset_tx: None,
        })
    }

    pub async fn subscribe(&mut self, topics: Vec<TopicName>) {
        let _ = self
            .event_tx
            .send(CoordinatorEvent::Subscribe(topics))
            .await;
    }

    pub async fn subscriptions(&mut self) -> Arc<RwLock<SubscriptionState>> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .event_tx
            .send(CoordinatorEvent::GetSubscriptionsRef(tx))
            .await;
        rx.await.unwrap()
    }

    pub async fn join_group(&mut self) {
        let _ = self.event_tx.send(CoordinatorEvent::JoinGroup).await;
    }

    pub async fn sync_group(&mut self) {
        let _ = self.event_tx.send(CoordinatorEvent::SyncGroup).await;
    }

    pub async fn maybe_leave_group(&mut self, reason: StrBytes) {
        let _ = self
            .event_tx
            .send(CoordinatorEvent::LeaveGroup(reason))
            .await;
    }

    pub async fn offset_fetch(&mut self) {
        let _ = self.event_tx.send(CoordinatorEvent::OffsetFetch).await;
    }

    pub async fn commit_async(&mut self) {
        if let Some(ref mut tx) = self.commit_offset_tx {
            let _ = tx.send(()).await;
        }
    }

    pub async fn prepare_fetch(&mut self) -> Result<()> {
        self.join_group().await;
        self.sync_group().await;
        self.offset_fetch().await;

        if self.options.auto_commit_enabled {
            // auto commit offset
            let interval_ms = self.options.auto_commit_interval_ms;
            coordinator_task!(self, interval_ms, OffsetCommit);
            info!(
                "Auto commit offset task is started, which group is {}.",
                self.options.group_id
            );
        } else {
            self.async_commit_offset().await?;
        }
        // heartbeat
        let interval_ms = self.options.rebalance_options.heartbeat_interval_ms;
        coordinator_task!(self, interval_ms, Heartbeat);
        info!(
            "Heartbeat task is started, which group is {}.",
            self.options.group_id
        );
        Ok(())
    }
}

impl<Exe: Executor> ConsumerCoordinator<Exe> {
    async fn async_commit_offset(&mut self) -> Result<()> {
        let (commit_offset_tx, mut commit_offset_rx) = unbounded();
        self.commit_offset_tx = Some(commit_offset_tx);
        let mut shutdown = self.notify_shutdown.subscribe();
        let mut event_tx = self.event_tx.clone();
        self.client.executor.spawn(Box::pin(async move {
            loop {
                let offset_commit = commit_offset_rx.next();
                let shutdown = shutdown.recv();

                pin_mut!(offset_commit);
                pin_mut!(shutdown);

                match select(offset_commit, shutdown).await {
                    Either::Left((Some(_), _)) => {
                        let _ = event_tx.send(CoordinatorEvent::OffsetCommit).await;
                    }
                    Either::Left((None, _)) | Either::Right(_) => {
                        break;
                    }
                }
            }
            info!("Async commit offset task finished.");
        }));
        Ok(())
    }
}

struct Inner<Exe: Executor> {
    client: Kafka<Exe>,
    node: Node,
    event_rx: UnboundedReceiver<CoordinatorEvent>,
    pub group_meta: ConsumerGroupMetadata,
    group_subscription: GroupSubscription,
    consumer_options: Arc<ConsumerOptions>,
    pub subscriptions: Arc<RwLock<SubscriptionState>>,
    assignors: Vec<PartitionAssignor>,
    state: MemberState,
}

impl<Exe: Executor> Inner<Exe> {
    pub async fn new(
        client: Kafka<Exe>,
        options: Arc<ConsumerOptions>,
        event_rx: UnboundedReceiver<CoordinatorEvent>,
    ) -> Result<Inner<Exe>> {
        let group_id = options.group_id.clone().to_str_bytes();

        let node = find_coordinator(&client, group_id.clone(), CoordinatorType::Group).await?;

        info!(
            "Find coordinator success, group {:?}, node: {}",
            group_id,
            node.address()
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
            event_rx,
            subscriptions: Arc::new(RwLock::new(SubscriptionState::default())),
            group_meta: ConsumerGroupMetadata::new(group_id.into()),
            group_subscription: GroupSubscription::default(),
            consumer_options: options,
            assignors,
            state: MemberState::UnJoined,
        })
    }

    pub async fn subscribe(&self, topics: Vec<TopicName>) -> Result<()> {
        self.subscriptions
            .write()
            .await
            .topics
            .extend(topics.clone());
        self.subscriptions.write().await.subscription_type = SubscriptionType::AutoTopics;
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

    pub async fn leave_group(&mut self, reason: StrBytes) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::LeaveGroupKey) {
            debug!(
                "Member {} send LeaveGroup request to coordinator {} due to {reason}",
                self.group_meta.member_id,
                self.node.address(),
            );

            let leave_group_request = self.leave_group_builder(version_range.max, reason)?;

            let leave_group_response = self
                .client
                .leave_group(&self.node, leave_group_request)
                .await?;

            match leave_group_response.error_code.err() {
                None => {
                    for member in leave_group_response.members {
                        if member.error_code.is_ok() {
                            debug!(
                                "Member {} leave group {} success.",
                                member.member_id, self.group_meta.group_id.0
                            );
                        } else {
                            error!(
                                "Member {} leave group {} failed.",
                                member.member_id, self.group_meta.group_id.0
                            );
                        }
                    }
                    info!(
                        "Leave group [{}] success, member: {}",
                        self.group_meta.group_id.0, self.group_meta.member_id
                    );
                    Ok(())
                }
                Some(error) => {
                    error!(
                        "Leave group [{}] failed, error: {error}",
                        self.group_meta.group_id.0
                    );
                    Err(error.into())
                }
            }
        } else {
            Err(Error::InvalidApiRequest(ApiKey::LeaveGroupKey))
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
                    self.subscriptions.write().await.assignments.clear();
                    for (topic, partitions) in assignment.partitions {
                        for partition in partitions.iter() {
                            let tp = TopicPartition::new0(topic.clone(), *partition);
                            let mut tp_state = TopicPartitionState::new(*partition);
                            tp_state.position.current_leader =
                                self.client.cluster_meta.current_leader(&tp);
                            self.subscriptions
                                .write()
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
        let all_consumed = self.subscriptions.read().await.all_consumed();
        if all_consumed.is_empty() {
            return Ok(());
        }

        let offset_commit_request = self.offset_commit_builder(all_consumed).await?;
        debug!("Send offset commit request: {:?}", offset_commit_request);

        let offset_commit_response = self
            .client
            .offset_commit(&self.node, offset_commit_request)
            .await?;

        for topic in offset_commit_response.topics {
            for partition in topic.partitions {
                if let Some(err) = partition.error_code.err() {
                    error!(
                        "Failed to commit offset for partition {}, error: {err}",
                        partition.partition_index
                    );
                }
            }
        }
        Ok(())
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
    fn rebalance_in_progress(&self) -> bool {
        self.state == MemberState::PreparingRebalance
            || self.state == MemberState::CompletingRebalance
    }

    pub async fn maybe_leave_group(&mut self, reason: StrBytes) -> Result<()> {
        // Starting from 2.3, only dynamic members will send LeaveGroupRequest to the broker,
        // consumer with valid group.instance.id is viewed as static member that never sends
        // LeaveGroup, and the membership expiration is only controlled by session timeout.
        if self.group_meta.group_instance_id.is_none()
            && self.state != MemberState::UnJoined
            && !self.group_meta.member_id.is_empty()
        {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            self.leave_group(reason).await?;
        }

        self.state = MemberState::UnJoined;
        self.group_meta.generation_id = DEFAULT_GENERATION_ID;
        self.group_meta.member_id = StrBytes::default();

        Ok(())
    }

    async fn join_group_protocol(&self) -> Result<IndexMap<StrBytes, JoinGroupRequestProtocol>> {
        let topics = self
            .subscriptions
            .read()
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
                self.subscriptions.read().await.partitions(),
            );
            let metadata = subscription.serialize_to_bytes()?;
            let mut protocol = JoinGroupRequestProtocol::default();
            protocol.metadata = metadata;
            protocols.insert(assignor.name().to_string().to_str_bytes(), protocol);
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

    fn leave_group_builder(&self, version: i16, reason: StrBytes) -> Result<LeaveGroupRequest> {
        let mut request = LeaveGroupRequest::default();

        request.group_id = self.group_meta.group_id.clone();
        if version >= 3 {
            let mut members = Vec::with_capacity(1);
            let mut member = MemberIdentity::default();
            member.member_id = self.group_meta.member_id.clone();
            member.group_instance_id = self.group_meta.group_instance_id.clone();
            member.reason = Some(reason);
            members.push(member);
            request.members = members;
        } else {
            request.member_id = self.group_meta.member_id.clone();
        }

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
            let mut topics = Vec::with_capacity(self.subscriptions.read().await.topics.len());
            for assign in self.subscriptions.read().await.topics.iter() {
                let partitions = self.client.cluster_meta.partitions(assign)?;

                let mut topic = OffsetFetchRequestTopic::default();
                topic.name = assign.clone();
                topic.partition_indexes = partitions.value().clone();
                topics.push(topic);
            }
            request.group_id = self.group_meta.group_id.clone();
            request.topics = Some(topics);
        } else {
            let mut topics = Vec::with_capacity(self.subscriptions.read().await.topics.len());
            for assign in self.subscriptions.read().await.topics.iter() {
                let partitions = self.client.cluster_meta.partitions(assign)?;

                let mut topic = OffsetFetchRequestTopics::default();
                topic.name = assign.clone();
                topic.partition_indexes = partitions.value().clone();
                topics.push(topic);
            }

            let mut group = OffsetFetchRequestGroup::default();
            group.group_id = self.group_meta.group_id.clone();
            group.topics = Some(topics);
            request.groups = vec![group];
        }

        Ok(request)
    }

    pub async fn offset_commit_builder(
        &self,
        all_consumed: HashMap<TopicPartition, OffsetMetadata>,
    ) -> Result<OffsetCommitRequest> {
        let mut topics: HashMap<TopicName, Vec<OffsetCommitRequestPartition>> =
            HashMap::with_capacity(self.subscriptions.read().await.topics.len());
        for (tp, meta) in all_consumed.iter() {
            let mut partition = OffsetCommitRequestPartition::default();
            partition.partition_index = tp.partition;
            partition.committed_offset = meta.committed_offset;
            partition.committed_leader_epoch = meta.committed_leader_epoch.unwrap_or(-1);
            partition.commit_timestamp = -1;
            partition.committed_metadata = meta.metadata.clone();

            match topics.get_mut(&tp.topic) {
                Some(partitions) => partitions.push(partition),
                None => {
                    let partitions = vec![partition];
                    topics.insert(tp.topic.clone(), partitions);
                }
            }
        }

        let mut offset_commit_topics = Vec::with_capacity(topics.len());
        for (topic, partitions) in topics {
            let mut request_topic = OffsetCommitRequestTopic::default();
            request_topic.name = topic;
            request_topic.partitions = partitions;
            offset_commit_topics.push(request_topic);
        }

        let mut request = OffsetCommitRequest::default();
        request.topics = offset_commit_topics;

        let mut generation = self.group_meta.generation_id;
        let mut member = self.group_meta.member_id.clone();
        if self
            .subscriptions
            .read()
            .await
            .has_auto_assigned_partitions()
        {
            // if the generation is null, we are not part of an active group (and we expect to be).
            // the only thing we can do is fail the commit and let the user rejoin the group in
            // poll().
            if self.state != MemberState::Stable {
                info!(
                    "Failing OffsetCommit request since the consumer is not part of an active \
                     group"
                );
                return if self.rebalance_in_progress() {
                    Err(Error::Custom(
                        "Offset commit cannot be completed since the consumer is undergoing a \
                         rebalance for auto partition assignment. You can try completing the \
                         rebalance by calling poll() and then retry the operation."
                            .into(),
                    ))
                } else {
                    Err(Error::Custom(
                        "Offset commit cannot be completed since the consumer is not part of an \
                         active group for auto partition assignment; it is likely that the \
                         consumer was kicked out of the group."
                            .into(),
                    ))
                };
            }
        } else {
            generation = DEFAULT_GENERATION_ID;
            member = StrBytes::from_str("");
        }

        request.group_id = self.group_meta.group_id.clone();
        request.generation_id = generation;
        request.member_id = member;
        request.group_instance_id = self.group_meta.group_instance_id.clone();
        request.retention_time_ms = -1;

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

async fn coordinator_loop<Exe: Executor>(mut coordinator: Inner<Exe>) -> Inner<Exe> {
    while let Some(event) = coordinator.event_rx.next().await {
        match event {
            CoordinatorEvent::JoinGroup => {
                let _ = coordinator.join_group().await;
            }
            CoordinatorEvent::SyncGroup => {
                let _ = coordinator.sync_group().await;
            }
            CoordinatorEvent::LeaveGroup(reason) => {
                let _ = coordinator.maybe_leave_group(reason).await;
            }
            CoordinatorEvent::OffsetFetch => {
                let _ = coordinator.offset_fetch().await;
            }
            CoordinatorEvent::OffsetCommit => {
                let _ = coordinator.offset_commit().await;
            }
            CoordinatorEvent::SeekOffset { partition, offset } => {
                let _ = coordinator
                    .subscriptions
                    .write()
                    .await
                    .seek_offsets
                    .insert(partition, offset);
            }
            CoordinatorEvent::ResetOffset {
                partition,
                strategy,
            } => {
                if let Some(tp_state) = coordinator
                    .subscriptions
                    .write()
                    .await
                    .assignments
                    .get_mut(&partition)
                {
                    if let Err(err) = tp_state.reset(strategy) {
                        error!("{err}");
                    }
                }
            }
            CoordinatorEvent::Heartbeat => {
                let _ = coordinator.heartbeat().await;
            }
            CoordinatorEvent::Subscribe(topics) => {
                let _ = coordinator.subscribe(topics).await;
            }
            CoordinatorEvent::Unsubscribe => {
                coordinator.subscriptions.write().await.unsubscribe();
            }
            CoordinatorEvent::TopicPartitionState {
                partition,
                position,
                log_start_offset,
                last_stable_offset,
                high_water_mark,
            } => {
                if let Some(tp_state) = coordinator
                    .subscriptions
                    .write()
                    .await
                    .assignments
                    .get_mut(&partition)
                {
                    if let Some(position) = position {
                        tp_state.position.offset = position.offset;
                        tp_state.position.offset_epoch = position.offset_epoch;

                        if let Some(epoch) = position.current_leader.epoch {
                            tp_state.position.current_leader.epoch = Some(epoch);
                        }
                    }

                    if log_start_offset >= 0 {
                        tp_state.log_start_offset = log_start_offset;
                    }
                    if last_stable_offset >= 0 {
                        tp_state.last_stable_offset = last_stable_offset;
                    }
                    if high_water_mark >= 0 {
                        tp_state.high_water_mark = high_water_mark;
                    }
                }
            }
            CoordinatorEvent::GetSubscriptionsRef(tx) => {
                let subscriptions = coordinator.subscriptions.clone();
                let _ = tx.send(subscriptions);
            }
            CoordinatorEvent::Shutdown => break,
        }
    }
    coordinator
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
            let mut tp = CpaTopicPartition::default();
            tp.partitions = partitions;
            assigned_partitions.insert(topic, tp);
        }

        let mut protocol_assignment = ConsumerProtocolAssignment::default();
        protocol_assignment.assigned_partitions = assigned_partitions;
        protocol_assignment.user_data = assignment.user_data;

        let assignment = to_version_prefixed_bytes(version, protocol_assignment)?;

        let mut request_assignment = SyncGroupRequestAssignment::default();
        request_assignment.member_id = member_id;
        request_assignment.assignment = assignment;
        sync_group_assignments.push(request_assignment);
    }
    Ok(sync_group_assignments)
}
