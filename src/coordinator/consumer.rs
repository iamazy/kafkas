use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::Arc,
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
use tracing::{debug, error, info, warn};

use crate::{
    client::Kafka,
    consumer::{
        partition_assignor::{
            Assignment, GroupSubscription, PartitionAssigner, PartitionAssignor, RangeAssignor,
            Subscription,
        },
        ConsumerGroupMetadata, RebalanceOptions, SubscriptionState, TopicPartitionState,
    },
    coordinator::{find_coordinator, CoordinatorType},
    error::{ConsumeError, Result},
    executor::Executor,
    metadata::{Node, TopicPartition},
    to_version_prefixed_bytes, Error, MemberId, ToStrBytes,
};

const CONSUMER_PROTOCOL_TYPE: &str = "consumer";

macro_rules! offset_fetch_block {
    ($self:ident, $source:ident) => {
        for topic in $source.topics {
            for partition in topic.partitions {
                if partition.error_code.is_ok() {
                    let tp = TopicPartition::new(topic.name.clone(), partition.partition_index);
                    if let Some(partition_state) =
                        $self.subscriptions.borrow_mut().assignments.get_mut(&tp)
                    {
                        partition_state.position.offset = partition.committed_offset;
                        partition_state.position.offset_epoch =
                            Some(partition.committed_leader_epoch);
                        debug!(
                            "Fetch topic [{} - {}] offset success, offset: {}",
                            tp.topic.0, partition.partition_index, partition.committed_offset
                        );
                    }
                } else {
                    error!(
                        "Fetch topic [{} - {}] offset error, {}",
                        topic.name.0,
                        partition.partition_index,
                        partition.error_code.err().unwrap()
                    );
                }
            }
        }
    };
}

pub struct ConsumerCoordinator<Exe: Executor> {
    client: Kafka<Exe>,
    node: Node,
    group_meta: ConsumerGroupMetadata,
    group_subscription: GroupSubscription,
    rebalance_options: RebalanceOptions,
    auto_commit_interval_ms: i32,
    auto_commit_enabled: bool,
    pub subscriptions: Arc<RefCell<SubscriptionState>>,
    assignors: Vec<PartitionAssignor>,
}

impl<Exe: Executor> ConsumerCoordinator<Exe> {
    pub async fn new<S: AsRef<str>>(client: Kafka<Exe>, group_id: S) -> Result<Self> {
        let group_id = group_id.as_ref().to_string().to_str_bytes();

        let node = find_coordinator(&client, group_id.clone(), CoordinatorType::Group).await?;

        info!(
            "Find coordinator success, group {:?}, node: {:?}",
            group_id, node
        );

        Ok(Self {
            client,
            node,
            auto_commit_interval_ms: 5000,
            auto_commit_enabled: true,
            subscriptions: Arc::new(RefCell::new(SubscriptionState::default())),
            group_meta: ConsumerGroupMetadata::new(group_id.into()),
            group_subscription: GroupSubscription::default(),
            rebalance_options: RebalanceOptions::default(),
            assignors: vec![PartitionAssignor::Range(RangeAssignor)],
        })
    }

    pub async fn subscribe(&self, topic: TopicName) -> Result<()> {
        self.subscriptions.borrow_mut().topics.insert(topic.clone());
        self.client.topics_metadata(vec![topic]).await?;
        Ok(())
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

    #[async_recursion::async_recursion(? Send)]
    pub async fn join_group(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::JoinGroupKey) {
            let join_group_response = self
                .client
                .join_group(&self.node, self.join_group_builder(version_range.max)?)
                .await?;

            match join_group_response.error_code.err() {
                Some(ResponseError::MemberIdRequired) => {
                    self.group_meta.member_id = join_group_response.member_id;
                    warn!(
                        "Join group with unknown member id, will rejoin group [{}] with member \
                         id: {}",
                        self.group_meta.group_id.0, self.group_meta.member_id
                    );
                    return self.join_group().await;
                }
                Some(error) => Err(Error::Response { error, msg: None }),
                None => {
                    self.group_meta.member_id = join_group_response.member_id;
                    self.group_meta.generation_id = join_group_response.generation_id;
                    self.group_meta.leader = join_group_response.leader;
                    self.group_meta.protocol_name = join_group_response.protocol_name;
                    self.group_meta.protocol_type = join_group_response.protocol_type;

                    let group_subscription = deserialize_member(join_group_response.members)?;
                    self.group_subscription = group_subscription;

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

        if leave_group_response.error_code.is_ok() {
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
        } else {
            Err(Error::Response {
                error: leave_group_response.error_code.err().unwrap(),
                msg: None,
            })
        }
    }

    pub async fn sync_group(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::SyncGroupKey) {
            let mut sync_group_response = self
                .client
                .sync_group(&self.node, self.sync_group_builder(version_range.max)?)
                .await?;
            if sync_group_response.error_code.is_ok() {
                if self.group_meta.protocol_name.is_none() {
                    self.group_meta.protocol_name = sync_group_response.protocol_name;
                }
                if self.group_meta.protocol_type.is_none() {
                    self.group_meta.protocol_type = sync_group_response.protocol_type;
                }
                let assignment =
                    Assignment::deserialize_from_bytes(&mut sync_group_response.assignment)?;
                for (topic, partitions) in assignment.partitions {
                    for partition in partitions.iter() {
                        self.subscriptions.borrow_mut().assignments.insert(
                            TopicPartition::new(topic.clone(), *partition),
                            TopicPartitionState::new(*partition),
                        );
                    }
                }
                info!(
                    "Sync group [{}] success, leader = {}, member_id = {}, generation_id = {}, \
                     protocol_type = {:?}, protocol_name = {:?}",
                    self.group_meta.group_id.0,
                    self.group_meta.leader,
                    self.group_meta.member_id,
                    self.group_meta.generation_id,
                    self.group_meta.protocol_type.as_ref(),
                    self.group_meta.protocol_name.as_ref(),
                );
                Ok(())
            } else {
                Err(Error::Response {
                    error: sync_group_response.error_code.err().unwrap(),
                    msg: None,
                })
            }
        } else {
            Err(Error::InvalidApiRequest(ApiKey::SyncGroupKey))
        }
    }

    pub async fn offset_fetch(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::OffsetFetchKey) {
            let mut offset_fetch_response = self
                .client
                .offset_fetch(&self.node, self.offset_fetch_builder(version_range.max)?)
                .await?;
            if offset_fetch_response.error_code.is_ok() {
                if let Some(group) = offset_fetch_response.groups.pop() {
                    offset_fetch_block!(self, group);
                } else {
                    offset_fetch_block!(self, offset_fetch_response);
                }
                Ok(())
            } else {
                Err(Error::Response {
                    error: offset_fetch_response.error_code.err().unwrap(),
                    msg: None,
                })
            }
        } else {
            Err(Error::InvalidApiRequest(ApiKey::OffsetFetchKey))
        }
    }

    pub async fn offset_commit(&mut self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::OffsetCommitKey) {
            let offset_commit_response = self
                .client
                .offset_commit(&self.node, self.offset_commit_builder(version_range.max)?)
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

    pub async fn heartbeat(&self) -> Result<()> {
        if let Some(version_range) = self.client.version_range(ApiKey::HeartbeatKey) {
            let heartbeat_response = self
                .client
                .heartbeat(&self.node, self.heartbeat_builder(version_range.max)?)
                .await?;
            if heartbeat_response.error_code.is_ok() {
                debug!(
                    "Heartbeat success, group: {}, member: {}",
                    self.group_meta.group_id.0, self.group_meta.member_id
                );
                Ok(())
            } else {
                Err(Error::Response {
                    error: heartbeat_response.error_code.err().unwrap(),
                    msg: None,
                })
            }
        } else {
            Err(Error::InvalidApiRequest(ApiKey::HeartbeatKey))
        }
    }
}

/// for request builder and response handler
impl<Exe: Executor> ConsumerCoordinator<Exe> {
    fn join_group_protocol(&self) -> Result<IndexMap<StrBytes, JoinGroupRequestProtocol>> {
        let mut topics = HashSet::with_capacity(self.subscriptions.borrow().topics.len());
        self.subscriptions
            .borrow_mut()
            .topics
            .iter()
            .for_each(|topic| {
                topics.insert(topic.clone());
            });
        let mut protocols = IndexMap::with_capacity(topics.len());
        for assignor in self.assignors.iter() {
            let subscription = Subscription::new(
                topics.clone(),
                assignor.subscription_user_data(&topics)?,
                self.subscriptions.borrow().partitions(),
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

    fn join_group_builder(&self, version: i16) -> Result<JoinGroupRequest> {
        let mut request = JoinGroupRequest::default();
        if version <= 9 {
            request.group_id = self.group_meta.group_id.clone();
            request.member_id = self.group_meta.member_id.clone();
            request.protocol_type = StrBytes::from_str(CONSUMER_PROTOCOL_TYPE);
            request.protocols = self.join_group_protocol()?;
            request.session_timeout_ms = self.rebalance_options.session_timeout_ms;
            if version >= 1 {
                request.rebalance_timeout_ms = self.rebalance_options.rebalance_timeout_ms;
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

            let assignor = self.look_up_assignor("range")?;
            if self.group_meta.member_id == self.group_meta.leader {
                let cluster = self.client.cluster_meta.clone();
                request.assignments =
                    serialize_assignments(assignor.assign(cluster, &self.group_subscription)?)?;
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

    pub fn offset_fetch_builder(&self, version: i16) -> Result<OffsetFetchRequest> {
        let mut request = OffsetFetchRequest::default();
        if version <= 7 {
            let mut topics = Vec::with_capacity(self.subscriptions.borrow().topics.len());
            for assign in self.subscriptions.borrow().topics.iter() {
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
            let mut topics = Vec::with_capacity(self.subscriptions.borrow().topics.len());
            for assign in self.subscriptions.borrow().topics.iter() {
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

    pub fn offset_commit_builder(&self, version: i16) -> Result<OffsetCommitRequest> {
        let mut request = OffsetCommitRequest::default();
        if version <= 8 {
            request.group_id = self.group_meta.group_id.clone();

            let mut topics: HashMap<TopicName, Vec<OffsetCommitRequestPartition>> =
                HashMap::with_capacity(self.subscriptions.borrow().topics.len());
            for (tp, tp_state) in self.subscriptions.borrow().assignments.iter() {
                let partition = OffsetCommitRequestPartition {
                    partition_index: tp_state.partition,
                    committed_offset: tp_state.position.offset,
                    committed_leader_epoch: tp_state.position.offset_epoch.unwrap_or(-1),
                    commit_timestamp: -1,
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
