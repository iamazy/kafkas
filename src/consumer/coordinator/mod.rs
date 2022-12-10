use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{
        consumer_protocol_assignment::{
            ConsumerProtocolAssignmentBuilder, TopicPartition as CpaTopicPartition,
        },
        describe_groups_request::DescribeGroupsRequestBuilder,
        heartbeat_request::HeartbeatRequestBuilder,
        join_group_request::{JoinGroupRequestBuilder, JoinGroupRequestProtocol},
        join_group_response::JoinGroupResponseMember,
        leave_group_request::LeaveGroupRequestBuilder,
        offset_commit_request::{
            OffsetCommitRequestBuilder, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_fetch_request::{
            OffsetFetchRequestBuilder, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
            OffsetFetchRequestTopics,
        },
        sync_group_request::{SyncGroupRequestAssignment, SyncGroupRequestBuilder},
        ConsumerProtocolAssignment, GroupId, TopicName,
    },
    protocol::{Message, StrBytes},
};
use tracing::{debug, error, info};

use crate::{
    client::Kafka,
    consumer::{
        partition_assignor::{
            Assignment, GroupSubscription, PartitionAssigner, PartitionAssignor, RangeAssignor,
            Subscription,
        },
        ConsumerGroupMetadata, OffsetMetadata, RebalanceConfig, SubscriptionState,
    },
    error::{ConsumeError, Result},
    executor::Executor,
    metadata::{Node, TopicPartition},
    to_version_prefixed_bytes, Error, ToStrBytes,
};

const PROTOCOL_TYPE: &str = "consumer";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CoordinatorType {
    Group,
    Transaction,
}

impl From<CoordinatorType> for i8 {
    fn from(value: CoordinatorType) -> Self {
        match value {
            CoordinatorType::Group => 0,
            CoordinatorType::Transaction => 1,
        }
    }
}

pub struct ConsumerCoordinator<Exe: Executor> {
    client: Kafka<Exe>,
    node: Node,
    group_meta: ConsumerGroupMetadata,
    group_subscription: GroupSubscription,
    rebalance_config: RebalanceConfig,
    auto_commit_interval_ms: i32,
    auto_commit_enabled: bool,
    subscriptions: SubscriptionState,
    assignors: Vec<PartitionAssignor>,
}

impl<Exe: Executor> ConsumerCoordinator<Exe> {
    pub async fn new<S: AsRef<str>>(client: Kafka<Exe>, group_id: S) -> Result<Self> {
        let group_id = group_id.as_ref().to_string().to_str_bytes();

        let node = client
            .find_coordinator(group_id.clone(), CoordinatorType::Group)
            .await?;

        info!(
            "find coordinator success, group {:?}, node: {:?}",
            group_id, node
        );

        Ok(Self {
            client,
            node,
            auto_commit_interval_ms: 5000,
            auto_commit_enabled: true,
            subscriptions: SubscriptionState::default(),
            group_meta: ConsumerGroupMetadata::new(group_id.clone().into()),
            group_subscription: GroupSubscription::default(),
            rebalance_config: RebalanceConfig::default(),
            assignors: vec![PartitionAssignor::Range(RangeAssignor)],
        })
    }

    pub async fn subscribe(&self, topic: TopicName) -> Result<()> {
        self.subscriptions.topics.insert(topic.clone());
        self.client.topics_metadata(vec![topic]).await?;
        Ok(())
    }

    pub async fn describe_groups(&mut self) -> Result<()> {
        let describe_groups_response = self
            .client
            .describe_groups(&self.node, self.describe_groups_builder()?.build()?)
            .await?;

        for group in describe_groups_response.groups {
            if group.error_code.is_err() {
                error!("describe group {:?} failed", group.group_id);
            }
        }

        Ok(())
    }

    pub async fn join_group(&mut self) -> Result<()> {
        let join_group_response = self
            .client
            .join_group(&self.node, self.join_group_builder()?.build()?)
            .await?;
        if join_group_response.error_code.is_ok() {
            self.group_meta.member_id = join_group_response.member_id;
            self.group_meta.generation_id = join_group_response.generation_id;
            self.group_meta.leader = join_group_response.leader;
            self.group_meta.protocol_name = join_group_response.protocol_name;
            self.group_meta.protocol_type = join_group_response.protocol_type;

            let group_subscription = deserialize_member(join_group_response.members)?;
            self.group_subscription = group_subscription;

            debug!(
                "join group [{:?}] success, leader = {}, member_id = {}, generation_id = {}",
                self.group_meta.group_id,
                self.group_meta.leader,
                self.group_meta.member_id,
                self.group_meta.generation_id
            );
            Ok(())
        } else {
            Err(Error::Response {
                error: join_group_response.error_code.err().unwrap(),
                msg: None,
            })
        }
    }

    pub async fn leave_group(&mut self) -> Result<()> {
        let leave_group_response = self
            .client
            .leave_group(&self.node, self.leave_group_builder()?.build()?)
            .await?;

        if leave_group_response.error_code.is_ok() {
            for member in leave_group_response.members {
                if member.error_code.is_ok() {
                    debug!(
                        "member {} leave group {:?} success.",
                        member.member_id, self.group_meta.group_id
                    );
                } else {
                    error!(
                        "member {} leave group {:?} failed.",
                        member.member_id, self.group_meta.group_id
                    );
                }
            }
            info!(
                "leave group {:?} success, member: {}",
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
        let mut sync_group_response = self
            .client
            .sync_group(&self.node, self.sync_group_builder()?.build()?)
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
                self.subscriptions.topics.insert(topic.clone());
                for partition in partitions.iter() {
                    self.subscriptions.offset_metadata.insert(
                        TopicPartition {
                            topic: topic.clone(),
                            partition: *partition,
                        },
                        OffsetMetadata::default(),
                    );
                }
                self.subscriptions
                    .assignment
                    .insert(topic.clone(), partitions);
            }
            debug!(
                "sync group [{:?}] success, leader = {}, member_id = {}, generation_id = {}, \
                 protocol_type = {:?}, protocol_name = {:?}",
                self.group_meta.group_id,
                self.group_meta.leader,
                self.group_meta.member_id,
                self.group_meta.generation_id,
                self.group_meta.protocol_type.as_ref(),
                self.group_meta.protocol_name.as_ref(),
            );
            debug!("{:?}", self.subscriptions);
            Ok(())
        } else {
            Err(Error::Response {
                error: sync_group_response.error_code.err().unwrap(),
                msg: None,
            })
        }
    }

    pub async fn offset_fetch(&mut self, version: i16) -> Result<()> {
        let offset_fetch_response = self
            .client
            .offset_fetch(
                &self.node,
                self.offset_fetch_builder(version).await?.build()?,
            )
            .await?;
        if offset_fetch_response.error_code.is_ok() {
            if version <= 7 {
                for topic in offset_fetch_response.topics {
                    for partition in topic.partitions {
                        if partition.error_code.is_ok() {
                            let tp = TopicPartition {
                                topic: topic.name.clone(),
                                partition: partition.partition_index,
                            };

                            if let Some(mut metadata) =
                                self.subscriptions.offset_metadata.get_mut(&tp)
                            {
                                metadata.metadata = partition.metadata;
                                metadata.committed_offset = partition.committed_offset;
                                metadata.committed_leader_epoch = partition.committed_leader_epoch;
                            }
                        } else {
                            error!(
                                "failed to fetch offset for partition {}",
                                partition.partition_index
                            );
                        }
                    }
                }
            }
            Ok(())
        } else {
            Err(Error::Response {
                error: offset_fetch_response.error_code.err().unwrap(),
                msg: None,
            })
        }
    }

    pub async fn offset_commit(&mut self) -> Result<()> {
        let offset_commit_response = self
            .client
            .offset_commit(&self.node, self.offset_commit_builder()?.build()?)
            .await?;
        for topic in offset_commit_response.topics {
            for partition in topic.partitions {
                if !partition.error_code.is_ok() {
                    error!(
                        "failed to commit offset for partition {}",
                        partition.partition_index
                    );
                }
            }
        }
        Ok(())
    }

    pub async fn heartbeat(&mut self) -> Result<()> {
        let heartbeat_response = self
            .client
            .heartbeat(&self.node, self.heartbeat_builder()?.build()?)
            .await?;
        if heartbeat_response.error_code.is_ok() {
            debug!(
                "heartbeat success, group: {:?}, member: {}",
                self.group_meta.group_id, self.group_meta.member_id
            );
            Ok(())
        } else {
            Err(Error::Response {
                error: heartbeat_response.error_code.err().unwrap(),
                msg: None,
            })
        }
    }
}

/// for request builder and response handler
impl<Exe: Executor> ConsumerCoordinator<Exe> {
    fn join_group_protocol_metadata(&self) -> Result<IndexMap<StrBytes, JoinGroupRequestProtocol>> {
        debug!(
            "joining group with current subscription: {:?}",
            self.subscriptions.topics
        );

        let mut topics = HashSet::with_capacity(self.subscriptions.topics.len());
        self.subscriptions.topics.iter().for_each(|topic| {
            topics.insert(topic.key().clone());
        });
        let mut protocols = IndexMap::with_capacity(topics.len());
        for assignor in self.assignors.iter() {
            let subscription = Subscription::new(
                topics.clone(),
                assignor.subscription_user_data(&topics)?,
                self.subscriptions.partitions(),
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

    fn describe_groups_builder(&self) -> Result<DescribeGroupsRequestBuilder> {
        let mut builder = DescribeGroupsRequestBuilder::default();
        builder
            .groups(vec![self.group_meta.group_id.clone()])
            .include_authorized_operations(false)
            .unknown_tagged_fields(Default::default());
        Ok(builder)
    }

    fn join_group_builder(&self) -> Result<JoinGroupRequestBuilder> {
        let mut builder = JoinGroupRequestBuilder::default();
        builder
            .group_id(self.group_meta.group_id.clone())
            .member_id(self.group_meta.member_id.clone())
            .group_instance_id(self.group_meta.group_instance_id.clone())
            .rebalance_timeout_ms(self.rebalance_config.rebalance_timeout_ms)
            .session_timeout_ms(self.rebalance_config.session_timeout_ms)
            .protocol_type(PROTOCOL_TYPE.to_string().to_str_bytes())
            .protocols(self.join_group_protocol_metadata()?)
            .reason(None)
            .unknown_tagged_fields(Default::default());
        Ok(builder)
    }

    fn leave_group_builder(&self) -> Result<LeaveGroupRequestBuilder> {
        let mut builder = LeaveGroupRequestBuilder::default();
        builder
            .group_id(self.group_meta.group_id.clone())
            .member_id(self.group_meta.member_id.clone())
            .members(Default::default())
            .unknown_tagged_fields(Default::default());

        // let mut members = Vec::new();
        // let member = MemberIdentity {
        //     member_id: self.group_meta.member_id.clone(),
        //     group_instance_id: self.group_meta.group_instance_id.clone(),
        //     ..Default::default()
        // };
        // members.push(member);
        // builder.members(members);

        Ok(builder)
    }

    fn sync_group_builder(&self) -> Result<SyncGroupRequestBuilder> {
        let mut builder = SyncGroupRequestBuilder::default();
        builder
            .group_id(self.group_meta.group_id.clone())
            .group_instance_id(self.group_meta.group_instance_id.clone())
            .generation_id(self.group_meta.generation_id)
            .member_id(self.group_meta.member_id.clone())
            .protocol_name(self.group_meta.protocol_name.clone())
            .protocol_type(self.group_meta.protocol_type.clone())
            .unknown_tagged_fields(Default::default());
        let assignor = self.look_up_assignor("range")?;
        if self.group_meta.member_id == self.group_meta.leader {
            let cluster = self.client.cluster_meta.clone();
            builder.assignments(serialize_assignments(
                assignor.assign(cluster, &self.group_subscription)?,
            )?);
        } else {
            builder.assignments(Default::default());
        }
        Ok(builder)
    }

    pub async fn offset_fetch_builder(&self, version: i16) -> Result<OffsetFetchRequestBuilder> {
        let mut builder = OffsetFetchRequestBuilder::default();
        builder.unknown_tagged_fields(Default::default());

        if version <= 7 {
            let mut topics = Vec::with_capacity(self.subscriptions.topics.len());
            for assign in self.subscriptions.topics.iter() {
                let partitions = self.client.partitions(assign.key()).await?;
                let topic = OffsetFetchRequestTopic {
                    name: assign.key().clone(),
                    partition_indexes: partitions.value().clone(),
                    ..Default::default()
                };
                topics.push(topic);
            }

            builder
                .group_id(self.group_meta.group_id.clone())
                .topics(Some(topics))
                .groups(Default::default())
                .require_stable(false);
        } else {
            let mut topics = Vec::with_capacity(self.subscriptions.topics.len());
            for assign in self.subscriptions.topics.iter() {
                let partitions = self.client.partitions(assign.key()).await?;
                let topic = OffsetFetchRequestTopics {
                    name: assign.key().clone(),
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
            builder
                .group_id(GroupId::default())
                .topics(None)
                .groups(vec![group])
                .require_stable(true);
        }

        Ok(builder)
    }

    pub fn offset_commit_builder(&self) -> Result<OffsetCommitRequestBuilder> {
        let mut builder = OffsetCommitRequestBuilder::default();
        builder
            .group_id(self.group_meta.group_id.clone())
            .group_instance_id(self.group_meta.group_instance_id.clone())
            .member_id(self.group_meta.member_id.clone())
            .generation_id(self.group_meta.generation_id)
            .retention_time_ms(-1)
            .unknown_tagged_fields(Default::default());

        let mut topics = Vec::with_capacity(self.subscriptions.topics.len());
        for (topic, partitions) in self.subscriptions.assignment.iter() {
            let mut offset_metadata = Vec::with_capacity(partitions.len());
            for partition in partitions {
                let tp = TopicPartition {
                    topic: topic.clone(),
                    partition: *partition,
                };

                if let Some(meta) = self.subscriptions.offset_metadata.get(&tp) {
                    let partition = OffsetCommitRequestPartition {
                        partition_index: *partition,
                        committed_offset: meta.committed_offset,
                        committed_leader_epoch: meta.committed_leader_epoch,
                        commit_timestamp: -1,
                        ..Default::default()
                    };

                    offset_metadata.push(partition);
                }
            }

            let topic = OffsetCommitRequestTopic {
                name: topic.clone(),
                partitions: offset_metadata,
                ..Default::default()
            };
            topics.push(topic);
        }

        builder.topics(topics);
        Ok(builder)
    }

    pub fn heartbeat_builder(&self) -> Result<HeartbeatRequestBuilder> {
        let mut builder = HeartbeatRequestBuilder::default();
        builder
            .group_id(self.group_meta.group_id.clone())
            .generation_id(self.group_meta.generation_id)
            .member_id(self.group_meta.member_id.clone())
            .group_instance_id(self.group_meta.group_instance_id.clone())
            .unknown_tagged_fields(Default::default());
        Ok(builder)
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
    assignments: HashMap<StrBytes, Assignment>,
) -> Result<Vec<SyncGroupRequestAssignment>> {
    let version = ConsumerProtocolAssignment::VERSIONS.max;
    let mut sync_group_assignments = Vec::with_capacity(assignments.len());
    for (member_id, assignment) in assignments {
        let mut builder = ConsumerProtocolAssignmentBuilder::default();
        let mut assigned_partitions = IndexMap::with_capacity(assignment.partitions.len());
        for (topic, partitions) in assignment.partitions {
            assigned_partitions.insert(topic, CpaTopicPartition { partitions });
        }
        builder.assigned_partitions(assigned_partitions);
        builder.user_data(assignment.user_data);
        let consumer_protocol_assignment = builder.build()?;
        let assignment = to_version_prefixed_bytes(version, consumer_protocol_assignment)?;

        sync_group_assignments.push(SyncGroupRequestAssignment {
            member_id,
            assignment,
            ..Default::default()
        });
    }
    Ok(sync_group_assignments)
}
