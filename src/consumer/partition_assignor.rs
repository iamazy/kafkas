use std::{
    cmp::{min, Ordering},
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use indexmap::IndexMap;
use kafka_protocol::{
    messages::{
        consumer_protocol_subscription::TopicPartition, ConsumerProtocolAssignment,
        ConsumerProtocolSubscription, TopicName,
    },
    protocol::{buf::ByteBuf, Decodable, Message, StrBytes},
};
use tracing::debug;

use crate::{
    consumer::ConsumerGroupMetadata, error::Result, metadata::Cluster, to_version_prefixed_bytes,
    Error, MemberId,
};

pub trait PartitionAssigner {
    fn name(&self) -> &'static str;

    fn member_assignments(
        &self,
        partitions_per_topic: HashMap<&TopicName, i32>,
        subscriptions: &HashMap<StrBytes, Subscription>,
    ) -> Result<HashMap<MemberId, Assignment>>;

    fn subscription_user_data(&self, _topics: &HashSet<TopicName>) -> Result<Option<Bytes>> {
        Ok(None)
    }

    fn on_assignment(
        &mut self,
        _assignment: Assignment,
        _metadata: ConsumerGroupMetadata,
    ) -> Result<()> {
        Ok(())
    }

    fn assign(
        &self,
        cluster: Arc<Cluster>,
        group_subscription: &GroupSubscription,
    ) -> Result<HashMap<MemberId, Assignment>> {
        let mut all_subscribed_topics = HashSet::new();
        for subscription in group_subscription.subscriptions.values() {
            subscription.topics.iter().for_each(|topic| {
                all_subscribed_topics.insert(topic);
            });
        }

        let mut partitions_per_topic = HashMap::new();
        for topic in all_subscribed_topics {
            let num_partitions = cluster.num_partitions(topic)?;
            if num_partitions > 0 {
                partitions_per_topic.insert(topic, num_partitions);
            } else {
                debug!(
                    "skipping assignment for topic {:?} since no metadata is available",
                    topic
                );
            }
        }

        let assignments =
            self.member_assignments(partitions_per_topic, &group_subscription.subscriptions)?;

        Ok(assignments)
    }
}

#[derive(Debug, Clone, Default)]
pub struct Subscription {
    topics: HashSet<TopicName>,
    user_data: Option<Bytes>,
    owned_partitions: HashMap<TopicName, Vec<i32>>,
    group_instance_id: Option<StrBytes>,
}

impl Subscription {
    pub fn new(
        topics: HashSet<TopicName>,
        user_data: Option<Bytes>,
        owned_partitions: HashMap<TopicName, Vec<i32>>,
    ) -> Self {
        Self {
            topics,
            user_data,
            owned_partitions,
            group_instance_id: None,
        }
    }

    pub fn group_instance_id(&mut self, group_instance_id: Option<StrBytes>) {
        self.group_instance_id = group_instance_id;
    }

    fn check_version(version: i16) -> Result<i16> {
        if version < ConsumerProtocolSubscription::VERSIONS.min {
            Err(Error::InvalidVersion(version))
        } else if version > ConsumerProtocolSubscription::VERSIONS.max {
            Ok(ConsumerProtocolSubscription::VERSIONS.max)
        } else {
            Ok(version)
        }
    }

    pub fn deserialize_from_bytes<B: ByteBuf>(buf: &mut B) -> Result<Self> {
        let version = buf.get_i16();
        let version = Subscription::check_version(version)?;

        let consumer_protocol_subscription = ConsumerProtocolSubscription::decode(buf, version)?;
        let mut topics = HashSet::with_capacity(consumer_protocol_subscription.topics.len());
        consumer_protocol_subscription
            .topics
            .iter()
            .for_each(|topic| {
                topics.insert(TopicName(topic.clone()));
            });

        let mut owned_partitions =
            HashMap::with_capacity(consumer_protocol_subscription.owned_partitions.len());
        for (topic, partitions) in consumer_protocol_subscription.owned_partitions {
            owned_partitions.insert(topic, partitions.partitions);
        }

        Ok(Subscription {
            topics,
            group_instance_id: None,
            owned_partitions,
            user_data: consumer_protocol_subscription.user_data,
        })
    }

    pub fn serialize_to_bytes(self) -> Result<Bytes> {
        let version = ConsumerProtocolSubscription::VERSIONS.max;
        let mut topics = Vec::with_capacity(self.topics.len());
        self.topics.iter().for_each(|topic| {
            topics.push(topic.0.clone());
        });
        let mut owned_partitions = IndexMap::with_capacity(self.topics.len());
        for (topic, partitions) in self.owned_partitions {
            owned_partitions.insert(topic, TopicPartition { partitions });
        }

        let protocol_subscription = ConsumerProtocolSubscription {
            topics,
            user_data: self.user_data,
            owned_partitions,
        };

        to_version_prefixed_bytes(version, protocol_subscription)
    }
}

#[derive(Debug, Clone, Default)]
pub struct GroupSubscription {
    subscriptions: HashMap<MemberId, Subscription>,
}

impl GroupSubscription {
    pub fn new(subscriptions: HashMap<MemberId, Subscription>) -> Self {
        Self { subscriptions }
    }
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub partitions: HashMap<TopicName, Vec<i32>>,
    pub user_data: Option<Bytes>,
}

impl Assignment {
    pub fn new(partitions: HashMap<TopicName, Vec<i32>>) -> Self {
        Self {
            partitions,
            user_data: None,
        }
    }

    fn check_version(version: i16) -> Result<i16> {
        if version < ConsumerProtocolAssignment::VERSIONS.min {
            Err(Error::InvalidVersion(version))
        } else if version > ConsumerProtocolAssignment::VERSIONS.max {
            Ok(ConsumerProtocolAssignment::VERSIONS.max)
        } else {
            Ok(version)
        }
    }

    pub fn deserialize_from_bytes<B: ByteBuf>(buf: &mut B) -> Result<Self> {
        let version = buf.get_i16();
        let version = Assignment::check_version(version)?;
        let consumer_protocol_assignment = ConsumerProtocolAssignment::decode(buf, version)?;

        let mut topic_partitions = HashMap::new();
        for (topic_name, partitions) in consumer_protocol_assignment.assigned_partitions {
            topic_partitions.insert(topic_name, partitions.partitions);
        }

        Ok(Assignment {
            partitions: topic_partitions,
            user_data: consumer_protocol_assignment.user_data,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MemberInfo<'a> {
    member_id: &'a StrBytes,
    group_instance_id: Option<&'a StrBytes>,
}

impl<'a> MemberInfo<'a> {
    fn sort(member_a: &MemberInfo, member_b: &MemberInfo) -> Ordering {
        if member_a.group_instance_id.is_some() && member_b.group_instance_id.is_some() {
            member_a
                .group_instance_id
                .unwrap()
                .cmp(member_b.group_instance_id.unwrap())
        } else if member_a.group_instance_id.is_some() {
            Ordering::Less
        } else if member_b.group_instance_id.is_some() {
            Ordering::Greater
        } else {
            member_a.member_id.cmp(member_b.member_id)
        }
    }
}

pub static SUPPORTED_PARTITION_ASSIGNORS: &[PartitionAssignor] = &[
    PartitionAssignor::Range(RangeAssignor),
    // PartitionAssignor::RoundRobin(RoundRobinAssignor),
    // PartitionAssignor::Sticky(StickyAssignor),
    // PartitionAssignor::CooperativeSticky(CooperativeStickyAssignor),
];

#[derive(Debug, Clone)]
pub enum PartitionAssignor {
    Range(RangeAssignor),
    RoundRobin(RoundRobinAssignor),
    Sticky(StickyAssignor),
    CooperativeSticky(CooperativeStickyAssignor),
}

impl PartitionAssigner for PartitionAssignor {
    fn name(&self) -> &'static str {
        match self {
            PartitionAssignor::Range(range) => range.name(),
            _ => unimplemented!(),
        }
    }

    fn assign(
        &self,
        cluster: Arc<Cluster>,
        group_subscription: &GroupSubscription,
    ) -> Result<HashMap<MemberId, Assignment>> {
        match self {
            PartitionAssignor::Range(range) => range.assign(cluster, group_subscription),
            _ => unimplemented!(),
        }
    }

    fn member_assignments(
        &self,
        partitions_per_topic: HashMap<&TopicName, i32>,
        subscriptions: &HashMap<StrBytes, Subscription>,
    ) -> Result<HashMap<MemberId, Assignment>> {
        match self {
            PartitionAssignor::Range(range) => {
                range.member_assignments(partitions_per_topic, subscriptions)
            }
            _ => unimplemented!(),
        }
    }
}

/// The range assignor works on a per-topic basis. For each topic, we lay out the available
/// partitions in numeric order and the consumers in lexicographic order. We then divide the number
/// of partitions by the total number of consumers to determine the number of partitions to assign
/// to each consumer. If it does not evenly divide, then the first few consumers will have one extra
/// partition. For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and
/// each topic has 3 partitions, resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
///
/// The assignment will be:
///
/// - C0: [t0p0, t0p1, t1p0, t1p1]
/// - C1: [t0p2, t1p2]
///
/// Since the introduction of static membership, we could leverage group.instance.id to make the
/// assignment behavior more sticky. For the above example, after one rolling bounce, group
/// coordinator will attempt to assign new member.id towards consumers, for example C0 -> C3 C1 ->
/// C2. The assignment could be completely shuffled to:
/// - C3 (was C0): [t0p2, t1p2] (before was [t0p0, t0p1, t1p0, t1p1])
/// - C2 (was C1): [t0p0, t0p1, t1p0, t1p1] (before was [t0p2, t1p2])
/// The assignment change was caused by the change of member.id relative order, and can be avoided
/// by setting the group.instance.id. Consumers will have individual instance ids I1, I2. As long as
/// 1. Number of members remain the same across generation 2. Static members' identities persist
/// across generation 3. Subscription pattern doesn't change for any member
///
/// The assignment will always be:
///
/// - I0: [t0p0, t0p1, t1p0, t1p1]
/// - I1: [t0p2, t1p2]
#[derive(Debug, Clone)]
pub struct RangeAssignor;

impl RangeAssignor {
    fn consumers_per_topic<'a>(
        &'a self,
        metadata: &'a HashMap<StrBytes, Subscription>,
    ) -> HashMap<&TopicName, Vec<MemberInfo>> {
        let mut topic_to_consumers = HashMap::new();
        for (member_id, subscription) in metadata {
            let member = MemberInfo {
                member_id,
                group_instance_id: subscription.group_instance_id.as_ref(),
            };
            for topic in &subscription.topics {
                if let Entry::Vacant(entry) = topic_to_consumers.entry(topic) {
                    let member_infos = vec![member.clone()];
                    entry.insert(member_infos);
                } else {
                    topic_to_consumers
                        .get_mut(&topic)
                        .unwrap()
                        .push(member.clone());
                }
            }
        }
        topic_to_consumers
    }
}

impl PartitionAssigner for RangeAssignor {
    fn name(&self) -> &'static str {
        "range"
    }

    fn member_assignments(
        &self,
        partitions_per_topic: HashMap<&TopicName, i32>,
        subscriptions: &HashMap<MemberId, Subscription>,
    ) -> Result<HashMap<MemberId, Assignment>> {
        let consumer_per_topic = self.consumers_per_topic(subscriptions);

        let mut assignment = HashMap::new();
        for (member_id, subscription) in subscriptions {
            let mut partitions = HashMap::new();
            for topic in subscription.topics.iter() {
                partitions.insert(topic.clone(), Vec::new());
            }
            assignment.insert(member_id.clone(), Assignment::new(partitions));
        }

        for (topic, mut members) in consumer_per_topic {
            let num_partitions = *partitions_per_topic.get(&topic).unwrap_or(&0);
            if num_partitions == 0 {
                continue;
            }
            let num_consumers_for_topic = members.len() as i32;
            members.sort_by(MemberInfo::sort);
            let num_partitions_per_consumer = num_partitions / num_consumers_for_topic;
            let consumers_with_extra_partition = num_partitions % num_consumers_for_topic;

            let mut partitions = Vec::new();
            for partition in 0..num_partitions {
                partitions.push(partition);
            }

            for i in 0..num_consumers_for_topic {
                let start =
                    num_partitions_per_consumer * i + min(i, consumers_with_extra_partition);
                let length = num_partitions_per_consumer
                    + (if i + 1 > consumers_with_extra_partition {
                        0
                    } else {
                        1
                    });
                if let Some(member) = members.get(i as usize) {
                    if let Some(assign) = assignment.get_mut(member.member_id) {
                        let slice = &partitions[start as usize..(start + length) as usize];
                        if let Some(topic) = assign.partitions.get_mut(topic) {
                            for item in slice {
                                topic.push(*item);
                            }
                        }
                    }
                }
            }
        }
        Ok(assignment)
    }
}

/// The round robin assignor lays out all the available partitions and all the available consumers.
/// It then proceeds to do a round robin assignment from partition to consumer. If the subscriptions
/// of all consumer instances are identical, then the partitions will be uniformly distributed.
/// (i.e., the partition ownership counts will be within a delta of exactly one across all
/// consumers.)
///
/// For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic
/// has 3 partitions, resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
///
/// The assignment will be:
///
/// - C0: [t0p0, t0p2, t1p1]
/// - C1: [t0p1, t1p0, t1p2]
///
/// When subscriptions differ across consumer instances, the assignment process still considers
/// each consumer instance in round robin fashion but skips over an instance if it is not
/// subscribed to the topic. Unlike the case when subscriptions are identical, this can result
/// in imbalanced assignments. For example, we have three consumers C0, C1, C2, and three topics
/// t0, t1, t2, with 1, 2, and 3 partitions, respectively. Therefore, the partitions are t0p0,
/// t1p0, t1p1, t2p0, t2p1, t2p2. C0 is subscribed to t0; C1 is subscribed to t0, t1; and C2 is
/// subscribed to t0, t1, t2.
///
/// That assignment will be:
///
/// - C0: [t0p0]
/// - C1: [t1p0]
/// - C2: [t1p1, t2p0, t2p1, t2p2]
///
/// Since the introduction of static membership, we could leverage group.instance.id to make the
/// assignment behavior more sticky. For example, we have three consumers with assigned
/// member.id C0, C1, C2, two topics t0 and t1, and each topic has 3 partitions, resulting in
/// partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2. We choose to honor the sorted order
/// based on ephemeral member.id.
///
/// The assignment will be:
///
/// - C0: [t0p0, t1p0]
/// - C1: [t0p1, t1p1]
/// - C2: [t0p2, t1p2]
///
/// After one rolling bounce, group coordinator will attempt to assign new member.id towards
/// consumers, for example C0 -> C5 C1 -> C3, C2 -> C4.
///
/// The assignment could be completely shuffled to:
///
/// - C3 (was C1): [t0p0, t1p0] (before was [t0p1, t1p1])
/// - C4 (was C2): [t0p1, t1p1] (before was [t0p2, t1p2])
/// - C5 (was C0): [t0p2, t1p2] (before was [t0p0, t1p0])
///
/// This issue could be mitigated by the introduction of static membership. Consumers will have
/// individual instance ids I1, I2, I3. As long as 1. Number of members remain the same across
/// generation 2. Static members' identities persist across generation 3. Subscription pattern
/// doesn't change for any member
///
/// The assignment will always be:
///
/// - I0: [t0p0, t1p0]
/// - I1: [t0p1, t1p1]
/// - I2: [t0p2, t1p2]
#[derive(Debug, Clone)]
pub struct RoundRobinAssignor;

/// The sticky assignor serves two purposes. First, it guarantees an assignment that is as balanced
/// as possible, meaning either:
/// - the numbers of topic partitions assigned to consumers differ by at most one; or
/// - each consumer that has 2+ fewer topic partitions than some other consumer cannot get any of
/// those topic partitions transferred to it.
///
/// Second, it preserved as many existing assignment as possible when a reassignment occurs.
/// This helps in saving some of the overhead processing when topic partitions move from one
/// consumer to another.
/// Starting fresh it would work by distributing the partitions over consumers as evenly as
/// possible. Even though this may sound similar to how round robin assignor works, the second
/// example below shows that it is not. During a reassignment it would perform the reassignment
/// in such a way that in the new assignment
///
/// 1. topic partitions are still distributed as evenly as possible, and
/// 2. topic partitions stay with their previously assigned consumers as much as possible.
///
/// Of course, the first goal above takes precedence over the second one.
///
/// **Example 1**. Suppose there are three consumers C0, C1, C2, four topics t0, t1, t2, t3,
/// and each topic has 2 partitions, resulting in partitions t0p0, t0p1, t1p0, t1p1, t2p0, t2p1,
/// t3p0, t3p1. Each consumer is subscribed to all three topics. The assignment with both sticky
/// and round robin assignors will be:
///
/// - C0: [t0p0, t1p1, t3p0]
/// - C1: [t0p1, t2p0, t3p1]
/// - C2: [t1p0, t2p1]
///
/// Now, let's assume C1 is removed and a reassignment is about to happen. The round robin assignor
/// would produce:
///
/// - C0: [t0p0, t1p0, t2p0, t3p0]
/// - C2: [t0p1, t1p1, t2p1, t3p1]
///
/// while the sticky assignor would result in:
///
/// - C0 [t0p0, t1p1, t3p0, t2p0]
/// - C2 [t1p0, t2p1, t0p1, t3p1]
///
/// preserving all the previous assignments (unlike the round robin assignor).
///
/// **Example 2**. There are three consumers C0, C1, C2, and three topics t0, t1, t2, with 1, 2,
/// and 3 partitions respectively. Therefore, the partitions are t0p0, t1p0, t1p1, t2p0, t2p1, t2p2.
/// C0 is subscribed to t0; C1 is subscribed to t0, t1; and C2 is subscribed to t0, t1, t2. The
/// round robin assignor would come up with the following assignment:
///
/// - C0 [t0p0]
/// - C1 [t1p0]
/// - C2 [t1p1, t2p0, t2p1, t2p2]
///
/// which is not as balanced as the assignment suggested by sticky assignor:
///
/// - C0 [t0p0]
/// - C1 [t1p0, t1p1]
/// - C2 [t2p0, t2p1, t2p2]
///
/// Now, if consumer C0 is removed, these two assignors would produce the following assignments.
/// Round Robin (preserves 3 partition assignments):
///
/// - C1 [t0p0, t1p1]
/// - C2 [t1p0, t2p0, t2p1, t2p2]
///
/// Sticky (preserves 5 partition assignments):
///
/// - C1 [t1p0, t1p1, t0p0]
/// - C2 [t2p0, t2p1, t2p2]
#[derive(Debug, Clone)]
pub struct StickyAssignor;

/// A cooperative version of the AbstractStickyAssignor. This follows the same (sticky) assignment
/// logic as StickyAssignor but allows for cooperative rebalancing while the StickyAssignor follows
/// the eager rebalancing protocol. See ConsumerPartitionAssignor.RebalanceProtocol for an
/// explanation of the rebalancing protocols.
///
/// Users should prefer this assignor for newer clusters.
///
/// To turn on cooperative rebalancing you must set all your consumers to use this
/// PartitionAssignor, or implement a custom one that returns RebalanceProtocol.COOPERATIVE in
/// supportedProtocols().
///
/// IMPORTANT: if upgrading from 2.3 or earlier, you must follow a specific upgrade path in order
/// to safely turn on cooperative rebalancing. See the upgrade guide  for details
#[derive(Debug, Clone)]
pub struct CooperativeStickyAssignor;
