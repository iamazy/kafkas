use std::{
    fmt::{Debug, Display, Formatter},
    sync::{Arc, Mutex},
};

use dashmap::{DashMap, DashSet};
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
        BrokerId, MetadataResponse, TopicName,
    },
    protocol::StrBytes,
    ResponseError,
};
use uuid::Uuid;

use crate::{
    consumer::LeaderAndEpoch,
    error::{Error, Result},
    NodeId, NodeRef, PartitionId, PartitionRef,
};

#[derive(Debug, Clone, Default)]
pub struct Topic {
    pub id: Uuid,
    pub name: StrBytes,
    pub is_internal: bool,
    pub partitions: Vec<Partition>,
}

impl Topic {
    pub fn available_partitions(&self) -> Vec<i32> {
        self.partitions
            .iter()
            .filter(|partition| partition.leader > 0)
            .map(|partition| partition.partition)
            .collect()
    }

    pub fn partitions(&self) -> Vec<i32> {
        self.partitions
            .iter()
            .map(|partition| partition.partition)
            .collect()
    }
}

impl From<(&TopicName, &MetadataResponseTopic)> for Topic {
    fn from((topic_name, topic): (&TopicName, &MetadataResponseTopic)) -> Self {
        Self {
            id: topic.topic_id,
            is_internal: topic.is_internal,
            partitions: topic
                .partitions
                .iter()
                .filter(|p| p.error_code.is_ok())
                .map(Into::into)
                .collect(),
            name: topic_name.0.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, Hash)]
pub struct Partition {
    pub partition: PartitionId,
    pub leader: NodeId,
    pub leader_epoch: i32,
    pub replicas: Vec<NodeId>,
    pub in_sync_replicas: Vec<NodeId>,
    pub offline_replicas: Vec<NodeId>,
}

impl From<&MetadataResponsePartition> for Partition {
    fn from(partition: &MetadataResponsePartition) -> Self {
        let partition_index = partition.partition_index;
        Self {
            partition: partition_index,
            leader: partition.leader_id.0,
            leader_epoch: partition.leader_epoch,
            replicas: partition.replica_nodes.iter().map(|node| node.0).collect(),
            in_sync_replicas: partition.isr_nodes.iter().map(|node| node.0).collect(),
            offline_replicas: partition
                .offline_replicas
                .iter()
                .map(|node| node.0)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct TopicIdPartition {
    pub topic_id: Uuid,
    pub partition: TopicPartition,
}

#[derive(Clone, Default, Eq, PartialEq, Hash)]
pub struct TopicPartition {
    pub topic: TopicName,
    pub partition: PartitionId,
}

impl TopicPartition {
    pub fn new(topic: TopicName, partition: PartitionId) -> Self {
        Self { topic, partition }
    }
}

impl Debug for TopicPartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicPartition")
            .field("topic", &self.topic.0)
            .field("partition", &self.partition)
            .finish()
    }
}

impl Display for TopicPartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "partition [{} - {}]", self.topic.0, self.partition)
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct Node {
    pub id: NodeId,
    address: String,
}

impl Node {
    pub fn new(id: BrokerId, host: StrBytes, port: i32) -> Self {
        Self {
            id: id.0,
            address: format!("{host}:{port}"),
        }
    }

    pub fn address(&self) -> &String {
        &self.address
    }
}

impl From<(&BrokerId, &MetadataResponseBroker)> for Node {
    fn from((id, broker): (&BrokerId, &MetadataResponseBroker)) -> Self {
        Node::new(*id, broker.host.clone(), broker.port)
    }
}

#[derive(Debug, Clone, Default)]
pub struct Cluster {
    pub id: Arc<Mutex<Option<StrBytes>>>,
    pub unauthorized_topics: DashSet<TopicName>,
    pub invalid_topics: DashSet<TopicName>,
    pub internal_topics: DashSet<TopicName>,
    pub controller: Arc<Mutex<NodeId>>,
    pub topics: DashMap<TopicName, Topic>,
    pub nodes: DashMap<NodeId, Node>,
    pub available_partitions: DashMap<TopicName, Vec<PartitionId>>,
    pub partitions: DashMap<TopicName, Vec<PartitionId>>,
    pub partitions_by_nodes: DashMap<NodeId, Vec<TopicPartition>>,
}

impl Cluster {
    pub fn new() -> Cluster {
        Default::default()
    }

    pub fn merge_meta(&self, other: MetadataResponse) -> Result<()> {
        let cluster_id = other.cluster_id;
        {
            let mut lock = self.id.lock()?;
            if matches!(*lock, None) {
                *lock = cluster_id;
            } else if *lock != cluster_id {
                return Err(Error::Custom(format!(
                    "cluster id: {cluster_id:?} is not equal to {:?}",
                    *lock
                )));
            }
        }
        {
            let mut lock = self.controller.lock()?;
            *lock = other.controller_id.0;
        }
        for broker in other.brokers.iter() {
            self.nodes.insert(**broker.0, broker.into());
        }
        for (topic_name, res) in other.topics.iter() {
            let error_code = res.error_code;
            if error_code.is_ok() {
                if res.is_internal {
                    self.internal_topics.insert(topic_name.clone());
                }
                let topic: Topic = (topic_name, res).into();
                if self.unauthorized_topics.contains(topic_name) {
                    self.unauthorized_topics.remove(topic_name);
                }
                if self.invalid_topics.contains(topic_name) {
                    self.invalid_topics.remove(topic_name);
                }
                self.topics.insert(topic_name.clone(), topic);
            } else {
                if self.topics.contains_key(topic_name) {
                    self.topics.remove(topic_name);
                }

                match error_code.err() {
                    Some(ResponseError::TopicAuthorizationFailed) => {
                        self.unauthorized_topics.insert(topic_name.clone());
                    }
                    Some(ResponseError::InvalidTopicException) => {
                        self.invalid_topics.insert(topic_name.clone());
                    }
                    _ => {}
                }
            }
        }

        self.partitions.clear();
        self.available_partitions.clear();
        self.partitions_by_nodes.clear();

        Ok(())
    }

    pub fn topic_id(&self, topic: &TopicName) -> Option<Uuid> {
        if let Some(topic) = self.topics.get(topic) {
            return Some(topic.id);
        }
        None
    }

    pub fn num_partitions(&self, topic: &TopicName) -> Result<i32> {
        if let Some(topic_entry) = self.topics.get(topic) {
            return Ok(topic_entry.value().partitions.len() as i32);
        }
        Ok(0)
    }

    pub fn partitions(&self, topic: &TopicName) -> Result<PartitionRef> {
        if let Some(partition_entry) = self.partitions.get(topic) {
            return Ok(partition_entry);
        } else if let Some(topic_entry) = self.topics.get(topic) {
            let partitions = topic_entry.partitions();
            self.partitions.insert(topic.clone(), partitions);
            return self.partitions(topic);
        }
        Err(Error::TopicNotAvailable {
            topic: topic.clone(),
        })
    }

    pub fn available_partitions(&self, topic: &TopicName) -> Result<PartitionRef> {
        if let Some(partition_entry) = self.available_partitions.get(topic) {
            return Ok(partition_entry);
        } else if let Some(topic_entry) = self.topics.get(topic) {
            let partitions = topic_entry.available_partitions();
            self.available_partitions.insert(topic.clone(), partitions);
            return self.available_partitions(topic);
        }
        Err(Error::TopicNotAvailable {
            topic: topic.clone(),
        })
    }

    pub fn current_leader(&self, partition: &TopicPartition) -> LeaderAndEpoch {
        let mut leader_epoch = LeaderAndEpoch::default();
        if let Some(entry) = self.topics.get(&partition.topic) {
            if let Some(partition) = entry
                .value()
                .partitions
                .iter()
                .find(|p| p.partition == partition.partition)
            {
                leader_epoch.leader = Some(partition.leader);
                leader_epoch.epoch = Some(partition.leader_epoch);
            }
        }
        leader_epoch
    }

    pub fn drain_node(&self, node: NodeId) -> Result<NodeRef> {
        if !self.nodes.contains_key(&node) {
            return Err(Error::NodeNotAvailable { node });
        }
        return if let Some(node_entry) = self.partitions_by_nodes.get(&node) {
            Ok(node_entry)
        } else {
            let mut topic_partitions = Vec::new();
            for topic_entry in self.topics.iter() {
                for partition in topic_entry.partitions.iter() {
                    topic_partitions.push(TopicPartition::new(
                        topic_entry.key().clone(),
                        partition.partition,
                    ));
                }
            }
            self.partitions_by_nodes.insert(node, topic_partitions);
            self.drain_node(node)
        };
    }
}
