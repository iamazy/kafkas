use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use futures::lock::Mutex;
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
    error::{Error, Result},
    NodeRef, PartitionRef,
};

pub(crate) const GROUP_METADATA_TOPIC_NAME: &str = "__consumer_offsets";
pub(crate) const TRANSACTION_STATE_TOPIC_NAME: &str = "__transaction_state";

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
    pub partition: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub in_sync_replicas: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

impl From<&MetadataResponsePartition> for Partition {
    fn from(partition: &MetadataResponsePartition) -> Self {
        let partition_index = partition.partition_index;
        Self {
            partition: partition_index,
            leader: partition.leader_id.0,
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
pub struct TopicPartition {
    pub topic: TopicName,
    pub partition: i32,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct Node {
    pub id: i32,
    pub host: String,
    pub port: u16,
    address: String,
}

impl Node {
    pub fn new(id: i32, host: String, port: u16) -> Self {
        Self {
            id,
            host: host.clone(),
            port,
            address: format!("{host}:{port}"),
        }
    }

    pub fn address(&self) -> &String {
        &self.address
    }
}

impl From<(&BrokerId, &MetadataResponseBroker)> for Node {
    fn from((id, broker): (&BrokerId, &MetadataResponseBroker)) -> Self {
        Node::new(id.0, broker.host.to_string(), broker.port as u16)
    }
}

#[derive(Debug, Clone, Default)]
pub struct Cluster {
    pub id: Arc<Mutex<Option<String>>>,
    pub unauthorized_topics: DashSet<TopicName>,
    pub invalid_topics: DashSet<TopicName>,
    pub internal_topics: DashSet<TopicName>,
    pub controller: Arc<Mutex<i32>>,
    pub topics: DashMap<TopicName, Topic>,
    pub nodes: DashMap<i32, Node>,
    pub available_partitions: DashMap<TopicName, Vec<i32>>,
    pub partitions: DashMap<TopicName, Vec<i32>>,
    pub partitions_by_nodes: DashMap<i32, Vec<(TopicName, Vec<i32>)>>,
}

impl Cluster {
    pub fn empty() -> Cluster {
        Default::default()
    }

    pub async fn merge_meta(&self, other: MetadataResponse) -> Result<()> {
        let cluster_id = other.cluster_id.map(|str| str.to_string());
        {
            let mut lock = self.id.lock().await;
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
            let mut lock = self.controller.lock().await;
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

    pub fn drain_node(&self, node: &Node) -> Result<NodeRef> {
        if !self.nodes.contains_key(&node.id) {
            return Err(Error::NodeNotAvailable { node: node.clone() });
        }
        return if let Some(node_entry) = self.partitions_by_nodes.get(&node.id) {
            Ok(node_entry)
        } else {
            let mut topic_partitions = Vec::new();
            for topic_entry in self.topics.iter() {
                let partitions = topic_entry
                    .value()
                    .partitions
                    .iter()
                    .filter(|p| p.leader == node.id)
                    .map(|p| p.partition)
                    .collect();
                topic_partitions.push((topic_entry.name.clone().into(), partitions));
            }
            self.partitions_by_nodes.insert(node.id, topic_partitions);
            self.drain_node(node)
        };
    }
}
