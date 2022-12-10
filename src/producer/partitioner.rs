use std::{
    future::Future,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use kafka_protocol::messages::TopicName;

use crate::{client::Kafka, error::Result, executor::Executor};

pub trait PartitionSelector<Exe: Executor> {
    type SelectFuture<'a>: Future<Output = Result<i32>> + Send + 'a
    where
        Self: 'a;

    fn select<'a>(
        &'a self,
        topic: &'a TopicName,
        key: Option<&'a Bytes>,
        value: Option<&'a Bytes>,
        client: Arc<Kafka<Exe>>,
    ) -> Self::SelectFuture<'a>;
}

#[derive(Debug, Clone)]
pub enum Partitioner {
    Default,
    RoundRobin,
    UniformSticky,
}

#[derive(Debug)]
pub enum PartitionerSelector {
    // Default(DefaultPartitioner),
    RoundRobin(RoundRobinPartitioner),
    // UniformSticky(UniformStickyPartitioner),
}

impl<Exe: Executor> PartitionSelector<Exe> for PartitionerSelector {
    type SelectFuture<'a> = impl Future<Output = Result<i32>> + Send + 'a;

    fn select<'a>(
        &'a self,
        topic: &'a TopicName,
        key: Option<&'a Bytes>,
        value: Option<&'a Bytes>,
        client: Arc<Kafka<Exe>>,
    ) -> Self::SelectFuture<'a> {
        match self {
            PartitionerSelector::RoundRobin(roundbin) => roundbin.select(topic, key, value, client),
        }
    }
}

/// The default partitioning strategy:
/// - If a partition is specified in the record, use it
/// - If no partition is specified but a key is present choose a partition based on a hash of the
/// key
/// - If no partition or key is present choose the sticky partition that changes when the batch is
/// full. See KIP-480 for details about sticky partitioning.
#[derive(Debug, Clone)]
pub struct DefaultPartitioner;

/// The "Round-Robin" partitioner This partitioning strategy can be used when user wants to
/// distribute the writes to all partitions equally. This is the behaviour regardless of record
/// key hash.
#[derive(Debug)]
pub struct RoundRobinPartitioner {
    count: AtomicI32,
}

impl Default for RoundRobinPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundRobinPartitioner {
    pub fn new() -> Self {
        Self {
            count: AtomicI32::new(1),
        }
    }

    fn next_value(&self) -> i32 {
        let value = self.count.fetch_add(1, Ordering::Relaxed);
        if value == i32::MAX {
            self.count.store(1, Ordering::Relaxed);
        }
        value
    }
}

impl<Exe: Executor> PartitionSelector<Exe> for RoundRobinPartitioner {
    type SelectFuture<'a> = impl Future<Output = Result<i32>> + Send + 'a;

    fn select<'a>(
        &'a self,
        topic: &'a TopicName,
        _key: Option<&'a Bytes>,
        _value: Option<&'a Bytes>,
        client: Arc<Kafka<Exe>>,
    ) -> Self::SelectFuture<'a> {
        async move {
            let partitions = client.partitions(topic).await?;
            let num_partitions = partitions.len();
            let next_value = self.next_value();
            let available_partitions = client.cluster_meta.available_partitions(topic)?;
            let num_available_partitions = available_partitions.len();
            if num_available_partitions != 0 {
                let part = (next_value & 0x7fffffff) % num_available_partitions as i32;
                Ok(available_partitions[part as usize])
            } else {
                Ok((next_value & 0x7fffffff) % num_partitions as i32)
            }
        }
    }
}

/// The `uniform-sticky` partitioning strategy:
/// - If a partition is specified in the record, use it
/// - Otherwise choose the sticky partition that changes when the batch is full. NOTE: In contrast
/// to the DefaultPartitioner, the record key is NOT used as part of the partitioning strategy in
/// this partitioner. Records with the same key are not guaranteed to be sent to the same partition.
/// See KIP-480 for details about sticky partitioning.
#[derive(Debug, Clone)]
pub struct UniformStickyPartitioner;
