use std::{
    collections::BTreeMap,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use chrono::Local;
use dashmap::{mapref::multiple::RefMutMulti, DashMap};
use futures::{channel::oneshot, StreamExt};
use fxhash::FxBuildHasher;
use indexmap::IndexMap;
use kafka_protocol::{
    messages::{
        produce_request::{PartitionProduceData, TopicProduceData},
        ProduceRequest, TopicName,
    },
    records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
    },
};
use tracing::{debug, error, warn};

use crate::{
    client::{Kafka, SerializeMessage},
    error::{Error, ProduceError, Result},
    executor::Executor,
    metadata::Node,
    producer::{
        batch::{ProducerBatch, Thunk},
        partitioner::{PartitionSelector, Partitioner, PartitionerSelector, RoundRobinPartitioner},
    },
    ToStrBytes,
};

pub mod aggregator;
pub mod batch;
pub mod partitioner;

pub struct SendFuture(pub(crate) oneshot::Receiver<Result<RecordMetadata>>);

impl Future for SendFuture {
    type Output = Result<RecordMetadata>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(_canceled)) => Poll::Ready(Err(ProduceError::Custom(
                "producer unexpectedly disconnected".into(),
            )
            .into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecordMetadata {
    pub partition: i32,
    pub key_size: usize,
    pub value_size: usize,
    pub offset: i64,
    pub timestamp: i64,
}

/// High-level producer record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProducerRecord {
    pub partition: i32,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: BTreeMap<String, Option<Bytes>>,
}

impl ProducerRecord {
    pub fn new() -> Self {
        Self {
            partition: -1,
            ..Default::default()
        }
    }

    pub fn partition(&mut self, partition: i32) {
        self.partition = partition;
    }

    pub fn key<K: Into<Bytes>>(&mut self, key: K) {
        self.key = Some(key.into());
    }

    pub fn value<V: Into<Bytes>>(&mut self, value: V) {
        self.value = Some(value.into());
    }

    pub fn add_header<K: Into<String>, V: Into<Bytes>>(&mut self, key: K, value: V) {
        self.headers.insert(key.into(), Some(value.into()));
    }
}

impl SerializeMessage for ProducerRecord {
    fn partition(&self) -> Option<i32> {
        Some(self.partition)
    }

    fn key(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    fn serialize_message(input: Self) -> Result<Record> {
        let time = Local::now();
        let mut headers = IndexMap::new();
        for (k, v) in input.headers {
            headers.insert(k.to_str_bytes(), v);
        }
        Ok(Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: -1,
            sequence: NO_SEQUENCE,
            timestamp: time.timestamp(),
            key: input.key,
            value: input.value,
            headers,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProducerOptions {
    pub linger_ms: Duration,
    pub batch_size: usize,
    pub acks: i16,
    pub delivery_timeout_ms: i32,
    // pub max_request_size: usize,
    // pub max_block_size: usize,
    pub compression_type: Compression,
    pub transactional_id: Option<String>,
    pub transactional_timeout_ms: Duration,
    pub partitioner: Partitioner,
}

impl ProducerOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for ProducerOptions {
    fn default() -> Self {
        Self {
            linger_ms: Duration::from_micros(1),
            batch_size: 16384,
            acks: 1,
            delivery_timeout_ms: 120_000,
            // max_request_size: 1048576, // 1 * 1024 * 1024
            // max_block_size: 60_000,
            compression_type: Compression::None,
            transactional_id: None,
            transactional_timeout_ms: Duration::from_millis(60_000),
            partitioner: Partitioner::RoundRobin,
        }
    }
}

pub struct Producer<Exe: Executor> {
    client: Arc<Kafka<Exe>>,
    producers: DashMap<TopicName, Arc<TopicProducer<Exe>>, FxBuildHasher>,
    partitioner: PartitionerSelector,
    num_records: AtomicUsize,
    options: ProducerOptions,
    encode_options: RecordEncodeOptions,
}

impl<Exe: Executor> Producer<Exe> {
    pub async fn new(client: Kafka<Exe>, options: ProducerOptions) -> Result<Arc<Self>> {
        let client = Arc::new(client);
        let producers = DashMap::with_hasher(FxBuildHasher::default());

        let partitioner = match options.partitioner {
            Partitioner::RoundRobin => {
                PartitionerSelector::RoundRobin(RoundRobinPartitioner::new())
            }
            _ => unimplemented!("only support roundbin partitioner"),
        };

        let encode_options = RecordEncodeOptions {
            version: 2,
            compression: options.compression_type,
        };

        let producer = Self {
            client,
            producers,
            partitioner,
            num_records: AtomicUsize::new(0),
            options,
            encode_options,
        };
        let producer = Arc::new(producer);
        let weak_producer = Arc::downgrade(&producer);

        let mut interval = producer
            .client
            .executor
            .interval(producer.options.linger_ms);

        let res = producer.client.executor.spawn(Box::pin(async move {
            while interval.next().await.is_some() {
                if let Some(strong_producer) = weak_producer.upgrade() {
                    if let Err(e) = strong_producer
                        .send_raw(&strong_producer.options, &strong_producer.encode_options)
                        .await
                    {
                        error!("send failed!, error: {e}");
                    }
                } else {
                    break;
                }
            }
        }));

        if res.is_err() {
            error!("the executor could not spawn the linger task");
            return Err(ProduceError::Custom(
                "the executor could not spawn the linger task".to_string(),
            )
            .into());
        }

        Ok(producer)
    }

    pub async fn send<T>(&self, topic: &TopicName, record: T) -> Result<SendFuture>
    where
        T: SerializeMessage + Sized,
    {
        let mut partition = record.partition().unwrap_or(-1);
        if partition < 0 {
            partition = self
                .partitioner
                .select(topic, record.key(), record.value(), self.client.clone())
                .await?;
        }

        let fut = if let Some(producer) = self.producers.get(topic) {
            producer.try_push(partition, record).await
        } else {
            let producer = TopicProducer::new(self.client.clone(), topic.clone()).await?;
            let fut = producer.try_push(partition, record).await;
            self.producers.insert(topic.clone(), producer);
            fut
        };

        return match fut {
            Ok(fut) => {
                self.num_records.fetch_add(1, Ordering::Relaxed);
                Ok(fut)
            }
            Err(Error::Produce(ProduceError::NoCapacity(record))) => {
                self.send_raw(&self.options, &self.encode_options).await?;
                let producer = self.producers.get(topic).unwrap();
                let fut = producer.try_push_raw(partition, record).await;
                if fut.is_ok() {
                    self.num_records.fetch_add(1, Ordering::Relaxed);
                    fut
                } else {
                    Err(ProduceError::MessageTooLarge.into())
                }
            }
            Err(err) => {
                error!(
                    "failed to push record, topic: {:?}, partition: {partition}, err: {err}",
                    topic,
                );
                Err(err)
            }
        };
    }

    async fn send_raw(
        &self,
        options: &ProducerOptions,
        encode_options: &RecordEncodeOptions,
    ) -> Result<()> {
        if self.num_records.load(Ordering::Relaxed) == 0 {
            debug!("no records to send.");
            return Ok(());
        }
        if let Ok(flush_list) = self.flush(encode_options).await {
            for (node, mut result) in flush_list {
                let request = ProduceRequest {
                    topic_data: result.data,
                    acks: options.acks,
                    timeout_ms: self.client.options.request_timeout_ms,
                    ..Default::default()
                };
                if let Ok(res) = self.client.produce(&node, request).await {
                    for (key, value) in res.responses {
                        if let Some(mut partitions_flush) = result.topics_thunks.remove(&key.0) {
                            for partition_res in value.partition_responses {
                                if let Some(thunks) = partitions_flush
                                    .partitions_thunks
                                    .remove(&partition_res.index)
                                {
                                    for thunk in thunks {
                                        self.num_records.fetch_sub(1, Ordering::Relaxed);

                                        if let Err(metadata) = thunk.done(&partition_res) {
                                            warn!(
                                                "fail to send record, {:?}, partition response: \
                                                 {:?}",
                                                metadata, partition_res
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn flush(
        &self,
        encode_options: &RecordEncodeOptions,
    ) -> Result<Vec<(Node, FlushResult)>> {
        let mut result = Vec::new();
        for node_entry in self.client.cluster_meta.nodes.iter() {
            if let Ok(node_topology) = self.client.cluster_meta.drain_node(node_entry.value()) {
                let node_topology = node_topology.value();
                let mut topic_data = IndexMap::new();
                let mut topics_thunks = BTreeMap::new();

                for (topic, partitions) in node_topology {
                    // flush topic produce data
                    if let Err(e) = self
                        .flush_topics(
                            topic,
                            partitions,
                            &mut topics_thunks,
                            &mut topic_data,
                            encode_options,
                        )
                        .await
                    {
                        error!(
                            "failed to flush topic produce data, topic: {:?}, error: {}",
                            topic, e
                        );
                    }
                }

                result.push((
                    node_entry.value().clone(),
                    FlushResult {
                        topics_thunks,
                        data: topic_data,
                    },
                ));
            }
        }
        Ok(result)
    }

    async fn flush_topics(
        &self,
        topic: &TopicName,
        partitions: &[i32],
        topics_thunks: &mut BTreeMap<TopicName, PartitionsFlush>,
        topic_data: &mut IndexMap<TopicName, TopicProduceData>,
        encode_options: &RecordEncodeOptions,
    ) -> Result<()> {
        if let Some(producer) = self.producers.get(topic) {
            let mut partitions_data = Vec::new();
            let mut partitions_thunks = BTreeMap::new();

            for mut entry in producer.batches.iter_mut() {
                if partitions.contains(entry.key()) {
                    if let Some((thunks, partition_produce_data)) =
                        self.flush_partitions(&mut entry, encode_options).await?
                    {
                        partitions_thunks.insert(*entry.key(), thunks);
                        partitions_data.push(partition_produce_data);
                    }
                }
            }
            if !partitions_data.is_empty() {
                let data = TopicProduceData {
                    partition_data: partitions_data,
                    ..Default::default()
                };

                topic_data.insert(topic.clone(), data);
                topics_thunks.insert(topic.clone(), PartitionsFlush { partitions_thunks });
            }
        }

        Ok(())
    }

    async fn flush_partitions(
        &self,
        entry: &mut RefMutMulti<'_, i32, ProducerBatch, FxBuildHasher>,
        encode_options: &RecordEncodeOptions,
    ) -> Result<Option<(Vec<Thunk>, PartitionProduceData)>> {
        let partition = *entry.key();
        let batch = entry.value_mut();
        let (thunks, records) = batch.flush()?;
        if !records.is_empty() {
            let mut buf = BytesMut::new();
            RecordBatchEncoder::encode(&mut buf, records.iter(), encode_options)?;
            return Ok(Some((
                thunks,
                PartitionProduceData {
                    index: partition,
                    records: Some(buf.freeze()),
                    ..Default::default()
                },
            )));
        }
        Ok(None)
    }
}

struct TopicProducer<Exe: Executor> {
    client: Arc<Kafka<Exe>>,
    topic: TopicName,
    batches: DashMap<i32, ProducerBatch, FxBuildHasher>,
}

impl<Exe: Executor> TopicProducer<Exe> {
    pub async fn new(client: Arc<Kafka<Exe>>, topic: TopicName) -> Result<Arc<TopicProducer<Exe>>> {
        let partitions = client.partitions(&topic).await?;
        let partitions = partitions.value();
        let num_partitions = partitions.len();
        let batches = DashMap::with_capacity_and_hasher(num_partitions, FxBuildHasher::default());
        for partition in partitions {
            batches.insert(*partition, ProducerBatch::new(1024 * 1024));
        }

        let producer = Self {
            client: client.clone(),
            topic: topic.clone(),
            batches,
        };

        Ok(Arc::new(producer))
    }

    pub async fn try_push<T: SerializeMessage + Sized>(
        &self,
        partition: i32,
        record: T,
    ) -> Result<SendFuture> {
        return match self.batches.get_mut(&partition) {
            Some(mut batch) => batch.try_push(T::serialize_message(record)?),
            None => Err(Error::PartitionNotAvailable {
                topic: self.topic.clone(),
                partition,
            }),
        };
    }

    pub async fn try_push_raw(&self, partition: i32, record: Record) -> Result<SendFuture> {
        return match self.batches.get_mut(&partition) {
            Some(mut batch) => batch.try_push(record),
            None => Err(Error::PartitionNotAvailable {
                topic: self.topic.clone(),
                partition,
            }),
        };
    }
}

struct FlushResult {
    topics_thunks: BTreeMap<TopicName, PartitionsFlush>,
    data: IndexMap<TopicName, TopicProduceData>,
}

struct PartitionsFlush {
    partitions_thunks: BTreeMap<i32, Vec<Thunk>>,
}
