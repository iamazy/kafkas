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
use dashmap::DashMap;
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
use tracing::{error, info, trace, warn};

use crate::{
    client::{Kafka, SerializeMessage},
    error::{Error, ProduceError, Result},
    executor::Executor,
    metadata::Node,
    producer::{
        batch::{ProducerBatch, Thunk},
        partitioner::{PartitionSelector, Partitioner, PartitionerSelector, RoundRobinPartitioner},
    },
    ToStrBytes, TopicPartition,
};

pub mod aggregator;
pub mod batch;
pub mod partitioner;

pub struct SendFuture(pub oneshot::Receiver<Result<RecordMetadata>>);

impl Future for SendFuture {
    type Output = Result<RecordMetadata>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(_canceled)) => Poll::Ready(Err(Error::Custom(
                "Producer unexpectedly disconnected".into(),
            ))),
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
            _ => unimplemented!("Only support roundbin partitioner"),
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

        producer.client.executor.spawn(Box::pin(async move {
            while interval.next().await.is_some() {
                match weak_producer.upgrade() {
                    Some(strong_producer) => {
                        if let Err(err) = strong_producer
                            .send_raw(&strong_producer.options, &strong_producer.encode_options)
                            .await
                        {
                            error!("Send failed!, error: {err}");
                        }
                    }
                    None => {
                        info!("Producer is shutting down.");
                        break;
                    }
                }
            }
        }));

        Ok(producer)
    }

    pub async fn send<T>(&self, topic: &TopicName, record: T) -> Result<SendFuture>
    where
        T: SerializeMessage + Sized,
    {
        let fut = match self.producers.get(topic) {
            Some(producer) => producer.try_push(&self.partitioner, record).await,
            None => {
                let producer = TopicProducer::new(self.client.clone(), topic.clone()).await?;
                let fut = producer.try_push(&self.partitioner, record).await;
                self.producers.insert(topic.clone(), producer);
                fut
            }
        };

        return match fut {
            Ok(fut) => {
                self.num_records.fetch_add(1, Ordering::Relaxed);
                Ok(fut)
            }
            Err(Error::Produce(ProduceError::NoCapacity((partition, record)))) => {
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
                    "failed to push record, topic: {}, err: {err}",
                    topic.as_str(),
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
            trace!("no records to send.");
            self.client.executor.delay(Duration::from_secs(1)).await;
            return Ok(());
        }
        if let Ok(flush_list) = self.flush(encode_options).await {
            for (node, mut result) in flush_list {
                let mut request = ProduceRequest::default();
                request.topic_data = result.data;
                request.acks = options.acks;
                request.timeout_ms = self.client.manager.options().request_timeout_ms;

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
            if let Ok(node) = self.client.cluster_meta.drain_node(node_entry.value().id) {
                let partitions = node.value();
                if partitions.is_empty() {
                    continue;
                }

                let mut topic_data = IndexMap::new();
                let mut topics_thunks = BTreeMap::new();

                for partition in partitions {
                    // flush topic produce data
                    if let Err(e) = self
                        .flush_topic_partition(
                            partition,
                            &mut topics_thunks,
                            &mut topic_data,
                            encode_options,
                        )
                        .await
                    {
                        error!(
                            "failed to flush topic produce data, topic: [{} - {}], error: {}",
                            partition.topic.as_str(),
                            partition.partition,
                            e
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

    async fn flush_topic_partition(
        &self,
        partition: &TopicPartition,
        topics_thunks: &mut BTreeMap<TopicName, PartitionsFlush>,
        topics_data: &mut IndexMap<TopicName, TopicProduceData>,
        encode_options: &RecordEncodeOptions,
    ) -> Result<()> {
        if let Some(producer) = self.producers.get_mut(&partition.topic) {
            if let Some(mut batch) = producer.batches.get_mut(&partition.partition) {
                if let Some((thunks, partition_produce_data)) = self
                    .flush_partition(batch.value_mut(), encode_options)
                    .await?
                {
                    match topics_data.get_mut(&partition.topic) {
                        Some(topic_data) => topic_data.partition_data.push(partition_produce_data),
                        None => {
                            let topic_data = vec![partition_produce_data];
                            let mut produce_data = TopicProduceData::default();
                            produce_data.partition_data = topic_data;
                            topics_data.insert(partition.topic.clone(), produce_data);
                        }
                    }

                    match topics_thunks.get_mut(&partition.topic) {
                        Some(topic_thunks) => {
                            topic_thunks
                                .partitions_thunks
                                .insert(batch.partition, thunks);
                        }
                        None => {
                            let mut partitions_thunks = BTreeMap::new();
                            partitions_thunks.insert(partition.partition, thunks);
                            topics_thunks.insert(
                                partition.topic.clone(),
                                PartitionsFlush { partitions_thunks },
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn flush_partition(
        &self,
        batch: &mut ProducerBatch,
        encode_options: &RecordEncodeOptions,
    ) -> Result<Option<(Vec<Thunk>, PartitionProduceData)>> {
        let partition = batch.partition;
        let (thunks, records) = batch.flush()?;
        if !records.is_empty() {
            let mut buf = BytesMut::new();
            RecordBatchEncoder::encode(&mut buf, records.iter(), encode_options)?;
            let mut produce_data = PartitionProduceData::default();
            produce_data.index = partition;
            produce_data.records = Some(buf.freeze());
            return Ok(Some((thunks, produce_data)));
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
        client.update_metadata(vec![topic.clone()]).await?;

        let partitions = client.cluster_meta.partitions(&topic)?;
        let partitions = partitions.value();
        let num_partitions = partitions.len();
        let batches = DashMap::with_capacity_and_hasher(num_partitions, FxBuildHasher::default());
        for partition in partitions {
            // TODO: split batch when got `MessageTooLarge` error
            batches.insert(*partition, ProducerBatch::new(100 * 1024, *partition));
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
        partitioner: &PartitionerSelector,
        record: T,
    ) -> Result<SendFuture> {
        let mut partition = record.partition().unwrap_or(-1);
        if partition < 0 {
            partition = partitioner.select(
                &self.topic,
                record.key(),
                record.value(),
                &self.client.cluster_meta,
            )?;
        }
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
