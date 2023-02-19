#![allow(dead_code)]
#![allow(clippy::mutable_key_type)]

use std::{collections::HashMap, fmt::Display};

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::mapref::one::Ref;
use kafka_protocol::{
    messages::TopicName,
    protocol::{Encodable, StrBytes},
};

mod client;
pub use client::{DeserializeMessage, Kafka, KafkaOptions, SerializeMessage};

mod connection;
mod connection_manager;

mod consumer;
pub use consumer::{
    Consumer, ConsumerOptions, ConsumerRecord, ConsumerRecords, OffsetResetStrategy,
    RebalanceOptions,
};

mod coordinator;

mod error;
pub use error::{Error, Result};

mod executor;
pub use executor::{AsyncStdExecutor, TokioExecutor};

mod metadata;
pub use metadata::TopicPartition;

mod producer;
pub use producer::{Producer, ProducerOptions, ProducerRecord};

mod protocol;

// kafka protocol
pub use kafka_protocol::records::{
    Compression, Record, TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH,
    NO_PRODUCER_ID, NO_SEQUENCE,
};

type NodeId = i32;
type PartitionId = i32;
type MemberId = StrBytes;

const UNKNOWN_OFFSET: i64 = -1;
const UNKNOWN_TIMESTAMP: i64 = -1;
const UNKNOWN_EPOCH: i32 = NO_PARTITION_LEADER_EPOCH;
const DEFAULT_GENERATION_ID: i32 = -1;
const INVALID_LOG_START_OFFSET: i64 = -1;

type PartitionRef<'a> = Ref<'a, TopicName, Vec<PartitionId>>;
type NodeRef<'a> = Ref<'a, NodeId, Vec<TopicPartition>>;

pub trait ToStrBytes {
    fn to_str_bytes(self) -> StrBytes;
}

impl ToStrBytes for String {
    fn to_str_bytes(self) -> StrBytes {
        unsafe { StrBytes::from_utf8_unchecked(Bytes::from(self)) }
    }
}

// bytes utils
fn to_version_prefixed_bytes<M: Encodable>(version: i16, message: M) -> Result<Bytes> {
    let message_size = message.compute_size(version)?;
    let mut bytes = BytesMut::with_capacity(message_size + 2);
    bytes.put_i16(version);
    message.encode(&mut bytes, version)?;
    Ok(bytes.freeze())
}

pub fn topic_name<S: AsRef<str>>(topic: S) -> TopicName {
    let topic = topic.as_ref().to_string().to_str_bytes();
    TopicName(topic)
}

fn map_to_list<K, V>(map: HashMap<K, V>) -> Vec<V> {
    let mut list = Vec::with_capacity(map.len());
    for (_, v) in map {
        list.push(v);
    }
    list
}

fn array_display<T: Display, I: Iterator<Item = T>>(array: I) -> String {
    let mut display = String::new();
    for item in array {
        display.extend(format!("{item}, ").chars());
    }
    display.remove(display.len() - 1);
    display.remove(display.len() - 1);
    display
}
