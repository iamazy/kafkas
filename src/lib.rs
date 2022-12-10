#![feature(type_alias_impl_trait)]
#![allow(clippy::mutable_key_type)]

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::mapref::one::Ref;
use kafka_protocol::{
    messages::TopicName,
    protocol::{Encodable, StrBytes},
};

pub mod client;
pub mod connection;
pub mod connection_manager;
pub mod consumer;

mod error;
pub use error::{Error, Result};
pub mod executor;
pub mod metadata;
pub mod producer;
pub mod protocol;

// kafka protocol
pub use kafka_protocol::records::{
    Compression, Record, TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH,
    NO_PRODUCER_ID, NO_SEQUENCE,
};
pub use producer::ProducerRecord;

const DEFAULT_SERVER_PORT: u16 = 9092;

pub type PartitionRef<'a> = Ref<'a, TopicName, Vec<i32>>;
pub type NodeRef<'a> = Ref<'a, i32, Vec<(TopicName, Vec<i32>)>>;

pub trait ToStrBytes {
    fn to_str_bytes(self) -> StrBytes;
}

impl ToStrBytes for String {
    fn to_str_bytes(self) -> StrBytes {
        unsafe { StrBytes::from_utf8_unchecked(Bytes::from(self)) }
    }
}

// bytes utils
pub fn to_version_prefixed_bytes<M: Encodable>(version: i16, message: M) -> Result<Bytes> {
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
