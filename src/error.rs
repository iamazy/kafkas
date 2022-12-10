use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, PoisonError,
};

use kafka_protocol::{
    messages::{
        consumer_protocol_assignment::ConsumerProtocolAssignmentBuilderError,
        describe_groups_request::DescribeGroupsRequestBuilderError,
        heartbeat_request::HeartbeatRequestBuilderError,
        join_group_request::JoinGroupRequestBuilderError,
        leave_group_request::LeaveGroupRequestBuilderError,
        offset_commit_request::OffsetCommitRequestBuilderError,
        offset_fetch_request::OffsetFetchRequestBuilderError,
        sync_group_request::SyncGroupRequestBuilderError, TopicName,
    },
    protocol::{DecodeError, EncodeError},
    records::Record,
    ResponseError,
};

use crate::{metadata::Node, producer::SendFuture};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Custom(String),
    Connection(ConnectionError),
    InvalidVersion(i16),
    TopicNotAvailable {
        topic: TopicName,
    },
    PartitionNotAvailable {
        topic: TopicName,
        partition: i32,
    },
    NodeNotAvailable {
        node: Node,
    },
    Produce(ProduceError),
    Consume(ConsumeError),
    Response {
        error: ResponseError,
        msg: Option<String>,
    },
}

impl<T> From<PoisonError<T>> for Error {
    fn from(value: PoisonError<T>) -> Self {
        Self::Custom(value.to_string())
    }
}

impl From<ConnectionError> for Error {
    fn from(err: ConnectionError) -> Self {
        Self::Connection(err)
    }
}

impl From<ConnectionError> for Box<Error> {
    fn from(err: ConnectionError) -> Self {
        Box::new(Error::Connection(err))
    }
}

impl From<ProduceError> for Error {
    fn from(err: ProduceError) -> Self {
        Error::Produce(err)
    }
}

impl From<ConsumeError> for Error {
    fn from(err: ConsumeError) -> Self {
        Error::Consume(err)
    }
}

impl From<EncodeError> for Error {
    fn from(_: EncodeError) -> Self {
        Error::Connection(ConnectionError::Encoding("encode error".into()))
    }
}

impl From<DecodeError> for Error {
    fn from(_: DecodeError) -> Self {
        Error::Connection(ConnectionError::Decoding("decode error".into()))
    }
}

impl From<DescribeGroupsRequestBuilderError> for Error {
    fn from(value: DescribeGroupsRequestBuilderError) -> Self {
        Error::Custom(value.to_string())
    }
}

impl From<LeaveGroupRequestBuilderError> for Error {
    fn from(value: LeaveGroupRequestBuilderError) -> Self {
        Error::Custom(value.to_string())
    }
}

impl From<HeartbeatRequestBuilderError> for Error {
    fn from(value: HeartbeatRequestBuilderError) -> Self {
        Error::Custom(value.to_string())
    }
}

impl From<OffsetCommitRequestBuilderError> for Error {
    fn from(value: OffsetCommitRequestBuilderError) -> Self {
        Error::Custom(value.to_string())
    }
}

impl From<OffsetFetchRequestBuilderError> for Error {
    fn from(value: OffsetFetchRequestBuilderError) -> Self {
        Error::Custom(value.to_string())
    }
}

impl From<JoinGroupRequestBuilderError> for Error {
    fn from(value: JoinGroupRequestBuilderError) -> Self {
        Error::Custom(value.to_string())
    }
}

impl From<SyncGroupRequestBuilderError> for Error {
    fn from(value: SyncGroupRequestBuilderError) -> Self {
        Error::Custom(value.to_string())
    }
}

impl From<ConsumerProtocolAssignmentBuilderError> for Error {
    fn from(value: ConsumerProtocolAssignmentBuilderError) -> Self {
        Error::Custom(value.to_string())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Custom(e) => write!(f, "{e}"),
            Error::Connection(e) => write!(f, "Connection error: {e}"),
            Error::InvalidVersion(v) => write!(f, "Invalid version: {v}"),
            Error::Produce(e) => write!(f, "Produce error: {e}"),
            Error::Consume(e) => write!(f, "Consume error: {e}"),
            Error::PartitionNotAvailable { topic, partition } => {
                write!(f, "Partition {partition} not available, topic: {topic:?}")
            }
            Error::TopicNotAvailable { topic } => {
                write!(f, "Topic not available, topic: {topic:?}")
            }
            Error::NodeNotAvailable { node } => {
                write!(f, "Node not available, node: {node:?}")
            }
            Error::Response { error, msg } => write!(f, "Error code: {error:?}, msg: {msg:?}"),
        }
    }
}

#[derive(Debug)]
pub enum ConnectionError {
    Io(std::io::Error),
    Disconnected,
    Unexpected(String),
    Decoding(String),
    Encoding(String),
    SocketAddr(String),
    UnexpectedResponse(String),
    Tls(native_tls::Error),
    NotFound,
    Canceled,
    Shutdown,
    Timeout,
}

impl From<EncodeError> for ConnectionError {
    fn from(_: EncodeError) -> Self {
        ConnectionError::Encoding("encode error".into())
    }
}

impl From<DecodeError> for ConnectionError {
    fn from(_: DecodeError) -> Self {
        ConnectionError::Decoding("decode error".into())
    }
}

impl From<std::io::Error> for ConnectionError {
    fn from(e: std::io::Error) -> Self {
        ConnectionError::Io(e)
    }
}

impl From<()> for ConnectionError {
    fn from(_: ()) -> Self {
        ConnectionError::NotFound
    }
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ConnectionError::Io(e) => write!(f, "{e}"),
            ConnectionError::Disconnected => write!(f, "Disconnected"),
            ConnectionError::Unexpected(e) => write!(f, "{e}"),
            ConnectionError::Decoding(e) => write!(f, "Error decoding message: {e}"),
            ConnectionError::Encoding(e) => write!(f, "Error encoding message: {e}"),
            ConnectionError::SocketAddr(e) => write!(f, "Error obtaining socket address: {e}"),
            ConnectionError::Tls(e) => write!(f, "Error connecting TLS stream: {e}"),
            ConnectionError::UnexpectedResponse(e) => {
                write!(f, "Unexpected response from kafka: {e}")
            }
            ConnectionError::NotFound => write!(f, "Error looking up URL"),
            ConnectionError::Canceled => write!(f, "Canceled request"),
            ConnectionError::Shutdown => write!(f, "The connection was shut down"),
            ConnectionError::Timeout => write!(f, "Connection timeout"),
        }
    }
}

pub enum ProduceError {
    Connection(ConnectionError),
    Custom(String),
    Io(std::io::Error),
    PartialSend(Vec<Result<SendFuture>>),
    /// Indiciates the error was part of sending a batch, and thus shared across the batch
    Batch(Arc<Error>),
    /// Indicates this producer has lost exclusive access to the topic. Client can decided whether
    /// to recreate or not
    Fenced,
    NoCapacity(Record),
    MessageTooLarge,
}

impl std::fmt::Display for ProduceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ProduceError::Connection(e) => write!(f, "Connection error: {e}"),
            ProduceError::Io(e) => write!(f, "Compression error: {e}"),
            ProduceError::Custom(e) => write!(f, "Custom error: {e}"),
            ProduceError::Batch(e) => write!(f, "Batch error: {e}"),
            ProduceError::PartialSend(e) => {
                let (successes, failures) = e.iter().fold((0, 0), |(s, f), r| match r {
                    Ok(_) => (s + 1, f),
                    Err(_) => (s, f + 1),
                });
                write!(
                    f,
                    "Partial send error - {successes} successful, {failures} failed"
                )?;

                if failures > 0 {
                    let first_error = e
                        .iter()
                        .find(|r| r.is_err())
                        .unwrap()
                        .as_ref()
                        .map(drop)
                        .unwrap_err();
                    write!(f, "first error: {first_error}")?;
                }
                Ok(())
            }
            ProduceError::Fenced => write!(f, "Producer is fenced"),
            ProduceError::NoCapacity(_) => write!(f, "Record aggregator has no capacity."),
            ProduceError::MessageTooLarge => write!(f, "Message too large."),
        }
    }
}

impl std::fmt::Debug for ProduceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProduceError::Connection(e) => write!(f, "Connection({e})"),
            ProduceError::Custom(msg) => write!(f, "Custom({msg})"),
            ProduceError::Io(e) => write!(f, "Io({e})"),
            ProduceError::Batch(e) => write!(f, "Batch({e})"),
            ProduceError::PartialSend(parts) => {
                write!(f, "PartialSend(")?;
                for (i, part) in parts.iter().enumerate() {
                    match part {
                        Ok(_) => write!(f, "Ok(SendFuture)")?,
                        Err(e) => write!(f, "Err({e})")?,
                    }
                    if i < (parts.len() - 1) {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
            ProduceError::Fenced => write!(f, "Producer is fenced"),
            ProduceError::NoCapacity(_) => write!(f, "Record aggregator has no capacity."),
            ProduceError::MessageTooLarge => write!(f, "Message too large."),
        }
    }
}

impl From<ConnectionError> for ProduceError {
    fn from(err: ConnectionError) -> Self {
        ProduceError::Connection(err)
    }
}

impl From<std::io::Error> for ProduceError {
    fn from(err: std::io::Error) -> Self {
        ProduceError::Io(err)
    }
}

pub enum ConsumeError {
    Connection(ConnectionError),
    Custom(String),
    Io(std::io::Error),
    PartitionAssignorNotAvailable(String),
}

impl std::fmt::Display for ConsumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ConsumeError::Connection(e) => write!(f, "Connection error: {e}"),
            ConsumeError::Io(e) => write!(f, "Decompression error: {e}"),
            ConsumeError::Custom(e) => write!(f, "Custom error: {e}"),
            ConsumeError::PartitionAssignorNotAvailable(name) => {
                write!(f, "PartitionAssignor: {name} not available")
            }
        }
    }
}

impl std::fmt::Debug for ConsumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumeError::Connection(e) => write!(f, "Connection({e})"),
            ConsumeError::Custom(msg) => write!(f, "Custom({msg})"),
            ConsumeError::Io(e) => write!(f, "Io({e})"),
            ConsumeError::PartitionAssignorNotAvailable(name) => {
                write!(f, "PartitionAssignorNotAvailable({name})")
            }
        }
    }
}

impl From<ConnectionError> for ConsumeError {
    fn from(err: ConnectionError) -> Self {
        ConsumeError::Connection(err)
    }
}

impl From<std::io::Error> for ConsumeError {
    fn from(err: std::io::Error) -> Self {
        ConsumeError::Io(err)
    }
}

#[derive(Clone)]
pub struct SharedError {
    error_set: Arc<AtomicBool>,
    error: Arc<Mutex<Option<ConnectionError>>>,
}

impl SharedError {
    pub fn new() -> SharedError {
        SharedError {
            error_set: Arc::new(AtomicBool::new(false)),
            error: Arc::new(Mutex::new(None)),
        }
    }

    pub fn is_set(&self) -> bool {
        self.error_set.load(Ordering::Relaxed)
    }

    pub fn remove(&self) -> Option<ConnectionError> {
        let mut lock = self.error.lock().unwrap();
        let error = lock.take();
        self.error_set.store(false, Ordering::Release);
        error
    }

    pub fn set(&self, error: ConnectionError) {
        let mut lock = self.error.lock().unwrap();
        *lock = Some(error);
        self.error_set.store(true, Ordering::Release);
    }
}

impl Default for SharedError {
    fn default() -> Self {
        Self::new()
    }
}
