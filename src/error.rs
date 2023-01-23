use std::{
    string::FromUtf8Error,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, PoisonError,
    },
};

use futures::channel::mpsc::SendError;
use kafka_protocol::{
    messages::{ApiKey, TopicName},
    protocol::{buf::NotEnoughBytesError, DecodeError, EncodeError},
    records::Record,
    ResponseError,
};

use crate::{producer::SendFuture, NodeId, PartitionId};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Custom(String),
    Connection(ConnectionError),
    InvalidVersion(i16),
    InvalidApiRequest(ApiKey),
    TopicNotAvailable { topic: TopicName },
    TopicAuthorizationError { topics: Vec<TopicName> },
    PartitionNotAvailable { topic: TopicName, partition: i32 },
    NodeNotAvailable { node: NodeId },
    Produce(ProduceError),
    Consume(ConsumeError),
}

impl From<ResponseError> for Error {
    fn from(value: ResponseError) -> Self {
        Error::Custom(value.to_string())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(value: FromUtf8Error) -> Self {
        Error::Custom(value.to_string())
    }
}

impl From<FromUtf8Error> for Box<Error> {
    fn from(value: FromUtf8Error) -> Self {
        Box::new(Error::Custom(value.to_string()))
    }
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

impl From<SendError> for Error {
    fn from(value: SendError) -> Self {
        Error::Custom(value.to_string())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Custom(e) => write!(f, "{e}"),
            Error::Connection(e) => write!(f, "Connection error: {e}"),
            Error::InvalidVersion(v) => write!(f, "Invalid version: {v}"),
            Error::InvalidApiRequest(v) => write!(f, "Invalid Api Request: {v:?}"),
            Error::Produce(e) => write!(f, "Produce error: {e}"),
            Error::Consume(e) => write!(f, "Consume error: {e}"),
            Error::PartitionNotAvailable { topic, partition } => {
                write!(f, "Partition {partition} not available, topic: {topic:?}")
            }
            Error::TopicNotAvailable { topic } => {
                write!(f, "Topic not available, topic: {topic:?}")
            }
            Error::TopicAuthorizationError { topics } => {
                write!(f, "Topic Authorization Error, topics: {topics:?}")
            }
            Error::NodeNotAvailable { node } => {
                write!(f, "Node not available, node: {node:?}")
            }
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

impl From<NotEnoughBytesError> for ConnectionError {
    fn from(_: NotEnoughBytesError) -> Self {
        Self::Decoding("Not enough bytes remaining".into())
    }
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
    NoCapacity((PartitionId, Record)),
    MessageTooLarge,
}

impl std::fmt::Display for ProduceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ProduceError::Connection(e) => write!(f, "Connection error: {e}"),
            ProduceError::Io(e) => write!(f, "Compression error: {e}"),
            ProduceError::Custom(e) => write!(f, "{e}"),
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
    CoordinatorNotAvailable,
    PartitionAssignorNotAvailable(String),
}

impl std::fmt::Display for ConsumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ConsumeError::Connection(e) => write!(f, "Connection error: {e}"),
            ConsumeError::Io(e) => write!(f, "Decompression error: {e}"),
            ConsumeError::Custom(e) => write!(f, "{e}"),
            ConsumeError::CoordinatorNotAvailable => write!(f, "Coordinator not available"),
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
            ConsumeError::CoordinatorNotAvailable => write!(f, "CoordinatorNotAvailable"),
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
