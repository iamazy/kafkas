use kafka_protocol::records::Record;

use crate::error::Result;

/// The error returned by [`Aggregator`] implementations
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Return value of [Aggregator::try_push].
#[derive(Debug)]
pub enum TryPush<I, T> {
    /// Insufficient capacity.
    ///
    /// Return [`Input`](Aggregator::Input) back to caller.
    NoCapacity(I),

    /// Aggregated input.
    Aggregated(T),
}

impl<I, T> TryPush<I, T> {
    pub fn unwrap_input(self) -> I {
        match self {
            Self::NoCapacity(input) => input,
            Self::Aggregated(_) => panic!("Aggregated"),
        }
    }

    pub fn unwrap_tag(self) -> T {
        match self {
            Self::NoCapacity(_) => panic!("NoCapacity"),
            Self::Aggregated(tag) => tag,
        }
    }
}

/// A type that receives one or more input and returns a single output
pub trait Aggregator: Send {
    /// The unaggregated input.
    type Input: Send;

    /// Tag used to deaggregate status.
    type Tag: Send + std::fmt::Debug;

    /// Try to append `record` implementations should return
    ///
    /// - `Ok(TryPush::Aggregated(_))` on success
    /// - `Ok(TryPush::NoCapacity(_))` if there is insufficient capacity in the `Aggregator`
    /// - `Err(_)` if an error is encountered
    ///
    /// [`Aggregator`] must only be modified if this method returns `Ok(None)`
    fn try_push(&mut self, record: Self::Input) -> Result<TryPush<Self::Input, Self::Tag>>;

    /// Flush the contents of this aggregator to Kafka
    fn flush(&mut self) -> Result<Vec<Record>>;
}

#[derive(Debug)]
struct AggregatorState {
    batch_size: usize,
    records: Vec<Record>,
}

impl Default for AggregatorState {
    fn default() -> Self {
        Self {
            batch_size: 0,
            records: Vec::with_capacity(1024),
        }
    }
}

/// a [`Aggregator`] that batches up to a certain number of bytes of [`Record`]
#[derive(Debug, Default)]
pub struct RecordAggregator {
    max_batch_size: usize,
    state: AggregatorState,
}

impl Aggregator for RecordAggregator {
    type Input = Record;
    type Tag = usize;

    fn try_push(&mut self, mut record: Self::Input) -> Result<TryPush<Self::Input, Self::Tag>> {
        let record_size: usize = record_size(&record);

        if self.state.batch_size + record_size > self.max_batch_size {
            return Ok(TryPush::NoCapacity(record));
        }

        let tag = self.state.records.len();

        record.offset = tag as i64;
        record.sequence = tag as i32;

        self.state.batch_size += record_size;
        self.state.records.push(record);
        Ok(TryPush::Aggregated(tag))
    }

    fn flush(&mut self) -> Result<Vec<Record>> {
        let state = std::mem::take(&mut self.state);
        self.state.batch_size = 0;
        Ok(state.records)
    }
}

impl RecordAggregator {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            max_batch_size,
            ..Default::default()
        }
    }
}

fn record_size(record: &Record) -> usize {
    let mut size = 0;
    if let Some(ref key) = record.key {
        size += key.len();
    }
    if let Some(ref value) = record.value {
        size += value.len();
    }
    for (k, v) in record.headers.iter() {
        size += k.len();
        if let Some(ref v) = v {
            size += v.len();
        }
    }
    size
}
