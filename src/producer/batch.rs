use futures::channel::oneshot::{channel, Sender};
use kafka_protocol::{
    error::ParseResponseErrorCode, messages::produce_response::PartitionProduceResponse,
    records::Record,
};

use super::{
    aggregator::{Aggregator, RecordAggregator, TryPush},
    RecordMetadata,
};
use crate::{error::{Error, ProduceError, Result}, PartitionId, producer::SendFuture};

#[derive(Debug)]
pub struct Thunk {
    sender: Sender<Result<RecordMetadata>>,
    relative_offset: i64,
    timestamp: i64,
    key_size: usize,
    value_size: usize,
}

impl Thunk {
    pub fn fail(self, err: Error) -> std::result::Result<(), Result<RecordMetadata>> {
        self.sender.send(Err(err))
    }

    pub fn done(
        self,
        res: &PartitionProduceResponse,
    ) -> std::result::Result<(), Result<RecordMetadata>> {
        let result = if res.error_code.is_ok() {
            Ok(RecordMetadata {
                partition: res.index,
                offset: res.base_offset + self.relative_offset,
                timestamp: self.timestamp,
                key_size: self.key_size,
                value_size: self.value_size,
            })
        } else if let Some(ref err) = res.error_message {
            Err(ProduceError::Custom(err.to_string()).into())
        } else if let Some(err) = res.error_code.err() {
            Err(ProduceError::Custom(err.to_string()).into())
        } else {
            Err(ProduceError::Custom("unknown error".to_string()).into())
        };
        self.sender.send(result)
    }
}

#[derive(Debug)]
pub struct ProducerBatch {
    partition: PartitionId,
    aggregator: RecordAggregator,
    thunks: Vec<Thunk>,
}

impl ProducerBatch {
    pub fn new(max_batch_size: usize, partition: PartitionId) -> Self {
        Self {
            partition,
            aggregator: RecordAggregator::new(max_batch_size),
            thunks: Vec::with_capacity(1024),
        }
    }

    pub fn try_push(&mut self, record: Record) -> Result<SendFuture> {
        let key_size = record.key.as_ref().map_or(0, |key| key.len());
        let value_size = record.value.as_ref().map_or(0, |value| value.len());
        let timestamp = record.timestamp;
        let relative_offset = match self.aggregator.try_push(record)? {
            TryPush::Aggregated(size) => size as i64,
            TryPush::NoCapacity(record) => {
                return Err(ProduceError::NoCapacity((self.partition, record)).into());
            }
        };
        let (sender, receiver) = channel();
        self.thunks.push(Thunk {
            sender,
            relative_offset,
            timestamp,
            key_size,
            value_size,
        });
        Ok(SendFuture(receiver))
    }

    pub fn flush(&mut self) -> Result<(Vec<Thunk>, Vec<Record>)> {
        let records = self.aggregator.flush()?;
        let thunks = std::mem::replace(&mut self.thunks, Vec::with_capacity(1024));
        Ok((thunks, records))
    }
}
