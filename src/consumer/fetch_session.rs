use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{
        fetch_request::FetchPartition, fetch_response::PartitionData, FetchResponse, TopicName,
    },
    ResponseError,
};
use tracing::{debug, info};
use uuid::Uuid;

use crate::{
    consumer::{fetcher::FetchMetadata, INITIAL_EPOCH, INVALID_SESSION_ID},
    metadata::TopicPartition,
    NodeId,
};

#[derive(Debug, Clone)]
pub struct FetchSession {
    pub node: NodeId,
    pub next_metadata: FetchMetadata,
    pub session_topic_names: HashMap<Uuid, TopicName>,
    pub session_partitions: HashMap<TopicPartition, FetchPartition>,
}

impl FetchSession {
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            next_metadata: FetchMetadata::new(INVALID_SESSION_ID, INITIAL_EPOCH),
            session_topic_names: HashMap::new(),
            session_partitions: HashMap::new(),
        }
    }

    pub fn add_topic(&mut self, topic_id: Uuid, topic: TopicName) {
        if topic_id != Uuid::nil() && !topic.is_empty() {
            self.session_topic_names.insert(topic_id, topic);
        }
    }

    fn fetch_response_partitions(
        &self,
        response: &FetchResponse,
        version: i16,
    ) -> HashSet<TopicPartition> {
        let mut partitions = HashSet::new();
        for response in response.responses.iter() {
            let name = if version < 13 {
                Some(&response.topic)
            } else {
                self.session_topic_names.get(&response.topic_id)
            };
            if let Some(name) = name {
                for partition in response.partitions.iter() {
                    partitions.insert(TopicPartition {
                        topic: name.clone(),
                        partition: partition.partition_index,
                    });
                }
            }
        }
        partitions
    }

    fn fetch_response_partitions_data(
        &self,
        response: FetchResponse,
        version: i16,
    ) -> HashMap<TopicPartition, PartitionData> {
        let mut partitions = HashMap::new();
        for response in response.responses {
            let name = if version < 13 {
                Some(&response.topic)
            } else {
                self.session_topic_names.get(&response.topic_id)
            };
            if let Some(name) = name {
                for partition in response.partitions {
                    partitions.insert(
                        TopicPartition {
                            topic: name.clone(),
                            partition: partition.partition_index,
                        },
                        partition,
                    );
                }
            }
        }
        partitions
    }

    fn find_missing<'a, T: Eq + Hash>(
        to_find: &HashSet<&'a T>,
        to_search: &HashSet<&'a T>,
    ) -> HashSet<&'a T> {
        let mut ret = HashSet::new();
        for find in to_find {
            if !to_search.contains(*find) {
                ret.insert(*find);
            }
        }
        ret
    }

    fn verify_full_fetched_partitions(
        &self,
        partitions: HashSet<&TopicPartition>,
        topic_ids: HashSet<&Uuid>,
        version: i16,
    ) -> Option<String> {
        let session_partitions: HashSet<&TopicPartition> = self.session_partitions.keys().collect();
        let extra = Self::find_missing(&partitions, &session_partitions);
        let omitted = Self::find_missing(&session_partitions, &partitions);
        let mut extra_ids: HashSet<&Uuid> = HashSet::new();

        let mut problem = String::new();
        if version >= 13 {
            let session_topic_names = self.session_topic_names.keys().collect();
            extra_ids.extend(Self::find_missing(&topic_ids, &session_topic_names));
        }

        if !omitted.is_empty() {
            problem.extend(format!("omitted partitions=({omitted:?}),").chars());
        }
        if !extra.is_empty() {
            problem.extend(format!("extra partitions=({extra:?}),").chars());
        }
        if !extra_ids.is_empty() {
            problem.extend(format!("extra ids=({extra_ids:?}),").chars());
        }
        if !omitted.is_empty() || !extra.is_empty() || !extra_ids.is_empty() {
            problem.extend(format!("response=({partitions:?})").chars());
            return Some(problem);
        }
        None
    }

    fn verify_incremental_fetched_partitions(
        &self,
        partitions: HashSet<&TopicPartition>,
        topic_ids: HashSet<&Uuid>,
        version: i16,
    ) -> Option<String> {
        let mut extra_ids: HashSet<&Uuid> = HashSet::new();

        if version >= 13 {
            let session_topic_names = self.session_topic_names.keys().collect();
            extra_ids.extend(Self::find_missing(&topic_ids, &session_topic_names));
        }

        let session_partitions: HashSet<&TopicPartition> = self.session_partitions.keys().collect();
        let extra = Self::find_missing(&partitions, &session_partitions);

        let mut problem = String::new();
        if !extra.is_empty() {
            problem.extend(format!("extra partitions=({extra:?}),").chars());
        }
        if !extra_ids.is_empty() {
            problem.extend(format!("extra ids=({extra_ids:?}),").chars());
        }
        if !extra.is_empty() || !extra_ids.is_empty() {
            problem.extend(format!("response=({partitions:?})").chars());
            return Some(problem);
        }
        None
    }

    pub(crate) fn handle_fetch_response(&mut self, response: &FetchResponse, version: i16) -> bool {
        if response.error_code.is_err() {
            let error = response.error_code.err().unwrap();
            info!(
                "Node {} was unable to process the fetch request with {}:{}.",
                self.node, self.next_metadata, error
            );
            if error == ResponseError::FetchSessionIdNotFound {
                self.next_metadata = FetchMetadata::initial();
            } else {
                self.next_metadata.next_close_existing();
            }
            return false;
        }
        let partitions = self.fetch_response_partitions(response, version);
        let topic_ids = response
            .responses
            .iter()
            .map(|topic| &topic.topic_id)
            .filter(|id| **id != Uuid::nil())
            .collect();
        return if self.next_metadata.is_full() {
            if partitions.is_empty() && response.throttle_time_ms > 0 {
                // Normally, an empty full fetch response would be invalid. However, KIP-219
                // specifies that if the broker wants to throttle the client, it will respond
                // to a full fetch request with an empty response and a throttleTimeMs
                // value set.  We don't want to log this with a warning, since it's not an error.
                // However, the empty full fetch response can't be processed, so it's still
                // appropriate to return false here.
                debug!(
                    "Node {} sent a empty full fetch response to indicate that this client should \
                     be throttled for {} ms.",
                    self.node, response.throttle_time_ms
                );
                self.next_metadata = FetchMetadata::initial();
                return false;
            }

            if let Some(problem) =
                self.verify_full_fetched_partitions(partitions.iter().collect(), topic_ids, version)
            {
                info!(
                    "Node {} sent an invalid full fetch response with {problem}",
                    self.node
                );
                self.next_metadata = FetchMetadata::initial();
                false
            } else if response.session_id == INVALID_SESSION_ID {
                debug!("Node {} sent a full fetch response", self.node);
                self.next_metadata = FetchMetadata::initial();
                true
            } else {
                debug!(
                    "Node {} sent a full fetch response that created a new incremental fetch \
                     session {}",
                    self.node, response.session_id
                );
                self.next_metadata = FetchMetadata::new_incremental(response.session_id);
                true
            }
        } else if let Some(problem) = self.verify_incremental_fetched_partitions(
            partitions.iter().collect(),
            topic_ids,
            version,
        ) {
            info!(
                "Node {} sent an invalid incremental fetch response with {problem}",
                self.node
            );
            self.next_metadata.next_close_existing();
            false
        } else if response.session_id == INVALID_SESSION_ID {
            debug!(
                "Node {} sent an incremental fetch response closing session {}",
                self.node, self.next_metadata.session_id
            );
            self.next_metadata = FetchMetadata::initial();
            true
        } else {
            debug!(
                "Node {} sent an incremental fetch response with throttleTimeMs = {} for session \
                 {}",
                self.node, response.throttle_time_ms, response.session_id
            );
            self.next_metadata.next_incremental();
            true
        };
    }
}
