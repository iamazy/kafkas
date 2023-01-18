use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{fetch_response::PartitionData, FetchResponse, TopicName},
    ResponseError,
};
use tracing::{debug, info};
use uuid::Uuid;

use crate::{
    consumer::{fetcher::FetchMetadata, INITIAL_EPOCH, INVALID_SESSION_ID},
    metadata::{TopicIdPartition, TopicPartition},
    NodeId,
};

#[derive(Debug, Clone)]
pub struct FetchSession {
    pub node: NodeId,
    pub next_metadata: FetchMetadata,
    pub session_topic_names: HashMap<Uuid, TopicName>,
    pub session_partitions: HashMap<TopicPartition, FetchRequestPartitionData>,
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

    fn verify_full_fetched_partitions(
        &self,
        partitions: HashSet<&TopicPartition>,
        topic_ids: HashSet<&Uuid>,
        version: i16,
    ) -> Option<String> {
        let session_partitions: HashSet<&TopicPartition> = self.session_partitions.keys().collect();
        let extra = find_missing(&partitions, &session_partitions);
        let omitted = find_missing(&session_partitions, &partitions);
        let mut extra_ids: HashSet<&Uuid> = HashSet::new();

        let mut problem = String::new();
        if version >= 13 {
            let session_topic_names = self.session_topic_names.keys().collect();
            extra_ids.extend(find_missing(&topic_ids, &session_topic_names));
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
            extra_ids.extend(find_missing(&topic_ids, &session_topic_names));
        }

        let session_partitions: HashSet<&TopicPartition> = self.session_partitions.keys().collect();
        let extra = find_missing(&partitions, &session_partitions);

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

#[derive(Debug)]
pub struct FetchRequestData {
    pub(crate) to_send: HashMap<TopicPartition, FetchRequestPartitionData>,
    pub(crate) to_forget: Vec<TopicIdPartition>,
    pub(crate) to_replace: Vec<TopicIdPartition>,
    pub(crate) session_partitions: HashMap<TopicPartition, FetchRequestPartitionData>,
    pub(crate) metadata: FetchMetadata,
    pub(crate) can_use_topic_ids: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchRequestPartitionData {
    pub(crate) topic_id: Uuid,
    pub(crate) fetch_offset: i64,
    pub(crate) log_start_offset: i64,
    pub(crate) max_bytes: i32,
    pub(crate) current_leader_epoch: Option<i32>,
    pub(crate) last_fetched_epoch: Option<i32>,
}

impl FetchRequestPartitionData {
    pub fn copied_from(&mut self, data: &FetchRequestPartitionData) {
        self.topic_id = data.topic_id;
        self.fetch_offset = data.fetch_offset;
        self.log_start_offset = data.log_start_offset;
        self.max_bytes = data.max_bytes;
        self.current_leader_epoch = data.current_leader_epoch;
        self.last_fetched_epoch = data.last_fetched_epoch;
    }
}

#[derive(Debug, Default)]
pub struct FetchRequestDataBuilder {
    next: HashMap<TopicPartition, FetchRequestPartitionData>,
    topic_names: HashMap<Uuid, TopicName>,
    partitions_without_topic_ids: usize,
    copy_session_partitions: bool,
}

impl FetchRequestDataBuilder {
    pub fn new() -> Self {
        Self {
            copy_session_partitions: true,
            ..Default::default()
        }
    }

    /// Mark that we want data from this partition in the upcoming fetch.
    pub fn add(&mut self, tp: TopicPartition, data: FetchRequestPartitionData) {
        if data.topic_id == Uuid::nil() {
            self.partitions_without_topic_ids += 1;
        } else {
            self.topic_names.insert(data.topic_id, tp.topic.clone());
        }
        self.next.insert(tp, data);
    }

    pub fn build(&mut self, session: &mut FetchSession) -> FetchRequestData {
        let mut can_use_topic_ids = self.partitions_without_topic_ids == 0;
        if session.next_metadata.is_full() {
            debug!(
                "Built full fetch {} for node {}",
                session.next_metadata, session.node
            );
            session.session_partitions.clear();
            session.session_partitions.extend(self.next.drain());

            session.session_topic_names.clear();
            if can_use_topic_ids {
                session.session_topic_names.extend(self.topic_names.drain());
            }

            let mut to_send = HashMap::with_capacity(session.session_partitions.len());
            for (tp, data) in session.session_partitions.iter() {
                to_send.insert(tp.clone(), data.clone());
            }
            return FetchRequestData {
                to_send: to_send.clone(),
                to_forget: Vec::new(),
                to_replace: Vec::new(),
                session_partitions: to_send,
                metadata: session.next_metadata,
                can_use_topic_ids,
            };
        }

        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut altered = Vec::new();
        let mut replaced = Vec::new();

        let mut session_remove = Vec::new();
        for (tp, prev_data) in session.session_partitions.iter_mut() {
            if let Some(next_data) = self.next.remove(tp) {
                if prev_data.topic_id != next_data.topic_id
                    && !prev_data.topic_id.is_nil()
                    && !next_data.topic_id.is_nil()
                {
                    let topic_id = prev_data.topic_id;
                    prev_data.copied_from(&next_data);
                    self.next.insert(tp.clone(), next_data);
                    replaced.push(TopicIdPartition {
                        topic_id,
                        partition: tp.clone(),
                    });
                } else if prev_data != &next_data {
                    let topic_id = next_data.topic_id;
                    prev_data.copied_from(&next_data);
                    self.next.insert(tp.clone(), next_data);
                    altered.push(TopicIdPartition {
                        topic_id,
                        partition: tp.clone(),
                    });
                }
            } else {
                session_remove.push(tp.clone());
                removed.push(TopicIdPartition {
                    topic_id: prev_data.topic_id,
                    partition: tp.clone(),
                });
                if can_use_topic_ids && prev_data.topic_id.is_nil() {
                    can_use_topic_ids = false;
                }
            }
        }

        for tp in session_remove.iter() {
            session.session_partitions.remove(tp);
        }

        for (tp, next_data) in self.next.iter() {
            if session.session_partitions.contains_key(tp) {
                break;
            }
            let topic_id = next_data.topic_id;
            session
                .session_partitions
                .insert(tp.clone(), next_data.clone());
            added.push(TopicIdPartition {
                topic_id,
                partition: tp.clone(),
            });
        }

        session.session_topic_names.clear();
        if can_use_topic_ids {
            session.session_topic_names.extend(self.topic_names.drain());
        }

        let mut to_send = HashMap::with_capacity(self.next.len());
        to_send.extend(self.next.drain());

        FetchRequestData {
            to_send,
            to_forget: removed,
            to_replace: replaced,
            session_partitions: session.session_partitions.clone(),
            metadata: session.next_metadata,
            can_use_topic_ids,
        }
    }
}
