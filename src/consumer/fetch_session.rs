use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    hash::Hash,
};

use indexmap::IndexMap;
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{fetch_response::PartitionData, FetchResponse, TopicName},
    ResponseError,
};
use tracing::{debug, info};
use uuid::Uuid;

use crate::{
    array_display,
    metadata::{TopicIdPartition, TopicPartition},
    NodeId,
};

/// The first epoch.  When used in a fetch request, indicates that the client
///   wants to create or recreate a session.
const INITIAL_EPOCH: i32 = 0;

/// An invalid epoch.  When used in a fetch request, indicates that the client
/// wants to close any existing session, and not create a new one.
const FINAL_EPOCH: i32 = -1;

/// The session ID used by clients with no session.
const INVALID_SESSION_ID: i32 = 0;

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
            problem
                .extend(format!("omitted partitions=({}),", array_display(omitted.iter())).chars());
        }
        if !extra.is_empty() {
            problem.extend(format!("extra partitions=({}),", array_display(extra.iter())).chars());
        }
        if !extra_ids.is_empty() {
            problem.extend(format!("extra ids=({}),", array_display(extra_ids.iter())).chars());
        }
        if !omitted.is_empty() || !extra.is_empty() || !extra_ids.is_empty() {
            problem.extend(format!("response=({})", array_display(partitions.iter())).chars());
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
            problem.extend(format!("extra partitions=({}),", array_display(extra.iter())).chars());
        }
        if !extra_ids.is_empty() {
            problem.extend(format!("extra ids=({}),", array_display(extra_ids.iter())).chars());
        }
        if !extra.is_empty() || !extra_ids.is_empty() {
            problem.extend(format!("response=({})", array_display(partitions.iter())).chars());
            return Some(problem);
        }
        None
    }

    pub(crate) fn handle_fetch_response(&mut self, response: &FetchResponse, version: i16) -> bool {
        if response.error_code.is_err() {
            let error = response.error_code.err().unwrap();
            info!(
                "Node {} was unable to process the fetch request with {}: {error}.",
                self.node, self.next_metadata
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

#[derive(Debug, Clone, Copy, Hash)]
pub struct FetchMetadata {
    pub session_id: i32,
    pub epoch: i32,
}

impl Display for FetchMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.session_id == INVALID_SESSION_ID {
            write!(f, "(session_id=INVALID, ")?;
        } else {
            write!(f, "(session_id={}, ", self.session_id)?;
        }

        if self.epoch == INITIAL_EPOCH {
            write!(f, "epoch=INITIAL)")
        } else if self.epoch == FINAL_EPOCH {
            write!(f, "epoch=FINAL)")
        } else {
            write!(f, "epoch={})", self.epoch)
        }
    }
}

impl FetchMetadata {
    pub fn new(session_id: i32, epoch: i32) -> Self {
        Self { session_id, epoch }
    }

    pub fn initial() -> Self {
        Self::new(INVALID_SESSION_ID, INITIAL_EPOCH)
    }

    /// Returns true if this is a full fetch request.
    pub fn is_full(&self) -> bool {
        self.epoch == INITIAL_EPOCH || self.epoch == FINAL_EPOCH
    }

    /// Return the metadata for the next full fetch request.
    pub fn new_incremental(session_id: i32) -> Self {
        Self::new(session_id, next_epoch(INITIAL_EPOCH))
    }

    /// Return the metadata for the next incremental response.
    pub fn next_incremental(&mut self) {
        self.epoch = next_epoch(self.epoch);
    }

    /// Return the metadata for the next error response.
    pub fn next_close_existing(&mut self) {
        self.epoch = INITIAL_EPOCH;
    }
}

fn next_epoch(prev_epoch: i32) -> i32 {
    if prev_epoch < 0 {
        FINAL_EPOCH
    } else if prev_epoch == i32::MAX {
        1
    } else {
        prev_epoch + 1
    }
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
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
    next: IndexMap<TopicPartition, FetchRequestPartitionData>,
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
            session.session_partitions.extend(self.next.drain(..));

            session.session_topic_names.clear();
            if can_use_topic_ids {
                session.session_topic_names.extend(self.topic_names.drain());
            }

            let mut to_send = HashMap::with_capacity(session.session_partitions.len());
            for (tp, data) in session.session_partitions.iter() {
                to_send.insert(tp.clone(), *data);
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
            match self.next.remove(tp) {
                Some(next_data) => {
                    // We basically check if the new partition had the same topic ID. If not,
                    // we add it to the "replaced" set. If the request is version 13 or higher, the
                    // replaced partition will be forgotten. In any case, we will send the new
                    // partition in the request.
                    if prev_data.topic_id != next_data.topic_id
                        && !prev_data.topic_id.is_nil()
                        && !next_data.topic_id.is_nil()
                    {
                        let topic_id = prev_data.topic_id;
                        prev_data.copied_from(&next_data);
                        // Re-add the replaced partition to the end of 'next'
                        self.next.insert(tp.clone(), next_data);
                        replaced.push(TopicIdPartition {
                            topic_id,
                            partition: tp.clone(),
                        });
                    } else if prev_data != &next_data {
                        let topic_id = next_data.topic_id;
                        prev_data.copied_from(&next_data);
                        // Re-add the altered partition to the end of 'next'
                        self.next.insert(tp.clone(), next_data);
                        altered.push(TopicIdPartition {
                            topic_id,
                            partition: tp.clone(),
                        });
                    }
                }
                None => {
                    // Remove this partition from the session.
                    session_remove.push(tp.clone());
                    // Indicate that we no longer want to listen to this partition.
                    removed.push(TopicIdPartition {
                        topic_id: prev_data.topic_id,
                        partition: tp.clone(),
                    });
                    // If we do not have this topic ID in the builder or the session, we can not use
                    // topic IDs.
                    if can_use_topic_ids && prev_data.topic_id.is_nil() {
                        can_use_topic_ids = false;
                    }
                }
            }
        }

        for tp in session_remove.iter() {
            session.session_partitions.remove(tp);
        }

        // Add any new partitions to the session.
        for (tp, next_data) in self.next.iter() {
            if session.session_partitions.contains_key(tp) {
                // In the previous loop, all the partitions which existed in both sessionPartitions
                // and next were moved to the end of next, or removed from next.  Therefore,
                // once we hit one of them, we know there are no more unseen entries to look
                // at in next.
                break;
            }
            let topic_id = next_data.topic_id;
            session.session_partitions.insert(tp.clone(), *next_data);
            added.push(TopicIdPartition {
                topic_id,
                partition: tp.clone(),
            });
        }

        session.session_topic_names.clear();

        // Add topic IDs to session if we can use them. If an ID is inconsistent, we will handle in
        // the receiving broker. If we switched from using topic IDs to not using them (or
        // vice versa), that error will also be handled in the receiving broker.
        if can_use_topic_ids {
            session.session_topic_names.extend(self.topic_names.clone());
        }

        let mut to_send = HashMap::with_capacity(self.next.len());
        to_send.extend(self.next.drain(..));

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
