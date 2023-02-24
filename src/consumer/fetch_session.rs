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
    pub session_partitions: IndexMap<TopicPartition, FetchRequestPartitionData>,
}

impl FetchSession {
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            next_metadata: FetchMetadata::new(INVALID_SESSION_ID, INITIAL_EPOCH),
            session_topic_names: HashMap::new(),
            session_partitions: IndexMap::new(),
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
    pub(crate) to_send: IndexMap<TopicPartition, FetchRequestPartitionData>,
    pub(crate) to_forget: Vec<TopicIdPartition>,
    pub(crate) to_replace: Vec<TopicIdPartition>,
    pub(crate) session_partitions: IndexMap<TopicPartition, FetchRequestPartitionData>,
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

            let mut to_send = IndexMap::with_capacity(session.session_partitions.len());
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

        let mut to_send = IndexMap::with_capacity(self.next.len());
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use indexmap::IndexMap;
    use kafka_protocol::{
        messages::{
            fetch_response::{FetchableTopicResponse, PartitionData},
            FetchRequest, FetchResponse,
        },
        protocol::{Message, StrBytes},
    };
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    use crate::{
        consumer::fetch_session::{
            FetchRequestDataBuilder, FetchRequestPartitionData, FetchSession, INITIAL_EPOCH,
            INVALID_SESSION_ID,
        },
        metadata::TopicIdPartition,
        TopicPartition,
    };

    struct ReqEntry {
        part: TopicPartition,
        data: FetchRequestPartitionData,
    }

    impl ReqEntry {
        fn new(
            topic: StrBytes,
            topic_id: Uuid,
            partition: i32,
            fetch_offset: i64,
            log_start_offset: i64,
            max_bytes: i32,
        ) -> Self {
            let tp = TopicPartition {
                topic: topic.into(),
                partition,
            };
            let data = FetchRequestPartitionData {
                topic_id,
                fetch_offset,
                log_start_offset,
                max_bytes,
                last_fetched_epoch: None,
                current_leader_epoch: None,
            };
            Self { part: tp, data }
        }
    }

    struct RespEntry {
        part: TopicIdPartition,
        data: PartitionData,
    }

    impl RespEntry {
        fn new(
            topic: StrBytes,
            partition: i32,
            topic_id: Uuid,
            high_water_mark: i64,
            last_stable_offset: i64,
        ) -> Self {
            let topic_id_partition = TopicIdPartition {
                topic_id,
                partition: TopicPartition {
                    topic: topic.into(),
                    partition,
                },
            };

            let mut data = PartitionData::default();
            data.partition_index = partition;
            data.high_watermark = high_water_mark;
            data.last_stable_offset = last_stable_offset;
            data.log_start_offset = 0;

            Self {
                part: topic_id_partition,
                data,
            }
        }
    }

    fn req_map(entries: Vec<ReqEntry>) -> IndexMap<TopicPartition, FetchRequestPartitionData> {
        let mut map = IndexMap::with_capacity(entries.len());
        for entry in entries {
            map.insert(entry.part, entry.data);
        }
        map
    }

    fn resp_map(entries: Vec<RespEntry>) -> IndexMap<TopicIdPartition, PartitionData> {
        let mut map = IndexMap::with_capacity(entries.len());
        for entry in entries {
            map.insert(entry.part, entry.data);
        }
        map
    }

    fn matching_topic(
        prev_topic: &Option<&mut FetchableTopicResponse>,
        cur_topic: &TopicIdPartition,
    ) -> bool {
        if let Some(prev_topic) = prev_topic {
            return if prev_topic.topic_id != Uuid::nil() {
                prev_topic.topic_id == cur_topic.topic_id
            } else {
                prev_topic.topic == cur_topic.partition.topic
            };
        }
        false
    }

    fn to_fetch_response(
        error: i16,
        throttle_time_ms: i32,
        session_id: i32,
        map: IndexMap<TopicIdPartition, PartitionData>,
    ) -> FetchResponse {
        let mut topic_responses = Vec::new();
        for (tp, mut data) in map {
            // Since PartitionData alone doesn't know the partition ID, we set it here
            data.partition_index = tp.partition.partition;
            // We have to keep the order of input topic-partition. Hence, we batch the partitions
            // only if the last batch is in the same topic group.

            let prev_topic = if topic_responses.is_empty() {
                None
            } else {
                topic_responses.last_mut()
            };

            if matching_topic(&prev_topic, &tp) {
                if let Some(prev_topic) = prev_topic {
                    prev_topic.partitions.push(data);
                }
            } else {
                let mut partition_data = FetchableTopicResponse::default();
                partition_data.topic = tp.partition.topic.clone();
                partition_data.topic_id = tp.topic_id;
                partition_data.partitions = vec![data];

                topic_responses.push(partition_data);
            }
        }

        let mut response = FetchResponse::default();
        response.throttle_time_ms = throttle_time_ms;
        response.error_code = error;
        response.session_id = session_id;
        response.responses = topic_responses;

        response
    }

    fn assert_map_equals(
        expected: &IndexMap<TopicPartition, FetchRequestPartitionData>,
        actual: &IndexMap<TopicPartition, FetchRequestPartitionData>,
    ) {
        let mut i = 1;

        let mut expected_iter = expected.iter();
        let mut actual_iter = actual.iter();

        loop {
            let expected_entry = expected_iter.next();
            if expected_entry.is_none() {
                break;
            }

            let actual_entry = actual_iter.next();
            if actual_entry.is_none() {
                panic!("Element {} not found.", i);
            }

            assert_eq!(
                expected_entry.unwrap().0,
                actual_entry.unwrap().0,
                "Element {} had a different TopicPartition than expected.",
                i
            );
            assert_eq!(
                expected_entry.unwrap().1,
                actual_entry.unwrap().1,
                "Element {} had a different PartitionData than expected.",
                i
            );

            i += 1;
        }

        if actual_iter.next().is_some() {
            panic!("Unexpected element {} found.", i);
        }
    }

    fn assert_maps_equals(
        expected: &IndexMap<TopicPartition, FetchRequestPartitionData>,
        actuals: Vec<&IndexMap<TopicPartition, FetchRequestPartitionData>>,
    ) {
        for actual in actuals.iter() {
            assert_map_equals(expected, actual);
        }
    }

    fn assert_list_equals(expected: &[TopicIdPartition], actual: &[TopicIdPartition]) {
        for expected_part in expected.iter() {
            if !actual.contains(expected_part) {
                panic!("Failed to find expected partition {expected_part:?}");
            }
        }

        for actual_part in actual.iter() {
            if !expected.contains(actual_part) {
                panic!("Found unexpected partition {actual_part:?}");
            }
        }
    }

    fn add_topic_id(
        topic_ids: &mut HashMap<StrBytes, Uuid>,
        topic_names: &mut HashMap<Uuid, StrBytes>,
        name: StrBytes,
        version: i16,
    ) {
        if version >= 13 {
            let uuid = Uuid::from_u128(rand::random::<u128>());
            topic_ids.insert(name.clone(), uuid);
            topic_names.insert(uuid, name);
        }
    }

    /// Test the handling of SESSIONLESS responses.
    /// Pre-KIP-227 brokers always supply this kind of response.
    #[test]
    fn test_session_less() {
        let mut topic_ids = HashMap::new();
        let mut topic_names = HashMap::new();

        // We want to test both on older versions that do not use topic IDs and on newer versions
        // that do.
        let versions = vec![12i16, FetchRequest::VERSIONS.max];

        for version in versions {
            let mut fetch_session = FetchSession::new(1);
            let mut fetch_session_builder = FetchRequestDataBuilder::new();

            add_topic_id(
                &mut topic_ids,
                &mut topic_names,
                StrBytes::from_str("foo"),
                version,
            );
            let foo_id = match topic_ids.get("foo") {
                Some(id) => *id,
                None => Uuid::nil(),
            };

            fetch_session_builder.add(
                TopicPartition::new("foo", 0),
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 0,
                    log_start_offset: 100,
                    max_bytes: 200,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );
            fetch_session_builder.add(
                TopicPartition::new("foo", 1),
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 10,
                    log_start_offset: 110,
                    max_bytes: 210,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );

            let data = fetch_session_builder.build(&mut fetch_session);

            assert_maps_equals(
                &req_map(vec![
                    ReqEntry::new(StrBytes::from_str("foo"), foo_id, 0, 0, 100, 200),
                    ReqEntry::new(StrBytes::from_str("foo"), foo_id, 1, 10, 110, 210),
                ]),
                vec![&data.to_send, &data.session_partitions],
            );

            assert_eq!(INVALID_SESSION_ID, data.metadata.session_id);
            assert_eq!(INITIAL_EPOCH, data.metadata.epoch);

            let resp_map = resp_map(vec![
                RespEntry::new(StrBytes::from_str("foo"), 0, foo_id, 0, 0),
                RespEntry::new(StrBytes::from_str("foo"), 1, foo_id, 0, 0),
            ]);
            let resp = to_fetch_response(0, 0, INVALID_SESSION_ID, resp_map);

            fetch_session.handle_fetch_response(&resp, version);

            let mut fetch_session_builder = FetchRequestDataBuilder::new();

            fetch_session_builder.add(
                TopicPartition::new("foo", 0),
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 0,
                    log_start_offset: 100,
                    max_bytes: 200,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );

            let data = fetch_session_builder.build(&mut fetch_session);
            assert_eq!(INVALID_SESSION_ID, data.metadata.session_id);
            assert_eq!(INITIAL_EPOCH, data.metadata.epoch);

            assert_maps_equals(
                &req_map(vec![ReqEntry::new(
                    StrBytes::from_str("foo"),
                    foo_id,
                    0,
                    0,
                    100,
                    200,
                )]),
                vec![&data.to_send, &data.session_partitions],
            );
        }
    }

    /// Test handling an incremental fetch session.
    #[test]
    fn test_incremental() {
        let mut topic_ids = HashMap::new();
        let mut topic_names = HashMap::new();

        // We want to test both on older versions that do not use topic IDs and on newer versions
        // that do.
        let versions = vec![12i16, FetchRequest::VERSIONS.max];

        for version in versions {
            let mut fetch_session = FetchSession::new(1);
            let mut fetch_session_builder = FetchRequestDataBuilder::new();

            add_topic_id(
                &mut topic_ids,
                &mut topic_names,
                StrBytes::from_str("foo"),
                version,
            );
            let foo_id = match topic_ids.get("foo") {
                Some(id) => *id,
                None => Uuid::nil(),
            };

            let foo0 = TopicPartition::new("foo", 0);
            let foo1 = TopicPartition::new("foo", 1);

            fetch_session_builder.add(
                foo0.clone(),
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 0,
                    log_start_offset: 100,
                    max_bytes: 200,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );

            fetch_session_builder.add(
                foo1.clone(),
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 10,
                    log_start_offset: 110,
                    max_bytes: 210,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );

            let data = fetch_session_builder.build(&mut fetch_session);

            assert_maps_equals(
                &req_map(vec![
                    ReqEntry::new(StrBytes::from_str("foo"), foo_id, 0, 0, 100, 200),
                    ReqEntry::new(StrBytes::from_str("foo"), foo_id, 1, 10, 110, 210),
                ]),
                vec![&data.to_send, &data.session_partitions],
            );

            assert_eq!(INVALID_SESSION_ID, data.metadata.session_id);
            assert_eq!(INITIAL_EPOCH, data.metadata.epoch);

            let resp = to_fetch_response(
                0,
                0,
                123,
                resp_map(vec![
                    RespEntry::new(StrBytes::from_str("foo"), 0, foo_id, 10, 20),
                    RespEntry::new(StrBytes::from_str("foo"), 1, foo_id, 10, 20),
                ]),
            );

            fetch_session.handle_fetch_response(&resp, version);

            // Test an incremental fetch request which adds one partition and modifies another.
            let mut fetch_session_builder = FetchRequestDataBuilder::new();
            add_topic_id(
                &mut topic_ids,
                &mut topic_names,
                StrBytes::from_str("bar"),
                version,
            );
            let bar_id = match topic_ids.get("bar") {
                Some(id) => *id,
                None => Uuid::nil(),
            };

            let bar0 = TopicPartition::new("bar", 0);

            fetch_session_builder.add(
                foo0.clone(),
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 0,
                    log_start_offset: 100,
                    max_bytes: 200,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );

            fetch_session_builder.add(
                foo1.clone(),
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 10,
                    log_start_offset: 120,
                    max_bytes: 210,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );

            fetch_session_builder.add(
                bar0.clone(),
                FetchRequestPartitionData {
                    topic_id: bar_id,
                    fetch_offset: 20,
                    log_start_offset: 200,
                    max_bytes: 200,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );

            let data2 = fetch_session_builder.build(&mut fetch_session);
            assert_eq!(data2.metadata.is_full(), false);

            assert_map_equals(
                &req_map(vec![
                    ReqEntry::new(StrBytes::from_str("foo"), foo_id, 0, 0, 100, 200),
                    ReqEntry::new(StrBytes::from_str("foo"), foo_id, 1, 10, 120, 210),
                    ReqEntry::new(StrBytes::from_str("bar"), bar_id, 0, 20, 200, 200),
                ]),
                &data2.session_partitions,
            );
            assert_map_equals(
                &req_map(vec![
                    ReqEntry::new(StrBytes::from_str("bar"), bar_id, 0, 20, 200, 200),
                    ReqEntry::new(StrBytes::from_str("foo"), foo_id, 1, 10, 120, 210),
                ]),
                &data2.to_send,
            );

            let resp = to_fetch_response(
                0,
                0,
                123,
                resp_map(vec![RespEntry::new(
                    StrBytes::from_str("foo"),
                    1,
                    foo_id,
                    20,
                    20,
                )]),
            );

            fetch_session.handle_fetch_response(&resp, version);

            // Skip building a new request.  Test that handling an invalid fetch session epoch
            // response results in a request which closes the session.
            //
            // 71 -> ResponseError::InvalidFetchSessionEpoch
            let resp = to_fetch_response(71, 0, INVALID_SESSION_ID, resp_map(vec![]));
            fetch_session.handle_fetch_response(&resp, version);

            let mut fetch_session_builder = FetchRequestDataBuilder::new();

            fetch_session_builder.add(
                foo0,
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 0,
                    log_start_offset: 100,
                    max_bytes: 200,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );

            fetch_session_builder.add(
                foo1,
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 10,
                    log_start_offset: 120,
                    max_bytes: 210,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );

            fetch_session_builder.add(
                bar0,
                FetchRequestPartitionData {
                    topic_id: bar_id,
                    fetch_offset: 20,
                    log_start_offset: 200,
                    max_bytes: 200,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );

            let data4 = fetch_session_builder.build(&mut fetch_session);
            assert_eq!(data4.metadata.is_full(), true);
            assert_eq!(data2.metadata.session_id, data4.metadata.session_id);
            assert_eq!(INITIAL_EPOCH, data4.metadata.epoch);

            assert_maps_equals(
                &req_map(vec![
                    ReqEntry::new(StrBytes::from_str("foo"), foo_id, 0, 0, 100, 200),
                    ReqEntry::new(StrBytes::from_str("foo"), foo_id, 1, 10, 120, 210),
                    ReqEntry::new(StrBytes::from_str("bar"), bar_id, 0, 20, 200, 200),
                ]),
                vec![&data4.session_partitions, &data4.to_send],
            );
        }
    }

    #[test]
    fn test_incremental_partition_removal() {
        let mut topic_ids = HashMap::new();
        let mut topic_names = HashMap::new();

        // We want to test both on older versions that do not use topic IDs and on newer versions
        // that do.
        let versions = vec![12i16, FetchRequest::VERSIONS.max];

        for version in versions {
            let mut fetch_session = FetchSession::new(1);
            let mut fetch_session_builder = FetchRequestDataBuilder::new();

            add_topic_id(
                &mut topic_ids,
                &mut topic_names,
                StrBytes::from_str("foo"),
                version,
            );
            add_topic_id(
                &mut topic_ids,
                &mut topic_names,
                StrBytes::from_str("bar"),
                version,
            );

            let foo_id = match topic_ids.get("foo") {
                Some(id) => *id,
                None => Uuid::nil(),
            };
            let bar_id = match topic_ids.get("bar") {
                Some(id) => *id,
                None => Uuid::nil(),
            };

            let foo0 = TopicPartition::new("foo", 0);
            let foo1 = TopicPartition::new("foo", 1);
            let bar0 = TopicPartition::new("bar", 0);

            fetch_session_builder.add(
                foo0.clone(),
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 0,
                    log_start_offset: 100,
                    max_bytes: 200,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );
            fetch_session_builder.add(
                foo1.clone(),
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 10,
                    log_start_offset: 110,
                    max_bytes: 210,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );
            fetch_session_builder.add(
                bar0.clone(),
                FetchRequestPartitionData {
                    topic_id: bar_id,
                    fetch_offset: 20,
                    log_start_offset: 120,
                    max_bytes: 220,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );
            let data = fetch_session_builder.build(&mut fetch_session);
            assert_maps_equals(
                &req_map(vec![
                    ReqEntry::new(StrBytes::from_str("foo"), foo_id, 0, 0, 100, 200),
                    ReqEntry::new(StrBytes::from_str("foo"), foo_id, 1, 10, 110, 210),
                    ReqEntry::new(StrBytes::from_str("bar"), bar_id, 0, 20, 120, 220),
                ]),
                vec![&data.to_send, &data.session_partitions],
            );
            assert_eq!(data.metadata.is_full(), true);

            let resp = to_fetch_response(
                0,
                0,
                123,
                resp_map(vec![
                    RespEntry::new(StrBytes::from_str("foo"), 0, foo_id, 10, 20),
                    RespEntry::new(StrBytes::from_str("foo"), 1, foo_id, 10, 20),
                    RespEntry::new(StrBytes::from_str("bar"), 0, bar_id, 10, 20),
                ]),
            );

            fetch_session.handle_fetch_response(&resp, version);

            // Test an incremental fetch request which removes two partitions.
            let mut fetch_session_builder = FetchRequestDataBuilder::new();
            fetch_session_builder.add(
                foo1.clone(),
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 10,
                    log_start_offset: 110,
                    max_bytes: 210,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );
            let data = fetch_session_builder.build(&mut fetch_session);
            assert_eq!(data.metadata.is_full(), false);
            assert_eq!(123, data.metadata.session_id);
            assert_eq!(1, data.metadata.epoch);
            assert_map_equals(
                &req_map(vec![ReqEntry::new(
                    StrBytes::from_str("foo"),
                    foo_id,
                    1,
                    10,
                    110,
                    210,
                )]),
                &data.session_partitions,
            );
            assert_map_equals(&req_map(vec![]), &data.to_send);

            let expected_to_forget2 = vec![
                TopicIdPartition {
                    topic_id: foo_id,
                    partition: foo0.clone(),
                },
                TopicIdPartition {
                    topic_id: bar_id,
                    partition: bar0.clone(),
                },
            ];
            assert_list_equals(&expected_to_forget2, &data.to_forget);

            // A FETCH_SESSION_ID_NOT_FOUND response triggers us to close the session.
            // The next request is a session establishing FULL request.
            //
            // 70 -> ResponseError::FetchSessionIdNotFound
            let resp = to_fetch_response(70, 0, INVALID_SESSION_ID, resp_map(vec![]));

            fetch_session.handle_fetch_response(&resp, version);

            let mut fetch_session_builder = FetchRequestDataBuilder::new();
            fetch_session_builder.add(
                foo0.clone(),
                FetchRequestPartitionData {
                    topic_id: foo_id,
                    fetch_offset: 0,
                    log_start_offset: 100,
                    max_bytes: 200,
                    current_leader_epoch: None,
                    last_fetched_epoch: None,
                },
            );
            let data = fetch_session_builder.build(&mut fetch_session);
            assert_eq!(data.metadata.is_full(), true);
            assert_eq!(INVALID_SESSION_ID, data.metadata.session_id);
            assert_eq!(INITIAL_EPOCH, data.metadata.epoch);
            assert_maps_equals(
                &req_map(vec![ReqEntry::new(
                    StrBytes::from_str("foo"),
                    foo_id,
                    0,
                    0,
                    100,
                    200,
                )]),
                vec![&data.session_partitions, &data.to_send],
            );
        }
    }
}
