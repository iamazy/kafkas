use std::collections::HashMap;

use bytes::{Buf, BytesMut};
pub use kafka_protocol::protocol::*;
use kafka_protocol::{messages::*, protocol::buf::ByteBuf};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::{error::ConnectionError, ToStrBytes};

const DEFAULT_API_VERSION: i16 = 0;

pub struct KafkaCodec {
    pub support_versions: HashMap<i16, VersionRange>,
    pub active_requests: HashMap<i32, RequestHeader>,
    length_codec: LengthDelimitedCodec,
}

impl Default for KafkaCodec {
    fn default() -> Self {
        KafkaCodec {
            support_versions: HashMap::default(),
            active_requests: HashMap::default(),
            length_codec: LengthDelimitedCodec::builder()
                .max_frame_length(i32::MAX as usize)
                .length_field_length(4)
                .new_codec(),
        }
    }
}

#[cfg(feature = "tokio-runtime")]
impl tokio_util::codec::Encoder<Command> for KafkaCodec {
    type Error = ConnectionError;

    fn encode(&mut self, cmd: Command, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut bytes = BytesMut::new();
        self.encode0(cmd, &mut bytes)?;
        self.length_codec
            .encode(bytes.get_bytes(bytes.len()), dst)?;
        Ok(())
    }
}

#[cfg(feature = "tokio-runtime")]
impl tokio_util::codec::Decoder for KafkaCodec {
    type Item = Command;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(mut bytes) = self.length_codec.decode(src)? {
            return self.decode_response(&mut bytes);
        }
        Ok(None)
    }
}

#[cfg(feature = "async-std-runtime")]
impl asynchronous_codec::Encoder for KafkaCodec {
    type Item = Command;
    type Error = ConnectionError;

    fn encode(&mut self, cmd: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut bytes = BytesMut::new();
        self.encode0(cmd, &mut bytes)?;
        self.length_codec
            .encode(bytes.get_bytes(bytes.len()), dst)?;
        Ok(())
    }
}

#[cfg(feature = "async-std-runtime")]
impl asynchronous_codec::Decoder for KafkaCodec {
    type Item = Command;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(mut bytes) = self.length_codec.decode(src)? {
            self.decode_response(&mut bytes)
        } else {
            Ok(None)
        }
    }
}

impl KafkaCodec {
    fn encode0(&mut self, cmd: Command, dst: &mut BytesMut) -> Result<(), ConnectionError> {
        if let Command::Request(req) = cmd {
            let header = req.header;
            #[rustfmt::skip]
            return match req.request {
                RequestKind::ProduceRequest(req) => self.encode_request(header, req, dst),
                RequestKind::FetchRequest(req) => self.encode_request(header, req, dst),
                RequestKind::ListOffsetsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::MetadataRequest(req) => self.encode_request(header, req, dst),
                RequestKind::LeaderAndIsrRequest(req) => self.encode_request(header, req, dst),
                RequestKind::StopReplicaRequest(req) => self.encode_request(header, req, dst),
                RequestKind::UpdateMetadataRequest(req) => self.encode_request(header, req, dst),
                RequestKind::ControlledShutdownRequest(req) => self.encode_request(header, req, dst),
                RequestKind::OffsetCommitRequest(req) => self.encode_request(header, req, dst),
                RequestKind::OffsetFetchRequest(req) => self.encode_request(header, req, dst),
                RequestKind::FindCoordinatorRequest(req) => self.encode_request(header, req, dst),
                RequestKind::JoinGroupRequest(req) => self.encode_request(header, req, dst),
                RequestKind::HeartbeatRequest(req) => self.encode_request(header, req, dst),
                RequestKind::LeaveGroupRequest(req) => self.encode_request(header, req, dst),
                RequestKind::SyncGroupRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DescribeGroupsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::ListGroupsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::SaslHandshakeRequest(req) => self.encode_request(header, req, dst),
                RequestKind::ApiVersionsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::CreateTopicsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DeleteTopicsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DeleteRecordsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::InitProducerIdRequest(req) => self.encode_request(header, req, dst),
                RequestKind::OffsetForLeaderEpochRequest(req) => self.encode_request(header, req, dst),
                RequestKind::AddPartitionsToTxnRequest(req) => self.encode_request(header, req, dst),
                RequestKind::AddOffsetsToTxnRequest(req) => self.encode_request(header, req, dst),
                RequestKind::EndTxnRequest(req) => self.encode_request(header, req, dst),
                RequestKind::WriteTxnMarkersRequest(req) => self.encode_request(header, req, dst),
                RequestKind::TxnOffsetCommitRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DescribeAclsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::CreateAclsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DeleteAclsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DescribeConfigsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::AlterConfigsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::AlterReplicaLogDirsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DescribeLogDirsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::SaslAuthenticateRequest(req) => self.encode_request(header, req, dst),
                RequestKind::CreatePartitionsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::CreateDelegationTokenRequest(req) => self.encode_request(header, req, dst),
                RequestKind::RenewDelegationTokenRequest(req) => self.encode_request(header, req, dst),
                RequestKind::ExpireDelegationTokenRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DescribeDelegationTokenRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DeleteGroupsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::ElectLeadersRequest(req) => self.encode_request(header, req, dst),
                RequestKind::IncrementalAlterConfigsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::AlterPartitionReassignmentsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::ListPartitionReassignmentsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::OffsetDeleteRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DescribeClientQuotasRequest(req) => self.encode_request(header, req, dst),
                RequestKind::AlterClientQuotasRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DescribeUserScramCredentialsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::AlterUserScramCredentialsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::VoteRequest(req) => self.encode_request(header, req, dst),
                RequestKind::BeginQuorumEpochRequest(req) => self.encode_request(header, req, dst),
                RequestKind::EndQuorumEpochRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DescribeQuorumRequest(req) => self.encode_request(header, req, dst),
                RequestKind::AlterPartitionRequest(req) => self.encode_request(header, req, dst),
                RequestKind::UpdateFeaturesRequest(req) => self.encode_request(header, req, dst),
                RequestKind::EnvelopeRequest(req) => self.encode_request(header, req, dst),
                RequestKind::FetchSnapshotRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DescribeClusterRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DescribeProducersRequest(req) => self.encode_request(header, req, dst),
                RequestKind::BrokerRegistrationRequest(req) => self.encode_request(header, req, dst),
                RequestKind::BrokerHeartbeatRequest(req) => self.encode_request(header, req, dst),
                RequestKind::UnregisterBrokerRequest(req) => self.encode_request(header, req, dst),
                RequestKind::DescribeTransactionsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::ListTransactionsRequest(req) => self.encode_request(header, req, dst),
                RequestKind::AllocateProducerIdsRequest(req) => self.encode_request(header, req, dst),
            };
        }
        Ok(())
    }
    fn encode_request<Req: Request>(
        &mut self,
        mut header: RequestHeader,
        req: Req,
        dst: &mut BytesMut,
    ) -> Result<(), ConnectionError> {
        let api_key = Req::KEY;
        let mut api_version = DEFAULT_API_VERSION;
        if api_key != ApiKey::ApiVersionsKey as i16 {
            let server_range = self.support_versions.get(&api_key).ok_or_else(|| {
                ConnectionError::Encoding(format!("apiKey {api_key} is not support"))
            })?;
            let supported_range = server_range.intersect(&Req::VERSIONS);
            if supported_range.is_empty() {
                return Err(ConnectionError::Encoding(format!(
                    "apiKey {} is not support.",
                    api_key
                )));
            }
            api_version = supported_range.max;
        }

        header.request_api_key = api_key;
        header.request_api_version = api_version;

        let header_version = Req::header_version(api_version);
        header.encode(dst, header_version)?;

        self.active_requests.insert(header.correlation_id, header);

        req.encode(dst, api_version)?;
        Ok(())
    }

    fn response_header_version(&self, api_key: i16, api_version: i16) -> i16 {
        if let Some(version_range) = self.support_versions.get(&api_key) {
            if api_version >= version_range.max {
                return 1;
            }
        }
        return 0;
    }

    fn decode_response(&mut self, src: &mut BytesMut) -> Result<Option<Command>, ConnectionError> {
        let mut correlation_id_bytes = src.try_peek_bytes(0..4)?;
        let correlation_id = correlation_id_bytes.get_i32();

        let request_header = self
            .active_requests
            .remove(&correlation_id)
            .ok_or_else(|| {
                ConnectionError::UnexpectedResponse(format!("correlation_id: {}", correlation_id))
            })?;

        let response_header_version = self.response_header_version(
            request_header.request_api_key,
            request_header.request_api_version,
        );

        // decode response header
        let response_header = ResponseHeader::decode(src, response_header_version)?;
        let header_version = request_header.request_api_version;

        let api_key = ApiKey::try_from(request_header.request_api_key)?;
        let response_kind = match api_key {
            ApiKey::ProduceKey => {
                let res = ProduceResponse::decode(src, header_version)?;
                ResponseKind::ProduceResponse(res)
            }
            ApiKey::FetchKey => {
                let res = FetchResponse::decode(src, header_version)?;
                ResponseKind::FetchResponse(res)
            }
            ApiKey::ListOffsetsKey => {
                let res = ListOffsetsResponse::decode(src, header_version)?;
                ResponseKind::ListOffsetsResponse(res)
            }
            ApiKey::MetadataKey => {
                let res = MetadataResponse::decode(src, header_version)?;
                ResponseKind::MetadataResponse(res)
            }
            ApiKey::LeaderAndIsrKey => {
                let res = LeaderAndIsrResponse::decode(src, header_version)?;
                ResponseKind::LeaderAndIsrResponse(res)
            }
            ApiKey::StopReplicaKey => {
                let res = StopReplicaResponse::decode(src, header_version)?;
                ResponseKind::StopReplicaResponse(res)
            }
            ApiKey::UpdateMetadataKey => {
                let res = UpdateMetadataResponse::decode(src, header_version)?;
                ResponseKind::UpdateMetadataResponse(res)
            }
            ApiKey::ControlledShutdownKey => {
                let res = ControlledShutdownResponse::decode(src, header_version)?;
                ResponseKind::ControlledShutdownResponse(res)
            }
            ApiKey::OffsetCommitKey => {
                let res = OffsetCommitResponse::decode(src, header_version)?;
                ResponseKind::OffsetCommitResponse(res)
            }
            ApiKey::OffsetFetchKey => {
                let res = OffsetFetchResponse::decode(src, header_version)?;
                ResponseKind::OffsetFetchResponse(res)
            }
            ApiKey::FindCoordinatorKey => {
                let res = FindCoordinatorResponse::decode(src, header_version)?;
                ResponseKind::FindCoordinatorResponse(res)
            }
            ApiKey::JoinGroupKey => {
                let res = JoinGroupResponse::decode(src, header_version)?;
                ResponseKind::JoinGroupResponse(res)
            }
            ApiKey::HeartbeatKey => {
                let res = HeartbeatResponse::decode(src, header_version)?;
                ResponseKind::HeartbeatResponse(res)
            }
            ApiKey::LeaveGroupKey => {
                let res = LeaveGroupResponse::decode(src, header_version)?;
                ResponseKind::LeaveGroupResponse(res)
            }
            ApiKey::SyncGroupKey => {
                let res = SyncGroupResponse::decode(src, header_version)?;
                ResponseKind::SyncGroupResponse(res)
            }
            ApiKey::DescribeGroupsKey => {
                let res = DescribeGroupsResponse::decode(src, header_version)?;
                ResponseKind::DescribeGroupsResponse(res)
            }
            ApiKey::ListGroupsKey => {
                let res = ListGroupsResponse::decode(src, header_version)?;
                ResponseKind::ListGroupsResponse(res)
            }
            ApiKey::SaslHandshakeKey => {
                let res = SaslHandshakeResponse::decode(src, header_version)?;
                ResponseKind::SaslHandshakeResponse(res)
            }
            ApiKey::ApiVersionsKey => {
                let res = ApiVersionsResponse::decode(src, header_version)?;
                for (k, v) in res.api_keys.iter() {
                    self.support_versions.insert(
                        *k,
                        VersionRange {
                            min: v.min_version,
                            max: v.max_version,
                        },
                    );
                }
                ResponseKind::ApiVersionsResponse(res)
            }
            ApiKey::CreateTopicsKey => {
                let res = CreateTopicsResponse::decode(src, header_version)?;
                ResponseKind::CreateTopicsResponse(res)
            }
            ApiKey::DeleteTopicsKey => {
                let res = DeleteTopicsResponse::decode(src, header_version)?;
                ResponseKind::DeleteTopicsResponse(res)
            }
            ApiKey::DeleteRecordsKey => {
                let res = DeleteRecordsResponse::decode(src, header_version)?;
                ResponseKind::DeleteRecordsResponse(res)
            }
            ApiKey::InitProducerIdKey => {
                let res = InitProducerIdResponse::decode(src, header_version)?;
                ResponseKind::InitProducerIdResponse(res)
            }
            ApiKey::OffsetForLeaderEpochKey => {
                let res = OffsetForLeaderEpochResponse::decode(src, header_version)?;
                ResponseKind::OffsetForLeaderEpochResponse(res)
            }
            ApiKey::AddPartitionsToTxnKey => {
                let res = AddPartitionsToTxnResponse::decode(src, header_version)?;
                ResponseKind::AddPartitionsToTxnResponse(res)
            }
            ApiKey::AddOffsetsToTxnKey => {
                let res = AddOffsetsToTxnResponse::decode(src, header_version)?;
                ResponseKind::AddOffsetsToTxnResponse(res)
            }
            ApiKey::EndTxnKey => {
                let res = EndTxnResponse::decode(src, header_version)?;
                ResponseKind::EndTxnResponse(res)
            }
            ApiKey::WriteTxnMarkersKey => {
                let res = WriteTxnMarkersResponse::decode(src, header_version)?;
                ResponseKind::WriteTxnMarkersResponse(res)
            }
            ApiKey::TxnOffsetCommitKey => {
                let res = TxnOffsetCommitResponse::decode(src, header_version)?;
                ResponseKind::TxnOffsetCommitResponse(res)
            }
            ApiKey::DescribeAclsKey => {
                let res = DescribeAclsResponse::decode(src, header_version)?;
                ResponseKind::DescribeAclsResponse(res)
            }
            ApiKey::CreateAclsKey => {
                let res = CreateAclsResponse::decode(src, header_version)?;
                ResponseKind::CreateAclsResponse(res)
            }
            ApiKey::DeleteAclsKey => {
                let res = DeleteAclsResponse::decode(src, header_version)?;
                ResponseKind::DeleteAclsResponse(res)
            }
            ApiKey::DescribeConfigsKey => {
                let res = DescribeConfigsResponse::decode(src, header_version)?;
                ResponseKind::DescribeConfigsResponse(res)
            }
            ApiKey::AlterConfigsKey => {
                let res = AlterConfigsResponse::decode(src, header_version)?;
                ResponseKind::AlterConfigsResponse(res)
            }
            ApiKey::AlterReplicaLogDirsKey => {
                let res = AlterReplicaLogDirsResponse::decode(src, header_version)?;
                ResponseKind::AlterReplicaLogDirsResponse(res)
            }
            ApiKey::DescribeLogDirsKey => {
                let res = DescribeLogDirsResponse::decode(src, header_version)?;
                ResponseKind::DescribeLogDirsResponse(res)
            }
            ApiKey::SaslAuthenticateKey => {
                let res = SaslAuthenticateResponse::decode(src, header_version)?;
                ResponseKind::SaslAuthenticateResponse(res)
            }
            ApiKey::CreatePartitionsKey => {
                let res = CreatePartitionsResponse::decode(src, header_version)?;
                ResponseKind::CreatePartitionsResponse(res)
            }
            ApiKey::CreateDelegationTokenKey => {
                let res = CreateDelegationTokenResponse::decode(src, header_version)?;
                ResponseKind::CreateDelegationTokenResponse(res)
            }
            ApiKey::RenewDelegationTokenKey => {
                let res = RenewDelegationTokenResponse::decode(src, header_version)?;
                ResponseKind::RenewDelegationTokenResponse(res)
            }
            ApiKey::ExpireDelegationTokenKey => {
                let res = ExpireDelegationTokenResponse::decode(src, header_version)?;
                ResponseKind::ExpireDelegationTokenResponse(res)
            }
            ApiKey::DescribeDelegationTokenKey => {
                let res = DescribeDelegationTokenResponse::decode(src, header_version)?;
                ResponseKind::DescribeDelegationTokenResponse(res)
            }
            ApiKey::DeleteGroupsKey => {
                let res = DeleteGroupsResponse::decode(src, header_version)?;
                ResponseKind::DeleteGroupsResponse(res)
            }
            ApiKey::ElectLeadersKey => {
                let res = ElectLeadersResponse::decode(src, header_version)?;
                ResponseKind::ElectLeadersResponse(res)
            }
            ApiKey::IncrementalAlterConfigsKey => {
                let res = IncrementalAlterConfigsResponse::decode(src, header_version)?;
                ResponseKind::IncrementalAlterConfigsResponse(res)
            }
            ApiKey::AlterPartitionReassignmentsKey => {
                let res = AlterPartitionReassignmentsResponse::decode(src, header_version)?;
                ResponseKind::AlterPartitionReassignmentsResponse(res)
            }
            ApiKey::ListPartitionReassignmentsKey => {
                let res = ListPartitionReassignmentsResponse::decode(src, header_version)?;
                ResponseKind::ListPartitionReassignmentsResponse(res)
            }
            ApiKey::OffsetDeleteKey => {
                let res = OffsetDeleteResponse::decode(src, header_version)?;
                ResponseKind::OffsetDeleteResponse(res)
            }
            ApiKey::DescribeClientQuotasKey => {
                let res = DescribeClientQuotasResponse::decode(src, header_version)?;
                ResponseKind::DescribeClientQuotasResponse(res)
            }
            ApiKey::AlterClientQuotasKey => {
                let res = AlterClientQuotasResponse::decode(src, header_version)?;
                ResponseKind::AlterClientQuotasResponse(res)
            }
            ApiKey::DescribeUserScramCredentialsKey => {
                let res = DescribeUserScramCredentialsResponse::decode(src, header_version)?;
                ResponseKind::DescribeUserScramCredentialsResponse(res)
            }
            ApiKey::AlterUserScramCredentialsKey => {
                let res = AlterUserScramCredentialsResponse::decode(src, header_version)?;
                ResponseKind::AlterUserScramCredentialsResponse(res)
            }
            ApiKey::VoteKey => {
                let res = VoteResponse::decode(src, header_version)?;
                ResponseKind::VoteResponse(res)
            }
            ApiKey::BeginQuorumEpochKey => {
                let res = BeginQuorumEpochResponse::decode(src, header_version)?;
                ResponseKind::BeginQuorumEpochResponse(res)
            }
            ApiKey::EndQuorumEpochKey => {
                let res = EndQuorumEpochResponse::decode(src, header_version)?;
                ResponseKind::EndQuorumEpochResponse(res)
            }
            ApiKey::DescribeQuorumKey => {
                let res = DescribeQuorumResponse::decode(src, header_version)?;
                ResponseKind::DescribeQuorumResponse(res)
            }
            ApiKey::AlterPartitionKey => {
                let res = AlterPartitionResponse::decode(src, header_version)?;
                ResponseKind::AlterPartitionResponse(res)
            }
            ApiKey::UpdateFeaturesKey => {
                let res = UpdateFeaturesResponse::decode(src, header_version)?;
                ResponseKind::UpdateFeaturesResponse(res)
            }
            ApiKey::EnvelopeKey => {
                let res = EnvelopeResponse::decode(src, header_version)?;
                ResponseKind::EnvelopeResponse(res)
            }
            ApiKey::FetchSnapshotKey => {
                let res = FetchSnapshotResponse::decode(src, header_version)?;
                ResponseKind::FetchSnapshotResponse(res)
            }
            ApiKey::DescribeClusterKey => {
                let res = DescribeClusterResponse::decode(src, header_version)?;
                ResponseKind::DescribeClusterResponse(res)
            }
            ApiKey::DescribeProducersKey => {
                let res = DescribeProducersResponse::decode(src, header_version)?;
                ResponseKind::DescribeProducersResponse(res)
            }
            ApiKey::BrokerRegistrationKey => {
                let res = BrokerRegistrationResponse::decode(src, header_version)?;
                ResponseKind::BrokerRegistrationResponse(res)
            }
            ApiKey::BrokerHeartbeatKey => {
                let res = BrokerHeartbeatResponse::decode(src, header_version)?;
                ResponseKind::BrokerHeartbeatResponse(res)
            }
            ApiKey::UnregisterBrokerKey => {
                let res = UnregisterBrokerResponse::decode(src, header_version)?;
                ResponseKind::UnregisterBrokerResponse(res)
            }
            ApiKey::DescribeTransactionsKey => {
                let res = DescribeTransactionsResponse::decode(src, header_version)?;
                ResponseKind::DescribeTransactionsResponse(res)
            }
            ApiKey::ListTransactionsKey => {
                let res = ListTransactionsResponse::decode(src, header_version)?;
                ResponseKind::ListTransactionsResponse(res)
            }
            ApiKey::AllocateProducerIdsKey => {
                let res = AllocateProducerIdsResponse::decode(src, header_version)?;
                ResponseKind::AllocateProducerIdsResponse(res)
            }
        };
        let response = KafkaResponse::new(response_header, response_kind);
        Ok(Some(Command::Response(response)))
    }
}

pub struct KafkaRequest {
    pub header: RequestHeader,
    pub request: RequestKind,
}

impl KafkaRequest {
    pub(crate) fn new(
        serial_id: i32,
        request: RequestKind,
    ) -> Result<KafkaRequest, ConnectionError> {
        let key = request_key(&request);
        let header = RequestHeader {
            request_api_key: key,
            correlation_id: serial_id,
            ..Default::default()
        };
        Ok(KafkaRequest { header, request })
    }

    pub fn client_id(&mut self, client_id: Option<String>) {
        if let Some(client_id) = client_id {
            self.header.client_id = Some(client_id.to_str_bytes());
        }
    }
}

pub struct KafkaResponse {
    pub header: ResponseHeader,
    pub response: ResponseKind,
}

impl KafkaResponse {
    pub fn new(header: ResponseHeader, response: ResponseKind) -> Self {
        Self { header, response }
    }
}

pub enum Command {
    Request(KafkaRequest),
    Response(KafkaResponse),
}

#[rustfmt::skip]
fn request_key(request: &RequestKind) -> i16 {
    match request {
        RequestKind::ProduceRequest(_) => <ProduceRequest as Request>::KEY,
        RequestKind::FetchRequest(_) => <FetchRequest as Request>::KEY,
        RequestKind::ListOffsetsRequest(_) => <ListOffsetsRequest as Request>::KEY,
        RequestKind::MetadataRequest(_) => <MetadataRequest as Request>::KEY,
        RequestKind::LeaderAndIsrRequest(_) => <LeaderAndIsrRequest as Request>::KEY,
        RequestKind::StopReplicaRequest(_) => <StopReplicaRequest as Request>::KEY,
        RequestKind::UpdateMetadataRequest(_) => <UpdateMetadataRequest as Request>::KEY,
        RequestKind::ControlledShutdownRequest(_) => <ControlledShutdownRequest as Request>::KEY,
        RequestKind::OffsetCommitRequest(_) => <OffsetCommitRequest as Request>::KEY,
        RequestKind::OffsetFetchRequest(_) => <OffsetFetchRequest as Request>::KEY,
        RequestKind::FindCoordinatorRequest(_) => <FindCoordinatorRequest as Request>::KEY,
        RequestKind::JoinGroupRequest(_) => <JoinGroupRequest as Request>::KEY,
        RequestKind::HeartbeatRequest(_) => <HeartbeatRequest as Request>::KEY,
        RequestKind::LeaveGroupRequest(_) => <LeaveGroupRequest as Request>::KEY,
        RequestKind::SyncGroupRequest(_) => <SyncGroupRequest as Request>::KEY,
        RequestKind::DescribeGroupsRequest(_) => <DescribeGroupsRequest as Request>::KEY,
        RequestKind::ListGroupsRequest(_) => <ListGroupsRequest as Request>::KEY,
        RequestKind::SaslHandshakeRequest(_) => <SaslHandshakeRequest as Request>::KEY,
        RequestKind::ApiVersionsRequest(_) => <ApiVersionsRequest as Request>::KEY,
        RequestKind::CreateTopicsRequest(_) => <CreateTopicsRequest as Request>::KEY,
        RequestKind::DeleteTopicsRequest(_) => <DeleteTopicsRequest as Request>::KEY,
        RequestKind::DeleteRecordsRequest(_) => <DeleteRecordsRequest as Request>::KEY,
        RequestKind::InitProducerIdRequest(_) => <InitProducerIdRequest as Request>::KEY,
        RequestKind::OffsetForLeaderEpochRequest(_) => <OffsetForLeaderEpochRequest as Request>::KEY,
        RequestKind::AddPartitionsToTxnRequest(_) => <AddPartitionsToTxnRequest as Request>::KEY,
        RequestKind::AddOffsetsToTxnRequest(_) => <AddOffsetsToTxnRequest as Request>::KEY,
        RequestKind::EndTxnRequest(_) => <EndTxnRequest as Request>::KEY,
        RequestKind::WriteTxnMarkersRequest(_) => <WriteTxnMarkersRequest as Request>::KEY,
        RequestKind::TxnOffsetCommitRequest(_) => <TxnOffsetCommitRequest as Request>::KEY,
        RequestKind::DescribeAclsRequest(_) => <DescribeAclsRequest as Request>::KEY,
        RequestKind::CreateAclsRequest(_) => <CreateAclsRequest as Request>::KEY,
        RequestKind::DeleteAclsRequest(_) => <DeleteAclsRequest as Request>::KEY,
        RequestKind::DescribeConfigsRequest(_) => <DescribeConfigsRequest as Request>::KEY,
        RequestKind::AlterConfigsRequest(_) => <AlterConfigsRequest as Request>::KEY,
        RequestKind::AlterReplicaLogDirsRequest(_) => <AlterReplicaLogDirsRequest as Request>::KEY,
        RequestKind::DescribeLogDirsRequest(_) => <DescribeLogDirsRequest as Request>::KEY,
        RequestKind::SaslAuthenticateRequest(_) => <SaslAuthenticateRequest as Request>::KEY,
        RequestKind::CreatePartitionsRequest(_) => <CreatePartitionsRequest as Request>::KEY,
        RequestKind::CreateDelegationTokenRequest(_) => <CreateDelegationTokenRequest as Request>::KEY,
        RequestKind::RenewDelegationTokenRequest(_) => <RenewDelegationTokenRequest as Request>::KEY,
        RequestKind::ExpireDelegationTokenRequest(_) => <ExpireDelegationTokenRequest as Request>::KEY,
        RequestKind::DescribeDelegationTokenRequest(_) => <DescribeDelegationTokenRequest as Request>::KEY,
        RequestKind::DeleteGroupsRequest(_) => <DeleteGroupsRequest as Request>::KEY,
        RequestKind::ElectLeadersRequest(_) => <ElectLeadersRequest as Request>::KEY,
        RequestKind::IncrementalAlterConfigsRequest(_) => <IncrementalAlterConfigsRequest as Request>::KEY,
        RequestKind::AlterPartitionReassignmentsRequest(_) => <AlterPartitionReassignmentsRequest as Request>::KEY,
        RequestKind::ListPartitionReassignmentsRequest(_) => <ListPartitionReassignmentsRequest as Request>::KEY,
        RequestKind::OffsetDeleteRequest(_) => <OffsetDeleteRequest as Request>::KEY,
        RequestKind::DescribeClientQuotasRequest(_) => <DescribeClientQuotasRequest as Request>::KEY,
        RequestKind::AlterClientQuotasRequest(_) => <AlterClientQuotasRequest as Request>::KEY,
        RequestKind::DescribeUserScramCredentialsRequest(_) => <DescribeUserScramCredentialsRequest as Request>::KEY,
        RequestKind::AlterUserScramCredentialsRequest(_) => <AlterUserScramCredentialsRequest as Request>::KEY,
        RequestKind::VoteRequest(_) => <VoteRequest as Request>::KEY,
        RequestKind::BeginQuorumEpochRequest(_) => <BeginQuorumEpochRequest as Request>::KEY,
        RequestKind::EndQuorumEpochRequest(_) => <EndQuorumEpochRequest as Request>::KEY,
        RequestKind::DescribeQuorumRequest(_) => <DescribeQuorumRequest as Request>::KEY,
        RequestKind::AlterPartitionRequest(_) => <AlterPartitionRequest as Request>::KEY,
        RequestKind::UpdateFeaturesRequest(_) => <UpdateFeaturesRequest as Request>::KEY,
        RequestKind::EnvelopeRequest(_) => <EnvelopeRequest as Request>::KEY,
        RequestKind::FetchSnapshotRequest(_) => <FetchSnapshotRequest as Request>::KEY,
        RequestKind::DescribeClusterRequest(_) => <DescribeClusterRequest as Request>::KEY,
        RequestKind::DescribeProducersRequest(_) => <DescribeProducersRequest as Request>::KEY,
        RequestKind::BrokerRegistrationRequest(_) => <BrokerRegistrationRequest as Request>::KEY,
        RequestKind::BrokerHeartbeatRequest(_) => <BrokerHeartbeatRequest as Request>::KEY,
        RequestKind::UnregisterBrokerRequest(_) => <UnregisterBrokerRequest as Request>::KEY,
        RequestKind::DescribeTransactionsRequest(_) => <DescribeTransactionsRequest as Request>::KEY,
        RequestKind::ListTransactionsRequest(_) => <ListTransactionsRequest as Request>::KEY,
        RequestKind::AllocateProducerIdsRequest(_) => <AllocateProducerIdsRequest as Request>::KEY,
    }
}
