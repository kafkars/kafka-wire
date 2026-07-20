//! The generated Rust message one vector is about, and what this repository
//! believes about it.
//!
//! This module owns the dispatch from a vector's upstream message name onto the
//! concrete `kafka-wire` type, owns encoding and decoding that type at a
//! stated version, and owns reading the generated descriptors back out so a
//! vector's hand-transcribed identity can be checked against them.
//!
//! It deliberately owns no field mapping — `json_value` builds a message from
//! canonical JSON — and no file access and no assertions; `corpus` reads the
//! files and the tests under `tests/` state the protocol claims.

use bytes::Bytes;
use kafka_wire::{
    AbortedTxn, AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, AddRaftVoterRequest, AddRaftVoterResponse,
    AllocateProducerIdsRequest, AllocateProducerIdsResponse, AlterClientQuotasRequest,
    AlterClientQuotasResponse, AlterConfigsRequest, AlterConfigsResponse,
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, AlterPartitionRequest,
    AlterPartitionResponse, AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse,
    AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsResponse,
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, ApiVersionsRequest,
    ApiVersionsResponse, AssignReplicasToDirsRequest, AssignReplicasToDirsResponse,
    BeginQuorumEpochRequest, BeginQuorumEpochResponse, BrokerHeartbeatRequest,
    BrokerHeartbeatResponse, BrokerRegistrationRequest, BrokerRegistrationResponse,
    ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, ConsumerProtocolAssignment,
    ConsumerProtocolSubscription, ControlRecordTypeSchema, ControllerRegistrationRequest,
    ControllerRegistrationResponse, CreateAclsRequest, CreateAclsResponse,
    CreateDelegationTokenRequest, CreateDelegationTokenResponse, CreatePartitionsRequest,
    CreatePartitionsResponse, CreateTopicsRequest, CreateTopicsResponse, DefaultPrincipalData,
    DeleteAclsRequest, DeleteAclsResponse, DeleteGroupsRequest, DeleteGroupsResponse,
    DeleteRecordsRequest, DeleteRecordsResponse, DeleteShareGroupOffsetsRequest,
    DeleteShareGroupOffsetsResponse, DeleteShareGroupStateRequest, DeleteShareGroupStateResponse,
    DescribeAclsRequest, DescribeAclsResponse, DescribeClientQuotasRequest,
    DescribeClientQuotasResponse, DescribeClusterRequest, DescribeClusterResponse,
    DescribeConfigsRequest, DescribeDelegationTokenRequest, DescribeDelegationTokenResponse,
    DescribeGroupsRequest, DescribeGroupsResponse, DescribeLogDirsRequest, DescribeLogDirsResponse,
    DescribeProducersRequest, DescribeProducersResponse, DescribeQuorumRequest,
    DescribeQuorumResponse, DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsResponse,
    DescribeTransactionsRequest, DescribeTransactionsResponse, DescribeUserScramCredentialsRequest,
    DescribeUserScramCredentialsResponse, ElectLeadersRequest, ElectLeadersResponse,
    EndQuorumEpochRequest, EndQuorumEpochResponse, EndTxnMarker, EndTxnRequest, EndTxnResponse,
    EnvelopeRequest, EnvelopeResponse, ExpireDelegationTokenRequest, ExpireDelegationTokenResponse,
    FetchRequest, FetchResponse, FetchSnapshotRequest, FetchSnapshotResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, GetTelemetrySubscriptionsRequest,
    GetTelemetrySubscriptionsResponse, HeartbeatRequest, HeartbeatResponse,
    IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse, InitProducerIdRequest,
    InitProducerIdResponse, InitializeShareGroupStateRequest, InitializeShareGroupStateResponse,
    KRaftVersionRecord, KafkaMessage, KafkaRequest, KafkaResponse, LeaderChangeMessage,
    LeaveGroupRequest, LeaveGroupResponse, ListConfigResourcesRequest, ListConfigResourcesResponse,
    ListGroupsRequest, ListGroupsResponse, ListOffsetsRequest, ListOffsetsResponse,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse, ListTransactionsRequest,
    ListTransactionsResponse, MessageDirection, OffsetCommitRequest, OffsetCommitResponse,
    OffsetDeleteRequest, OffsetDeleteResponse, OffsetForLeaderEpochRequest,
    OffsetForLeaderEpochResponse, ProduceRequest, ProduceResponse, PushTelemetryRequest,
    PushTelemetryResponse, ReadShareGroupStateRequest, ReadShareGroupStateResponse,
    ReadShareGroupStateSummaryRequest, ReadShareGroupStateSummaryResponse, RemoveRaftVoterRequest,
    RemoveRaftVoterResponse, RenewDelegationTokenRequest, RenewDelegationTokenResponse,
    RequestHeader, ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse, ShareAcknowledgeRequest, ShareAcknowledgeResponse,
    ShareFetchResponse, ShareGroupDescribeRequest, ShareGroupDescribeResponse,
    SnapshotFooterRecord, SnapshotHeaderRecord, StreamsGroupTopologyDescriptionUpdateRequest,
    StreamsGroupTopologyDescriptionUpdateResponse, SyncGroupRequest, SyncGroupResponse,
    TxnOffsetCommitRequest, TxnOffsetCommitResponse, UnregisterBrokerRequest,
    UnregisterBrokerResponse, UpdateFeaturesRequest, UpdateFeaturesResponse,
    UpdateRaftVoterRequest, UpdateRaftVoterResponse, VoteRequest, VoteResponse, VotersRecord,
    WriteShareGroupStateRequest, WriteShareGroupStateResponse, WriteTxnMarkersRequest,
    WriteTxnMarkersResponse,
};
use kafka_wire_core::{ApiVersion, DecodeLimits, KafkaDecode, KafkaEncode, VersionRange};

use crate::corpus::Vector;
use crate::json_value::{
    self, Fields, api_versions_request, sasl_handshake_request, sasl_handshake_response,
};

/// Every message the corpus can judge, as one arm per generated type.
///
/// The arms are declared once here and expanded into the enum and into the
/// decode, encode, facts, and flexibility dispatches below. Adding an enabled
/// message is therefore a single line rather than five parallel edits that can
/// disagree with one another.
macro_rules! subjects {
    ($mac:ident) => {
        $mac! {
            AbortedTxn => Framing,
            AddOffsetsToTxnRequest => Request,
            AddOffsetsToTxnResponse => Response,
            AddPartitionsToTxnRequest => Request,
            AddPartitionsToTxnResponse => Response,
            AddRaftVoterRequest => Request,
            AddRaftVoterResponse => Response,
            AllocateProducerIdsRequest => Request,
            AllocateProducerIdsResponse => Response,
            AlterClientQuotasRequest => Request,
            AlterClientQuotasResponse => Response,
            AlterConfigsRequest => Request,
            AlterConfigsResponse => Response,
            AlterPartitionReassignmentsRequest => Request,
            AlterPartitionReassignmentsResponse => Response,
            AlterPartitionRequest => Request,
            AlterPartitionResponse => Response,
            AlterReplicaLogDirsRequest => Request,
            AlterReplicaLogDirsResponse => Response,
            AlterShareGroupOffsetsRequest => Request,
            AlterShareGroupOffsetsResponse => Response,
            AlterUserScramCredentialsRequest => Request,
            AlterUserScramCredentialsResponse => Response,
            ApiVersionsRequest => Request,
            ApiVersionsResponse => Response,
            AssignReplicasToDirsRequest => Request,
            AssignReplicasToDirsResponse => Response,
            BeginQuorumEpochRequest => Request,
            BeginQuorumEpochResponse => Response,
            BrokerHeartbeatRequest => Request,
            BrokerHeartbeatResponse => Response,
            BrokerRegistrationRequest => Request,
            BrokerRegistrationResponse => Response,
            ConsumerGroupDescribeRequest => Request,
            ConsumerGroupDescribeResponse => Response,
            ConsumerProtocolAssignment => Framing,
            ConsumerProtocolSubscription => Framing,
            ControlRecordTypeSchema => Framing,
            ControllerRegistrationRequest => Request,
            ControllerRegistrationResponse => Response,
            CreateAclsRequest => Request,
            CreateAclsResponse => Response,
            CreateDelegationTokenRequest => Request,
            CreateDelegationTokenResponse => Response,
            CreatePartitionsRequest => Request,
            CreatePartitionsResponse => Response,
            CreateTopicsRequest => Request,
            CreateTopicsResponse => Response,
            DefaultPrincipalData => Framing,
            DeleteAclsRequest => Request,
            DeleteAclsResponse => Response,
            DeleteGroupsRequest => Request,
            DeleteGroupsResponse => Response,
            DeleteRecordsRequest => Request,
            DeleteRecordsResponse => Response,
            DeleteShareGroupOffsetsRequest => Request,
            DeleteShareGroupOffsetsResponse => Response,
            DeleteShareGroupStateRequest => Request,
            DeleteShareGroupStateResponse => Response,
            DescribeAclsRequest => Request,
            DescribeAclsResponse => Response,
            DescribeClientQuotasRequest => Request,
            DescribeClientQuotasResponse => Response,
            DescribeClusterRequest => Request,
            DescribeClusterResponse => Response,
            DescribeConfigsRequest => Request,
            DescribeDelegationTokenRequest => Request,
            DescribeDelegationTokenResponse => Response,
            DescribeGroupsRequest => Request,
            DescribeGroupsResponse => Response,
            DescribeLogDirsRequest => Request,
            DescribeLogDirsResponse => Response,
            DescribeProducersRequest => Request,
            DescribeProducersResponse => Response,
            DescribeQuorumRequest => Request,
            DescribeQuorumResponse => Response,
            DescribeShareGroupOffsetsRequest => Request,
            DescribeShareGroupOffsetsResponse => Response,
            DescribeTransactionsRequest => Request,
            DescribeTransactionsResponse => Response,
            DescribeUserScramCredentialsRequest => Request,
            DescribeUserScramCredentialsResponse => Response,
            ElectLeadersRequest => Request,
            ElectLeadersResponse => Response,
            EndQuorumEpochRequest => Request,
            EndQuorumEpochResponse => Response,
            EndTxnMarker => Framing,
            EndTxnRequest => Request,
            EndTxnResponse => Response,
            EnvelopeRequest => Request,
            EnvelopeResponse => Response,
            ExpireDelegationTokenRequest => Request,
            ExpireDelegationTokenResponse => Response,
            FetchRequest => Request,
            FetchResponse => Response,
            FetchSnapshotRequest => Request,
            FetchSnapshotResponse => Response,
            FindCoordinatorRequest => Request,
            FindCoordinatorResponse => Response,
            GetTelemetrySubscriptionsRequest => Request,
            GetTelemetrySubscriptionsResponse => Response,
            HeartbeatRequest => Request,
            HeartbeatResponse => Response,
            IncrementalAlterConfigsRequest => Request,
            IncrementalAlterConfigsResponse => Response,
            InitProducerIdRequest => Request,
            InitProducerIdResponse => Response,
            InitializeShareGroupStateRequest => Request,
            InitializeShareGroupStateResponse => Response,
            KRaftVersionRecord => Framing,
            LeaderChangeMessage => Framing,
            LeaveGroupRequest => Request,
            LeaveGroupResponse => Response,
            ListConfigResourcesRequest => Request,
            ListConfigResourcesResponse => Response,
            ListGroupsRequest => Request,
            ListGroupsResponse => Response,
            ListOffsetsRequest => Request,
            ListOffsetsResponse => Response,
            ListPartitionReassignmentsRequest => Request,
            ListPartitionReassignmentsResponse => Response,
            ListTransactionsRequest => Request,
            ListTransactionsResponse => Response,
            OffsetCommitRequest => Request,
            OffsetCommitResponse => Response,
            OffsetDeleteRequest => Request,
            OffsetDeleteResponse => Response,
            OffsetForLeaderEpochRequest => Request,
            OffsetForLeaderEpochResponse => Response,
            ProduceRequest => Request,
            ProduceResponse => Response,
            PushTelemetryRequest => Request,
            PushTelemetryResponse => Response,
            ReadShareGroupStateRequest => Request,
            ReadShareGroupStateResponse => Response,
            ReadShareGroupStateSummaryRequest => Request,
            ReadShareGroupStateSummaryResponse => Response,
            RemoveRaftVoterRequest => Request,
            RemoveRaftVoterResponse => Response,
            RenewDelegationTokenRequest => Request,
            RenewDelegationTokenResponse => Response,
            RequestHeader => Framing,
            ResponseHeader => Framing,
            SaslAuthenticateRequest => Request,
            SaslAuthenticateResponse => Response,
            SaslHandshakeRequest => Request,
            SaslHandshakeResponse => Response,
            ShareAcknowledgeRequest => Request,
            ShareAcknowledgeResponse => Response,
            ShareFetchResponse => Response,
            ShareGroupDescribeRequest => Request,
            ShareGroupDescribeResponse => Response,
            SnapshotFooterRecord => Framing,
            SnapshotHeaderRecord => Framing,
            StreamsGroupTopologyDescriptionUpdateRequest => Request,
            StreamsGroupTopologyDescriptionUpdateResponse => Response,
            SyncGroupRequest => Request,
            SyncGroupResponse => Response,
            TxnOffsetCommitRequest => Request,
            TxnOffsetCommitResponse => Response,
            UnregisterBrokerRequest => Request,
            UnregisterBrokerResponse => Response,
            UpdateFeaturesRequest => Request,
            UpdateFeaturesResponse => Response,
            UpdateRaftVoterRequest => Request,
            UpdateRaftVoterResponse => Response,
            VoteRequest => Request,
            VoteResponse => Response,
            VotersRecord => Framing,
            WriteShareGroupStateRequest => Request,
            WriteShareGroupStateResponse => Response,
            WriteTxnMarkersRequest => Request,
            WriteTxnMarkersResponse => Response,
        }
    };
}

macro_rules! declare_subject {
    ($($name:ident => $direction:ident,)*) => {
        /// One generated message, held as the concrete type the vector names.
        ///
        /// `PartialEq` without `Eq`: the client-quota messages carry an `f64`,
        /// which is not `Eq`, and this enum can be no stronger than its arms.
        #[derive(Clone, Debug, PartialEq)]
        pub enum Subject {
            $(
                #[doc = concat!("`", stringify!($name), "` body.")]
                $name($name),
            )*
        }
    };
}
subjects!(declare_subject);

/// Static protocol facts this repository generated for one message.
#[derive(Clone, Copy, Debug)]
pub struct Facts {
    /// Numeric Kafka API key, absent for a schema that frames a message.
    pub api_key: Option<i16>,
    /// Request or response direction, absent for the same reason.
    pub direction: Option<MessageDirection>,
    /// Inclusive supported version range.
    pub supported_versions: VersionRange,
}

/// Report what this repository believes about `message`.
macro_rules! declare_facts {
    ($($name:ident => $direction:ident,)*) => {
        /// Report what this repository believes about `message`.
        pub fn facts(message: &str) -> Result<Facts, String> {
            match message {
                $(
                    stringify!($name) => Ok(Facts {
                        api_key: direction_api_key!($name, $direction),
                        direction: direction_of!($direction),
                        supported_versions: $name::SUPPORTED_VERSIONS,
                    }),
                )*
                _ => Err(unknown(message)),
            }
        }
    };
}

/// The API key constant lives on a different trait per direction.
macro_rules! direction_api_key {
    ($name:ident, Request) => {
        Some(<$name as KafkaRequest>::API_KEY.value())
    };
    ($name:ident, Response) => {
        Some(<$name as KafkaResponse>::API_KEY.value())
    };
    // A framing schema implements neither direction trait, which is exactly
    // what "answers to no API key" means in the type system.
    ($name:ident, Framing) => {
        None
    };
}

macro_rules! direction_of {
    (Request) => {
        Some(MessageDirection::Request)
    };
    (Response) => {
        Some(MessageDirection::Response)
    };
    (Framing) => {
        None
    };
}

subjects!(declare_facts);

/// Report whether this repository encodes `message` flexibly at `version`.
macro_rules! declare_is_flexible {
    ($($name:ident => $direction:ident,)*) => {
        /// Report whether this repository encodes `message` flexibly at `version`.
        pub fn is_flexible(message: &str, version: i16) -> Result<bool, String> {
            let version = ApiVersion::new(version);
            match message {
                $(stringify!($name) => Ok($name::is_flexible(version)),)*
                _ => Err(unknown(message)),
            }
        }
    };
}

subjects!(declare_is_flexible);

impl Subject {
    /// Build the message a vector describes from its canonical JSON value.
    pub fn from_vector(vector: &Vector) -> Result<Self, String> {
        let mut fields = Fields::new(&vector.name, &vector.json_value)?;
        let subject = match vector.message.as_str() {
            "ApiVersionsRequest" => {
                Self::ApiVersionsRequest(api_versions_request(&mut fields, vector)?)
            }
            "SaslHandshakeRequest" => {
                Self::SaslHandshakeRequest(sasl_handshake_request(&mut fields)?)
            }
            "SaslHandshakeResponse" => {
                Self::SaslHandshakeResponse(sasl_handshake_response(&mut fields)?)
            }
            other => return Err(unknown(other)),
        };

        json_value::Fields::finish(fields)?;
        Ok(subject)
    }

    /// Decode one message body, rejecting trailing bytes.
    pub fn decode(message: &str, version: i16, bytes: Bytes) -> Result<Self, String> {
        let version = ApiVersion::new(version);
        let limits = DecodeLimits::default();
        macro_rules! declare_decode {
            ($($name:ident => $direction:ident,)*) => {
                match message {
                    $(
                        stringify!($name) => {
                            $name::decode_from_bytes(bytes, version, limits).map(Self::$name)
                        }
                    )*
                    other => return Err(unknown(other)),
                }
            };
        }
        subjects!(declare_decode).map_err(|error| error.to_string())
    }

    /// Encode this message at `version`.
    pub fn encode(&self, version: i16) -> Result<Bytes, String> {
        let version = ApiVersion::new(version);
        macro_rules! declare_encode {
            ($($name:ident => $direction:ident,)*) => {
                match self {
                    $(Self::$name(message) => message.encode_to_bytes(version),)*
                }
            };
        }
        subjects!(declare_encode).map_err(|error| error.to_string())
    }
}

fn unknown(message: &str) -> String {
    format!("no generated Rust type for message `{message}`")
}
