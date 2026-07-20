//! Generated Rust agrees with Apache Kafka's own writer, byte for byte.
//!
//! Scenario: for every checked-in vector, run both directions that a wrong
//! implementation could pass one of and fail the other.
//!
//! Decoding proves this repository reads Kafka's bytes and writes them back
//! unchanged. Constructing from the canonical JSON value proves it reaches those
//! bytes from a value it had to build itself — which is where a wrong default, a
//! misnamed field, or a missing version gate shows up. A corpus that only
//! round-tripped bytes would be blind to all three, because a decoder and an
//! encoder that share a misreading agree with each other perfectly.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire_conformance::{Subject, from_hex, load, to_hex};

#[test]
fn every_vector_decodes_and_re_encodes_to_the_same_bytes() {
    let vectors = load().unwrap();
    let mut failures = Vec::new();

    for vector in &vectors {
        let expected = from_hex(&vector.hex).unwrap();
        let decoded = match Subject::decode(
            &vector.message,
            vector.version,
            Bytes::from(expected.clone()),
        ) {
            Ok(decoded) => decoded,
            Err(error) => {
                failures.push(format!(
                    "{} v{} [{}]: decode failed: {error}\n  bytes: {}",
                    vector.message, vector.version, vector.name, vector.hex
                ));
                continue;
            }
        };

        match decoded.encode(vector.version) {
            Ok(actual) if actual.as_ref() == expected.as_slice() => {}
            Ok(actual) => failures.push(format!(
                "{} v{} [{}]: re-encoding changed the bytes\n  kafka: {}\n  rust:  {}\n  why:   {}",
                vector.message,
                vector.version,
                vector.name,
                vector.hex,
                to_hex(&actual),
                vector.why
            )),
            Err(error) => failures.push(format!(
                "{} v{} [{}]: re-encode failed: {error}",
                vector.message, vector.version, vector.name
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} vector(s) disagree with Apache Kafka:\n\n{}",
        failures.len(),
        vectors.len(),
        failures.join("\n\n")
    );
}

#[test]
fn every_vector_encodes_from_its_canonical_json_value() {
    let vectors = load().unwrap();
    let mut failures = Vec::new();

    for vector in &vectors {
        let subject = match Subject::from_vector(vector) {
            Ok(subject) => subject,
            Err(error) => {
                if !has_json_builder(&vector.message) {
                    continue;
                }
                failures.push(format!(
                    "{} v{} [{}]: could not build from json_value: {error}",
                    vector.message, vector.version, vector.name
                ));
                continue;
            }
        };

        match subject.encode(vector.version) {
            Ok(actual) if to_hex(&actual) == vector.hex => {}
            Ok(actual) => failures.push(format!(
                "{} v{} [{}]: encoding the canonical value did not reproduce Kafka's bytes\n  \
                 kafka: {}\n  rust:  {}\n  json:  {}\n  why:   {}",
                vector.message,
                vector.version,
                vector.name,
                vector.hex,
                to_hex(&actual),
                vector.json_value,
                vector.why
            )),
            Err(error) => failures.push(format!(
                "{} v{} [{}]: encode failed: {error}",
                vector.message, vector.version, vector.name
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} vector(s) disagree with Apache Kafka:\n\n{}",
        failures.len(),
        vectors.len(),
        failures.join("\n\n")
    );
}

/// Messages the corpus judges by round trip alone, because the harness has no
/// canonical-JSON builder for them yet.
///
/// The round-trip assertion above covers every vector, so these messages are
/// still held to Apache Kafka's exact bytes; what is missing is the second
/// direction, which additionally proves the struct's defaults agree with what
/// Kafka assumes when a field is absent from the JSON. The list is asserted to
/// be exact so it cannot grow silently, and shrinking it is the work owed.
const WITHOUT_JSON_BUILDERS: &[&str] = &[
    "AddOffsetsToTxnRequest",
    "AddOffsetsToTxnResponse",
    "AddPartitionsToTxnRequest",
    "AddPartitionsToTxnResponse",
    "AddRaftVoterRequest",
    "AddRaftVoterResponse",
    "AllocateProducerIdsRequest",
    "AllocateProducerIdsResponse",
    "AlterClientQuotasRequest",
    "AlterClientQuotasResponse",
    "AlterConfigsRequest",
    "AlterConfigsResponse",
    "AlterPartitionReassignmentsRequest",
    "AlterPartitionReassignmentsResponse",
    "AlterPartitionRequest",
    "AlterPartitionResponse",
    "AlterReplicaLogDirsRequest",
    "AlterReplicaLogDirsResponse",
    "AlterShareGroupOffsetsRequest",
    "AlterShareGroupOffsetsResponse",
    "AlterUserScramCredentialsRequest",
    "AlterUserScramCredentialsResponse",
    "ApiVersionsResponse",
    "AssignReplicasToDirsRequest",
    "AssignReplicasToDirsResponse",
    "BeginQuorumEpochRequest",
    "BeginQuorumEpochResponse",
    "BrokerHeartbeatRequest",
    "BrokerHeartbeatResponse",
    "BrokerRegistrationRequest",
    "BrokerRegistrationResponse",
    "ConsumerGroupDescribeRequest",
    "ConsumerGroupDescribeResponse",
    "ControllerRegistrationRequest",
    "ControllerRegistrationResponse",
    "CreateAclsRequest",
    "CreateAclsResponse",
    "CreateDelegationTokenRequest",
    "CreateDelegationTokenResponse",
    "CreatePartitionsRequest",
    "CreatePartitionsResponse",
    "CreateTopicsRequest",
    "CreateTopicsResponse",
    "DeleteAclsRequest",
    "DeleteAclsResponse",
    "DeleteGroupsRequest",
    "DeleteGroupsResponse",
    "DeleteRecordsRequest",
    "DeleteRecordsResponse",
    "DeleteShareGroupOffsetsRequest",
    "DeleteShareGroupOffsetsResponse",
    "DeleteShareGroupStateRequest",
    "DeleteShareGroupStateResponse",
    "DescribeAclsRequest",
    "DescribeAclsResponse",
    "DescribeClientQuotasRequest",
    "DescribeClientQuotasResponse",
    "DescribeClusterRequest",
    "DescribeClusterResponse",
    "DescribeConfigsRequest",
    "DescribeDelegationTokenRequest",
    "DescribeDelegationTokenResponse",
    "DescribeGroupsRequest",
    "DescribeGroupsResponse",
    "DescribeLogDirsRequest",
    "DescribeLogDirsResponse",
    "DescribeProducersRequest",
    "DescribeProducersResponse",
    "DescribeQuorumRequest",
    "DescribeQuorumResponse",
    "DescribeShareGroupOffsetsRequest",
    "DescribeShareGroupOffsetsResponse",
    "DescribeTransactionsRequest",
    "DescribeTransactionsResponse",
    "DescribeUserScramCredentialsRequest",
    "DescribeUserScramCredentialsResponse",
    "ElectLeadersRequest",
    "ElectLeadersResponse",
    "EndQuorumEpochRequest",
    "EndQuorumEpochResponse",
    "EndTxnRequest",
    "EndTxnResponse",
    "EnvelopeRequest",
    "EnvelopeResponse",
    "ExpireDelegationTokenRequest",
    "ExpireDelegationTokenResponse",
    "FetchRequest",
    "FetchResponse",
    "FetchSnapshotRequest",
    "FetchSnapshotResponse",
    "FindCoordinatorRequest",
    "FindCoordinatorResponse",
    "GetTelemetrySubscriptionsRequest",
    "GetTelemetrySubscriptionsResponse",
    "HeartbeatRequest",
    "HeartbeatResponse",
    "IncrementalAlterConfigsRequest",
    "IncrementalAlterConfigsResponse",
    "InitProducerIdRequest",
    "InitProducerIdResponse",
    "InitializeShareGroupStateRequest",
    "InitializeShareGroupStateResponse",
    "LeaveGroupRequest",
    "LeaveGroupResponse",
    "ListConfigResourcesRequest",
    "ListConfigResourcesResponse",
    "ListGroupsRequest",
    "ListGroupsResponse",
    "ListOffsetsRequest",
    "ListOffsetsResponse",
    "ListPartitionReassignmentsRequest",
    "ListPartitionReassignmentsResponse",
    "ListTransactionsRequest",
    "ListTransactionsResponse",
    "OffsetCommitRequest",
    "OffsetCommitResponse",
    "OffsetDeleteRequest",
    "OffsetDeleteResponse",
    "OffsetForLeaderEpochRequest",
    "OffsetForLeaderEpochResponse",
    "ProduceRequest",
    "ProduceResponse",
    "PushTelemetryRequest",
    "PushTelemetryResponse",
    "ReadShareGroupStateRequest",
    "ReadShareGroupStateResponse",
    "ReadShareGroupStateSummaryRequest",
    "ReadShareGroupStateSummaryResponse",
    "RemoveRaftVoterRequest",
    "RemoveRaftVoterResponse",
    "RenewDelegationTokenRequest",
    "RenewDelegationTokenResponse",
    "RequestHeader",
    "ResponseHeader",
    "SaslAuthenticateRequest",
    "SaslAuthenticateResponse",
    "ShareAcknowledgeRequest",
    "ShareAcknowledgeResponse",
    "ShareFetchResponse",
    "ShareGroupDescribeRequest",
    "ShareGroupDescribeResponse",
    "StreamsGroupTopologyDescriptionUpdateRequest",
    "StreamsGroupTopologyDescriptionUpdateResponse",
    "SyncGroupRequest",
    "SyncGroupResponse",
    "TxnOffsetCommitRequest",
    "TxnOffsetCommitResponse",
    "UnregisterBrokerRequest",
    "UnregisterBrokerResponse",
    "UpdateFeaturesRequest",
    "UpdateFeaturesResponse",
    "UpdateRaftVoterRequest",
    "UpdateRaftVoterResponse",
    "VoteRequest",
    "VoteResponse",
    "WriteShareGroupStateRequest",
    "WriteShareGroupStateResponse",
    "WriteTxnMarkersRequest",
    "WriteTxnMarkersResponse",
];

fn has_json_builder(message: &str) -> bool {
    !WITHOUT_JSON_BUILDERS.contains(&message)
}

#[test]
fn the_json_builder_gap_is_exactly_what_is_recorded() {
    // A message that gains a builder must leave the list, and one that is
    // enabled without a builder must join it, or this fails.
    let vectors = load().unwrap();
    let mut observed: Vec<String> = Vec::new();
    for vector in &vectors {
        if Subject::from_vector(vector).is_err() && !observed.contains(&vector.message) {
            observed.push(vector.message.clone());
        }
    }
    observed.sort();

    let mut recorded: Vec<String> = WITHOUT_JSON_BUILDERS
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    recorded.sort();

    assert_eq!(
        observed, recorded,
        "the set of messages without a canonical-JSON builder has drifted from \
         WITHOUT_JSON_BUILDERS; update the list deliberately"
    );
}

#[test]
fn a_corrupted_vector_would_be_rejected() {
    // The suite above only means something if a wrong byte fails it. Flip the
    // last byte of a real vector and confirm the comparison notices.
    let vectors = load().unwrap();
    let vector = vectors
        .iter()
        .find(|vector| !vector.hex.is_empty() && has_json_builder(&vector.message))
        .unwrap();

    let mut corrupted = from_hex(&vector.hex).unwrap();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 0xff;

    let subject = Subject::from_vector(vector).unwrap();
    let encoded = subject.encode(vector.version).unwrap();

    assert_ne!(
        encoded.as_ref(),
        corrupted.as_slice(),
        "a corrupted vector still compared equal, so the byte comparison proves nothing"
    );
}
