//! Public version-negotiation metadata follows the compiler's unstable policy.
//!
//! Scenario: the one unstable request in the pinned corpus remains explicitly
//! usable at its highest version while default negotiation stops one version
//! lower, and typed and reflected metadata agree.

#![allow(clippy::unwrap_used)]

use kafka_wire::{
    API_DESCRIPTORS, ApiDescriptor, INIT_PRODUCER_ID_API_DESCRIPTOR, InitProducerIdRequest,
    KafkaMessage, KafkaRequest, MESSAGE_DESCRIPTORS, MessageDescriptor, MessageDirection,
    SASL_HANDSHAKE_API_DESCRIPTOR, SaslHandshakeRequest,
};
use kafka_wire_core::{ApiVersion, KafkaEncode, VersionRange};

#[test]
fn init_producer_id_keeps_v6_explicit_but_negotiates_to_v5() {
    assert!(InitProducerIdRequest::supports(ApiVersion::new(6)));
    assert_eq!(
        INIT_PRODUCER_ID_API_DESCRIPTOR.latest_stable_version(),
        Some(ApiVersion::new(5))
    );
    InitProducerIdRequest::default()
        .encode_to_bytes(ApiVersion::new(6))
        .unwrap();

    assert_eq!(
        *InitProducerIdRequest::API_DESCRIPTOR,
        INIT_PRODUCER_ID_API_DESCRIPTOR
    );
}

#[test]
fn stable_requests_negotiate_to_their_supported_maximum() {
    assert_eq!(
        SASL_HANDSHAKE_API_DESCRIPTOR.latest_stable_version(),
        Some(SaslHandshakeRequest::SUPPORTED_VERSIONS.max())
    );
}

#[test]
fn a_sole_unstable_version_has_no_default_negotiation_candidate() {
    const REQUEST: MessageDescriptor = MessageDescriptor::new(
        9_000,
        "OnlyUnstableRequest",
        MessageDirection::Request,
        VersionRange::new(0, 0),
        None,
    );
    const RESPONSE: MessageDescriptor = MessageDescriptor::new(
        9_000,
        "OnlyUnstableResponse",
        MessageDirection::Response,
        VersionRange::new(0, 0),
        None,
    );
    const API: ApiDescriptor = ApiDescriptor::new(
        9_000,
        &REQUEST,
        &RESPONSE,
        VersionRange::new(0, 0),
        None,
        true,
    );

    assert_eq!(API.latest_stable_version(), None);
}

#[test]
fn every_reflected_api_is_one_consistent_pair() {
    assert_eq!(API_DESCRIPTORS.len(), 90);
    assert_eq!(MESSAGE_DESCRIPTORS.len(), API_DESCRIPTORS.len() * 2);

    for (index, api) in API_DESCRIPTORS.iter().enumerate() {
        assert_eq!(api.request.api_key, api.api_key);
        assert_eq!(api.response.api_key, api.api_key);
        assert_eq!(api.request.direction, MessageDirection::Request);
        assert_eq!(api.response.direction, MessageDirection::Response);
        assert_eq!(api.request.supported_versions, api.supported_versions);
        assert_eq!(api.response.supported_versions, api.supported_versions);
        assert_eq!(api.request.flexible_versions, api.flexible_versions);
        assert_eq!(api.response.flexible_versions, api.flexible_versions);
        if let Some(next) = API_DESCRIPTORS.get(index + 1) {
            assert!(api.api_key.value() < next.api_key.value());
        }
    }
}
