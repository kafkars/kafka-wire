//! Public version-negotiation metadata follows the compiler's unstable policy.
//!
//! Scenario: the one unstable request in the pinned corpus remains explicitly
//! usable at its highest version while default negotiation stops one version
//! lower, and typed and reflected metadata agree.

#![allow(clippy::unwrap_used)]

use kafka_wire::{
    INIT_PRODUCER_ID_REQUEST_DESCRIPTOR, InitProducerIdRequest, KafkaMessage, KafkaRequest,
    MessageDescriptor, MessageDirection, SASL_HANDSHAKE_REQUEST_DESCRIPTOR, SaslHandshakeRequest,
};
use kafka_wire_core::{ApiVersion, KafkaEncode, VersionRange};

#[test]
fn init_producer_id_keeps_v6_explicit_but_negotiates_to_v5() {
    assert!(InitProducerIdRequest::supports(ApiVersion::new(6)));
    assert_eq!(
        InitProducerIdRequest::latest_stable_version(),
        Some(ApiVersion::new(5))
    );
    InitProducerIdRequest::default()
        .encode_to_bytes(ApiVersion::new(6))
        .unwrap();

    assert_eq!(
        INIT_PRODUCER_ID_REQUEST_DESCRIPTOR.latest_stable_version(),
        Some(ApiVersion::new(5))
    );
}

#[test]
fn stable_requests_negotiate_to_their_supported_maximum() {
    assert_eq!(
        SaslHandshakeRequest::latest_stable_version(),
        Some(SaslHandshakeRequest::SUPPORTED_VERSIONS.max())
    );
    assert_eq!(
        SASL_HANDSHAKE_REQUEST_DESCRIPTOR.latest_stable_version(),
        Some(SaslHandshakeRequest::SUPPORTED_VERSIONS.max())
    );
}

#[test]
fn a_sole_unstable_version_has_no_default_negotiation_candidate() {
    let descriptor = MessageDescriptor::new(
        9_000,
        "OnlyUnstableRequest",
        MessageDirection::Request,
        VersionRange::new(0, 0),
        None,
        true,
    );

    assert_eq!(descriptor.latest_stable_version(), None);
}
