//! Scenario: a request leaves this library as a complete, wire-legal frame.
//!
//! The header and body codecs are already held to Apache Kafka's own bytes by
//! the conformance corpus, so what is proven here is the part the corpus cannot
//! see: that a frame is a length prefix counting everything after it, followed
//! by the header, followed by the body, with the prefix filled in after the
//! fact. The body bytes below are quoted from the broker-authored vector for
//! `ApiVersionsRequest` v3 rather than invented here.

use bytes::BytesMut;
use kafka_wire::{
    ApiVersionsRequest, KafkaMessage, KafkaRequest, OutboundFrameLimits, encode_request,
    response_header_version_for,
};
use kafka_wire_core::{ApiKey, ApiVersion, EncodeError, StrBytes};

/// `ApiVersionsRequest` v3 `named_client`, as Apache Kafka encodes it.
const BODY_V3: &[u8] = &[0x05, 0x61, 0x63, 0x6d, 0x65, 0x04, 0x31, 0x2e, 0x30, 0x00];
const TEST_LIMITS: OutboundFrameLimits = OutboundFrameLimits::new(1024);

fn request() -> ApiVersionsRequest {
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = StrBytes::from("acme");
    request.client_software_version = StrBytes::from("1.0");
    request
}

#[test]
fn a_request_frame_is_a_length_prefix_then_header_then_body() {
    let mut buffer = BytesMut::new();
    let written = encode_request(
        &mut buffer,
        7,
        None,
        &request(),
        ApiVersion::new(3),
        TEST_LIMITS,
    )
    .unwrap_or_else(|error| panic!("v3 is supported: {error}"));

    // int32 api key 18, int16 version 3, int32 correlation 7, then a null
    // client id as a legacy int16 -1 — v2 pins ClientId to the legacy prefix so
    // a broker can read the header before it knows the version — then the
    // header's own empty tagged-field section.
    let header: &[u8] = &[
        0x00, 0x12, 0x00, 0x03, 0x00, 0x00, 0x00, 0x07, 0xff, 0xff, 0x00,
    ];
    let mut expected = Vec::new();
    let Ok(size) = u32::try_from(header.len() + BODY_V3.len()) else {
        panic!("the frame is far below u32::MAX")
    };
    expected.extend_from_slice(&size.to_be_bytes());
    expected.extend_from_slice(header);
    expected.extend_from_slice(BODY_V3);

    assert_eq!(buffer.as_ref(), expected.as_slice());
    assert_eq!(written, expected.len());
}

#[test]
fn the_length_prefix_counts_everything_after_itself() {
    let mut buffer = BytesMut::new();
    encode_request(
        &mut buffer,
        1,
        None,
        &request(),
        ApiVersion::new(3),
        TEST_LIMITS,
    )
    .unwrap_or_else(|error| panic!("v3 is supported: {error}"));

    let Ok(head) = <[u8; 4]>::try_from(&buffer[..4]) else {
        panic!("a frame always begins with its four-byte prefix")
    };
    let prefix = i32::from_be_bytes(head);
    let Ok(prefix) = usize::try_from(prefix) else {
        panic!("a written prefix is never negative")
    };
    assert_eq!(
        prefix,
        buffer.len() - 4,
        "the prefix must describe the bytes that follow it, not the whole frame"
    );
}

#[test]
fn a_rejected_request_leaves_no_partial_frame_behind() {
    // A buffer a client is pipelining into must not gain a half-written frame
    // when one request cannot be represented.
    let mut buffer = BytesMut::new();
    encode_request(
        &mut buffer,
        1,
        None,
        &request(),
        ApiVersion::new(3),
        TEST_LIMITS,
    )
    .unwrap_or_else(|error| panic!("v3 is supported: {error}"));
    let after_good = buffer.len();

    let refused = encode_request(
        &mut buffer,
        2,
        None,
        &request(),
        ApiVersion::new(99),
        TEST_LIMITS,
    );
    assert!(refused.is_err(), "v99 is outside the supported range");
    assert_eq!(
        buffer.len(),
        after_good,
        "the refused frame left bytes behind"
    );
}

#[test]
fn api_versions_keeps_the_legacy_response_header() {
    // The one reviewed exception in `spec/overrides/headers.toml`: every other
    // flexible response is framed with a v1 header, but ApiVersions v3+ answers
    // with v0 so a client can parse the reply before it has negotiated.
    assert_eq!(
        response_header_version_for::<ApiVersionsRequest>(ApiVersion::new(3)),
        Ok(0)
    );
    assert_eq!(
        response_header_version_for::<ApiVersionsRequest>(ApiVersion::new(0)),
        Ok(0)
    );
    assert_eq!(
        response_header_version_for::<ApiVersionsRequest>(ApiVersion::new(99)),
        Err(EncodeError::UnsupportedVersion {
            message: "ApiVersionsRequest",
            version: ApiVersion::new(99),
            supported: ApiVersionsRequest::SUPPORTED_VERSIONS,
        })
    );
    assert_eq!(
        <ApiVersionsRequest as KafkaRequest>::API_KEY,
        ApiKey::new(18)
    );
}

#[test]
fn an_outbound_budget_rejects_the_exact_size_before_writing() {
    let mut buffer = BytesMut::from(&b"prior frame"[..]);
    let before = buffer.clone();
    let error = encode_request(
        &mut buffer,
        7,
        None,
        &request(),
        ApiVersion::new(3),
        OutboundFrameLimits::new(0),
    );

    assert_eq!(
        error,
        Err(EncodeError::FrameLimitExceeded {
            actual: 11 + BODY_V3.len(),
            limit: 0,
        })
    );
    assert_eq!(buffer, before, "preflight rejection wrote frame bytes");
}
