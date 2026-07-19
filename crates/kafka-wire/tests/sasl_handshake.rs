//! `SaslHandshake` vectors prove paired legacy request and response generation.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire::{RequestResponsePair, SaslHandshakeRequest, SaslHandshakeResponse};
use kafka_wire_core::{ApiVersion, DecodeLimits, KafkaDecode, KafkaEncode, StrBytes};

#[test]
fn request_matches_the_legacy_string_layout() {
    let mut request = SaslHandshakeRequest::default();
    request.mechanism = StrBytes::from("PLAIN");
    let expected = Bytes::from_static(b"\x00\x05PLAIN");

    assert_eq!(
        request.encode_to_bytes(ApiVersion::new(1)).unwrap(),
        expected
    );
}

#[test]
fn response_round_trips_the_string_array() {
    let mut response = SaslHandshakeResponse::default();
    response.mechanisms = vec![StrBytes::from("PLAIN"), StrBytes::from("SCRAM-SHA-256")];

    let bytes = response.encode_to_bytes(ApiVersion::new(1)).unwrap();
    let decoded = SaslHandshakeResponse::decode_from_bytes(
        bytes,
        ApiVersion::new(1),
        DecodeLimits::default(),
    )
    .unwrap();

    assert_eq!(decoded, response);
}

#[test]
fn request_exposes_its_generated_response_type() {
    fn assert_pair<R: RequestResponsePair<Response = SaslHandshakeResponse>>() {}

    assert_pair::<SaslHandshakeRequest>();
}
