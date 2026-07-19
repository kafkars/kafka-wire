//! `ApiVersions` request vectors cover legacy omission and flexible fields.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire::ApiVersionsRequest;
use kafka_wire_core::{ApiVersion, DecodeLimits, KafkaDecode, KafkaEncode, StrBytes};

#[test]
fn version_zero_has_an_empty_body() {
    let request = ApiVersionsRequest::default();

    let bytes = request.encode_to_bytes(ApiVersion::new(0)).unwrap();

    assert!(bytes.is_empty());
}

#[test]
fn version_three_uses_compact_strings_and_tag_count() {
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = StrBytes::from("acme");
    request.client_software_version = StrBytes::from("1.0");
    let expected = Bytes::from_static(b"\x05acme\x041.0\x00");

    let bytes = request.encode_to_bytes(ApiVersion::new(3)).unwrap();
    let decoded = ApiVersionsRequest::decode_from_bytes(
        bytes.clone(),
        ApiVersion::new(3),
        DecodeLimits::default(),
    )
    .unwrap();

    assert_eq!(bytes, expected);
    assert_eq!(decoded, request);
}

#[test]
fn version_five_includes_nullable_cluster_and_node_identity() {
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = StrBytes::from("acme");
    request.client_software_version = StrBytes::from("1.0");
    let expected = Bytes::from_static(b"\x05acme\x041.0\x00\xff\xff\xff\xff\x00");

    let bytes = request.encode_to_bytes(ApiVersion::new(5)).unwrap();

    assert_eq!(bytes, expected);
}
