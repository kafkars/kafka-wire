//! Active known tag IDs remain schema-owned when their typed values are default.
//!
//! These scenarios exercise public construction and a cross-version forward
//! where the same number changes from unknown to known.

use bytes::BytesMut;
use kafka_wire::{ApiVersionsResponse, BrokerHeartbeatRequest, ProtocolEq};
use kafka_wire_core::{
    ApiVersion, Bytes, DecodeLimits, EncodeError, KafkaDecode, KafkaEncode, TaggedField,
    TaggedFields,
};

fn retained(tag: u32, payload: Bytes) -> TaggedFields {
    TaggedFields::from_sorted(vec![TaggedField::new(tag, payload)])
        .unwrap_or_else(|error| panic!("one retained tag is ordered: {error}"))
}

#[test]
fn api_versions_default_epoch_still_owns_tag_one() {
    let mut value = ApiVersionsResponse::default();
    value.unknown_tagged_fields = retained(1, Bytes::copy_from_slice(&42_i64.to_be_bytes()));
    let mut output = BytesMut::from(&b"prior frame"[..]);
    let before = output.clone();

    assert_eq!(
        value.encode_into(&mut output, ApiVersion::new(3)),
        Err(EncodeError::KnownTagConflict {
            message: "ApiVersionsResponse",
            tag: 1,
            version: ApiVersion::new(3),
        })
    );
    assert_eq!(output, before, "tag conflict wrote partial frame bytes");
}

#[test]
fn broker_heartbeat_claims_tag_zero_only_after_it_becomes_known() {
    let mut value = BrokerHeartbeatRequest::default();
    value.unknown_tagged_fields = retained(0, Bytes::from_static(&[0xaa]));

    let version_zero = ApiVersion::new(0);
    let encoded = value
        .encode_to_bytes(version_zero)
        .unwrap_or_else(|error| panic!("tag zero is unknown in v0: {error}"));
    let decoded =
        BrokerHeartbeatRequest::decode_from_bytes(encoded, version_zero, DecodeLimits::default())
            .unwrap_or_else(|error| panic!("forwarded v0 tag must decode: {error}"));
    assert!(decoded.protocol_eq(&value));

    let mut output = BytesMut::from(&b"prior frame"[..]);
    let before = output.clone();
    assert_eq!(
        value.encode_into(&mut output, ApiVersion::new(1)),
        Err(EncodeError::KnownTagConflict {
            message: "BrokerHeartbeatRequest",
            tag: 0,
            version: ApiVersion::new(1),
        })
    );
    assert_eq!(output, before, "active tag conflict wrote frame bytes");
}
