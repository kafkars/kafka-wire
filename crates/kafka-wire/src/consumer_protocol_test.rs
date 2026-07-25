//! Scenarios for classic consumer-protocol version envelopes.

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{ApiVersion, DecodeError, DecodeLimits, EncodeError, StrBytes, VersionRange};

use super::{
    ConsumerProtocolAssignment, ConsumerProtocolSubscription,
    consumer_protocol_assignment::TopicPartition as AssignmentTopicPartition,
    consumer_protocol_subscription::TopicPartition as SubscriptionTopicPartition,
    decode_consumer_protocol_assignment, decode_consumer_protocol_subscription,
    encode_consumer_protocol_assignment, encode_consumer_protocol_subscription,
};

#[test]
fn subscription_prefixes_broker_authored_v0_body_bytes() {
    let subscription = ConsumerProtocolSubscription {
        topics: vec![StrBytes::from("orders"), StrBytes::from("payments")],
        user_data: Some(Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef])),
        ..ConsumerProtocolSubscription::default()
    };
    let mut payload = BytesMut::from(&b"prior"[..]);

    let written =
        encode_consumer_protocol_subscription(&mut payload, &subscription, ApiVersion::new(0))
            .unwrap_or_else(|error| panic!("encode subscription: {error}"));

    let expected = [
        0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x06, b'o', b'r', b'd', b'e', b'r', b's', 0x00,
        0x08, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', 0x00, 0x00, 0x00, 0x04, 0xde, 0xad,
        0xbe, 0xef,
    ];
    assert_eq!(written, expected.len());
    assert_eq!(&payload[5..], expected);

    let (version, decoded) =
        decode_consumer_protocol_subscription(payload.freeze().slice(5..), DecodeLimits::default())
            .unwrap_or_else(|error| panic!("decode subscription: {error}"));
    assert_eq!(version, ApiVersion::new(0));
    assert_eq!(decoded, subscription);
}

#[test]
fn assignment_prefixes_broker_authored_v0_body_bytes() {
    let assignment = ConsumerProtocolAssignment {
        assigned_partitions: vec![AssignmentTopicPartition {
            topic: StrBytes::from("v"),
            partitions: vec![1],
        }],
        user_data: Some(Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef])),
    };
    let mut payload = BytesMut::new();

    encode_consumer_protocol_assignment(&mut payload, &assignment, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode assignment: {error}"));

    let expected = [
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, b'v', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef,
    ];
    assert_eq!(payload.as_ref(), expected);

    let (version, decoded) =
        decode_consumer_protocol_assignment(payload.freeze(), DecodeLimits::default())
            .unwrap_or_else(|error| panic!("decode assignment: {error}"));
    assert_eq!(version, ApiVersion::new(0));
    assert_eq!(decoded, assignment);
}

#[test]
fn subscription_preserves_the_exact_v3_prefix_and_fields() {
    let subscription = ConsumerProtocolSubscription {
        topics: vec![StrBytes::from("events")],
        user_data: None,
        owned_partitions: vec![SubscriptionTopicPartition {
            topic: StrBytes::from("events"),
            partitions: vec![2],
        }],
        generation_id: 7,
        rack_id: Some(StrBytes::from("rack-a")),
    };
    let mut payload = BytesMut::new();
    encode_consumer_protocol_subscription(&mut payload, &subscription, ApiVersion::new(3))
        .unwrap_or_else(|error| panic!("encode subscription: {error}"));

    assert_eq!(&payload[..2], &[0x00, 0x03]);
    let (version, decoded) =
        decode_consumer_protocol_subscription(payload.freeze(), DecodeLimits::default())
            .unwrap_or_else(|error| panic!("decode subscription: {error}"));
    assert_eq!(version, ApiVersion::new(3));
    assert_eq!(decoded, subscription);
}

#[test]
fn decode_rejects_trailing_bytes_after_the_body() {
    let mut payload = BytesMut::new();
    encode_consumer_protocol_assignment(
        &mut payload,
        &ConsumerProtocolAssignment::default(),
        ApiVersion::new(0),
    )
    .unwrap_or_else(|error| panic!("encode assignment: {error}"));
    payload.extend_from_slice(&[0xff]);

    assert_eq!(
        decode_consumer_protocol_assignment(payload.freeze(), DecodeLimits::default()),
        Err(DecodeError::TrailingBytes { remaining: 1 })
    );
}

#[test]
fn unsupported_encode_version_restores_the_callers_buffer() {
    let mut payload = BytesMut::from(&b"retained"[..]);

    assert_eq!(
        encode_consumer_protocol_subscription(
            &mut payload,
            &ConsumerProtocolSubscription::default(),
            ApiVersion::new(4),
        ),
        Err(EncodeError::UnsupportedVersion {
            message: "ConsumerProtocolSubscription",
            version: ApiVersion::new(4),
            supported: VersionRange::new(0, 3),
        })
    );
    assert_eq!(payload.as_ref(), b"retained");
}

#[test]
fn unsupported_decode_version_is_rejected_before_the_body() {
    assert_eq!(
        decode_consumer_protocol_assignment(
            Bytes::from_static(&[0x00, 0x04]),
            DecodeLimits::default(),
        ),
        Err(DecodeError::UnsupportedVersion {
            message: "ConsumerProtocolAssignment",
            version: ApiVersion::new(4),
            supported: VersionRange::new(0, 3),
        })
    );
}

#[test]
fn decode_limits_cover_the_prefix_and_body_as_one_payload() {
    let payload = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff]);
    let mut limits = DecodeLimits::default();
    limits.max_frame_bytes = payload.len() - 1;

    assert_eq!(
        decode_consumer_protocol_assignment(payload, limits),
        Err(DecodeError::LimitExceeded {
            kind: "frame",
            length: 10,
            limit: 9,
            offset: 0,
        })
    );
}
