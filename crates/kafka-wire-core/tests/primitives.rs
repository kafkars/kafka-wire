//! Exact byte stories for primitive legacy and flexible encodings.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire_core::{
    ApiVersion, DecodeLimits, Decoder, Encoder, KafkaEncode, StrBytes, TaggedField, TaggedFields,
};

#[test]
fn compact_strings_and_tagged_fields_round_trip() {
    #[derive(Debug)]
    struct Example {
        value: StrBytes,
        tags: TaggedFields,
    }

    impl KafkaEncode for Example {
        fn encode<T: kafka_wire_core::EncodeTarget>(
            &self,
            encoder: &mut Encoder<T>,
            _version: ApiVersion,
        ) -> Result<(), kafka_wire_core::EncodeError> {
            encoder.write_compact_string(&self.value)?;
            encoder.write_tagged_fields(&self.tags)
        }
    }

    let tags =
        TaggedFields::from_sorted(vec![TaggedField::new(3, Bytes::from_static(b"xy"))]).unwrap();
    let value = Example {
        value: StrBytes::from("raft"),
        tags,
    };

    let bytes = value.encode_to_bytes(ApiVersion::new(0)).unwrap();
    assert_eq!(bytes.as_ref(), b"\x05raft\x01\x03\x02xy");

    let mut decoder = Decoder::new(bytes, DecodeLimits::default()).unwrap();
    assert_eq!(decoder.read_compact_string().unwrap().as_str(), "raft");
    assert_eq!(decoder.read_tagged_fields().unwrap().len(), 1);
    decoder.finish().unwrap();
}

#[test]
fn decoder_rejects_string_lengths_above_the_budget() {
    let mut limits = DecodeLimits::default();
    limits.max_string_bytes = 2;
    let mut decoder = Decoder::new(Bytes::from_static(b"\x00\x03abc"), limits).unwrap();

    let error = decoder.read_string().unwrap_err();
    assert!(matches!(
        error,
        kafka_wire_core::DecodeError::LimitExceeded {
            kind: "string",
            length: 3,
            limit: 2,
            ..
        }
    ));
}
