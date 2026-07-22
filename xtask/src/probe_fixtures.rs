//! Runtime scenario sources installed into the generated-code scratch crate.
//!
//! This file owns the behavioral assertions themselves. Scratch-tree layout
//! and filesystem writes remain in the probe module.

/// Runtime proof that positional decode locals preserve both sibling values.
pub(crate) const ADVERSARIAL_DECODE_TEST: &str = "//! Generated decode locals preserve field identity at runtime.\n\
     \n\
     use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, Decoder, KafkaDecode};\n\
     use protocol_probe::adversarial_decode::AdversarialDecodeRequest;\n\
     \n\
     #[test]\n\
     fn sibling_names_decode_from_their_own_wire_positions() {\n\
     \x20   let input = Bytes::from_static(&[\n\
     \x20       0, 0, 0, 11,\n\
     \x20       0, 0, 0, 22,\n\
     \x20   ]);\n\
     \x20   let mut decoder = Decoder::new(input, DecodeLimits::default()).unwrap();\n\
     \x20   let decoded = AdversarialDecodeRequest::decode(\n\
     \x20       &mut decoder,\n\
     \x20       ApiVersion::new(0),\n\
     \x20   )\n\
     \x20   .unwrap();\n\
     \n\
     \x20   assert_eq!(decoded.version, 11);\n\
     \x20   assert_eq!(decoded.version_value, 22);\n\
     \x20   decoder.finish().unwrap();\n\
     }\n";

/// Runtime proof that recursive defaults and equality compare floats by bits.
pub(crate) const ADVERSARIAL_DEFAULTS_TEST: &str = "//! Nested protocol defaults preserve exact float payloads.\n\
     \n\
     use kafka_wire_core::{ApiVersion, DecodeLimits, EncodeError, KafkaDecode, KafkaEncode};\n\
     use protocol_probe::{\n\
     \x20   ProtocolEq,\n\
     \x20   adversarial_defaults::AdversarialDefaultsRequest,\n\
     };\n\
     \n\
     #[test]\n\
     fn nested_defaults_and_round_trip_equality_are_bit_exact() {\n\
     \x20   let legacy = ApiVersion::new(0);\n\
     \x20   let flexible = ApiVersion::new(1);\n\
     \x20   let value = AdversarialDefaultsRequest::default();\n\
     \x20   assert!(value.protocol_eq(&AdversarialDefaultsRequest::default()));\n\
     \x20   assert_eq!(value.encode_to_bytes(legacy).unwrap().len(), 8);\n\
     \n\
     \x20   let encoded_default = value.encode_to_bytes(flexible).unwrap();\n\
     \x20   assert_eq!(encoded_default.last(), Some(&0), \"default known tags must be absent\");\n\
     \n\
     \x20   let mut changed_zero = AdversarialDefaultsRequest::default();\n\
     \x20   changed_zero.gated_negative_zero.value = 0.0;\n\
     \x20   assert!(matches!(\n\
     \x20       changed_zero.encode_to_bytes(legacy),\n\
     \x20       Err(EncodeError::FieldNotRepresentable { field: \"GatedNegativeZero\", .. })\n\
     \x20   ));\n\
     \x20   assert!(!changed_zero.protocol_eq(&value));\n\
     \n\
     \x20   let mut changed_tag = AdversarialDefaultsRequest::default();\n\
     \x20   changed_tag.tagged_negative_zero.value = 0.0;\n\
     \x20   let encoded = changed_tag.encode_to_bytes(flexible).unwrap();\n\
     \x20   assert!(encoded.ends_with(&[1, 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0]));\n\
     \x20   let decoded = AdversarialDefaultsRequest::decode_from_bytes(\n\
     \x20       encoded, flexible, DecodeLimits::default(),\n\
     \x20   )\n\
     \x20   .unwrap();\n\
     \x20   assert!(decoded.protocol_eq(&changed_tag));\n\
     \n\
     \x20   let mut changed_deep = AdversarialDefaultsRequest::default();\n\
     \x20   changed_deep.deep.inner.value = f64::from_bits(0x7ff8_0000_0000_0042);\n\
     \x20   assert!(matches!(\n\
     \x20       changed_deep.encode_to_bytes(legacy),\n\
     \x20       Err(EncodeError::FieldNotRepresentable { field: \"Deep\", .. })\n\
     \x20   ));\n\
     }\n";
