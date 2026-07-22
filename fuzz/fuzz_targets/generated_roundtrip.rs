//! Fuzzes every generated request and response across every supported version.

#![no_main]

use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, KafkaDecode, KafkaEncode};
use kafka_wire::ProtocolEq;
use libfuzzer_sys::fuzz_target;

#[path = "../../crates/kafka-wire/src/generated/fuzz_roundtrip.rs"]
mod generated_dispatch;

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }
    let message_selector = u16::from_le_bytes([data[0], data[1]]);
    let version_selector = u16::from_le_bytes([data[2], data[3]]);
    generated_dispatch::dispatch(message_selector, version_selector, &data[4..]);
});

fn round_trip<T>(body: &[u8], version: ApiVersion)
where
    T: KafkaDecode + KafkaEncode + ProtocolEq + std::fmt::Debug,
{
    let bytes = Bytes::copy_from_slice(body);
    if let Ok(value) = T::decode_from_bytes(bytes, version, DecodeLimits::default()) {
        let encoded = value
            .encode_to_bytes(version)
            .unwrap_or_else(|error| panic!("decoded value failed to encode: {error}"));
        let decoded = T::decode_from_bytes(encoded.clone(), version, DecodeLimits::default())
            .unwrap_or_else(|error| panic!("encoded value failed to decode: {error}"));
        assert!(
            decoded.protocol_eq(&value),
            "decode-encode-decode changed protocol state"
        );
        let canonical = decoded
            .encode_to_bytes(version)
            .unwrap_or_else(|error| panic!("round-tripped value failed to encode: {error}"));
        assert_eq!(canonical, encoded, "encoding did not stabilize");
    }
}
