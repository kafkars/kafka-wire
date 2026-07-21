//! Fuzzes the generated ApiVersions request/response pair across versions 0-5.

#![no_main]

use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};
use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, KafkaDecode, KafkaEncode};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((&selector, body)) = data.split_first() else {
        return;
    };
    let version = ApiVersion::new(i16::from(selector % 6));
    round_trip::<ApiVersionsRequest>(body, version);
    round_trip::<ApiVersionsResponse>(body, version);
});

fn round_trip<T>(body: &[u8], version: ApiVersion)
where
    T: KafkaDecode + KafkaEncode + std::fmt::Debug + PartialEq,
{
    let bytes = Bytes::copy_from_slice(body);
    if let Ok(value) = T::decode_from_bytes(bytes, version, DecodeLimits::default()) {
        let encoded = value
            .encode_to_bytes(version)
            .unwrap_or_else(|error| panic!("decoded value failed to encode: {error}"));
        let decoded = T::decode_from_bytes(encoded.clone(), version, DecodeLimits::default())
            .unwrap_or_else(|error| panic!("encoded value failed to decode: {error}"));
        assert_eq!(decoded, value, "decode-encode-decode changed the value");
        let canonical = decoded
            .encode_to_bytes(version)
            .unwrap_or_else(|error| panic!("round-tripped value failed to encode: {error}"));
        assert_eq!(canonical, encoded, "encoding did not stabilize");
    }
}
