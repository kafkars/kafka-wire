//! Fuzzes generated request/response codecs at every supported API version.

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

fn round_trip<T: KafkaDecode + KafkaEncode>(body: &[u8], version: ApiVersion) {
    let bytes = Bytes::copy_from_slice(body);
    if let Ok(value) = T::decode_from_bytes(bytes, version, DecodeLimits::default()) {
        let _ = value.encode_to_bytes(version);
    }
}
