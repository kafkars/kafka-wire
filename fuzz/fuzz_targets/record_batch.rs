//! Fuzzes record-batch framing, CRC, compression, records, and header decoding.

#![no_main]

use bytes::Bytes;
use kafka_wire_records::{RecordBatch, RecordDecodeLimits, RecordEncodeLimits};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut input = Bytes::copy_from_slice(data);
    if let Ok(batch) = RecordBatch::decode(&mut input, RecordDecodeLimits::default()) {
        let encoded = batch
            .encode_to_bytes(RecordEncodeLimits::default())
            .unwrap_or_else(|error| panic!("decoded batch failed to encode: {error}"));
        let mut encoded_input = encoded.clone();
        let decoded = RecordBatch::decode(&mut encoded_input, RecordDecodeLimits::default())
            .unwrap_or_else(|error| panic!("encoded batch failed to decode: {error}"));
        assert!(
            encoded_input.is_empty(),
            "encoded batch left trailing bytes"
        );
        assert_eq!(decoded, batch, "decode-encode-decode changed the batch");
        let canonical = decoded
            .encode_to_bytes(RecordEncodeLimits::default())
            .unwrap_or_else(|error| panic!("round-tripped batch failed to encode: {error}"));
        assert_eq!(canonical, encoded, "batch encoding did not stabilize");
    }
});
