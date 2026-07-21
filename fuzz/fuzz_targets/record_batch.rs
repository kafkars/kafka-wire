//! Fuzzes record-batch framing, CRC, compression, records, and header decoding.

#![no_main]

use bytes::Bytes;
use kafka_wire_records::{RecordBatch, RecordDecodeLimits};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut input = Bytes::copy_from_slice(data);
    if let Ok(batch) = RecordBatch::decode(&mut input, RecordDecodeLimits::default()) {
        let _ = batch.encode_to_bytes();
    }
});
