//! Fuzzes JSONC comment stripping and raw Kafka schema deserialization.

#![no_main]

use kafka_wire_schema::{SourceFile, parse_jsonc};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(source) = SourceFile::from_bytes("fuzz.json", data.to_vec()) {
        let _ = parse_jsonc(&source);
    }
});
