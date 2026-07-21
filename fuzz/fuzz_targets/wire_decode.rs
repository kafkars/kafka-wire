//! Fuzzes primitive, length, array, string, varint, and tagged-field decoding.

#![no_main]

use kafka_wire_core::{Bytes, DecodeLimits, Decoder};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let input = Bytes::copy_from_slice(data);
    let limits = DecodeLimits::default();

    macro_rules! decode {
        ($body:expr) => {
            if let Ok(mut decoder) = Decoder::new(input.clone(), limits) {
                let _ = $body(&mut decoder);
            }
        };
    }

    decode!(Decoder::read_bool);
    decode!(Decoder::read_i64);
    decode!(Decoder::read_float64);
    decode!(Decoder::read_varint);
    decode!(Decoder::read_varlong);
    decode!(Decoder::read_unsigned_varint);
    decode!(Decoder::read_unsigned_varlong);
    decode!(Decoder::read_string);
    decode!(Decoder::read_compact_nullable_string);
    decode!(Decoder::read_bytes);
    decode!(Decoder::read_compact_nullable_bytes);
    decode!(Decoder::read_tagged_fields);

    if let Ok(mut decoder) = Decoder::new(input, limits) {
        if let Ok(count) = decoder.read_array_len() {
            let _ = decoder.read_vec(count, Decoder::read_i8);
        }
    }
});
