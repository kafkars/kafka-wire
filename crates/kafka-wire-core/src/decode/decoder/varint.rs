//! Signed (zigzag) and unsigned base-128 variable-length integer decoding.
//!
//! This module owns the `VARINT` (i32), `VARLONG` (i64), and unsigned-varlong
//! (u64) readers. Unlike `read_unsigned_varint`, these reject a non-canonical
//! (overlong) encoding: a value that could have fit in fewer bytes is malformed,
//! so exactly one byte string decodes to each value. Each reader also enforces
//! its maximum byte length and rejects a final byte whose high bits would spill
//! past the target integer width.

use super::super::DecodeError;
use super::Decoder;

/// Maximum bytes in a canonical 32-bit varint (`ceil(32 / 7)`).
const VARINT_MAX_BYTES: usize = 5;

/// Maximum bytes in a canonical 64-bit varint (`ceil(64 / 7)`).
const VARLONG_MAX_BYTES: usize = 10;

impl Decoder {
    /// Reads a signed 32-bit varint (canonical unsigned varint, then un-zigzag).
    pub fn read_varint(&mut self) -> Result<i32, DecodeError> {
        let offset = self.offset();
        let raw = self.read_base128(VARINT_MAX_BYTES, offset)?;
        let raw = u32::try_from(raw).map_err(|_| DecodeError::MalformedVarint { offset })?;
        Ok(zigzag_decode_i32(raw))
    }

    /// Reads a signed 64-bit varlong (canonical unsigned varlong, then un-zigzag).
    pub fn read_varlong(&mut self) -> Result<i64, DecodeError> {
        let offset = self.offset();
        let raw = self.read_base128(VARLONG_MAX_BYTES, offset)?;
        Ok(zigzag_decode_i64(raw))
    }

    /// Reads an unsigned 64-bit varlong, up to ten canonical bytes.
    pub fn read_unsigned_varlong(&mut self) -> Result<u64, DecodeError> {
        let offset = self.offset();
        self.read_base128(VARLONG_MAX_BYTES, offset)
    }

    /// Reads a canonical little-endian base-128 group sequence into a `u64`.
    ///
    /// Rejects three malformed shapes with `MalformedVarint`: a sequence that
    /// never terminates within `max_bytes`; a final byte whose payload bits
    /// exceed the 64-bit width; and an overlong encoding, whose terminating byte
    /// is a redundant zero that a canonical writer would never emit.
    fn read_base128(&mut self, max_bytes: usize, offset: usize) -> Result<u64, DecodeError> {
        let mut value = 0_u64;
        let mut shift = 0_u32;

        for index in 0..max_bytes {
            let byte = self.take(1)?[0];
            let payload = u64::from(byte & 0x7f);

            let available = 64_u32.saturating_sub(shift);
            if available < 7 && payload >> available != 0 {
                return Err(DecodeError::MalformedVarint { offset });
            }
            value |= payload << shift;

            if byte & 0x80 == 0 {
                if index > 0 && byte == 0 {
                    return Err(DecodeError::MalformedVarint { offset });
                }
                return Ok(value);
            }
            shift += 7;
        }

        Err(DecodeError::MalformedVarint { offset })
    }
}

/// Maps an unsigned zigzag representation back to its signed 32-bit value.
///
/// The transform is `(n >> 1) ^ -(n & 1)` computed without a lossy `as` cast:
/// the low bit is broadcast to a full mask, and the reinterpretation between the
/// unsigned and signed domains goes through the byte representation.
fn zigzag_decode_i32(value: u32) -> i32 {
    let magnitude = value >> 1;
    let mask = (value & 1).wrapping_neg();
    i32::from_ne_bytes((magnitude ^ mask).to_ne_bytes())
}

/// Maps an unsigned zigzag representation back to its signed 64-bit value.
fn zigzag_decode_i64(value: u64) -> i64 {
    let magnitude = value >> 1;
    let mask = (value & 1).wrapping_neg();
    i64::from_ne_bytes((magnitude ^ mask).to_ne_bytes())
}
