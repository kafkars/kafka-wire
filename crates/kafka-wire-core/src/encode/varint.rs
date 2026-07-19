//! Signed (zigzag) and unsigned base-128 variable-length integer encoding.
//!
//! This module owns the `VARINT` (i32), `VARLONG` (i64), and unsigned-varlong
//! (u64) writers. They are named distinctly from `write_unsigned_varint` because
//! the wire meaning differs: the signed forms zigzag-map their value first so
//! small-magnitude negatives stay short. Unsigned varlong exists so a ten-byte
//! `u64` can be written where the five-byte `u32` writer cannot reach.

use super::{EncodeError, EncodeTarget, Encoder};

impl<T: EncodeTarget> Encoder<T> {
    /// Writes a signed 32-bit varint (zigzag, then unsigned varint).
    #[inline]
    pub fn write_varint(&mut self, value: i32) -> Result<(), EncodeError> {
        self.write_unsigned_varint(zigzag_encode_i32(value))
    }

    /// Writes a signed 64-bit varlong (zigzag, then unsigned varlong).
    #[inline]
    pub fn write_varlong(&mut self, value: i64) -> Result<(), EncodeError> {
        self.write_unsigned_varlong(zigzag_encode_i64(value))
    }

    /// Writes an unsigned 64-bit varlong, up to ten bytes.
    pub fn write_unsigned_varlong(&mut self, mut value: u64) -> Result<(), EncodeError> {
        let mut encoded = [0_u8; 10];
        let mut length = 0_usize;

        loop {
            let low = u8::try_from(value & 0x7f).map_err(|_| EncodeError::LengthOverflow {
                kind: "unsigned varlong",
                length: usize::try_from(value).unwrap_or(usize::MAX),
                maximum: usize::MAX,
            })?;
            value >>= 7;
            encoded[length] = if value == 0 { low } else { low | 0x80 };
            length += 1;
            if value == 0 {
                break;
            }
        }

        self.write_raw_slice(&encoded[..length])
    }
}

/// Maps a signed 32-bit integer to its unsigned zigzag representation.
///
/// The transform is `(n << 1) ^ (n >> 31)` computed without a lossy `as` cast:
/// the sign bit is broadcast by an arithmetic shift, and the reinterpretation
/// between the signed and unsigned domains goes through the byte representation.
fn zigzag_encode_i32(value: i32) -> u32 {
    let doubled = value << 1;
    let sign = value >> 31;
    u32::from_ne_bytes((doubled ^ sign).to_ne_bytes())
}

/// Maps a signed 64-bit integer to its unsigned zigzag representation.
fn zigzag_encode_i64(value: i64) -> u64 {
    let doubled = value << 1;
    let sign = value >> 63;
    u64::from_ne_bytes((doubled ^ sign).to_ne_bytes())
}
