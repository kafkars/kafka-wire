//! Compression framing failures remain errors rather than plausible output.
//!
//! These scenarios exercise size decisions that cannot be reached by allocating
//! a multi-gigabyte test buffer.

use kafka_wire_core::EncodeError;

use crate::compression::xerial_block_length;

#[test]
fn the_largest_xerial_block_length_is_representable() {
    assert_eq!(
        xerial_block_length(usize::try_from(u32::MAX).unwrap_or(usize::MAX)),
        Ok(u32::MAX)
    );
}

#[cfg(target_pointer_width = "64")]
#[test]
fn an_xerial_block_past_u32_is_rejected() {
    let length = usize::try_from(u64::from(u32::MAX) + 1).unwrap_or(usize::MAX);
    assert!(matches!(
        xerial_block_length(length),
        Err(EncodeError::LengthOverflow {
            kind: "xerial snappy block",
            ..
        })
    ));
}
