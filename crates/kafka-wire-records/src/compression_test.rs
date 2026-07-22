//! Compression framing failures remain errors rather than plausible output.
//!
//! These scenarios exercise framing and decoder-memory decisions that ordinary
//! round trips cannot reach without impractical allocations.

use std::io::Write as _;

use bytes::Bytes;
use kafka_wire_core::EncodeError;

use crate::attributes::Compression;
use crate::compression::{xerial_block_length, zstd_window_log};
use crate::error::RecordError;

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

#[test]
fn zstd_window_budget_rounds_up_without_exceeding_the_codec_domain() {
    assert_eq!(zstd_window_log(0), 10);
    assert_eq!(zstd_window_log(1_024), 10);
    assert_eq!(zstd_window_log(1_025), 11);
    assert_eq!(zstd_window_log(100 * 1024 * 1024), 27);
    assert_eq!(zstd_window_log(usize::MAX), 31);
}

#[test]
fn zstd_refuses_an_oversized_window() -> Result<(), Box<dyn std::error::Error>> {
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3)?;
    encoder.window_log(20)?;
    encoder.write_all(b"one byte")?;
    let frame = encoder.finish()?;

    assert!(matches!(
        Compression::Zstd.decompress(Bytes::from(frame), 1_024),
        Err(RecordError::CompressionFailed { codec: "zstd", .. })
    ));
    Ok(())
}

#[test]
fn uncompressed_payloads_keep_their_existing_byte_owner() {
    let payload = Bytes::from_static(b"already bounded");
    let allocation = payload.as_ptr();
    let decoded = Compression::None
        .decompress(payload, 1_024)
        .unwrap_or_else(|error| panic!("uncompressed payload: {error}"));

    assert_eq!(decoded.as_ptr(), allocation);
}
