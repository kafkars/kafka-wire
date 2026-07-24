//! The four codecs a batch's records payload may be compressed with.
//!
//! The batch header is never compressed — only the records that follow it — so a
//! broker can route a batch whose codec it does not implement. That is why the
//! codec lives in the attributes rather than being inferred from the payload.
//!
//! **Both directions are held to Apache Kafka, by two different instruments.**
//! Decompression is held to Kafka's bytes: the corpus carries a batch Kafka
//! compressed with each codec, and decoding one must yield exactly the records
//! of its uncompressed twin.
//!
//! Compression cannot be held to bytes, and does not need to be. Byte-identical
//! output would require this crate's compressor to agree with Java's `Deflater`,
//! `zstd-jni`, `lz4-java`, and `snappy-java` down to each encoder's internal
//! choices; asserting that would be asserting a coincidence. The property a
//! producer actually needs is that the broker can READ what it wrote, and that
//! is a question only the broker can answer. `RecordOracle --verify` asks it:
//! every codec's payload is re-encoded here, handed to Kafka's own
//! `MemoryRecords` reader, and `spec/records/verified.json` records the records
//! Kafka got back. `kafka-wire-conformance` holds them to the records the batch
//! started with.
//!
//! What remains unproven is narrower than it used to be, and worth naming: the
//! compression *level* and internal choices are this crate's, so a payload here
//! is legal and readable rather than identical to Java's.

use std::{fmt, io::Read as _};

use bytes::Bytes;
use kafka_wire_core::EncodeError;

use crate::attributes::Compression;
use crate::error::RecordError;

/// Xerial's framing: an 8-byte magic, then two `int32` versions, then blocks.
///
/// Kafka's snappy is this format and not the standard snappy frame format that
/// `snap`'s own `FrameDecoder` implements. The two are different container
/// formats around the same block codec, so reaching for the obvious API produces
/// a payload no broker can read — and one that this crate would happily read
/// back, which is exactly why the corpus decides it.
pub(super) const XERIAL_MAGIC: [u8; 8] = [0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00];

/// Zstd's supported streaming window range in the linked implementation.
const ZSTD_MIN_WINDOW_LOG: u32 = 10;
const ZSTD_MAX_WINDOW_LOG: u32 = 31;

impl Compression {
    /// Decompresses one records payload.
    pub(crate) fn decompress(
        self,
        payload: Bytes,
        per_batch_limit: usize,
    ) -> Result<Bytes, RecordError> {
        match self {
            Self::None => checked_uncompressed(payload, per_batch_limit),
            Self::Gzip => read_bounded(
                "gzip",
                flate2::read::GzDecoder::new(payload.as_ref()),
                per_batch_limit,
            ),
            Self::Snappy => decompress_xerial(payload.as_ref(), per_batch_limit),
            Self::Lz4 => read_bounded(
                "lz4",
                lz4_flex::frame::FrameDecoder::new(payload.as_ref()),
                per_batch_limit,
            ),
            Self::Zstd => {
                let mut decoder = zstd::stream::read::Decoder::new(payload.as_ref())
                    .map_err(|error| Self::failed("zstd", &error))?;
                decoder
                    .window_log_max(zstd_window_log(per_batch_limit))
                    .map_err(|error| Self::failed("zstd", &error))?;
                read_bounded("zstd", decoder, per_batch_limit)
            }
        }
    }

    pub(super) fn failed(codec: &'static str, error: &dyn fmt::Display) -> RecordError {
        RecordError::CompressionFailed {
            codec,
            detail: error.to_string(),
        }
    }
}

pub(crate) fn zstd_window_log(limit: usize) -> u32 {
    let bytes = limit.max(1);
    let ceiling = usize::BITS - bytes.saturating_sub(1).leading_zeros();
    ceiling.clamp(ZSTD_MIN_WINDOW_LOG, ZSTD_MAX_WINDOW_LOG)
}

fn checked_uncompressed(payload: Bytes, limit: usize) -> Result<Bytes, RecordError> {
    if payload.len() > limit {
        return Err(RecordError::DecompressionLimitExceeded {
            codec: "uncompressed",
            limit,
        });
    }
    Ok(payload)
}

fn read_bounded(
    codec: &'static str,
    mut reader: impl std::io::Read,
    limit: usize,
) -> Result<Bytes, RecordError> {
    let mut out = Vec::with_capacity(limit.min(8 * 1_024));
    reader
        .by_ref()
        .take(u64::try_from(limit).unwrap_or(u64::MAX))
        .read_to_end(&mut out)
        .map_err(|error| Compression::failed(codec, &error))?;
    let mut overflow = [0_u8; 1];
    if reader
        .read(&mut overflow)
        .map_err(|error| Compression::failed(codec, &error))?
        != 0
    {
        return Err(RecordError::DecompressionLimitExceeded { codec, limit });
    }
    Ok(Bytes::from(out))
}

fn decompress_xerial(payload: &[u8], limit: usize) -> Result<Bytes, RecordError> {
    let header = XERIAL_MAGIC.len() + 8;
    if payload.len() < header || payload[..XERIAL_MAGIC.len()] != XERIAL_MAGIC {
        return Err(RecordError::CompressionFailed {
            codec: "snappy",
            detail: "payload does not open with the xerial magic Kafka writes".to_owned(),
        });
    }

    let mut out = Vec::new();
    let mut rest = &payload[header..];
    while !rest.is_empty() {
        let (length, block) =
            rest.split_at_checked(4)
                .ok_or_else(|| RecordError::CompressionFailed {
                    codec: "snappy",
                    detail: "a xerial block length was cut short".to_owned(),
                })?;
        let length = u32::from_be_bytes([length[0], length[1], length[2], length[3]]) as usize;
        let (block, tail) =
            block
                .split_at_checked(length)
                .ok_or_else(|| RecordError::CompressionFailed {
                    codec: "snappy",
                    detail: format!("a xerial block claims {length} bytes past the payload"),
                })?;
        let expanded = snap::raw::decompress_len(block)
            .map_err(|error| Compression::failed("snappy", &error))?;
        let total =
            out.len()
                .checked_add(expanded)
                .ok_or(RecordError::DecompressionLimitExceeded {
                    codec: "snappy",
                    limit,
                })?;
        if total > limit {
            return Err(RecordError::DecompressionLimitExceeded {
                codec: "snappy",
                limit,
            });
        }
        let mut decoder = snap::raw::Decoder::new();
        out.extend_from_slice(
            &decoder
                .decompress_vec(block)
                .map_err(|error| Compression::failed("snappy", &error))?,
        );
        rest = tail;
    }
    Ok(Bytes::from(out))
}

pub(crate) fn xerial_block_length(length: usize) -> Result<u32, EncodeError> {
    u32::try_from(length).map_err(|_| EncodeError::LengthOverflow {
        kind: "xerial snappy block",
        length,
        maximum: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
    })
}
