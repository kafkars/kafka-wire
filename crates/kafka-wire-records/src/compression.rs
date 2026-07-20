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

use std::{
    fmt,
    io::{Read as _, Write as _},
};

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
const XERIAL_MAGIC: [u8; 8] = [0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00];

impl Compression {
    /// Decompresses one records payload.
    pub(crate) fn decompress(self, payload: &[u8], limit: usize) -> Result<Vec<u8>, RecordError> {
        match self {
            Self::None => checked_uncompressed(payload, limit),
            Self::Gzip => read_bounded("gzip", flate2::read::GzDecoder::new(payload), limit),
            Self::Snappy => decompress_xerial(payload, limit),
            Self::Lz4 => read_bounded("lz4", lz4_flex::frame::FrameDecoder::new(payload), limit),
            Self::Zstd => {
                let decoder = zstd::stream::read::Decoder::new(payload)
                    .map_err(|error| Self::failed("zstd", &error))?;
                read_bounded("zstd", decoder, limit)
            }
        }
    }

    /// Compresses one records payload.
    ///
    /// The output is a payload Apache Kafka reads back unchanged, not a
    /// reproduction of what Java would have emitted for the same input. See the
    /// module note for which instrument establishes which.
    pub(crate) fn compress(self, records: &[u8]) -> Result<Vec<u8>, RecordError> {
        match self {
            Self::None => Ok(records.to_vec()),
            Self::Gzip => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder
                    .write_all(records)
                    .and_then(|()| encoder.finish())
                    .map_err(|error| Self::failed("gzip", &error))
            }
            Self::Snappy => compress_xerial(records),
            Self::Lz4 => {
                let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
                encoder
                    .write_all(records)
                    .map_err(|error| Self::failed("lz4", &error))?;
                encoder
                    .finish()
                    .map_err(|error| Self::failed("lz4", &std::io::Error::other(error)))
            }
            Self::Zstd => {
                zstd::stream::encode_all(records, 3).map_err(|error| Self::failed("zstd", &error))
            }
        }
    }

    fn failed(codec: &'static str, error: &dyn fmt::Display) -> RecordError {
        RecordError::CompressionFailed {
            codec,
            detail: error.to_string(),
        }
    }
}

fn checked_uncompressed(payload: &[u8], limit: usize) -> Result<Vec<u8>, RecordError> {
    if payload.len() > limit {
        return Err(RecordError::DecompressionLimitExceeded {
            codec: "uncompressed",
            limit,
        });
    }
    Ok(payload.to_vec())
}

fn read_bounded(
    codec: &'static str,
    reader: impl std::io::Read,
    limit: usize,
) -> Result<Vec<u8>, RecordError> {
    let observed = limit.saturating_add(1);
    let mut reader = reader.take(u64::try_from(observed).unwrap_or(u64::MAX));
    let mut out = Vec::new();
    reader
        .read_to_end(&mut out)
        .map_err(|error| Compression::failed(codec, &error))?;
    if out.len() > limit {
        return Err(RecordError::DecompressionLimitExceeded { codec, limit });
    }
    Ok(out)
}

fn decompress_xerial(payload: &[u8], limit: usize) -> Result<Vec<u8>, RecordError> {
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
    Ok(out)
}

fn compress_xerial(records: &[u8]) -> Result<Vec<u8>, RecordError> {
    let mut out = Vec::from(XERIAL_MAGIC);
    // Version and compatible-version, both 1, exactly as Kafka writes them.
    out.extend_from_slice(&1_i32.to_be_bytes());
    out.extend_from_slice(&1_i32.to_be_bytes());
    let block = snap::raw::Encoder::new()
        .compress_vec(records)
        .map_err(|error| Compression::failed("snappy", &error))?;
    let length = xerial_block_length(block.len())?;
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(&block);
    Ok(out)
}

pub(crate) fn xerial_block_length(length: usize) -> Result<u32, EncodeError> {
    u32::try_from(length).map_err(|_| EncodeError::LengthOverflow {
        kind: "xerial snappy block",
        length,
        maximum: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
    })
}
