//! One record inside a batch, and its headers.
//!
//! Everything here is varint-framed and relative. A record states its own length
//! first, then deltas from the batch's base offset and base timestamp, then a
//! key and value whose lengths are zigzag varints so that null is `-1` and empty
//! is `0`. Those two are different frames and mean different things — a null
//! value is a tombstone — so nothing here may collapse them.

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{DecodeLimits, Decoder, EncodeError, EncodeTarget, Encoder, StrBytes};

use crate::error::RecordError;

/// One key/value pair attached to a record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordHeader {
    /// Header key. Never null on the wire, unlike the value.
    pub key: StrBytes,
    /// Header value, which may be absent.
    pub value: Option<Bytes>,
}

/// One record inside a batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    /// Reserved by the protocol; Kafka writes zero and ignores what it reads.
    pub attributes: i8,
    /// Milliseconds after the batch's base timestamp.
    pub timestamp_delta: i64,
    /// Offset relative to the batch's base offset.
    pub offset_delta: i32,
    /// Record key, absent when the producer supplied none.
    pub key: Option<Bytes>,
    /// Record value. Absent is a tombstone, and is not the same as empty.
    pub value: Option<Bytes>,
    /// Headers, in the order the producer attached them.
    pub headers: Vec<RecordHeader>,
}

impl Record {
    /// Reads one length-prefixed record from `decoder`.
    ///
    /// The declared body becomes a bounded child decoder before any field is
    /// read. A field cannot observe its successor record, and trailing child
    /// bytes still report the declared-versus-consumed disagreement.
    pub fn decode(decoder: &mut Decoder) -> Result<Self, RecordError> {
        let declared_offset = decoder.offset();
        let declared = decoder.read_varint()?;
        if declared < 0 {
            return Err(RecordError::NegativeRecordLength {
                length: declared,
                offset: declared_offset,
            });
        }
        let declared = usize::try_from(declared).map_err(|_| {
            RecordError::Wire(kafka_wire_core::DecodeError::LengthOverflow {
                kind: "record",
                offset: declared_offset,
            })
        })?;
        let mut body = decoder.take_child(declared)?;

        let attributes = body.read_i8()?;
        let timestamp_delta = body.read_varlong()?;
        let offset_delta = body.read_varint()?;
        let key = read_varint_bytes(&mut body, "record key")?;
        let value = read_varint_bytes(&mut body, "record value")?;

        let header_count_offset = body.offset();
        let header_count = body.read_varint()?;
        let header_count =
            usize::try_from(header_count).map_err(|_| RecordError::RecordSizeMismatch {
                declared,
                consumed: declared - body.remaining(),
            })?;
        body.check_collection_limit("record headers", header_count, header_count_offset)?;
        let mut headers = Vec::with_capacity(header_count.min(body.remaining()));
        for _ in 0..header_count {
            headers.push(RecordHeader::decode(&mut body)?);
        }

        let consumed = declared - body.remaining();
        if consumed != declared {
            return Err(RecordError::RecordSizeMismatch { declared, consumed });
        }
        Ok(Self {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        })
    }

    /// Writes this record, length prefix included.
    ///
    /// The body is laid out first because the length prefix precedes it and is
    /// itself a varint, so its width depends on what follows. This is the same
    /// buffer-then-prefix shape a tagged field needs, and for the same reason.
    pub fn encode<T: EncodeTarget>(&self, encoder: &mut Encoder<T>) -> Result<(), EncodeError> {
        let mut body = BytesMut::new();
        let mut inner = Encoder::new(&mut body);
        inner.write_i8(self.attributes)?;
        inner.write_varlong(self.timestamp_delta)?;
        inner.write_varint(self.offset_delta)?;
        write_varint_bytes(&mut inner, self.key.as_deref())?;
        write_varint_bytes(&mut inner, self.value.as_deref())?;
        inner.write_varint(i32::try_from(self.headers.len()).map_err(|_| {
            EncodeError::LengthOverflow {
                kind: "record headers",
                length: self.headers.len(),
                maximum: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
            }
        })?)?;
        for header in &self.headers {
            header.encode(&mut inner)?;
        }

        let length = i32::try_from(body.len()).map_err(|_| EncodeError::LengthOverflow {
            kind: "record",
            length: body.len(),
            maximum: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
        })?;
        encoder.write_varint(length)?;
        encoder.write_raw_slice(&body)
    }
}

impl RecordHeader {
    fn decode(decoder: &mut Decoder) -> Result<Self, RecordError> {
        let Some((key_length, key_prefix_offset)) = read_varint_length(decoder)? else {
            return Err(RecordError::NullHeaderKey);
        };
        let key = decoder.take_string_field("record header key", key_length, key_prefix_offset)?;
        Ok(Self {
            key,
            value: read_varint_bytes(decoder, "record header value")?,
        })
    }

    fn encode<T: EncodeTarget>(&self, encoder: &mut Encoder<T>) -> Result<(), EncodeError> {
        write_varint_bytes(encoder, Some(self.key.as_bytes()))?;
        write_varint_bytes(encoder, self.value.as_deref())
    }
}

/// Reads a zigzag-varint length-prefixed byte string, where `-1` is null.
fn read_varint_bytes(
    decoder: &mut Decoder,
    kind: &'static str,
) -> Result<Option<Bytes>, RecordError> {
    let Some((length, prefix_offset)) = read_varint_length(decoder)? else {
        return Ok(None);
    };
    decoder
        .take_byte_field(kind, length, prefix_offset)
        .map(Some)
        .map_err(RecordError::from)
}

fn read_varint_length(decoder: &mut Decoder) -> Result<Option<(usize, usize)>, RecordError> {
    let prefix_offset = decoder.offset();
    let length = decoder.read_varint()?;
    if length == -1 {
        return Ok(None);
    }
    if length < -1 {
        return Err(RecordError::InvalidRecordFieldLength { length });
    }
    let length = usize::try_from(length).unwrap_or(usize::MAX);
    Ok(Some((length, prefix_offset)))
}

/// Writes one, spelling absent as `-1` and empty as `0`.
fn write_varint_bytes<T: EncodeTarget>(
    encoder: &mut Encoder<T>,
    value: Option<&[u8]>,
) -> Result<(), EncodeError> {
    match value {
        None => encoder.write_varint(-1),
        Some(bytes) => {
            let length = i32::try_from(bytes.len()).map_err(|_| EncodeError::LengthOverflow {
                kind: "record field",
                length: bytes.len(),
                maximum: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
            })?;
            encoder.write_varint(length)?;
            encoder.write_raw_slice(bytes)
        }
    }
}

/// Appends `count` records to `buffer`, for the batch encoder.
pub(crate) fn encode_all(records: &[Record], buffer: &mut BytesMut) -> Result<(), EncodeError> {
    let mut encoder = Encoder::new(buffer);
    for record in records {
        record.encode(&mut encoder)?;
    }
    Ok(())
}

/// Reads exactly `count` records, refusing a payload that holds a different
/// number than the batch header promised.
pub(crate) fn decode_all(
    payload: Bytes,
    count: usize,
    limits: DecodeLimits,
) -> Result<Vec<Record>, RecordError> {
    let mut decoder = Decoder::new(payload, limits)?;
    let mut records = Vec::with_capacity(count.min(decoder.remaining()));
    for _ in 0..count {
        if decoder.remaining() == 0 {
            return Err(RecordError::RecordCountMismatch {
                declared: count,
                actual: records.len(),
            });
        }
        records.push(Record::decode(&mut decoder)?);
    }
    if decoder.remaining() != 0 {
        // Kafka's own reader stops at the declared count, so trailing bytes are
        // a peer writing more records than it counted. Naming it keeps a
        // truncated read from passing as a complete one.
        return Err(RecordError::RecordCountMismatch {
            declared: count,
            actual: records.len() + 1,
        });
    }
    Ok(records)
}
