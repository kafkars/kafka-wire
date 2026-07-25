//! Version-prefixed payloads carried by the classic consumer group protocol.
//!
//! `JoinGroup` metadata and `SyncGroup` assignments are Kafka `BYTES` fields
//! whose contents begin with an `int16` schema version followed by one generated
//! consumer-protocol body. This module owns that inner prefix only; the generated
//! request codecs continue to own the outer byte-string length.

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{
    ApiVersion, DecodeError, DecodeLimits, Decoder, EncodeError, Encoder, KafkaDecode, KafkaEncode,
};

use crate::{ConsumerProtocolAssignment, ConsumerProtocolSubscription};

const VERSION_PREFIX_BYTES: usize = size_of::<i16>();

/// Appends one version-prefixed classic consumer subscription payload.
///
/// On failure `buffer` is restored to its original length.
pub fn encode_consumer_protocol_subscription(
    buffer: &mut BytesMut,
    subscription: &ConsumerProtocolSubscription,
    version: ApiVersion,
) -> Result<usize, EncodeError> {
    encode_payload(buffer, subscription, version)
}

/// Decodes one complete version-prefixed classic consumer subscription payload.
///
/// The returned version is the exact prefix carried by the peer. Trailing bytes
/// and versions outside the generated subscription schema are rejected.
pub fn decode_consumer_protocol_subscription(
    payload: Bytes,
    limits: DecodeLimits,
) -> Result<(ApiVersion, ConsumerProtocolSubscription), DecodeError> {
    decode_payload(payload, limits)
}

/// Appends one version-prefixed classic consumer assignment payload.
///
/// On failure `buffer` is restored to its original length.
pub fn encode_consumer_protocol_assignment(
    buffer: &mut BytesMut,
    assignment: &ConsumerProtocolAssignment,
    version: ApiVersion,
) -> Result<usize, EncodeError> {
    encode_payload(buffer, assignment, version)
}

/// Decodes one complete version-prefixed classic consumer assignment payload.
///
/// The returned version is the exact prefix carried by the peer. Trailing bytes
/// and versions outside the generated assignment schema are rejected.
pub fn decode_consumer_protocol_assignment(
    payload: Bytes,
    limits: DecodeLimits,
) -> Result<(ApiVersion, ConsumerProtocolAssignment), DecodeError> {
    decode_payload(payload, limits)
}

fn encode_payload<M>(
    buffer: &mut BytesMut,
    message: &M,
    version: ApiVersion,
) -> Result<usize, EncodeError>
where
    M: KafkaEncode,
{
    let start = buffer.len();
    match encode_payload_inner(buffer, message, version) {
        Ok(written) => Ok(written),
        Err(error) => {
            buffer.truncate(start);
            Err(error)
        }
    }
}

fn encode_payload_inner<M>(
    buffer: &mut BytesMut,
    message: &M,
    version: ApiVersion,
) -> Result<usize, EncodeError>
where
    M: KafkaEncode,
{
    let body_bytes = message.encoded_len(version)?;
    let predicted =
        body_bytes
            .checked_add(VERSION_PREFIX_BYTES)
            .ok_or(EncodeError::LengthOverflow {
                kind: "consumer protocol payload",
                length: body_bytes,
                maximum: usize::MAX - VERSION_PREFIX_BYTES,
            })?;
    buffer.reserve(predicted);
    let actual = {
        let mut encoder = Encoder::new(buffer);
        encoder.write_i16(version.value())?;
        message.encode(&mut encoder, version)?;
        encoder.len()
    };
    if actual == predicted {
        Ok(actual)
    } else {
        Err(EncodeError::SizeMismatch { predicted, actual })
    }
}

fn decode_payload<M>(payload: Bytes, limits: DecodeLimits) -> Result<(ApiVersion, M), DecodeError>
where
    M: KafkaDecode,
{
    let mut decoder = Decoder::new(payload, limits)?;
    let version = ApiVersion::new(decoder.read_i16()?);
    let message = M::decode(&mut decoder, version)?;
    decoder.finish()?;
    Ok((version, message))
}
