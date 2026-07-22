//! Whole-frame encoding: the length prefix, the header, and the body.
//!
//! A Kafka connection carries `int32 size` followed by that many bytes of
//! header and message. This file owns assembling those three parts and nothing
//! about what any of them contain: the header and the body are generated types
//! that encode themselves, and which header version frames them is generated
//! policy from `spec/overrides/`.
//!
//! It deliberately owns no socket, no correlation-id allocation, and no
//! retry or version negotiation. Those are a client's concerns, above this
//! crate's boundary.

use bytes::BytesMut;
use kafka_wire_core::{ApiVersion, EncodeError, KafkaEncode, StrBytes};

use crate::{KafkaRequest, RequestHeader, request_header_version, response_header_version};

/// The four bytes a frame reserves for its own length.
const LENGTH_PREFIX: usize = 4;

/// Caller-owned byte budget for one outbound Kafka frame body.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OutboundFrameLimits {
    max_frame_bytes: usize,
}

impl OutboundFrameLimits {
    /// Creates a limit for the bytes counted by Kafka's frame-length prefix.
    pub const fn new(max_frame_bytes: usize) -> Self {
        Self { max_frame_bytes }
    }

    /// Returns the configured header-plus-message byte ceiling.
    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }
}

/// Encodes one request as a complete wire frame, returning the bytes written.
///
/// Exact header and body sizes are computed before reserving or writing. The
/// frame is rejected before allocation when it exceeds either Kafka's signed
/// 32-bit prefix or the caller's outbound budget.
///
/// On failure the buffer is truncated back to where it started, so a rejected
/// request leaves no partial frame for the next write to append to.
pub fn encode_request<R>(
    buffer: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<StrBytes>,
    request: &R,
    version: ApiVersion,
    limits: OutboundFrameLimits,
) -> Result<usize, EncodeError>
where
    R: KafkaRequest + KafkaEncode,
{
    let start = buffer.len();
    match encode_request_inner(buffer, correlation_id, client_id, request, version, limits) {
        Ok(written) => Ok(written),
        Err(error) => {
            buffer.truncate(start);
            Err(error)
        }
    }
}

fn encode_request_inner<R>(
    buffer: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<StrBytes>,
    request: &R,
    version: ApiVersion,
    limits: OutboundFrameLimits,
) -> Result<usize, EncodeError>
where
    R: KafkaRequest + KafkaEncode,
{
    let header_version = ApiVersion::new(request_header_version(R::is_flexible(version)));
    let header = RequestHeader {
        request_api_key: R::API_KEY.value(),
        request_api_version: version.value(),
        correlation_id,
        client_id,
        ..RequestHeader::default()
    };
    let header_len = header.encoded_len(header_version)?;
    let request_len = request.encoded_len(version)?;
    let body = header_len
        .checked_add(request_len)
        .ok_or(EncodeError::FrameTooLarge { bytes: usize::MAX })?;
    let size = i32::try_from(body).map_err(|_| EncodeError::FrameTooLarge { bytes: body })?;
    if body > limits.max_frame_bytes {
        return Err(EncodeError::FrameLimitExceeded {
            actual: body,
            limit: limits.max_frame_bytes,
        });
    }
    let frame_len = LENGTH_PREFIX
        .checked_add(body)
        .ok_or(EncodeError::FrameTooLarge { bytes: body })?;

    let start = buffer.len();
    buffer.reserve(frame_len);
    buffer.extend_from_slice(&[0; LENGTH_PREFIX]);
    header.encode_into(buffer, header_version)?;
    request.encode_into(buffer, version)?;

    let actual = buffer.len() - start - LENGTH_PREFIX;
    if actual != body {
        return Err(EncodeError::SizeMismatch {
            predicted: body,
            actual,
        });
    }
    buffer[start..start + LENGTH_PREFIX].copy_from_slice(&size.to_be_bytes());
    Ok(buffer.len() - start)
}

/// The response header version a reply to `R` at `version` is framed with.
///
/// Exposed because a client reads the header before it knows the body type, so
/// it needs this answer separately from any decode. An unsupported request
/// version is rejected before header policy is consulted.
pub fn response_header_version_for<R>(version: ApiVersion) -> Result<i16, EncodeError>
where
    R: KafkaRequest,
{
    crate::message::ensure_encode_version::<R>(version)?;
    Ok(response_header_version(
        R::API_KEY,
        version,
        R::is_flexible(version),
    ))
}
