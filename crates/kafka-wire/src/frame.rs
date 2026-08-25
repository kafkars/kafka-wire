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
use kafka_wire_core::{ApiVersion, EncodeError, Encoder, KafkaEncode, StrBytes};

use crate::{KafkaRequest, RequestHeader, request_header_version, response_header_version};

/// The four bytes a frame reserves for its own length.
const LENGTH_PREFIX: usize = 4;

/// Exact transport-facing facts for one outbound request frame.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RequestFrameMeasure {
    /// Complete bytes written on the wire, including the length prefix.
    pub wire_bytes: usize,
    /// Header version required to decode the corresponding response.
    pub response_header_version: ApiVersion,
}

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

/// Measures one request frame without allocating its output buffer.
///
/// The returned byte count includes Kafka's four-byte length prefix, while
/// `limits` continues to apply to the header and message bytes counted by that
/// prefix. The correlation ID is absent because every value occupies the same
/// four bytes on the wire.
pub fn measure_request<R>(
    request: &R,
    version: ApiVersion,
    client_id: Option<&StrBytes>,
    limits: OutboundFrameLimits,
) -> Result<RequestFrameMeasure, EncodeError>
where
    R: KafkaRequest + KafkaEncode,
{
    let header = request_header::<R>(0, client_id.cloned(), version);
    Ok(preflight_request(&header, request, version, limits)?.measure)
}

/// Encodes one request as a complete wire frame, returning the bytes written.
///
/// Exact header and body sizes are computed before reserving or writing. The
/// frame is rejected before reserving its output or materializing a tagged
/// payload when it exceeds either Kafka's signed 32-bit prefix or the caller's
/// outbound budget.
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
    let header = request_header::<R>(correlation_id, client_id, version);
    let preflight = preflight_request(&header, request, version, limits)?;

    let start = buffer.len();
    buffer.reserve(preflight.measure.wire_bytes);
    buffer.extend_from_slice(&[0; LENGTH_PREFIX]);
    let actual = {
        let mut encoder = Encoder::new(buffer);
        header.encode(&mut encoder, preflight.request_header_version)?;
        request.encode(&mut encoder, version)?;
        encoder.len()
    };
    if actual != preflight.body_bytes {
        return Err(EncodeError::SizeMismatch {
            predicted: preflight.body_bytes,
            actual,
        });
    }
    buffer[start..start + LENGTH_PREFIX].copy_from_slice(&preflight.length_prefix);
    Ok(buffer.len() - start)
}

struct RequestFramePreflight {
    measure: RequestFrameMeasure,
    request_header_version: ApiVersion,
    body_bytes: usize,
    length_prefix: [u8; LENGTH_PREFIX],
}

fn request_header<R>(
    correlation_id: i32,
    client_id: Option<StrBytes>,
    version: ApiVersion,
) -> RequestHeader
where
    R: KafkaRequest,
{
    RequestHeader {
        request_api_key: R::API_KEY.value(),
        request_api_version: version.value(),
        correlation_id,
        client_id,
        ..RequestHeader::default()
    }
}

fn preflight_request<R>(
    header: &RequestHeader,
    request: &R,
    version: ApiVersion,
    limits: OutboundFrameLimits,
) -> Result<RequestFramePreflight, EncodeError>
where
    R: KafkaRequest + KafkaEncode,
{
    crate::message::ensure_encode_version::<R>(version)?;
    let request_header_version = ApiVersion::new(request_header_version(R::is_flexible(version)));
    let header_bytes = header.encoded_len(request_header_version)?;
    let request_bytes = request.encoded_len(version)?;
    let body_bytes = header_bytes
        .checked_add(request_bytes)
        .ok_or(EncodeError::FrameTooLarge { bytes: usize::MAX })?;
    let prefix =
        i32::try_from(body_bytes).map_err(|_| EncodeError::FrameTooLarge { bytes: body_bytes })?;
    if body_bytes > limits.max_frame_bytes {
        return Err(EncodeError::FrameLimitExceeded {
            actual: body_bytes,
            limit: limits.max_frame_bytes,
        });
    }
    let wire_bytes = LENGTH_PREFIX
        .checked_add(body_bytes)
        .ok_or(EncodeError::FrameTooLarge { bytes: body_bytes })?;
    let response_header_version = ApiVersion::new(response_header_version(
        R::API_KEY,
        version,
        R::is_flexible(version),
    ));

    Ok(RequestFramePreflight {
        measure: RequestFrameMeasure {
            wire_bytes,
            response_header_version,
        },
        request_header_version,
        body_bytes,
        length_prefix: prefix.to_be_bytes(),
    })
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
