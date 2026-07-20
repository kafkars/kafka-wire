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

/// Encodes one request as a complete wire frame, returning the bytes written.
///
/// The length prefix counts everything after itself, so it cannot be written
/// until the header and body have been. The frame reserves the four bytes,
/// encodes into the buffer, and then fills them in — the same reserve-and-
/// backpatch a length-delimited format always needs.
///
/// On failure the buffer is truncated back to where it started, so a rejected
/// request leaves no partial frame for the next write to append to.
pub fn encode_request<R>(
    buffer: &mut BytesMut,
    correlation_id: i32,
    client_id: Option<StrBytes>,
    request: &R,
    version: ApiVersion,
) -> Result<usize, EncodeError>
where
    R: KafkaRequest + KafkaEncode,
{
    let start = buffer.len();
    match encode_request_inner(buffer, correlation_id, client_id, request, version) {
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
) -> Result<usize, EncodeError>
where
    R: KafkaRequest + KafkaEncode,
{
    let start = buffer.len();
    buffer.extend_from_slice(&[0; LENGTH_PREFIX]);

    let header_version = ApiVersion::new(request_header_version(R::is_flexible(version)));
    let header = RequestHeader {
        request_api_key: R::API_KEY.value(),
        request_api_version: version.value(),
        correlation_id,
        client_id,
        ..RequestHeader::default()
    };
    header.encode_into(buffer, header_version)?;
    request.encode_into(buffer, version)?;

    let body = buffer.len() - start - LENGTH_PREFIX;
    let size = i32::try_from(body).map_err(|_| EncodeError::FrameTooLarge { bytes: body })?;
    buffer[start..start + LENGTH_PREFIX].copy_from_slice(&size.to_be_bytes());
    Ok(buffer.len() - start)
}

/// The response header version a reply to `R` at `version` is framed with.
///
/// Exposed because a client reads the header before it knows the body type, so
/// it needs this answer separately from any decode.
pub fn response_header_version_for<R>(version: ApiVersion) -> i16
where
    R: KafkaRequest,
{
    response_header_version(R::API_KEY, version, R::is_flexible(version))
}
