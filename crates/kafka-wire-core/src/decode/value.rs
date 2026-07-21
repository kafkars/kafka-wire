//! Complete-message decoding contract.
//!
//! Generated messages implement this trait; the default helper enforces complete
//! consumption of a message body.

use bytes::Bytes;

use crate::ApiVersion;

use super::{DecodeError, DecodeLimits, Decoder};

/// A value that can decode itself using a negotiated Kafka version.
pub trait KafkaDecode: Sized {
    /// Decodes the value from `decoder`.
    fn decode(decoder: &mut Decoder, version: ApiVersion) -> Result<Self, DecodeError>;

    /// Decodes one complete message body and rejects trailing bytes.
    fn decode_from_bytes(
        bytes: Bytes,
        version: ApiVersion,
        limits: DecodeLimits,
    ) -> Result<Self, DecodeError> {
        let mut decoder = Decoder::new(bytes, limits)?;
        let value = Self::decode(&mut decoder, version)?;
        decoder.finish()?;
        Ok(value)
    }
}
