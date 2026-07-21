//! Complete-message encoding contract.
//!
//! Generated messages implement this trait; the default helpers guarantee that
//! sizing and byte emission execute the same implementation.

use bytes::{Bytes, BytesMut};

use crate::ApiVersion;

use super::{EncodeError, EncodeTarget, Encoder};

/// A value that can encode itself using a negotiated Kafka version.
pub trait KafkaEncode {
    /// Encodes the value into `encoder`.
    fn encode<T: EncodeTarget>(
        &self,
        encoder: &mut Encoder<T>,
        version: ApiVersion,
    ) -> Result<(), EncodeError>;

    /// Performs any value-level preflight required before byte emission.
    ///
    /// Generated implementations use this hook so `encode_into` validates one
    /// complete value once before its sizing and writing passes. Handwritten
    /// implementations need not override it unless they have a separate
    /// preflight of their own.
    #[doc(hidden)]
    fn validate_encoding(&self, _version: ApiVersion) -> Result<(), EncodeError> {
        Ok(())
    }

    /// Emits a value whose preflight has already succeeded.
    ///
    /// The default delegates to `encode`, preserving the contract for existing
    /// handwritten implementations. Generated implementations override this
    /// with their unchecked body so sizing, writing, and nested descent do not
    /// repeat recursive validation.
    #[doc(hidden)]
    fn encode_validated<T: EncodeTarget>(
        &self,
        encoder: &mut Encoder<T>,
        version: ApiVersion,
    ) -> Result<(), EncodeError> {
        self.encode(encoder, version)
    }

    /// Calculates the encoded length after one complete preflight.
    fn encoded_len(&self, version: ApiVersion) -> Result<usize, EncodeError> {
        self.validate_encoding(version)?;
        let mut encoder = Encoder::sizing();
        self.encode_validated(&mut encoder, version)?;
        Ok(encoder.len())
    }

    /// Appends the encoded value to `buffer` and returns the bytes it wrote.
    ///
    /// The buffer may already hold earlier frames; a pipelining client reuses one
    /// buffer for a size prefix, a request header, and a body. Only this value's
    /// bytes are counted, so the predicted-versus-written self-check keeps
    /// working across every message in the stream.
    ///
    /// On failure the buffer is truncated back to its previous length, so a
    /// rejected message never leaves a partial frame behind for the next write.
    fn encode_into(
        &self,
        buffer: &mut BytesMut,
        version: ApiVersion,
    ) -> Result<usize, EncodeError> {
        let predicted = self.encoded_len(version)?;
        let start = buffer.len();
        buffer.reserve(predicted);

        let written = {
            let mut encoder = Encoder::new(buffer);
            let outcome = self.encode_validated(&mut encoder, version);
            outcome.map(|()| encoder.len())
        };

        match written {
            Ok(actual) if actual == predicted => Ok(actual),
            Ok(actual) => {
                buffer.truncate(start);
                Err(EncodeError::SizeMismatch { predicted, actual })
            }
            Err(error) => {
                buffer.truncate(start);
                Err(error)
            }
        }
    }

    /// Encodes the value into a newly allocated immutable byte buffer.
    fn encode_to_bytes(&self, version: ApiVersion) -> Result<Bytes, EncodeError> {
        let mut buffer = BytesMut::new();
        self.encode_into(&mut buffer, version)?;
        Ok(buffer.freeze())
    }
}
