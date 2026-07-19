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

    /// Calculates the encoded length through the normal encoding path.
    fn encoded_len(&self, version: ApiVersion) -> Result<usize, EncodeError> {
        let mut encoder = Encoder::sizing();
        self.encode(&mut encoder, version)?;
        Ok(encoder.len())
    }

    /// Encodes the value into a newly allocated immutable byte buffer.
    fn encode_to_bytes(&self, version: ApiVersion) -> Result<Bytes, EncodeError> {
        let predicted = self.encoded_len(version)?;
        let mut buffer = BytesMut::with_capacity(predicted);

        {
            let mut encoder = Encoder::new(&mut buffer);
            self.encode(&mut encoder, version)?;
        }

        let actual = buffer.len();
        if actual != predicted {
            return Err(EncodeError::SizeMismatch { predicted, actual });
        }

        Ok(buffer.freeze())
    }
}
