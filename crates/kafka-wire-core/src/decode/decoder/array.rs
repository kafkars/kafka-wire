//! Legacy and compact non-null array length decoding.

use super::super::DecodeError;
use super::Decoder;

impl Decoder {
    /// Reads exactly `length` elements, each through `element`.
    ///
    /// The counterpart of the length readers above, and the reason they are
    /// separate: the prefix is the one part of an array that changes with the
    /// encoding regime, and the elements are the one part that does not. Keeping
    /// the collect loop here means a generated decode states the regime once and
    /// then says what it is reading, rather than restating the same four lines
    /// per array — which, measured over the corpus, was 381 repetitions.
    ///
    /// `length` is already bounded by the reader that produced it: both length
    /// readers reject a count the remaining bytes cannot back, so the reservation
    /// here cannot be driven past the frame a peer actually sent.
    pub fn read_vec<T, F>(&mut self, length: usize, mut element: F) -> Result<Vec<T>, DecodeError>
    where
        F: FnMut(&mut Self) -> Result<T, DecodeError>,
    {
        let mut values = Vec::with_capacity(length);
        for _ in 0..length {
            values.push(element(self)?);
        }
        Ok(values)
    }

    /// Reads and validates a legacy non-null array length.
    pub fn read_array_len(&mut self) -> Result<usize, DecodeError> {
        let offset = self.offset();
        let length = self.read_i32()?;
        if length < 0 {
            return Err(DecodeError::NegativeLength {
                kind: "array",
                length: i64::from(length),
                offset,
            });
        }

        let length = usize::try_from(length).map_err(|_| DecodeError::LengthOverflow {
            kind: "array",
            offset,
        })?;
        self.check_collection_limit("array", length, offset)?;
        self.check_element_count("array", length, offset)?;
        Ok(length)
    }

    /// Reads and validates a compact non-null array length.
    pub fn read_compact_array_len(&mut self) -> Result<usize, DecodeError> {
        let offset = self.offset();
        let encoded = self.read_unsigned_varint()?;
        if encoded == 0 {
            return Err(DecodeError::NullNotAllowed {
                kind: "compact array",
                offset,
            });
        }

        let length = usize::try_from(encoded - 1).map_err(|_| DecodeError::LengthOverflow {
            kind: "compact array",
            offset,
        })?;
        self.check_collection_limit("compact array", length, offset)?;
        self.check_element_count("compact array", length, offset)?;
        Ok(length)
    }

    /// Reads and validates a legacy nullable array length.
    ///
    /// The `int32` `-1` sentinel decodes to `None`; any other negative length is
    /// malformed. A present length is bounded by the element budget and by the
    /// bytes that remain before it can back a reservation.
    pub fn read_nullable_array_len(&mut self) -> Result<Option<usize>, DecodeError> {
        let offset = self.offset();
        let length = self.read_i32()?;
        if length == -1 {
            return Ok(None);
        }
        if length < 0 {
            return Err(DecodeError::NegativeLength {
                kind: "nullable array",
                length: i64::from(length),
                offset,
            });
        }

        let length = usize::try_from(length).map_err(|_| DecodeError::LengthOverflow {
            kind: "nullable array",
            offset,
        })?;
        self.check_collection_limit("nullable array", length, offset)?;
        self.check_element_count("nullable array", length, offset)?;
        Ok(Some(length))
    }

    /// Reads and validates a compact nullable array length.
    ///
    /// The varint `0` sentinel decodes to `None`; otherwise the stored count is
    /// `varint - 1`, bounded by the element budget and by the bytes that remain.
    pub fn read_compact_nullable_array_len(&mut self) -> Result<Option<usize>, DecodeError> {
        let offset = self.offset();
        let encoded = self.read_unsigned_varint()?;
        if encoded == 0 {
            return Ok(None);
        }

        let length = usize::try_from(encoded - 1).map_err(|_| DecodeError::LengthOverflow {
            kind: "compact nullable array",
            offset,
        })?;
        self.check_collection_limit("compact nullable array", length, offset)?;
        self.check_element_count("compact nullable array", length, offset)?;
        Ok(Some(length))
    }
}
