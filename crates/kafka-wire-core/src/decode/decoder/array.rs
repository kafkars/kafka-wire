//! Legacy and compact non-null array length decoding.

use super::super::DecodeError;
use super::Decoder;

/// Array count already checked against the decoder's configured budget.
///
/// Only array-prefix readers can construct this token, so `read_vec` cannot be
/// called with an arbitrary peer-controlled `usize`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BoundedCount(usize);

impl BoundedCount {
    /// Returns the validated element count.
    pub const fn get(self) -> usize {
        self.0
    }
}

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
    /// `length` is already bounded by the reader that produced it. This method
    /// deliberately does not reserve that peer-controlled count up front.
    pub fn read_vec<T, F>(
        &mut self,
        length: BoundedCount,
        mut element: F,
    ) -> Result<Vec<T>, DecodeError>
    where
        F: FnMut(&mut Self) -> Result<T, DecodeError>,
    {
        // Do not reserve directly from a peer count. Capacity grows only as
        // elements successfully decode, while the opaque count remains bounded
        // by `max_array_elements`.
        let mut values = Vec::new();
        for _ in 0..length.get() {
            values.push(element(self)?);
        }
        Ok(values)
    }

    /// Reads and validates a legacy non-null array length.
    pub fn read_array_len(&mut self) -> Result<BoundedCount, DecodeError> {
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
        Ok(BoundedCount(length))
    }

    /// Reads and validates a compact non-null array length.
    pub fn read_compact_array_len(&mut self) -> Result<BoundedCount, DecodeError> {
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
        Ok(BoundedCount(length))
    }

    /// Reads and validates a legacy nullable array length.
    ///
    /// The `int32` `-1` sentinel decodes to `None`; any other negative length is
    /// malformed. A present length is bounded by the configured element budget
    /// before it can back a reservation.
    pub fn read_nullable_array_len(&mut self) -> Result<Option<BoundedCount>, DecodeError> {
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
        Ok(Some(BoundedCount(length)))
    }

    /// Reads and validates a compact nullable array length.
    ///
    /// The varint `0` sentinel decodes to `None`; otherwise the stored count is
    /// `varint - 1`, bounded by the configured element budget before it can back
    /// a reservation.
    pub fn read_compact_nullable_array_len(&mut self) -> Result<Option<BoundedCount>, DecodeError> {
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
        Ok(Some(BoundedCount(length)))
    }
}
