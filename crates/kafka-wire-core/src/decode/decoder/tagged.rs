//! Unknown tagged-field decoding with strict order and aggregate byte budgets.

use crate::{TaggedField, TaggedFields, TaggedFieldsError};

use super::super::DecodeError;
use super::Decoder;

impl Decoder {
    /// Reads unknown tagged fields and validates count, size, and order.
    pub fn read_tagged_fields(&mut self) -> Result<TaggedFields, DecodeError> {
        let count_offset = self.offset();
        let count = self.read_unsigned_varint()?;
        let count = usize::try_from(count).map_err(|_| DecodeError::LengthOverflow {
            kind: "tagged field count",
            offset: count_offset,
        })?;
        Self::check_limit(
            "tagged field count",
            count,
            self.limits.max_tagged_fields,
            count_offset,
        )?;
        self.check_element_count("tagged field count", count, count_offset)?;

        let mut fields = Vec::with_capacity(count);
        let mut previous = None;
        let mut total_bytes = 0_usize;
        for _ in 0..count {
            let tag_offset = self.offset();
            let tag = self.read_unsigned_varint()?;
            if let Some(previous_tag) = previous {
                if tag <= previous_tag {
                    return Err(DecodeError::TaggedFieldOrder {
                        previous: previous_tag,
                        current: tag,
                        offset: tag_offset,
                    });
                }
            }

            let length_offset = self.offset();
            let length = self.read_unsigned_varint()?;
            let length = usize::try_from(length).map_err(|_| DecodeError::LengthOverflow {
                kind: "tagged field",
                offset: length_offset,
            })?;
            Self::check_limit(
                "tagged field",
                length,
                self.limits.max_tag_bytes,
                length_offset,
            )?;

            total_bytes = total_bytes
                .checked_add(length)
                .ok_or(DecodeError::LengthOverflow {
                    kind: "total tagged fields",
                    offset: length_offset,
                })?;
            Self::check_limit(
                "total tagged fields",
                total_bytes,
                self.limits.max_total_tag_bytes,
                length_offset,
            )?;

            fields.push(TaggedField::new(tag, self.take(length)?));
            previous = Some(tag);
        }

        TaggedFields::from_sorted(fields).map_err(|error| match error {
            TaggedFieldsError::Duplicate { tag } => DecodeError::TaggedFieldOrder {
                previous: tag,
                current: tag,
                offset: count_offset,
            },
            TaggedFieldsError::OutOfOrder { previous, current } => DecodeError::TaggedFieldOrder {
                previous,
                current,
                offset: count_offset,
            },
        })
    }
}
