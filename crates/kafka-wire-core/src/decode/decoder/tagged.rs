//! Tagged-field decoding with strict order, aggregate byte budgets, and
//! dispatch to the tags this build knows.

use crate::{TaggedField, TaggedFields, TaggedFieldsError};

use super::super::DecodeError;
use super::Decoder;

/// What a dispatch closure did with one tagged-field entry.
///
/// Named rather than spelled as a boolean because the two answers have
/// different consequences: a decoded entry must have consumed its declared size
/// exactly, and a retained one must survive byte-for-byte.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TagOutcome {
    /// The closure recognised the tag and read its value.
    Decoded,
    /// The tag is unknown to this build. Its bytes are kept verbatim so a
    /// message can survive a round trip through a build older than its peer.
    Retained,
}

impl Decoder {
    /// Reads a tagged-field section in which every entry is unknown.
    pub fn read_tagged_fields(&mut self) -> Result<TaggedFields, DecodeError> {
        self.read_tagged_fields_with(|_, _| Ok(TagOutcome::Retained))
    }

    /// Reads a tagged-field section, offering each entry to `dispatch` first.
    ///
    /// `dispatch` receives the entry's tag and a decoder over that entry's
    /// payload alone, bounded by the size the peer declared. An entry it claims
    /// must consume that payload exactly: the size is the peer's statement about
    /// the entry, and reading less than it means one side has the wrong schema,
    /// which is reported rather than absorbed. Everything it declines is
    /// retained in the returned `TaggedFields`.
    pub fn read_tagged_fields_with<F>(
        &mut self,
        mut dispatch: F,
    ) -> Result<TaggedFields, DecodeError>
    where
        F: FnMut(u32, &mut Self) -> Result<TagOutcome, DecodeError>,
    {
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

        let mut fields = Vec::new();
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

            let payload_offset = self.offset();
            let payload = self.take(length)?;
            // The entry gets a decoder of its own so a known tag's value cannot
            // read past the size the peer declared for it, whatever the schema
            // this build holds says the value should look like.
            let mut entry = Self::child(payload.clone(), self.limits, payload_offset);
            match dispatch(tag, &mut entry)? {
                TagOutcome::Decoded => {
                    let remaining = entry.remaining();
                    if remaining != 0 {
                        return Err(DecodeError::TaggedFieldSize {
                            tag,
                            size: length,
                            consumed: length - remaining,
                            offset: length_offset,
                        });
                    }
                }
                TagOutcome::Retained => fields.push(TaggedField::new(tag, payload)),
            }
            previous = Some(tag);
        }

        // The retained entries are a subsequence of a run this loop already
        // proved ascending, so this cannot fail. It is spelled as the same
        // construction every other `TaggedFields` goes through rather than a
        // private bypass, so the invariant has exactly one enforcement point.
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
