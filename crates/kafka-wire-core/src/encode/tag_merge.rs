//! Ordered emission of known and retained tagged fields.
//!
//! Inline claim/measurement storage belongs to `known`; this module owns the
//! single ascending wire run and its measured-payload checks.

use crate::{TaggedField, TaggedFields, TaggedFieldsError};

use super::{
    EmittedKnownTag, EncodeError, EncodeTarget, Encoder, KnownTag, KnownTags, PremeasuredWrite,
};

impl<T: EncodeTarget> Encoder<T> {
    /// Writes one tagged-field section holding both known and retained tags.
    pub fn write_merged_tagged_fields<const N: usize>(
        &mut self,
        mut known: KnownTags<N>,
        unknown: &TaggedFields,
        mut write_known: impl FnMut(u32, &mut Encoder<T>) -> Result<(), EncodeError>,
    ) -> Result<(), EncodeError> {
        if known.is_empty() {
            return self.write_tagged_fields(unknown);
        }

        known.sort_and_validate()?;
        ensure_disjoint(known.fields(), unknown)?;
        let emitted_count = known
            .fields()
            .iter()
            .filter(|field| field.emitted().is_some())
            .count();
        let count =
            emitted_count
                .checked_add(unknown.len())
                .ok_or(EncodeError::LengthOverflow {
                    kind: "tagged field count",
                    length: usize::MAX,
                    maximum: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
                })?;
        let count = u32::try_from(count).map_err(|_| EncodeError::LengthOverflow {
            kind: "tagged field count",
            length: count,
            maximum: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
        })?;
        self.write_unsigned_varint(count)?;

        let mut known = known
            .fields()
            .iter()
            .filter_map(KnownTag::emitted)
            .peekable();
        let mut unknown = unknown.iter().peekable();
        loop {
            match (known.peek(), unknown.peek()) {
                (Some(left), Some(right)) if left.tag < right.tag() => {
                    self.write_known_tag(*left, &mut write_known)?;
                    known.next();
                }
                (Some(left), Some(right)) if left.tag > right.tag() => {
                    self.write_unknown_tag(right)?;
                    unknown.next();
                }
                (Some(left), Some(_)) => {
                    return Err(TaggedFieldsError::Duplicate { tag: left.tag }.into());
                }
                (Some(left), None) => {
                    self.write_known_tag(*left, &mut write_known)?;
                    known.next();
                }
                (None, Some(right)) => {
                    self.write_unknown_tag(right)?;
                    unknown.next();
                }
                (None, None) => return Ok(()),
            }
        }
    }

    fn write_known_tag(
        &mut self,
        field: EmittedKnownTag,
        write: &mut impl FnMut(u32, &mut Encoder<T>) -> Result<(), EncodeError>,
    ) -> Result<(), EncodeError> {
        self.write_unsigned_varint(field.tag)?;
        self.write_unsigned_varint(field.length)?;
        let predicted = usize::try_from(field.length).unwrap_or(usize::MAX);
        match self.prepare_premeasured(predicted)? {
            PremeasuredWrite::Accounted => Ok(()),
            PremeasuredWrite::WritePayload => {
                let start = self.len();
                write(field.tag, self)?;
                let actual = self.len().saturating_sub(start);
                if actual != predicted {
                    return Err(EncodeError::SizeMismatch { predicted, actual });
                }
                Ok(())
            }
        }
    }

    fn write_unknown_tag(&mut self, field: &TaggedField) -> Result<(), EncodeError> {
        self.write_unsigned_varint(field.tag())?;
        let length =
            u32::try_from(field.data().len()).map_err(|_| EncodeError::LengthOverflow {
                kind: "tagged field",
                length: field.data().len(),
                maximum: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
            })?;
        self.write_unsigned_varint(length)?;
        self.write_raw_slice(field.data())
    }
}

fn ensure_disjoint(known: &[KnownTag], unknown: &TaggedFields) -> Result<(), TaggedFieldsError> {
    let mut known = known.iter().peekable();
    let mut unknown = unknown.iter().peekable();
    while let (Some(left), Some(right)) = (known.peek(), unknown.peek()) {
        match left.tag.cmp(&right.tag()) {
            std::cmp::Ordering::Less => {
                known.next();
            }
            std::cmp::Ordering::Greater => {
                unknown.next();
            }
            std::cmp::Ordering::Equal => {
                return Err(TaggedFieldsError::Duplicate { tag: left.tag });
            }
        }
    }
    Ok(())
}
