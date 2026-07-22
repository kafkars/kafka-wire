//! The known half of a tagged-field section.
//!
//! A tagged entry is `varint tag`, `varint size`, then that many payload bytes.
//! The size precedes the payload, so a sizing target measures each known value
//! first. Every active schema tag remains claimed even when its default payload
//! is omitted. An emitted claim also retains its predicted length; the actual
//! value is encoded directly into a byte target after its prefix.
//!
//! Unknown tags need none of this. They arrive as payload bytes already and are
//! carried verbatim by `TaggedFields`.

use crate::{TaggedField, TaggedFields, TaggedFieldsError};

use super::{EncodeError, EncodeTarget, Encoder, PremeasuredWrite, SizeTarget};

#[derive(Debug)]
struct KnownTag {
    tag: u32,
    length: Option<u32>,
}

#[derive(Clone, Copy, Debug)]
struct EmittedKnownTag {
    tag: u32,
    length: u32,
}

impl KnownTag {
    fn emitted(&self) -> Option<EmittedKnownTag> {
        self.length.map(|length| EmittedKnownTag {
            tag: self.tag,
            length,
        })
    }
}

/// The known tagged fields of one structure, collected before they are written.
#[derive(Debug, Default)]
pub struct KnownTags {
    fields: Vec<KnownTag>,
}

impl KnownTags {
    /// Creates an empty collection.
    #[must_use]
    pub const fn new() -> Self {
        Self { fields: Vec::new() }
    }

    /// Returns whether no schema tag has been claimed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Reserves one active schema tag whether or not it emits a payload.
    ///
    /// Default-valued known fields are absent from the wire, but their numeric
    /// tags remain owned by the schema and may not be forwarded as unknown.
    pub fn claim(&mut self, tag: u32) -> Result<(), EncodeError> {
        if self.fields.iter().any(|field| field.tag == tag) {
            return Err(TaggedFieldsError::Duplicate { tag }.into());
        }
        self.fields.push(KnownTag { tag, length: None });
        Ok(())
    }

    /// Measures one known tag's value and records its exact wire length.
    ///
    /// The closure receives a sizing encoder, so payload bytes are never
    /// materialized during preflight. It must not write the tag or size.
    ///
    /// The measurement attaches to an earlier claim when one exists; otherwise
    /// emitting the value also claims its tag. Measuring one tag twice is a
    /// duplicate, while a claim without a measurement remains intentionally
    /// absent from the wire.
    pub fn measure<F>(&mut self, tag: u32, value: F) -> Result<(), EncodeError>
    where
        F: FnOnce(&mut Encoder<SizeTarget>) -> Result<(), EncodeError>,
    {
        let existing = self.fields.iter().position(|field| field.tag == tag);
        if existing.is_some_and(|index| self.fields[index].length.is_some()) {
            return Err(TaggedFieldsError::Duplicate { tag }.into());
        }

        let mut encoder = Encoder::sizing();
        value(&mut encoder)?;
        let length = u32::try_from(encoder.len()).map_err(|_| EncodeError::LengthOverflow {
            kind: "tagged field",
            length: encoder.len(),
            maximum: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
        })?;
        if let Some(index) = existing {
            self.fields[index].length = Some(length);
        } else {
            self.fields.push(KnownTag {
                tag,
                length: Some(length),
            });
        }
        Ok(())
    }

    fn into_sorted(mut self) -> Result<Vec<KnownTag>, TaggedFieldsError> {
        self.fields.sort_by_key(|field| field.tag);
        for pair in self.fields.windows(2) {
            if pair[0].tag == pair[1].tag {
                return Err(TaggedFieldsError::Duplicate { tag: pair[0].tag });
            }
        }
        Ok(self.fields)
    }
}

impl<T: EncodeTarget> Encoder<T> {
    /// Writes one tagged-field section holding both known and retained tags.
    ///
    /// The two populations are merged into one ascending run rather than
    /// appended, because the wire format has a single ordering across the whole
    /// section. A tag claimed by both — a known field colliding with an unknown
    /// one the peer sent under the same number — is a named error rather than
    /// two entries the peer must choose between.
    pub fn write_merged_tagged_fields(
        &mut self,
        known: KnownTags,
        unknown: &TaggedFields,
        mut write_known: impl FnMut(u32, &mut Encoder<T>) -> Result<(), EncodeError>,
    ) -> Result<(), EncodeError> {
        if known.is_empty() {
            return self.write_tagged_fields(unknown);
        }

        let known = known.into_sorted()?;
        ensure_disjoint(&known, unknown)?;
        let emitted_count = known.iter().filter(|field| field.length.is_some()).count();
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

        let mut known = known.iter().filter_map(KnownTag::emitted).peekable();
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
