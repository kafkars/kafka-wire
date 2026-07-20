//! The known half of a tagged-field section.
//!
//! A tagged entry is `varint tag`, `varint size`, then that many payload bytes.
//! The size precedes the payload, so a writer has to know how long the value is
//! before it can write either — which means a known tag's value is encoded into
//! a buffer of its own first. That buffering is the whole reason this type
//! exists: generated code says what a tag's value is, and this says how a tag
//! becomes bytes.
//!
//! Unknown tags need none of this. They arrive as payload bytes already and are
//! carried verbatim by `TaggedFields`.

use bytes::BytesMut;

use crate::{TaggedField, TaggedFields};

use super::{BufferTarget, EncodeError, EncodeTarget, Encoder};

/// The known tagged fields of one structure, collected before they are written.
#[derive(Debug, Default)]
pub struct KnownTags {
    fields: Vec<TaggedField>,
}

impl KnownTags {
    /// Creates an empty collection.
    #[must_use]
    pub const fn new() -> Self {
        Self { fields: Vec::new() }
    }

    /// Returns whether no known tag has been written.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Encodes one known tag's value and records it under `tag`.
    ///
    /// The closure receives an encoder over a fresh buffer, so what it writes is
    /// the entry's payload exactly — it must not write the tag or the size, and
    /// whatever it does write becomes the declared size.
    ///
    /// Callers decide *whether* to call this. A tagged field is omitted when it
    /// holds its protocol default, which is what makes the section sparse, and
    /// that comparison belongs to the field, not here.
    pub fn write<F>(&mut self, tag: u32, value: F) -> Result<(), EncodeError>
    where
        F: FnOnce(&mut Encoder<BufferTarget<'_>>) -> Result<(), EncodeError>,
    {
        let mut payload = BytesMut::new();
        let mut encoder = Encoder::new(&mut payload);
        value(&mut encoder)?;
        self.fields.push(TaggedField::new(tag, payload.freeze()));
        Ok(())
    }

    pub(super) fn into_fields(self) -> Vec<TaggedField> {
        self.fields
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
    ) -> Result<(), EncodeError> {
        if known.is_empty() {
            return self.write_tagged_fields(unknown);
        }

        let mut merged = known.into_fields();
        merged.extend(unknown.iter().cloned());
        let merged = TaggedFields::from_unsorted(merged)?;
        self.write_tagged_fields(&merged)
    }
}
