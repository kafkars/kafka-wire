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

use crate::TaggedFieldsError;

use super::{EncodeError, Encoder, SizeTarget};

#[derive(Clone, Copy, Debug)]
enum KnownTagState {
    Claimed,
    Emitted { length: u32 },
}

#[derive(Clone, Copy, Debug)]
pub(super) struct KnownTag {
    pub(super) tag: u32,
    state: KnownTagState,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct EmittedKnownTag {
    pub(super) tag: u32,
    pub(super) length: u32,
}

impl KnownTag {
    const EMPTY: Self = Self {
        tag: 0,
        state: KnownTagState::Claimed,
    };

    pub(super) fn emitted(&self) -> Option<EmittedKnownTag> {
        match self.state {
            KnownTagState::Claimed => None,
            KnownTagState::Emitted { length } => Some(EmittedKnownTag {
                tag: self.tag,
                length,
            }),
        }
    }
}

/// The known tagged fields of one structure, held in a fixed inline buffer.
#[derive(Debug)]
pub struct KnownTags<const N: usize> {
    fields: [KnownTag; N],
    len: usize,
}

impl<const N: usize> KnownTags<N> {
    /// Creates an empty collection.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            fields: [KnownTag::EMPTY; N],
            len: 0,
        }
    }

    /// Returns whether no schema tag has been claimed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Reserves one active schema tag whether or not it emits a payload.
    ///
    /// Default-valued known fields are absent from the wire, but their numeric
    /// tags remain owned by the schema and may not be forwarded as unknown.
    pub fn claim(&mut self, tag: u32) -> Result<(), EncodeError> {
        if self.fields().iter().any(|field| field.tag == tag) {
            return Err(TaggedFieldsError::Duplicate { tag }.into());
        }
        if self.len == N {
            return Err(EncodeError::KnownTagCapacityExceeded { capacity: N });
        }
        self.fields[self.len] = KnownTag {
            tag,
            state: KnownTagState::Claimed,
        };
        self.len += 1;
        Ok(())
    }

    /// Measures one known tag's value and records its exact wire length.
    ///
    /// The closure receives a sizing encoder, so payload bytes are never
    /// materialized during preflight. It must not write the tag or size.
    ///
    /// The measurement attaches to an earlier claim. An unclaimed measurement
    /// is a named generator/runtime contract failure, measuring one tag twice
    /// is a duplicate, and an unmeasured claim remains absent from the wire.
    pub fn measure<F>(&mut self, tag: u32, value: F) -> Result<(), EncodeError>
    where
        F: FnOnce(&mut Encoder<SizeTarget>) -> Result<(), EncodeError>,
    {
        let index = self
            .fields()
            .iter()
            .position(|field| field.tag == tag)
            .ok_or(EncodeError::UnclaimedKnownTag { tag })?;
        if matches!(self.fields[index].state, KnownTagState::Emitted { .. }) {
            return Err(TaggedFieldsError::Duplicate { tag }.into());
        }

        let mut encoder = Encoder::sizing();
        value(&mut encoder)?;
        let length = u32::try_from(encoder.len()).map_err(|_| EncodeError::LengthOverflow {
            kind: "tagged field",
            length: encoder.len(),
            maximum: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
        })?;
        self.fields[index].state = KnownTagState::Emitted { length };
        Ok(())
    }

    pub(super) fn fields(&self) -> &[KnownTag] {
        &self.fields[..self.len]
    }

    pub(super) fn sort_and_validate(&mut self) -> Result<(), TaggedFieldsError> {
        self.fields[..self.len].sort_by_key(|field| field.tag);
        for pair in self.fields().windows(2) {
            if pair[0].tag == pair[1].tag {
                return Err(TaggedFieldsError::Duplicate { tag: pair[0].tag });
            }
        }
        Ok(())
    }
}

impl<const N: usize> Default for KnownTags<N> {
    fn default() -> Self {
        Self::new()
    }
}
