//! Ordered storage for unknown Kafka tagged fields.
//!
//! This module preserves raw payload bytes and enforces the strictly increasing
//! tag order required by flexible encodings.

use bytes::Bytes;
use thiserror::Error;

/// One unknown tagged field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaggedField {
    tag: u32,
    data: Bytes,
}

impl TaggedField {
    /// Creates a tagged field.
    pub const fn new(tag: u32, data: Bytes) -> Self {
        Self { tag, data }
    }

    /// Returns the numeric tag.
    pub const fn tag(&self) -> u32 {
        self.tag
    }

    /// Returns the raw tagged-field payload.
    pub const fn data(&self) -> &Bytes {
        &self.data
    }
}

/// Ordered unknown tagged fields.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TaggedFields {
    fields: Vec<TaggedField>,
}

impl TaggedFields {
    /// Validates and stores fields that are already ordered by tag.
    pub fn from_sorted(fields: Vec<TaggedField>) -> Result<Self, TaggedFieldsError> {
        let mut previous = None;
        for field in &fields {
            if let Some(previous_tag) = previous {
                if field.tag == previous_tag {
                    return Err(TaggedFieldsError::Duplicate { tag: field.tag });
                }
                if field.tag < previous_tag {
                    return Err(TaggedFieldsError::OutOfOrder {
                        previous: previous_tag,
                        current: field.tag,
                    });
                }
            }
            previous = Some(field.tag);
        }

        Ok(Self { fields })
    }

    /// Validates and stores fields supplied in any order.
    ///
    /// One tagged-field section carries two populations that arrive sorted by
    /// different things: the tags this build knows, in schema declaration order,
    /// and the tags it does not, in the order the peer wrote them. The wire
    /// format has one section in ascending tag order, so the two must be merged
    /// rather than concatenated — an unknown tag numerically below a known one
    /// has to precede it.
    ///
    /// The sort is stable, so a tag claimed by both populations stays adjacent
    /// and is reported as a duplicate rather than silently emitted twice.
    pub fn from_unsorted(mut fields: Vec<TaggedField>) -> Result<Self, TaggedFieldsError> {
        fields.sort_by_key(TaggedField::tag);
        Self::from_sorted(fields)
    }

    /// Returns an iterator in ascending tag order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &TaggedField> {
        self.fields.iter()
    }

    /// Returns the number of fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns whether no fields are present.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// Tagged-field construction failure.
#[non_exhaustive]
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TaggedFieldsError {
    /// Two fields used the same numeric tag.
    #[error("duplicate tagged field {tag}")]
    Duplicate {
        /// Repeated tag.
        tag: u32,
    },

    /// Fields were not supplied in ascending tag order.
    #[error("tagged field {current} followed {previous}")]
    OutOfOrder {
        /// Previous tag.
        previous: u32,
        /// Current tag.
        current: u32,
    },
}
