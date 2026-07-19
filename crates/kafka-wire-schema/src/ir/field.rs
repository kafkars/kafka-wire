//! One normalized message field and the upstream metadata attached to it.
//!
//! This file owns the field record. It deliberately does not own the type
//! language (`field_type.rs`), default values (`value.rs`), or the invariants
//! that relate a field's version sets to its parent's (`validate/`).

use super::{DefaultValue, EntityType, FieldName, FieldType, VersionSet};

/// One normalized message field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Field {
    /// Protocol and Rust names.
    pub name: FieldName,
    /// Semantic field type.
    pub ty: FieldType,
    /// Declared presence versions.
    pub versions: VersionSet,
    /// Declared nullable versions.
    pub nullable_versions: VersionSet,
    /// Declared tagged versions.
    pub tagged_versions: VersionSet,
    /// Flexible tag number.
    pub tag: Option<u32>,
    /// Typed protocol default.
    pub default: DefaultValue,
    /// Whether non-default values may be omitted in older versions.
    pub ignorable: bool,
    /// In-memory map key metadata.
    pub map_key: bool,
    /// The domain entity this field's value names, when upstream says so.
    ///
    /// Preserved rather than discarded because it is the only machine-readable
    /// statement that a given `int32` is a broker id: a client routing a
    /// request or validating a topic name has no other source for that fact.
    pub entity_type: Option<EntityType>,
    /// Whether upstream marks this `bytes` field as safe to alias in place.
    ///
    /// A hint, not an obligation. It says a copy is avoidable here, which lets
    /// a decoder hand out a borrowed slice instead of an owned buffer.
    pub zero_copy: bool,
    /// Per-field override of the message's flexible versions.
    ///
    /// Present only where upstream pins a field to an encoding its message
    /// version would not otherwise use — `RequestHeader.ClientId` keeps the
    /// legacy two-byte length prefix in flexible versions so that a broker can
    /// read the header of an `ApiVersionsRequest` before it knows which version
    /// the client chose.
    pub flexible_versions: Option<VersionSet>,
    /// Human-facing documentation.
    pub about: String,
    /// Inline struct fields, when this field declares its element shape.
    pub fields: Vec<Self>,
}

impl Field {
    /// Returns whether this field declares the struct it refers to, inline.
    ///
    /// A struct-typed field either carries the declaration (`fields` present)
    /// or refers to one made elsewhere in the same message; the two cases
    /// resolve differently and every caller has to tell them apart.
    pub fn declares_struct(&self) -> bool {
        !self.fields.is_empty()
    }
}
