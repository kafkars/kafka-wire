//! Which encoding and nullability regime a field uses over which versions.
//!
//! This file owns one question, asked twice: across the versions a field
//! actually appears in, where is it compact rather than legacy, and where is it
//! nullable rather than not? Both answers are windows rather than booleans, and
//! the two windows are independent — `MetadataRequest.Topics` turns nullable at
//! v1 in a message that turns flexible at v9 — so a codec has to be chosen per
//! intersection rather than once per field.
//!
//! It deliberately owns no emission. Which `Decoder` or `Encoder` method carries
//! a field is `codec`'s decision; this says only which windows that decision has
//! to be made over, so the emitters there read one answer instead of each
//! recomputing it and drifting apart.

use kafka_wire_schema::{Field, Message, VersionSet};

use crate::render::field::version;

/// The versions this field is present in, within what the message supports.
pub(super) fn present(field: &Field, message: &Message) -> VersionSet {
    field.versions.intersection(&message.valid_versions)
}

/// Which of the versions a field appears in declare it nullable.
///
/// A third axis alongside the compact/legacy split, and independent of it:
/// `MetadataRequest.Topics` is nullable from v1 in a message that turns
/// flexible at v9, so the two windows cut the version range in different
/// places and the codec has to be chosen per intersection.
pub(super) enum Nullability {
    /// Never nullable anywhere the message supports it.
    Never,
    /// Nullable in every version it appears in.
    Always,
    /// Nullable in some of them. The two windows partition its presence.
    Gated {
        /// Versions where the field is nullable.
        nullable: VersionSet,
        /// Versions where it appears and is not.
        plain: VersionSet,
    },
}

pub(super) fn nullability_of(field: &Field, message: &Message) -> Nullability {
    let present = present(field, message);
    let nullable = field.nullable_versions.intersection(&present);
    if nullable.is_empty() {
        return Nullability::Never;
    }
    if nullable == present {
        return Nullability::Always;
    }
    Nullability::Gated {
        plain: present.difference(&nullable),
        nullable,
    }
}

/// Which length prefix a field uses across the versions it is present in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Encoding {
    /// Present only in flexible versions, so the compact form is unconditional.
    Compact,
    /// Present only in pre-flexible versions, so the legacy form is unconditional.
    Legacy,
    /// Present on both sides of the flexible boundary, so the gate is emitted.
    VersionGated,
}

pub(super) fn encoding_of(field: &Field, message: &Message) -> Encoding {
    encoding_over(&present(field, message), field, message)
}

/// The regime a field uses across one window of its presence, which may be
/// narrower than the whole of it when nullability cuts it in two.
pub(super) fn encoding_over(present: &VersionSet, field: &Field, message: &Message) -> Encoding {
    // A field may pin itself to an encoding its message would not otherwise
    // use. `RequestHeader.ClientId` declares `flexibleVersions: "none"` and so
    // keeps the legacy two-byte prefix even in a flexible header, which is what
    // lets a broker read the header of a request before it knows the version
    // the client chose. Ignoring the override would put a varint there.
    let flexible = field
        .flexible_versions
        .clone()
        .unwrap_or_else(|| message.effective_flexible_versions());
    if present.is_subset_of(&flexible) {
        Encoding::Compact
    } else if present.intersection(&flexible).is_empty() {
        Encoding::Legacy
    } else {
        Encoding::VersionGated
    }
}

pub(crate) fn is_nullable(field: &Field, message: &Message) -> bool {
    !field
        .nullable_versions
        .intersection(&message.valid_versions)
        .is_empty()
}

/// The predicate under which this field is present but must not be null.
///
/// `None` unless the field is nullable in only some of the versions it appears
/// in. Rendered over the non-nullable half of its presence rather than as the
/// complement of the nullable half within the whole message, so the emitted
/// condition names no version where the field is absent anyway.
pub(crate) fn null_forbidden_condition(field: &Field, message: &Message) -> Option<String> {
    match nullability_of(field, message) {
        Nullability::Gated { plain, .. } => Some(version::condition_for(&plain, message)),
        Nullability::Never | Nullability::Always => None,
    }
}
