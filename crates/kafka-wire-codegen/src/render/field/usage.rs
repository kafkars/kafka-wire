//! Semantic version-parameter usage for generated field writers.
//!
//! This file owns the answer before source text exists. Formatting and helper
//! spellings therefore cannot accidentally change whether an emitted parameter
//! is named `version` or `_version`.

use kafka_wire_schema::{Field, FieldType, Message};

use super::{
    regime::{Encoding, encoding_of},
    version::presence_condition,
};

/// Whether encoding the field's value itself consults the negotiated version.
pub(crate) fn encoded_value_uses_version(field: &Field, message: &Message) -> bool {
    contains_struct(&field.ty)
        || (has_regime_dependent_prefix(&field.ty)
            && encoding_of(field, message) == Encoding::VersionGated)
}

/// Whether one inline field writer consults the negotiated version.
pub(crate) fn inline_write_uses_version(field: &Field, message: &Message) -> bool {
    presence_condition(field, message).is_some() || encoded_value_uses_version(field, message)
}

fn contains_struct(ty: &FieldType) -> bool {
    match ty {
        FieldType::Struct(_) => true,
        FieldType::Array(element) => contains_struct(element),
        _ => false,
    }
}

fn has_regime_dependent_prefix(ty: &FieldType) -> bool {
    matches!(
        ty,
        FieldType::String | FieldType::Bytes | FieldType::Records | FieldType::Array(_)
    )
}
