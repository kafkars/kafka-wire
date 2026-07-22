//! Resolution of normalized struct declarations to their owned field bodies.
//!
//! This file owns pairing the schema IR's ordered declaration table with the
//! bodies stored on `commonStructs` and inline fields. It deliberately does not
//! emit Rust or reinterpret a declaration's effective version window.

use kafka_wire_schema::{Field, FieldType, Message, StructOrigin, StructRef, VersionSet};

use crate::GenerationError;

/// One declaration paired with the body and effective windows its codecs use.
pub(crate) struct RenderableStruct<'a> {
    pub(crate) name: &'a StructRef,
    pub(crate) fields: &'a [Field],
    pub(crate) versions: &'a VersionSet,
    pub(crate) flexible_versions: VersionSet,
}

/// Every struct this message declares, paired with its members, in protocol
/// declaration order.
///
/// `commonStructs` and an inline body are two spellings of one concept, but the
/// members live in different places. The IR table fixes identity and order;
/// this walk only reconnects each table entry to its single source body.
pub(crate) fn declared_structs(
    message: &Message,
) -> Result<Vec<RenderableStruct<'_>>, GenerationError> {
    let mut declared = Vec::new();
    let message_flexible = message.effective_flexible_versions();
    for declaration in message.structs.declarations() {
        let fields = declaration_fields(message, declaration.name.declared(), declaration.origin)
            .ok_or_else(|| GenerationError::InternalInvariant {
            message: message.name.protocol().to_owned(),
            invariant: format!(
                "struct table declaration `{}` has no matching {:?} body",
                declaration.name.declared(),
                declaration.origin,
            ),
        })?;
        declared.push(RenderableStruct {
            name: &declaration.name,
            fields,
            versions: &declaration.versions,
            flexible_versions: declaration.versions.intersection(&message_flexible),
        });
    }
    Ok(declared)
}

fn declaration_fields<'a>(
    message: &'a Message,
    declared: &str,
    origin: StructOrigin,
) -> Option<&'a [Field]> {
    if origin == StructOrigin::Common {
        return message
            .common_structs
            .iter()
            .find(|common| common.name.declared() == declared)
            .map(|common| common.fields.as_slice());
    }

    message
        .common_structs
        .iter()
        .find_map(|common| find_inline(&common.fields, declared))
        .or_else(|| find_inline(&message.fields, declared))
}

fn find_inline<'a>(fields: &'a [Field], declared: &str) -> Option<&'a [Field]> {
    for field in fields {
        if !field.fields.is_empty()
            && struct_reference(&field.ty).is_some_and(|reference| reference.declared() == declared)
        {
            return Some(&field.fields);
        }
        if let Some(found) = find_inline(&field.fields, declared) {
            return Some(found);
        }
    }
    None
}

/// The struct a type denotes, looking through an array element.
fn struct_reference(ty: &FieldType) -> Option<&StructRef> {
    match ty {
        FieldType::Struct(reference) => Some(reference),
        FieldType::Array(element) => struct_reference(element),
        _ => None,
    }
}
