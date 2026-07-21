//! Collection of one message's struct declarations into its table.
//!
//! This file owns the walk that finds every struct a message declares — the
//! `commonStructs` block and every inline field body — and records each one
//! once, in protocol declaration order. It deliberately does not own the naming
//! rule (`ir/struct_ref.rs`), which already ran when the field types were
//! parsed, nor whether the resulting table is well formed
//! (`validate/structs.rs`).
//!
//! The walk is total: a duplicate name is recorded twice rather than dropped,
//! because losing one of two same-named declarations here would hide the very
//! collision the module-scoped naming rule's guard requires be reported.

use crate::{
    CommonStruct, Field, StructDeclaration, StructOrigin, StructRef, StructTable, VersionSet,
};

/// Indexes every struct declaration a lowered message makes.
///
/// Order is each `commonStructs` entry followed by the inline bodies nested
/// inside it, then the inline bodies of the root field tree. That is the order
/// upstream wrote the declarations in, which the earlier flat naming rule fixed as generated item
/// order and the module-scoped naming rule leaves untouched.
pub(crate) fn collect_struct_table(
    common_structs: &[CommonStruct],
    fields: &[Field],
    valid_versions: &VersionSet,
) -> StructTable {
    let mut declarations = Vec::new();

    for common in common_structs {
        let effective = common.versions.intersection(valid_versions);
        declarations.push(StructDeclaration {
            name: common.name.clone(),
            versions: effective.clone(),
            origin: StructOrigin::Common,
            references: references(&common.fields),
        });
        collect_inline(&common.fields, &effective, &mut declarations);
    }
    collect_inline(fields, valid_versions, &mut declarations);

    StructTable::new(declarations)
}

/// Records the struct each field declares inline, depth first in field order.
///
/// Recursion is bounded by the field tree itself, which `lower/field.rs` has
/// already rejected past its nesting limit, so this walk cannot be driven
/// deeper than that bound by a crafted schema.
fn collect_inline(
    fields: &[Field],
    parent_versions: &VersionSet,
    declarations: &mut Vec<StructDeclaration>,
) {
    for field in fields {
        let effective = field.versions.intersection(parent_versions);
        if !field.declares_struct() {
            continue;
        }

        // A field with inline members but a primitive type is malformed rather
        // than a declaration; `validate/field.rs` reports it as
        // `KAFKA_SCHEMA_UNEXPECTED_NESTED_FIELDS`. There is no struct identity
        // to record for it, so it contributes nothing here.
        if let Some(reference) = field.ty.struct_reference() {
            declarations.push(StructDeclaration {
                name: reference.clone(),
                // An inline body exists exactly where the field carrying it
                // exists, including every enclosing declaration's window.
                versions: effective.clone(),
                origin: StructOrigin::Inline,
                references: references(&field.fields),
            });
        }
        collect_inline(&field.fields, &effective, declarations);
    }
}

/// Returns the structs one declaration's immediate members refer to.
fn references(fields: &[Field]) -> Vec<StructRef> {
    fields
        .iter()
        .filter_map(|field| field.ty.struct_reference())
        .cloned()
        .collect()
}
