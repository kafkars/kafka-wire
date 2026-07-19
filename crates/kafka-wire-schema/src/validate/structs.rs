//! The per-message struct table: unique declarations, bound references, cycles.
//!
//! This file owns the question "is this message's struct table well formed" —
//! does every name it declares appear exactly once, does every name a field
//! refers to resolve to one of those declarations, and is the resulting
//! reference graph acyclic.
//!
//! It deliberately does not own the naming rule (`ir/struct_ref.rs`) or
//! uniqueness across messages (`uniqueness.rs`). Qualification runs during
//! lowering and cannot make a message's own table well formed; this pass is
//! what proves the table those qualified names were built from.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Field, Message, StructOrigin, StructRef};

use super::{ValidationError, error::diagnostic};

pub(super) fn validate_structs(message: &Message, errors: &mut Vec<ValidationError>) {
    validate_unique_declarations(message, errors);
    validate_references(message, errors);
    validate_acyclic(message, errors);
}

/// Reports a struct name this message declares more than once.
///
/// the earlier flat naming rule qualifies nested structs by their owning message, which is exactly
/// sufficient only because no message declares one name with two shapes. That
/// is a measured property of today's corpus, so it is checked on every run
/// rather than trusted: an upstream schema that breaks it must fail here with
/// both declaration sites named, never silently merge two shapes into one Rust
/// type or emit the same type twice.
fn validate_unique_declarations(message: &Message, errors: &mut Vec<ValidationError>) {
    let mut declared: BTreeMap<&str, StructOrigin> = BTreeMap::new();

    for declaration in message.structs.declarations() {
        let name = declaration.name.declared();
        if let Some(previous) = declared.insert(name, declaration.origin) {
            errors.push(diagnostic(
                message,
                None,
                "KAFKA_SCHEMA_DUPLICATE_STRUCT",
                &format!(
                    "struct `{name}` is declared twice in one message, {} and {}",
                    previous.describe(),
                    declaration.origin.describe()
                ),
            ));
        }
    }
}

/// Reports struct references with no declaration, and declarations never used.
fn validate_references(message: &Message, errors: &mut Vec<ValidationError>) {
    let mut referenced = BTreeSet::new();
    for fields in message
        .common_structs
        .iter()
        .map(|common| common.fields.as_slice())
        .chain(std::iter::once(message.fields.as_slice()))
    {
        collect_references(message, fields, &mut referenced, errors);
    }

    for common in &message.common_structs {
        if !referenced.contains(common.name.declared()) {
            errors.push(diagnostic(
                message,
                None,
                "KAFKA_SCHEMA_UNUSED_COMMON_STRUCT",
                &format!(
                    "common struct `{}` is never referred to",
                    common.name.declared()
                ),
            ));
        }
    }
}

/// Walks the field tree so an unbound reference is reported at its own field.
///
/// Resolution is what turns a qualified name into a real type, so a name that
/// binds to nothing is a schema fault and is reported here — naming the field,
/// the spelling, and the type it would otherwise have been emitted as. The
/// alternative is a renderer emitting a reference to a type it never declares,
/// which surfaces as rustc failing on generated code with no path back to the
/// schema that caused it.
fn collect_references<'a>(
    message: &Message,
    fields: &'a [Field],
    referenced: &mut BTreeSet<&'a str>,
    errors: &mut Vec<ValidationError>,
) {
    for field in fields {
        if let Some(reference) = field.ty.struct_reference() {
            referenced.insert(reference.declared());

            if message.structs.resolve(reference.declared()).is_none() {
                errors.push(diagnostic(
                    message,
                    Some(field),
                    "KAFKA_SCHEMA_UNRESOLVED_STRUCT",
                    &format!(
                        "struct `{}` is referred to but never declared, inline or in \
                         commonStructs; nothing would define the `{}` it resolves to",
                        reference.declared(),
                        reference.rust_type(),
                    ),
                ));
            }
        }
        collect_references(message, &field.fields, referenced, errors);
    }
}

/// Reports a struct that can reach itself through the reference graph.
///
/// Inline declarations cannot cycle — JSON nesting is a tree — but a
/// `commonStructs` entry is reachable by name from anywhere in the message, so
/// two of them may refer to each other. Left undetected that is not a bad
/// diagnostic later; it is an infinitely sized Rust type and a resolver that
/// does not terminate.
fn validate_acyclic(message: &Message, errors: &mut Vec<ValidationError>) {
    let mut graph: BTreeMap<&str, &[StructRef]> = BTreeMap::new();
    for declaration in message.structs.declarations() {
        // First declaration wins, matching `StructTable::resolve`. A second is
        // already reported above, and picking a different edge set here would
        // make the cycle report disagree with the resolution it describes.
        graph
            .entry(declaration.name.declared())
            .or_insert(declaration.references.as_slice());
    }

    let mut settled: BTreeSet<&str> = BTreeSet::new();

    for root in graph.keys() {
        if settled.contains(root) {
            continue;
        }

        let mut path: Vec<&str> = vec![root];
        let mut on_path: BTreeSet<&str> = [*root].into_iter().collect();
        let mut cursor: Vec<usize> = vec![0];

        while let Some(current) = path.last().copied() {
            let step = cursor.last().copied().unwrap_or(0);
            let next = graph
                .get(current)
                .and_then(|references| references.get(step))
                .map(StructRef::declared);

            let Some(next) = next else {
                path.pop();
                cursor.pop();
                on_path.remove(current);
                settled.insert(current);
                continue;
            };
            if let Some(last) = cursor.last_mut() {
                *last += 1;
            }

            if on_path.contains(next) {
                errors.push(diagnostic(
                    message,
                    None,
                    "KAFKA_SCHEMA_STRUCT_CYCLE",
                    &format!("struct `{next}` participates in a reference cycle via `{current}`"),
                ));
                // `next` stays unsettled: it is still on the path and will be
                // marked when it pops, so a second, independent cycle through
                // it is still reported rather than swallowed here.
                continue;
            }
            if settled.contains(next) || !graph.contains_key(next) {
                continue;
            }

            path.push(next);
            on_path.insert(next);
            cursor.push(0);
        }
    }
}
