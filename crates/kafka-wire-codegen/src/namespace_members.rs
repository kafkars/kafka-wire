//! Claims compiler-synthesized members beside schema-authored struct fields.
//!
//! This file owns member scope only. Module, export, and crate-root namespaces
//! remain in `namespace.rs`.

use std::collections::BTreeMap;

use crate::{GenerationError, render::declared_structs, source::MessageSource};

pub(crate) fn validate_synthesized_members(source: &MessageSource) -> Result<(), GenerationError> {
    let message = &source.message;
    claim_members(
        message.name.rust_type(),
        &message.fields,
        !message.effective_flexible_versions().is_empty(),
    )?;
    for declaration in declared_structs(message)? {
        claim_members(
            declaration.name.rust_type(),
            declaration.fields,
            !declaration.flexible_versions.is_empty(),
        )?;
    }
    Ok(())
}

fn claim_members(
    owner: &str,
    fields: &[kafka_wire_schema::Field],
    flexible: bool,
) -> Result<(), GenerationError> {
    if !flexible {
        return Ok(());
    }
    let namespace = format!("the generated `{owner}` member namespace");
    let mut claimed = BTreeMap::new();
    for field in fields {
        claim(
            &mut claimed,
            &namespace,
            field.name.rust_field(),
            &format!("schema field {}", field.name.protocol()),
        )?;
    }
    claim(
        &mut claimed,
        &namespace,
        "unknown_tagged_fields",
        "compiler-synthesized unknown tagged-field storage",
    )
}

fn claim(
    claimed: &mut BTreeMap<String, String>,
    namespace: &str,
    symbol: &str,
    producer: &str,
) -> Result<(), GenerationError> {
    if let Some(first) = claimed.get(symbol) {
        return Err(GenerationError::GeneratedSymbolCollision {
            namespace: namespace.to_owned(),
            symbol: symbol.to_owned(),
            first: first.clone(),
            second: producer.to_owned(),
        });
    }
    claimed.insert(symbol.to_owned(), producer.to_owned());
    Ok(())
}
