//! Recursive protocol-equality implementations for generated structures.
//!
//! The handwritten trait owns primitive and container semantics. This emitter
//! owns only structural descent through schema fields and synthesized tag
//! storage; it does not alter the public `PartialEq` derived by DTOs.

use kafka_wire_schema::{Field, Message};

use crate::render::text::RustText;

use super::imports::{ExternalSymbol as S, spell};

pub(super) fn render_protocol_eq(
    rust: &mut RustText,
    rust_type: &str,
    fields: &[Field],
    message: &Message,
    flexible: bool,
) {
    let protocol_eq = spell(message, S::ProtocolEq);
    let mut comparisons = fields
        .iter()
        .map(|field| {
            let name = field.name.rust_field();
            format!("{protocol_eq}::protocol_eq(&self.{name}, &other.{name})")
        })
        .collect::<Vec<_>>();
    if flexible {
        comparisons.push(format!(
            "{protocol_eq}::protocol_eq(\
             &self.unknown_tagged_fields, &other.unknown_tagged_fields)"
        ));
    }

    rust.open(format!("impl {protocol_eq} for {rust_type}"));
    rust.open("fn protocol_eq(&self, other: &Self) -> bool");
    if comparisons.is_empty() {
        rust.line("true");
    } else {
        let last = comparisons.len() - 1;
        for (index, comparison) in comparisons.into_iter().enumerate() {
            let suffix = if index == last { "" } else { " &&" };
            rust.line(format!("{comparison}{suffix}"));
        }
    }
    rust.close("");
    rust.close("");
    rust.blank();
}
