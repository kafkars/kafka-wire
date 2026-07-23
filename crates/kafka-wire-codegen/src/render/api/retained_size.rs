//! Recursive retained-size implementations for generated structures.

use kafka_wire_schema::{Field, Message};

use crate::render::text::RustText;

use super::imports::{ExternalSymbol as S, spell};

pub(super) fn render_retained_size(
    rust: &mut RustText,
    rust_type: &str,
    fields: &[Field],
    message: &Message,
    flexible: bool,
) {
    let footprint = spell(message, S::RetainedFootprint);
    let retained_size = spell(message, S::RetainedSize);
    let mut members = fields
        .iter()
        .map(|field| field.name.rust_field().to_owned())
        .collect::<Vec<_>>();
    if flexible {
        members.push("unknown_tagged_fields".to_owned());
    }

    rust.open(format!("impl {retained_size} for {rust_type}"));
    rust.open(format!("fn retained_size(&self) -> {footprint}"));
    rust.line(format!("{footprint}::EMPTY"));
    for member in members {
        rust.line(format!(
            ".saturating_add({retained_size}::retained_size(&self.{member}))"
        ));
    }
    rust.close("");
    rust.close("");
    rust.blank();
}
