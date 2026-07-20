//! Generated encode/decode bodies with visible version and representability gates.

use kafka_wire_schema::{FieldType, Message};

use crate::{
    GenerationError,
    render::{field, text::RustText},
};

use super::tagged::{LegacyTags, is_tagged, render_tagged_decode, render_tagged_encode};

pub(super) fn render_decode(rust: &mut RustText, message: &Message) -> Result<(), GenerationError> {
    rust.open(format!("impl KafkaDecode for {}", message.name.rust_type()));
    rust.open("fn decode(decoder: &mut Decoder, version: ApiVersion) -> Result<Self, DecodeError>");
    rust.line("crate::message::ensure_decode_version::<Self>(version)?;");
    rust.blank();

    render_reads(rust, &message.fields, message)?;
    if !message.effective_flexible_versions().is_empty() {
        render_tagged_decode(rust, &message.fields, message)?;
    }

    rust.blank();
    let has_tagged_fields = !message.effective_flexible_versions().is_empty();
    render_construction(rust, &message.fields, has_tagged_fields);
    rust.close("");
    rust.close("");
    rust.blank();
    Ok(())
}

pub(super) fn render_encode(rust: &mut RustText, message: &Message) -> Result<(), GenerationError> {
    rust.open(format!("impl KafkaEncode for {}", message.name.rust_type()));
    rust.line("fn encode<T: EncodeTarget>(");
    rust.line("    &self,");
    rust.line("    encoder: &mut Encoder<T>,");
    rust.line("    version: ApiVersion,");
    rust.open(") -> Result<(), EncodeError>");
    rust.line("crate::message::ensure_encode_version::<Self>(version)?;");
    render_representability_checks(rust, message);

    render_writes(rust, &message.fields, message)?;
    if !message.effective_flexible_versions().is_empty() {
        render_tagged_encode(rust, &message.fields, message, LegacyTags::Refuse)?;
    }

    rust.blank();
    rust.line("Ok(())");
    rust.close("");
    rust.close("");
    rust.blank();
    Ok(())
}

/// The local one decoded field binds to.
///
/// A decode body already uses `version`, `decoder`, `encoder`, the `length` and
/// `values` an array loop needs, and the `tag` a tagged-field dispatch matches
/// on. Upstream really does declare a field named `Version`, whose local would
/// otherwise shadow the `ApiVersion` parameter and hand an `i16` to everything
/// downstream of it. The struct field keeps its own name; only the local moves,
/// so the shadowing cannot happen and the generated type is unaffected.
pub(super) fn local(field: &kafka_wire_schema::Field) -> String {
    const RESERVED: &[&str] = &[
        "version", "decoder", "encoder", "length", "values", "tag", "known",
    ];
    let name = field.name.rust_field();
    if RESERVED.contains(&name) {
        return format!("{name}_value");
    }
    name.to_owned()
}

/// Emits one `let` per field, reading it or substituting its default.
///
/// Shared by messages and by the structs they declare: a struct's members are
/// versioned against the same message, so the presence gates and defaults are
/// decided by exactly the same rules.
pub(super) fn render_reads(
    rust: &mut RustText,
    fields: &[kafka_wire_schema::Field],
    message: &Message,
) -> Result<(), GenerationError> {
    for field in fields {
        // A tagged field is read from its own entry in the section at the end,
        // not from this position in the body. `render_tagged_decode` owns it.
        if is_tagged(field) {
            continue;
        }
        if let FieldType::Array(element) = &field.ty {
            let (read, _) = field::element_codec(element, field, message)?;
            let (length, _) = field::array_length_codec(field, message);
            let nullable = field::is_nullable(field, message);
            let name = local(field);
            // An array is gated by version exactly as a scalar is. Emitting the
            // block unconditionally read a later version's field out of an
            // earlier version's bytes, which Apache Kafka's own vectors caught.
            match field::presence_condition(field, message) {
                None => {
                    rust.open(format!("let {name} ="));
                    render_array_body(rust, &length, &read, nullable);
                    rust.close(";");
                }
                Some(condition) => {
                    rust.open(format!("let {name} = if {condition}"));
                    render_array_body(rust, &length, &read, nullable);
                    rust.reopen("} else {");
                    rust.line(field::default_expression(field, message));
                    rust.close(";");
                }
            }
            continue;
        }
        let expression = field::read_expression(field, message)?;
        match field::presence_condition(field, message) {
            None => rust.line(format!("let {} = {expression};", local(field))),
            Some(condition) => {
                rust.open(format!("let {} = if {condition}", local(field)));
                rust.line(expression);
                rust.reopen("} else {");
                rust.line(field::default_expression(field, message));
                rust.close(";");
            }
        }
    }
    Ok(())
}

/// Emits one write per field, gated where the field is not present in every
/// version. The counterpart of `render_reads`.
pub(super) fn render_writes(
    rust: &mut RustText,
    fields: &[kafka_wire_schema::Field],
    message: &Message,
) -> Result<(), GenerationError> {
    for field in fields {
        // The counterpart of the skip in `render_reads`: a tagged field is
        // written into the section, and only when it is not at its default.
        if is_tagged(field) {
            continue;
        }
        if let FieldType::Array(element) = &field.ty {
            let (_, write) = field::element_codec(element, field, message)?;
            let (_, length) = field::array_length_codec(field, message);
            let nullable = field::is_nullable(field, message);
            let name = field.name.rust_field();
            let gate = field::presence_condition(field, message);
            if let Some(condition) = &gate {
                rust.open(format!("if {condition}"));
            }
            if nullable {
                render_nullable_array_encode(rust, name, &length, &write);
            } else {
                render_array_encode(rust, name, &length, &write);
            }
            if gate.is_some() {
                rust.close("");
            }
            continue;
        }
        let statement = field::write_statement(field, message)?;
        match field::presence_condition(field, message) {
            None => rust.line(statement),
            Some(condition) => {
                rust.open(format!("if {condition}"));
                rust.line(statement);
                rust.close("");
            }
        }
    }
    Ok(())
}

/// Emits the `Ok(Self { .. })` that closes a decode body.
pub(super) fn render_construction(
    rust: &mut RustText,
    fields: &[kafka_wire_schema::Field],
    tagged: bool,
) {
    if fields.len() == 1 && !tagged {
        rust.line(format!("Ok(Self {{ {} }})", binding(&fields[0])));
        return;
    }
    rust.open("Ok(Self");
    for field in fields {
        rust.line(format!("{},", binding(field)));
    }
    if tagged {
        rust.line("unknown_tagged_fields,");
    }
    rust.close(")");
}

/// How one field appears in `Ok(Self { .. })`: shorthand, or named when its
/// local had to move out of the way of a name the body already uses.
fn binding(field: &kafka_wire_schema::Field) -> String {
    let name = field.name.rust_field();
    let local = local(field);
    if local == name {
        return name.to_owned();
    }
    format!("{name}: {local}")
}

fn render_representability_checks(rust: &mut RustText, message: &Message) {
    let conditional = message
        .fields
        .iter()
        .filter_map(|candidate| {
            field::absence_condition(candidate, message)
                .filter(|_| !candidate.ignorable)
                .map(|condition| (candidate, condition))
        })
        .collect::<Vec<_>>();
    if conditional.is_empty() {
        rust.blank();
        return;
    }

    rust.blank();
    for (candidate, condition) in conditional {
        rust.open(format!(
            "if {condition} && {}",
            field::non_default_condition(candidate, message)
        ));
        rust.open("return Err(EncodeError::FieldNotRepresentable");
        rust.line("message: Self::NAME,");
        rust.line(format!("field: {:?},", candidate.name.protocol()));
        rust.line("version,");
        rust.close(");");
        rust.close("");
    }
    rust.blank();
}

/// The array read as one expression, so a version gate can wrap it.
///
/// A nullable array keeps absent and empty distinct: the nullable readers
/// return `Option<usize>`, and only the present arm allocates.
pub(super) fn render_array_body(rust: &mut RustText, length: &str, element: &str, nullable: bool) {
    if nullable {
        rust.open(format!("match {length}"));
        rust.line("None => None,");
        rust.open("Some(length) =>");
        rust.line("let mut values = Vec::with_capacity(length);");
        rust.open("for _ in 0..length");
        rust.line(format!("values.push({element});"));
        rust.close("");
        rust.line("Some(values)");
        rust.close("");
        rust.close("");
        return;
    }
    rust.line(format!("let length = {length};"));
    rust.line("let mut values = Vec::with_capacity(length);");
    rust.open("for _ in 0..length");
    rust.line(format!("values.push({element});"));
    rust.close("");
    rust.line("values");
}

/// Writes the prefix once, then the elements only when the array is present.
pub(super) fn render_nullable_array_encode(
    rust: &mut RustText,
    name: &str,
    length: &str,
    element: &str,
) {
    rust.line(length);
    rust.open(format!("if let Some(values) = &self.{name}"));
    rust.open("for value in values");
    rust.line(element);
    rust.close("");
    rust.close("");
}

pub(super) fn render_array_encode(rust: &mut RustText, name: &str, length: &str, element: &str) {
    rust.line(length);
    rust.open(format!("for value in &self.{name}"));
    rust.line(element);
    rust.close("");
}
