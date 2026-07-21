//! Generated encode/decode bodies with visible version and representability gates.

use kafka_wire_schema::{FieldType, Message};

use crate::{
    GenerationError,
    render::{field, text::RustText},
};

use super::imports::spell;
use super::tagged::{is_tagged, render_tagged_decode, render_tagged_encode};

pub(super) fn render_decode(rust: &mut RustText, message: &Message) -> Result<(), GenerationError> {
    rust.open(format!(
        "impl {} for {}",
        spell(message, "KafkaDecode"),
        message.name.rust_type()
    ));
    rust.open(format!(
        "fn decode(decoder: &mut {}, version: {}) -> Result<Self, {}>",
        spell(message, "Decoder"),
        spell(message, "ApiVersion"),
        spell(message, "DecodeError"),
    ));
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
    rust.open(format!(
        "impl {} for {}",
        spell(message, "KafkaEncode"),
        message.name.rust_type()
    ));
    rust.line(format!("fn encode<T: {}>(", spell(message, "EncodeTarget")));
    rust.line("    &self,");
    rust.line(format!(
        "    encoder: &mut {}<T>,",
        spell(message, "Encoder")
    ));
    rust.line(format!("    version: {},", spell(message, "ApiVersion")));
    rust.open(format!(
        ") -> Result<(), {}>",
        spell(message, "EncodeError")
    ));
    rust.line("self.validate_for_version(version)?;");
    rust.blank();

    render_writes(rust, &message.fields, message)?;
    if !message.effective_flexible_versions().is_empty() {
        render_tagged_encode(rust, &message.fields, message)?;
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
        "version",
        "decoder",
        "encoder",
        "length",
        "values",
        "tag",
        "known",
        // Bound by the tagged decode as the retained-tag accumulator, and named
        // in `Ok(Self { .. })` besides, so a field of this name would be
        // assigned the section it was supposed to sit beside.
        "unknown_tagged_fields",
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
                    render_array_body(rust, message, &length, &read, nullable);
                    rust.close(";");
                }
                Some(condition) => {
                    rust.open(format!("let {name} = if {condition}"));
                    render_array_body(rust, message, &length, &read, nullable);
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

/// The array read as one expression, so a version gate can wrap it.
///
/// The regime is stated once by the length reader and the elements are read by
/// `Decoder::read_vec`, rather than every array restating the same collect loop.
/// A nullable array keeps absent and empty distinct: the nullable readers return
/// `Option<usize>`, and only the present arm allocates.
pub(super) fn render_array_body(
    rust: &mut RustText,
    message: &Message,
    length: &str,
    element: &str,
    nullable: bool,
) {
    let read = element_closure(message, element);
    rust.line(format!("let length = {length};"));
    if nullable {
        rust.line(format!(
            "length.map(|length| decoder.read_vec(length, {read})).transpose()?"
        ));
        return;
    }
    rust.line(format!("decoder.read_vec(length, {read})?"));
}

/// One element read, as a closure returning the `Result` `read_vec` wants.
///
/// Every element expression is a fallible call the field emitter has already
/// suffixed with `?`. Dropping that suffix hands back the `Result` itself, which
/// is exactly the closure body — wrapping it instead would emit `Ok(x?)`, which
/// says nothing and which the lints on checked-in output reject. A gated element
/// puts its `?` inside each arm rather than at the end, so it keeps the wrapper.
fn element_closure(message: &Message, element: &str) -> String {
    let Some(fallible) = element.strip_suffix('?') else {
        return format!("|decoder| Ok({element})");
    };
    // A scalar element is a bare method call on the decoder, and naming the
    // method is both shorter and what the lints on checked-in output ask for:
    // `Decoder::read_i32` rather than a closure that only forwards to it.
    if let Some(method) = fallible
        .strip_prefix("decoder.")
        .and_then(|call| call.strip_suffix("()"))
        .filter(|method| {
            method
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
    {
        return format!("{}::{method}", spell(message, "Decoder"));
    }
    format!("|decoder| {fallible}")
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
