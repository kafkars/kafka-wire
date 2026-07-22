//! Generated encode/decode bodies with visible version and representability gates.

use kafka_wire_schema::{FieldType, Message};

use crate::{
    GenerationError,
    render::{field, text::RustText},
};

use super::imports::{ExternalSymbol as S, spell};
use super::tagged::{is_tagged, render_tagged_decode, render_tagged_encode};

pub(super) fn render_decode(rust: &mut RustText, message: &Message) -> Result<(), GenerationError> {
    rust.open(format!(
        "impl {} for {}",
        spell(message, S::KafkaDecode),
        message.name.rust_type()
    ));
    rust.open(format!(
        "fn decode(decoder: &mut {}, version: {}) -> {}<Self, {}>",
        spell(message, S::Decoder),
        spell(message, S::ApiVersion),
        spell(message, S::Result),
        spell(message, S::DecodeError),
    ));
    rust.line("crate::message::ensure_decode_version::<Self>(version)?;");
    rust.blank();

    render_reads(rust, &message.fields, message)?;
    if !message.effective_flexible_versions().is_empty() {
        render_tagged_decode(rust, &message.fields, message)?;
    }

    rust.blank();
    let has_tagged_fields = !message.effective_flexible_versions().is_empty();
    render_construction(rust, &message.fields, has_tagged_fields, message);
    rust.close("");
    rust.close("");
    rust.blank();
    Ok(())
}

pub(super) fn render_encode(rust: &mut RustText, message: &Message) -> Result<(), GenerationError> {
    render_struct_encode(
        rust,
        message.name.rust_type(),
        &message.fields,
        message,
        !message.effective_flexible_versions().is_empty(),
    )
}

/// Emits one checked public encoder around one reusable validated write body.
pub(super) fn render_struct_encode(
    rust: &mut RustText,
    rust_type: &str,
    fields: &[kafka_wire_schema::Field],
    message: &Message,
    flexible: bool,
) -> Result<(), GenerationError> {
    render_validated_encode_body(rust, rust_type, fields, message, flexible)?;
    rust.open(format!(
        "impl {} for {}",
        spell(message, S::KafkaEncode),
        rust_type
    ));
    rust.line(format!(
        "fn encode<T: {}>(",
        spell(message, S::EncodeTarget)
    ));
    rust.line("    &self,");
    rust.line(format!(
        "    encoder: &mut {}<T>,",
        spell(message, S::Encoder)
    ));
    rust.line(format!("    version: {},", spell(message, S::ApiVersion)));
    rust.open(format!(
        ") -> {}<(), {}>",
        spell(message, S::Result),
        spell(message, S::EncodeError)
    ));
    rust.line("self.validate_for_version(version)?;");
    rust.line(format!(
        "{rust_type}::encode_validated(self, encoder, version)"
    ));
    rust.close("");
    rust.blank();
    let validated_body =
        format!("    |encoder| {rust_type}::encode_validated(self, encoder, version),");
    rust.open(format!(
        "fn encoded_len(&self, version: {}) -> {}<usize, {}>",
        spell(message, S::ApiVersion),
        spell(message, S::Result),
        spell(message, S::EncodeError),
    ));
    rust.line(format!("{}(", spell(message, S::EncodedLenWith)));
    rust.line("    || self.validate_for_version(version),");
    rust.line(&validated_body);
    rust.line(")");
    rust.close("");
    rust.blank();
    rust.line("fn encode_into(");
    rust.line("    &self,");
    rust.line(format!("    buffer: &mut {},", spell(message, S::BytesMut)));
    rust.line(format!("    version: {},", spell(message, S::ApiVersion)));
    rust.open(format!(
        ") -> {}<usize, {}>",
        spell(message, S::Result),
        spell(message, S::EncodeError)
    ));
    rust.line(format!("{}(", spell(message, S::EncodeIntoWith)));
    rust.line("    buffer,");
    rust.line("    || self.validate_for_version(version),");
    rust.line(&validated_body);
    rust.line(validated_body);
    rust.line(")");
    rust.close("");
    rust.close("");
    rust.blank();
    Ok(())
}

fn render_validated_encode_body(
    rust: &mut RustText,
    rust_type: &str,
    fields: &[kafka_wire_schema::Field],
    message: &Message,
    flexible: bool,
) -> Result<(), GenerationError> {
    let mut body = RustText::default();
    render_writes(&mut body, fields, message)?;
    if flexible {
        render_tagged_encode(&mut body, fields, message)?;
    }
    body.blank();
    body.line(format!("{}(())", spell(message, S::Ok)));
    let body = body.finish();
    let version = if validated_body_uses_version(&body) {
        "version"
    } else {
        "_version"
    };

    rust.open(format!("impl {rust_type}"));
    rust.line(format!(
        "fn encode_validated<T: {}>(",
        spell(message, S::EncodeTarget)
    ));
    rust.line("    &self,");
    rust.line(format!(
        "    encoder: &mut {}<T>,",
        spell(message, S::Encoder)
    ));
    rust.line(format!("    {version}: {},", spell(message, S::ApiVersion)));
    rust.open(format!(
        ") -> {}<(), {}>",
        spell(message, S::Result),
        spell(message, S::EncodeError)
    ));
    for line in body.lines() {
        if line.is_empty() {
            rust.blank();
        } else {
            rust.line(line);
        }
    }
    rust.close("");
    rust.close("");
    rust.blank();
    Ok(())
}

fn validated_body_uses_version(body: &str) -> bool {
    body.contains("version.value()")
        || body.contains("Self::is_flexible(version)")
        || body.contains("encoder, version)")
}

/// The compiler-owned local one decoded field binds to.
///
/// Position, rather than a schema name plus a collision suffix, makes every
/// binding unique by construction. Construction below always spells the member
/// and local explicitly, so schema vocabulary never enters the emitter's local
/// namespace.
pub(super) fn local(index: usize) -> String {
    format!("__kw_field_{index}")
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
    for (index, field) in fields.iter().enumerate() {
        // A tagged field is read from its own entry in the section at the end,
        // not from this position in the body. `render_tagged_decode` owns it.
        if is_tagged(field) {
            continue;
        }
        if let FieldType::Array(element) = &field.ty {
            let (read, _) = field::element_codec(element, field, message)?;
            let (length, _) = field::array_length_codec(field, message);
            let nullable = field::is_nullable(field, message);
            let name = local(index);
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
            None => rust.line(format!("let {} = {expression};", local(index))),
            Some(condition) => {
                rust.open(format!("let {} = if {condition}", local(index)));
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
                render_nullable_array_encode(rust, message, name, &length, &write);
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
    message: &Message,
) {
    if fields.len() == 1 && !tagged {
        rust.line(format!(
            "{}(Self {{ {} }})",
            spell(message, S::Ok),
            binding(0, &fields[0])
        ));
        return;
    }
    rust.open(format!("{}(Self", spell(message, S::Ok)));
    for (index, field) in fields.iter().enumerate() {
        rust.line(format!("{},", binding(index, field)));
    }
    if tagged {
        rust.line("unknown_tagged_fields,");
    }
    rust.close(")");
}

/// How one schema member is assigned from its compiler-owned decode local.
fn binding(index: usize, field: &kafka_wire_schema::Field) -> String {
    format!("{}: {}", field.name.rust_field(), local(index))
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
        return format!("|decoder| {}({element})", spell(message, S::Ok));
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
        return format!("{}::{method}", spell(message, S::Decoder));
    }
    format!("|decoder| {fallible}")
}

/// Writes the prefix once, then the elements only when the array is present.
pub(super) fn render_nullable_array_encode(
    rust: &mut RustText,
    message: &Message,
    name: &str,
    length: &str,
    element: &str,
) {
    rust.line(length);
    rust.open(format!(
        "if let {}(values) = &self.{name}",
        spell(message, S::Some)
    ));
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
