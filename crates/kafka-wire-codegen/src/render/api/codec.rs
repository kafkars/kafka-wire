//! Generated encode/decode bodies with visible version and representability gates.

use kafka_wire_schema::Message;

use crate::{
    GenerationError,
    render::{field, text::RustText},
};

pub(super) fn render_decode(rust: &mut RustText, message: &Message) -> Result<(), GenerationError> {
    rust.open(format!("impl KafkaDecode for {}", message.name.rust_type()));
    rust.open("fn decode(decoder: &mut Decoder, version: ApiVersion) -> Result<Self, DecodeError>");
    rust.line("crate::message::ensure_decode_version::<Self>(version)?;");
    rust.blank();

    for field in &message.fields {
        if field::is_legacy_string_array(field) {
            render_array_decode(rust, field.name.rust_field());
            continue;
        }
        let expression = field::read_expression(field, message)?;
        match field::presence_condition(field, message) {
            None => rust.line(format!("let {} = {expression};", field.name.rust_field())),
            Some(condition) => {
                rust.open(format!("let {} = if {condition}", field.name.rust_field()));
                rust.line(expression);
                rust.reopen("} else {");
                rust.line(field::default_expression(field, message)?);
                rust.close(";");
            }
        }
    }
    if !message.effective_flexible_versions().is_empty() {
        rust.open("let unknown_tagged_fields = if Self::is_flexible(version)");
        rust.line("decoder.read_tagged_fields()?");
        rust.reopen("} else {");
        rust.line("TaggedFields::default()");
        rust.close(";");
    }

    rust.blank();
    let has_tagged_fields = !message.effective_flexible_versions().is_empty();
    if message.fields.len() == 1 && !has_tagged_fields {
        rust.line(format!(
            "Ok(Self {{ {} }})",
            message.fields[0].name.rust_field()
        ));
    } else {
        rust.open("Ok(Self");
        for field in &message.fields {
            rust.line(format!("{},", field.name.rust_field()));
        }
        if has_tagged_fields {
            rust.line("unknown_tagged_fields,");
        }
        rust.close(")");
    }
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
    render_representability_checks(rust, message)?;

    for field in &message.fields {
        if field::is_legacy_string_array(field) {
            render_array_encode(rust, field.name.rust_field());
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
    if !message.effective_flexible_versions().is_empty() {
        rust.blank();
        rust.open("if Self::is_flexible(version)");
        rust.line("encoder.write_tagged_fields(&self.unknown_tagged_fields)?;");
        rust.reopen("} else if !self.unknown_tagged_fields.is_empty() {");
        rust.open("return Err(EncodeError::TaggedFieldsNotRepresentable");
        rust.line("message: Self::NAME,");
        rust.line("version,");
        rust.close(");");
        rust.close("");
    }

    rust.blank();
    rust.line("Ok(())");
    rust.close("");
    rust.close("");
    rust.blank();
    Ok(())
}

fn render_representability_checks(
    rust: &mut RustText,
    message: &Message,
) -> Result<(), GenerationError> {
    let conditional = message
        .fields
        .iter()
        .filter_map(|candidate| {
            field::presence_condition(candidate, message)
                .filter(|_| !candidate.ignorable)
                .map(|condition| (candidate, condition))
        })
        .collect::<Vec<_>>();
    if conditional.is_empty() {
        rust.blank();
        return Ok(());
    }

    rust.blank();
    for (candidate, condition) in conditional {
        rust.open(format!(
            "if !({condition}) && {}",
            field::non_default_condition(candidate, message)?
        ));
        rust.open("return Err(EncodeError::FieldNotRepresentable");
        rust.line("message: Self::NAME,");
        rust.line(format!("field: {:?},", candidate.name.protocol()));
        rust.line("version,");
        rust.close(");");
        rust.close("");
    }
    rust.blank();
    Ok(())
}

fn render_array_decode(rust: &mut RustText, name: &str) {
    rust.line("let length = decoder.read_array_len()?;");
    rust.line(format!("let mut {name} = Vec::with_capacity(length);"));
    rust.open("for _ in 0..length");
    rust.line(format!("{name}.push(decoder.read_string()?);"));
    rust.close("");
}

fn render_array_encode(rust: &mut RustText, name: &str) {
    rust.line(format!("encoder.write_array_len(self.{name}.len())?;"));
    rust.open(format!("for value in &self.{name}"));
    rust.line("encoder.write_string(value)?;");
    rust.close("");
}
