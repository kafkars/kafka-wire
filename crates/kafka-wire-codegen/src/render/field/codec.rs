//! Primitive read and write expressions selected by field encoding mode.

use kafka_wire_schema::{Field, FieldType, Message};

pub(crate) fn read_expression(field: &Field, message: &Message) -> String {
    let present = field.versions.intersection(&message.valid_versions);
    let flexible = message.effective_flexible_versions();
    let nullable = !field
        .nullable_versions
        .intersection(&message.valid_versions)
        .is_empty();
    let compact_only = present.is_subset_of(&flexible);
    let legacy_only = present.intersection(&flexible).is_empty();

    let legacy = read_method(field, nullable, false);
    let compact = read_method(field, nullable, true);
    if compact_only {
        compact
    } else if legacy_only {
        legacy
    } else {
        format!("if Self::is_flexible(version) {{ {compact} }} else {{ {legacy} }}")
    }
}

pub(crate) fn write_statement(field: &Field, message: &Message) -> String {
    let present = field.versions.intersection(&message.valid_versions);
    let flexible = message.effective_flexible_versions();
    let nullable = !field
        .nullable_versions
        .intersection(&message.valid_versions)
        .is_empty();
    let compact_only = present.is_subset_of(&flexible);
    let legacy_only = present.intersection(&flexible).is_empty();

    let legacy = write_method(field, nullable, false);
    let compact = write_method(field, nullable, true);
    if compact_only {
        compact
    } else if legacy_only {
        legacy
    } else {
        format!("if Self::is_flexible(version) {{ {compact} }} else {{ {legacy} }}")
    }
}

fn read_method(field: &Field, nullable: bool, compact: bool) -> String {
    match &field.ty {
        FieldType::String if nullable && compact => {
            "decoder.read_compact_nullable_string()?".to_owned()
        }
        FieldType::String if nullable => "decoder.read_nullable_string()?".to_owned(),
        FieldType::String if compact => "decoder.read_compact_string()?".to_owned(),
        FieldType::String => "decoder.read_string()?".to_owned(),
        FieldType::Int16 => "decoder.read_i16()?".to_owned(),
        FieldType::Int32 => "decoder.read_i32()?".to_owned(),
        FieldType::Array(_) => "/* array read rendered as a structured block */".to_owned(),
        other => format!("/* unsupported read {other:?} */"),
    }
}

fn write_method(field: &Field, nullable: bool, compact: bool) -> String {
    let name = field.name.rust_field();
    match &field.ty {
        FieldType::String if nullable && compact => {
            format!("encoder.write_compact_nullable_string(self.{name}.as_ref())?;")
        }
        FieldType::String if nullable => {
            format!("encoder.write_nullable_string(self.{name}.as_ref())?;")
        }
        FieldType::String if compact => {
            format!("encoder.write_compact_string(&self.{name})?;")
        }
        FieldType::String => format!("encoder.write_string(&self.{name})?;"),
        FieldType::Int16 => format!("encoder.write_i16(self.{name})?;"),
        FieldType::Int32 => format!("encoder.write_i32(self.{name})?;"),
        FieldType::Array(_) => "/* array write rendered as a structured block */".to_owned(),
        other => format!("/* unsupported write {other:?} */"),
    }
}
