//! Synthetic normalized messages for exercising the field renderers.
//!
//! This module owns one thing: constructing a `Message` or `Field` that isolates
//! exactly the protocol situation a renderer test wants to pin down. Version
//! sets are written the way upstream writes them (`"0-4"`, `"3+"`, `"none"`)
//! so a test case reads as the schema it stands for rather than as a struct
//! literal.
//!
//! It deliberately owns no assertion and no expected output. Every claim about
//! what the generator emits belongs in the sibling `*_test.rs` that names the
//! renderer making it.

use std::path::PathBuf;

use kafka_wire_schema::{
    DefaultValue, Field, FieldName, FieldType, Message, MessageKind, MessageName, StructRef,
    StructTable, VersionSet,
};

/// The one message name every probe uses, so struct references can qualify.
const OWNER: &str = "ProbeRequest";

/// Parses a version set written the way an upstream schema writes it.
pub(super) fn versions(spec: &str) -> VersionSet {
    spec.parse()
        .unwrap_or_else(|error| panic!("parse version set `{spec}`: {error}"))
}

/// One request message carrying `fields`, with the given version declarations.
///
/// `valid` and `flexible` are the two declarations that decide every encoding
/// question the field renderers answer, so they are the only knobs here.
pub(super) fn message(valid: &str, flexible: &str, fields: Vec<Field>) -> Message {
    Message {
        source: PathBuf::from("spec/probe/ProbeRequest.json"),
        api_key: Some(0),
        kind: MessageKind::Request,
        listeners: Vec::new(),
        name: MessageName::new(OWNER),
        valid_versions: versions(valid),
        flexible_versions: versions(flexible),
        latest_version_unstable: false,
        common_structs: Vec::new(),
        fields,
        structs: StructTable::default(),
    }
}

/// A reference to a struct declared by the probe message.
pub(super) fn struct_type(declared: &str) -> FieldType {
    FieldType::Struct(StructRef::qualify(&MessageName::new(OWNER), declared))
}

/// One non-null, ungated, untagged field of the given type.
///
/// Every renderer property under test is a departure from this baseline, so a
/// case sets exactly the declarations it is about and leaves the rest alone.
pub(super) fn field(name: &str, ty: FieldType, present: &str) -> Field {
    Field {
        name: FieldName::new(name),
        ty,
        versions: versions(present),
        nullable_versions: VersionSet::none(),
        tagged_versions: VersionSet::none(),
        tag: None,
        default: DefaultValue::Empty,
        ignorable: false,
        map_key: false,
        entity_type: None,
        zero_copy: false,
        flexible_versions: None,
        about: "Probe field.".to_owned(),
        fields: Vec::new(),
    }
}

/// The same field, declared nullable across every version it is present in.
pub(super) fn nullable(mut field: Field) -> Field {
    field.nullable_versions = field.versions.clone();
    field.default = DefaultValue::Null;
    field
}
