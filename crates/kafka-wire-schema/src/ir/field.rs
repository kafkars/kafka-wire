//! Normalized field types and versioned field metadata.

use super::{DefaultValue, FieldName, VersionSet};

/// Backend-neutral Kafka field type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldType {
    /// Boolean.
    Bool,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Unsigned 16-bit integer.
    Uint16,
    /// Signed 32-bit integer.
    Int32,
    /// Unsigned 32-bit integer.
    Uint32,
    /// Signed 64-bit integer.
    Int64,
    /// UUID.
    Uuid,
    /// UTF-8 protocol string.
    String,
    /// Opaque bytes.
    Bytes,
    /// Opaque record set.
    Records,
    /// Ordered array.
    Array(Box<Self>),
    /// Named inline or common struct.
    Struct(String),
}

impl FieldType {
    /// Parses a Kafka source type spelling.
    pub fn parse(source: &str) -> Self {
        if let Some(element) = source.strip_prefix("[]") {
            return Self::Array(Box::new(Self::parse(element)));
        }
        match source {
            "bool" => Self::Bool,
            "int8" => Self::Int8,
            "int16" => Self::Int16,
            "uint16" => Self::Uint16,
            "int32" => Self::Int32,
            "uint32" => Self::Uint32,
            "int64" => Self::Int64,
            "uuid" => Self::Uuid,
            "string" => Self::String,
            "bytes" => Self::Bytes,
            "records" => Self::Records,
            other => Self::Struct(other.to_owned()),
        }
    }

    /// Returns whether Kafka permits this shape to be nullable.
    pub const fn permits_null(&self) -> bool {
        matches!(
            self,
            Self::String | Self::Bytes | Self::Records | Self::Array(_) | Self::Struct(_)
        )
    }
}

/// One normalized message field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Field {
    /// Protocol and Rust names.
    pub name: FieldName,
    /// Semantic field type.
    pub ty: FieldType,
    /// Declared presence versions.
    pub versions: VersionSet,
    /// Declared nullable versions.
    pub nullable_versions: VersionSet,
    /// Declared tagged versions.
    pub tagged_versions: VersionSet,
    /// Flexible tag number.
    pub tag: Option<u32>,
    /// Typed protocol default.
    pub default: DefaultValue,
    /// Whether non-default values may be omitted in older versions.
    pub ignorable: bool,
    /// In-memory map key metadata.
    pub map_key: bool,
    /// Human-facing documentation.
    pub about: String,
    /// Inline struct fields, when present.
    pub fields: Vec<Self>,
}
