//! Kafka protocol source adapter and normalized semantic schema.
//!
//! This crate is the compiler front end. It may read pinned upstream files, but
//! it has no knowledge of runtime encoding types or generated Rust modules.

mod ir;
mod load;
mod lower;
mod raw;
mod source;
mod validate;

pub use ir::{
    ApiName, CommonStruct, DefaultValue, EntityType, EntityTypeParseError, Field, FieldName,
    FieldType, FloatDefault, Message, MessageKind, MessageName, Qualification, RustIdent,
    RustIdentError, StructDeclaration, StructOrigin, StructRef, StructTable, TypeParseError,
    VersionParseError, VersionRange, VersionSet,
};
pub use load::{SchemaError, load_message, load_message_with, load_source, load_source_with};
pub use lower::{LowerError, lower_message};
pub use raw::{RawCommonStruct, RawField, RawMessage, RawMessageKind};
pub use source::{SourceError, SourceFile, parse_jsonc};
pub use validate::{
    SchemaException, SchemaExceptions, ValidationError, ValidationErrors, validate_message,
    validate_message_with, validate_struct_names,
};
