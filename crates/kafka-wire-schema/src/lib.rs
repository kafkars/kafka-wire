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
    DefaultValue, Field, FieldName, FieldType, Message, MessageKind, MessageName,
    VersionParseError, VersionRange, VersionSet,
};
pub use load::{SchemaError, load_message};
pub use lower::{LowerError, lower_message};
pub use raw::{RawField, RawMessage, RawMessageKind};
pub use source::{SourceError, SourceFile, parse_jsonc};
pub use validate::{ValidationError, ValidationErrors, validate_message};
