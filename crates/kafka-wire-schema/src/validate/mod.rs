//! Semantic schema validation after lowering.

mod annotation;
mod default;
mod error;
mod exception;
mod field;
mod message;
mod structs;
mod tag;
mod uniqueness;

pub use error::{ValidationError, ValidationErrors};
pub use exception::{SchemaException, SchemaExceptions};

pub use message::{validate_message, validate_message_with};
pub use uniqueness::validate_struct_names;
