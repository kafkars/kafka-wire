//! Semantic schema validation after lowering.

mod default;
mod error;
mod field;
mod message;

pub use error::{ValidationError, ValidationErrors};

pub use message::validate_message;
