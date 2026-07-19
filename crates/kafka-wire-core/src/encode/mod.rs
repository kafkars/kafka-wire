//! Encoding facade.
//!
//! Primitive writers, targets, errors, and complete-message traits live in
//! focused child modules.

mod encoder;
mod error;
mod target;
mod value;

pub use encoder::Encoder;
pub use error::EncodeError;
pub use target::{BufferTarget, EncodeTarget, SizeTarget};
pub use value::KafkaEncode;
