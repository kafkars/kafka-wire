//! Encoding facade.
//!
//! Primitive writers, targets, errors, and complete-message traits live in
//! focused child modules.

mod array_len;
mod bytes;
mod encoder;
mod error;
mod target;
mod uuid;
mod value;
mod varint;

pub use encoder::Encoder;
pub use error::EncodeError;
pub use target::{BufferTarget, EncodeTarget, SizeTarget};
pub use value::KafkaEncode;
