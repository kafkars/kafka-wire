//! Decoding facade.
//!
//! Resource limits, primitive readers, errors, and complete-message traits live in
//! focused child modules.

mod decoder;
mod error;
mod limits;
mod value;

pub use decoder::Decoder;
pub use error::DecodeError;
pub use limits::DecodeLimits;
pub use value::KafkaDecode;
