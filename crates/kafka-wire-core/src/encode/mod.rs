//! Encoding facade.
//!
//! Primitive writers, targets, errors, and complete-message traits live in
//! focused child modules.

mod array_len;
mod bytes;
mod encoder;
mod error;
mod known;
mod tag_merge;
mod target;
mod uuid;
mod value;
mod varint;

pub use encoder::Encoder;
pub use error::EncodeError;
pub use known::KnownTags;
use known::{EmittedKnownTag, KnownTag};
pub use target::{BufferTarget, EncodeTarget, PremeasuredWrite, SizeTarget};
pub use value::{KafkaEncode, encode_into_with, encoded_len_with};
