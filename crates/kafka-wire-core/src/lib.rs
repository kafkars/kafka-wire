//! Sans-I/O Kafka wire primitives.
//!
//! This crate owns API-agnostic encodings, decoding limits, versions, wire
//! strings, and unknown tagged fields. It deliberately knows no Kafka API names
//! and performs no networking or filesystem access.

mod api;
mod decode;
mod encode;
mod string;
mod tagged;

pub use api::{ApiKey, ApiVersion, VersionRange};
pub use decode::{DecodeError, DecodeLimits, Decoder, KafkaDecode};
pub use encode::{BufferTarget, EncodeError, EncodeTarget, Encoder, KafkaEncode, SizeTarget};
pub use string::StrBytes;
pub use tagged::{TaggedField, TaggedFields, TaggedFieldsError};
