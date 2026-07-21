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
mod uuid;

pub use api::{ApiKey, ApiVersion, VersionRange};
pub use decode::{BoundedCount, DecodeError, DecodeLimits, Decoder, KafkaDecode, TagOutcome};
pub use encode::{
    BufferTarget, EncodeError, EncodeTarget, Encoder, KafkaEncode, KnownTags, SizeTarget,
};
// Re-exported because `read_bytes` hands one out: a consumer of the generated
// protocol must be able to name the type it receives without taking a direct
// dependency on `bytes` and pinning a second version of it.
pub use bytes::Bytes;
pub use string::StrBytes;
pub use tagged::{TaggedField, TaggedFields, TaggedFieldsError};
pub use uuid::Uuid;
