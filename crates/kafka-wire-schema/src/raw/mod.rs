//! Faithful deserialization vocabulary for Apache Kafka message JSON.

mod field;
mod message;

pub use field::RawField;
pub use message::{RawMessage, RawMessageKind};
