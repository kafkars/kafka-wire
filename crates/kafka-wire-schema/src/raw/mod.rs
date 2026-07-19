//! Faithful deserialization vocabulary for Apache Kafka message JSON.

mod common_struct;
mod field;
mod message;

pub use common_struct::RawCommonStruct;
pub use field::RawField;
pub use message::{RawMessage, RawMessageKind};
