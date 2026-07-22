//! Generated, version-aware Kafka wire messages.
//!
//! Callers use this flat facade. Internal module placement and generated file
//! names are not part of the public API.

mod descriptor;
mod frame;
mod generated;
mod message;

pub use descriptor::{ApiDescriptor, MessageDescriptor, MessageDirection};
pub use frame::{OutboundFrameLimits, encode_request, response_header_version_for};
pub use message::{KafkaMessage, KafkaRequest, KafkaResponse, RequestResponsePair};

// The generated half of the flat facade: one `pub use` naming every generated
// item, emitted by `kafka-wire-codegen` and hashed in MANIFEST.json beside the
// modules it names. Included rather than declared because these names must land
// at the crate root, and `include!` is the only construct that puts items there
// from another file — which is what retired the wildcard re-export this line
// replaced. Adding an API changes this list in the diff, by construction.
include!("generated/exports.rsi");
