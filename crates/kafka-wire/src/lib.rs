//! Generated, version-aware Kafka wire messages.
//!
//! Callers use this flat facade. Internal module placement and generated file
//! names are not part of the public API.

mod consumer_protocol;
mod descriptor;
mod frame;
mod generated;
mod message;
mod retained;

#[cfg(test)]
mod consumer_protocol_test;
#[cfg(test)]
mod retained_test;
#[cfg(test)]
mod tagged_claims_test;

pub use consumer_protocol::{
    decode_consumer_protocol_assignment, decode_consumer_protocol_subscription,
    encode_consumer_protocol_assignment, encode_consumer_protocol_subscription,
};
pub use descriptor::{ApiDescriptor, MessageDescriptor, MessageDirection};
pub use frame::{
    OutboundFrameLimits, RequestFrameMeasure, encode_request, measure_request,
    response_header_version_for,
};
pub use message::{KafkaMessage, KafkaRequest, KafkaResponse, ProtocolEq, RequestResponsePair};
pub use retained::{RetainedFootprint, RetainedSize};

// The generated half of the flat facade: one `pub use` naming every generated
// item, emitted by `kafka-wire-codegen` and hashed in MANIFEST.json beside the
// modules it names. Included rather than declared because these names must land
// at the crate root, and `include!` is the only construct that puts items there
// from another file — which is what retired the wildcard re-export this line
// replaced. Adding an API changes this list in the diff, by construction.
include!("generated/exports.rsi");
