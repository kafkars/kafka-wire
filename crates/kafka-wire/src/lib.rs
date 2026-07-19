//! Generated, version-aware Kafka wire messages.
//!
//! Callers use this flat facade. Internal module placement and generated file
//! names are not part of the public API.

mod descriptor;
mod generated;
mod message;

pub use descriptor::{MessageDescriptor, MessageDirection};
pub use generated::{
    API_VERSIONS_REQUEST_DESCRIPTOR, ApiVersionsRequest, MESSAGE_DESCRIPTORS,
    SASL_HANDSHAKE_REQUEST_DESCRIPTOR, SASL_HANDSHAKE_RESPONSE_DESCRIPTOR, SaslHandshakeRequest,
    SaslHandshakeResponse,
};
pub use message::{KafkaMessage, KafkaRequest, KafkaResponse, RequestResponsePair};
