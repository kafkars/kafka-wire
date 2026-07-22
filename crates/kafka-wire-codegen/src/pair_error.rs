//! API-pair construction diagnostics.
//!
//! These errors own failures in checked name, direction, version, flexibility,
//! and negotiation-policy compatibility. Rendering and filesystem diagnostics
//! remain in the generator-wide error vocabulary.

use thiserror::Error;

/// A request and response could not form one validated Kafka API pair.
#[derive(Debug, Error)]
pub enum PairError {
    /// A directional message did not carry a checked API identity.
    #[error("invalid API name derived from {message}: {reason}")]
    InvalidApiName {
        /// Protocol message name.
        message: String,
        /// Failed normalization.
        reason: String,
    },
    /// Two messages claimed the same direction for one API key.
    #[error("duplicate {direction} message for API key {api_key}: {left} and {right}")]
    DuplicateDirection {
        /// API key.
        api_key: i16,
        /// Request or response.
        direction: &'static str,
        /// First message.
        left: String,
        /// Second message.
        right: String,
    },
    /// One request and response sharing an API key had different API stems.
    #[error("API key {api_key} has mismatched pair names: {request} and {response}")]
    NameMismatch {
        /// API key.
        api_key: i16,
        /// Request name.
        request: String,
        /// Response name.
        response: String,
    },
    /// An API key did not have both directional schemas.
    #[error("API key {api_key} has no {direction} schema")]
    MissingDirection {
        /// Numeric Kafka API key.
        api_key: i16,
        /// Missing request or response direction.
        direction: &'static str,
    },
    /// The two directions disagreed on their supported versions.
    #[error(
        "request/response supported-version mismatch for API {api_key}: request `{request}`, response `{response}`"
    )]
    SupportedVersions {
        /// Numeric Kafka API key.
        api_key: i16,
        /// Request version set.
        request: String,
        /// Response version set.
        response: String,
    },
    /// The two directions disagreed on their effective flexible versions.
    #[error(
        "request/response flexible-version mismatch for API {api_key}: request `{request}`, response `{response}`"
    )]
    FlexibleVersions {
        /// Numeric Kafka API key.
        api_key: i16,
        /// Request effective flexible set.
        request: String,
        /// Response effective flexible set.
        response: String,
    },
    /// Response metadata tried to own request-side negotiation policy.
    #[error(
        "invalid unstable-version policy for API {api_key}: response {response} declares latestVersionUnstable"
    )]
    UnstablePolicy {
        /// Numeric Kafka API key.
        api_key: i16,
        /// Response claiming the request-owned policy.
        response: String,
    },
}
