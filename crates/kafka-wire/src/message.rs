//! Stable traits relating generated messages to Kafka API metadata.

use kafka_wire_core::{
    ApiKey, ApiVersion, DecodeError, EncodeError, KafkaDecode, KafkaEncode, VersionRange,
};

/// Shared metadata and wire contracts for every generated message.
pub trait KafkaMessage: KafkaEncode + KafkaDecode {
    /// Upstream protocol name.
    const NAME: &'static str;
    /// Inclusive supported version range.
    const SUPPORTED_VERSIONS: VersionRange;
    /// Flexible versions intersected with supported versions.
    const FLEXIBLE_VERSIONS: Option<VersionRange>;

    /// Returns whether this message supports `version`.
    fn supports(version: ApiVersion) -> bool {
        Self::SUPPORTED_VERSIONS.contains(version)
    }

    /// Returns whether `version` uses flexible encoding.
    fn is_flexible(version: ApiVersion) -> bool {
        Self::FLEXIBLE_VERSIONS.is_some_and(|range| range.contains(version))
    }
}

/// Client-to-server Kafka message.
pub trait KafkaRequest: KafkaMessage {
    /// Numeric Kafka API key.
    const API_KEY: ApiKey;
}

/// Server-to-client Kafka message.
pub trait KafkaResponse: KafkaMessage {
    /// Numeric Kafka API key.
    const API_KEY: ApiKey;
}

/// Request whose generated response type is available in this crate.
pub trait RequestResponsePair: KafkaRequest {
    /// Response associated with this request API key.
    type Response: KafkaResponse;
}

pub(crate) fn ensure_decode_version<M: KafkaMessage>(
    version: ApiVersion,
) -> Result<(), DecodeError> {
    if M::supports(version) {
        Ok(())
    } else {
        Err(DecodeError::UnsupportedVersion {
            message: M::NAME,
            version,
            supported: M::SUPPORTED_VERSIONS,
        })
    }
}

pub(crate) fn ensure_encode_version<M: KafkaMessage>(
    version: ApiVersion,
) -> Result<(), EncodeError> {
    if M::supports(version) {
        Ok(())
    } else {
        Err(EncodeError::UnsupportedVersion {
            message: M::NAME,
            version,
            supported: M::SUPPORTED_VERSIONS,
        })
    }
}
