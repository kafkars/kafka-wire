//! Stable traits relating generated messages to Kafka API metadata.

use kafka_wire_core::{
    ApiKey, ApiVersion, DecodeError, EncodeError, KafkaDecode, KafkaEncode, VersionRange,
};

use crate::ApiDescriptor;

/// Shared metadata and wire contracts for every generated message.
///
/// Generated unchecked writers are private implementation details. In
/// particular, downstream safe code cannot recover the old hidden trait hook
/// and use it to bypass representability validation:
///
/// ```compile_fail
/// use kafka_wire::update_features_request::FeatureUpdateKey;
/// use kafka_wire_core::{ApiVersion, BytesMut, Encoder, KafkaEncode};
///
/// let mut value = FeatureUpdateKey::default();
/// value.allow_downgrade = true;
/// let mut bytes = BytesMut::new();
/// let mut encoder = Encoder::new(&mut bytes);
/// <FeatureUpdateKey as KafkaEncode>::encode_validated(
///     &value,
///     &mut encoder,
///     ApiVersion::new(1),
/// )?;
/// # Ok::<(), kafka_wire_core::EncodeError>(())
/// ```
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
    /// Pair-level metadata shared with the generated response type.
    const API_DESCRIPTOR: &'static ApiDescriptor;
}

pub(crate) const fn latest_stable_version(
    supported: VersionRange,
    latest_unstable: bool,
) -> Option<ApiVersion> {
    if !latest_unstable {
        return Some(supported.max());
    }
    let minimum = supported.min().value();
    let maximum = supported.max().value();
    if maximum == minimum {
        None
    } else {
        Some(ApiVersion::new(maximum - 1))
    }
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
