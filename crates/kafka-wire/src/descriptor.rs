//! Static reflection vocabulary emitted beside typed messages.

use kafka_wire_core::{ApiKey, VersionRange};

/// Direction of one Kafka protocol message.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MessageDirection {
    /// Client to server.
    Request,
    /// Server to client.
    Response,
}

/// Static metadata for one generated message.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MessageDescriptor {
    /// Numeric Kafka API key.
    pub api_key: ApiKey,
    /// Upstream protocol name.
    pub name: &'static str,
    /// Request or response direction.
    pub direction: MessageDirection,
    /// Inclusive supported versions.
    pub supported_versions: VersionRange,
    /// Flexible versions intersected with supported versions.
    pub flexible_versions: Option<VersionRange>,
}

impl MessageDescriptor {
    /// Creates static message metadata.
    pub const fn new(
        api_key: i16,
        name: &'static str,
        direction: MessageDirection,
        supported_versions: VersionRange,
        flexible_versions: Option<VersionRange>,
    ) -> Self {
        Self {
            api_key: ApiKey::new(api_key),
            name,
            direction,
            supported_versions,
            flexible_versions,
        }
    }
}

/// Static metadata shared by one validated request/response API pair.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ApiDescriptor {
    /// Numeric Kafka API key.
    pub api_key: ApiKey,
    /// Directional request metadata.
    pub request: &'static MessageDescriptor,
    /// Directional response metadata.
    pub response: &'static MessageDescriptor,
    /// Inclusive versions supported by both directions.
    pub supported_versions: VersionRange,
    /// Flexible versions supported by both directions.
    pub flexible_versions: Option<VersionRange>,
    /// Whether the highest supported version is excluded from default negotiation.
    pub latest_version_unstable: bool,
}

impl ApiDescriptor {
    /// Creates static metadata from one compiler-validated API pair.
    pub const fn new(
        api_key: i16,
        request: &'static MessageDescriptor,
        response: &'static MessageDescriptor,
        supported_versions: VersionRange,
        flexible_versions: Option<VersionRange>,
        latest_version_unstable: bool,
    ) -> Self {
        Self {
            api_key: ApiKey::new(api_key),
            request,
            response,
            supported_versions,
            flexible_versions,
            latest_version_unstable,
        }
    }

    /// Highest version suitable for default negotiation.
    pub const fn latest_stable_version(self) -> Option<kafka_wire_core::ApiVersion> {
        crate::message::latest_stable_version(self.supported_versions, self.latest_version_unstable)
    }
}
