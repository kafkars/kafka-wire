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
    /// Whether the highest version is excluded from default negotiation.
    pub latest_version_unstable: bool,
}

impl MessageDescriptor {
    /// Creates static message metadata.
    pub const fn new(
        api_key: i16,
        name: &'static str,
        direction: MessageDirection,
        supported_versions: VersionRange,
        flexible_versions: Option<VersionRange>,
        latest_version_unstable: bool,
    ) -> Self {
        Self {
            api_key: ApiKey::new(api_key),
            name,
            direction,
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
