//! Normalized message metadata, struct table, and root field order.

use std::path::PathBuf;

use super::{CommonStruct, Field, MessageName, StructTable, VersionSet};

/// What kind of schema a source file declares.
///
/// Upstream keeps four kinds of declaration in one directory and one language.
/// Only requests and responses are API messages with a key; headers frame every
/// request and response, and data schemas describe structures that travel
/// inside records rather than on their own.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MessageKind {
    /// Client-to-server message.
    Request,
    /// Server-to-client message.
    Response,
    /// Request or response frame header.
    Header,
    /// Structure carried inside records rather than sent on its own.
    Data,
}

impl MessageKind {
    /// Returns whether this kind is addressed by a numeric API key.
    ///
    /// Headers and data schemas are not dispatched, so an API key on one is a
    /// schema error rather than metadata worth keeping.
    pub const fn carries_api_key(self) -> bool {
        matches!(self, Self::Request | Self::Response)
    }

    /// Returns the name suffix upstream gives this kind, when it fixes one.
    pub const fn name_suffix(self) -> Option<&'static str> {
        match self {
            Self::Request => Some("Request"),
            Self::Response => Some("Response"),
            Self::Header => Some("Header"),
            Self::Data => None,
        }
    }
}

/// One normalized Kafka protocol message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    /// Source file path used for provenance and diagnostics.
    pub source: PathBuf,
    /// Numeric Kafka API key, absent for headers and data schemas.
    pub api_key: Option<i16>,
    /// What kind of schema this is.
    pub kind: MessageKind,
    /// Listener scopes preserved from upstream.
    pub listeners: Vec<String>,
    /// Protocol and Rust names.
    pub name: MessageName,
    /// Declared supported versions.
    pub valid_versions: VersionSet,
    /// Declared flexible versions.
    pub flexible_versions: VersionSet,
    /// Whether the highest valid version is still subject to change.
    ///
    /// Upstream sets this while a KIP is in flight. A client must not negotiate
    /// up to an unstable version by default, so this is protocol policy a
    /// version-negotiating client needs, not a build-time annotation.
    pub latest_version_unstable: bool,
    /// Structs declared at message level and referred to by name.
    pub common_structs: Vec<CommonStruct>,
    /// Ordered root fields.
    pub fields: Vec<Field>,
    /// Every struct this message declares, however it declared it.
    ///
    /// `commonStructs` and inline field bodies are two spellings of one
    /// concept, and every pass after lowering — resolution, collision checking,
    /// emission — cares only about the concept. This is that one table, in
    /// protocol declaration order.
    pub structs: StructTable,
}

impl Message {
    /// Returns flexible versions restricted to currently valid message versions.
    pub fn effective_flexible_versions(&self) -> VersionSet {
        self.flexible_versions.intersection(&self.valid_versions)
    }
}
