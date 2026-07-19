//! Normalized message metadata and root field order.

use std::path::PathBuf;

use super::{Field, MessageName, VersionSet};

/// Kafka message direction.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MessageKind {
    /// Client-to-server message.
    Request,
    /// Server-to-client message.
    Response,
}

/// One normalized Kafka protocol message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    /// Source file path used for provenance and diagnostics.
    pub source: PathBuf,
    /// Numeric Kafka API key.
    pub api_key: i16,
    /// Request or response direction.
    pub kind: MessageKind,
    /// Listener scopes preserved from upstream.
    pub listeners: Vec<String>,
    /// Protocol and Rust names.
    pub name: MessageName,
    /// Declared supported versions.
    pub valid_versions: VersionSet,
    /// Declared flexible versions.
    pub flexible_versions: VersionSet,
    /// Ordered root fields.
    pub fields: Vec<Field>,
}

impl Message {
    /// Returns flexible versions restricted to currently valid message versions.
    pub fn effective_flexible_versions(&self) -> VersionSet {
        self.flexible_versions.intersection(&self.valid_versions)
    }
}
