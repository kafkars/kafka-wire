//! Encoding failure vocabulary.
//!
//! Errors distinguish malformed values, version incompatibility, and internal
//! sizing divergence without naming a concrete Kafka API in the runtime.

use thiserror::Error;

use crate::{ApiVersion, VersionRange};

/// Kafka wire encoding failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EncodeError {
    /// A generated message does not support the requested version.
    #[error("{message} does not support version {version}; supported versions are {supported}")]
    UnsupportedVersion {
        /// Protocol message name.
        message: &'static str,
        /// Requested version.
        version: ApiVersion,
        /// Supported range.
        supported: VersionRange,
    },

    /// A value cannot fit in the wire length prefix.
    #[error("{kind} length {length} exceeds the wire maximum {maximum}")]
    LengthOverflow {
        /// Kind of length-prefixed value.
        kind: &'static str,
        /// Actual length.
        length: usize,
        /// Maximum representable length.
        maximum: usize,
    },

    /// A non-default field would be lost at the requested version.
    #[error("{message}.{field} is not representable in version {version}")]
    FieldNotRepresentable {
        /// Protocol message name.
        message: &'static str,
        /// Protocol field name.
        field: &'static str,
        /// Requested version.
        version: ApiVersion,
    },

    /// A null value was supplied to a non-nullable version.
    #[error("{message}.{field} cannot be null in version {version}")]
    NullNotAllowed {
        /// Protocol message name.
        message: &'static str,
        /// Protocol field name.
        field: &'static str,
        /// Requested version.
        version: ApiVersion,
    },

    /// Unknown tagged fields cannot be represented in a legacy version.
    #[error("{message} cannot encode unknown tagged fields in version {version}")]
    TaggedFieldsNotRepresentable {
        /// Protocol message name.
        message: &'static str,
        /// Requested version.
        version: ApiVersion,
    },

    /// The sizing and writing targets observed different byte counts.
    #[error("encoded length predicted {predicted} bytes but wrote {actual}")]
    SizeMismatch {
        /// Length reported by `SizeTarget`.
        predicted: usize,
        /// Length written by `BufferTarget`.
        actual: usize,
    },
}
