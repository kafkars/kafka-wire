//! Encoding failure vocabulary.
//!
//! Errors distinguish malformed values, version incompatibility, and internal
//! sizing divergence without naming a concrete Kafka API in the runtime.

use thiserror::Error;

use crate::{ApiVersion, TaggedFieldsError, VersionRange};

/// Kafka wire encoding failure.
#[non_exhaustive]
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

    /// A retained unknown field uses a tag the selected schema version owns.
    #[error("{message} tag {tag} is known in version {version} and cannot also be retained")]
    KnownTagConflict {
        /// Protocol message or structure name.
        message: &'static str,
        /// Numeric tag claimed by both representations.
        tag: u32,
        /// Version in which the tag is known.
        version: ApiVersion,
    },

    /// A known tagged value was measured without first claiming its numeric ID.
    #[error("known tagged field {tag} was measured without an active claim")]
    UnclaimedKnownTag {
        /// Tag missing from the active claim set.
        tag: u32,
    },

    /// More active known tags were claimed than the inline buffer can hold.
    #[error("known tagged-field capacity {capacity} was exceeded")]
    KnownTagCapacityExceeded {
        /// Fixed capacity selected by the caller.
        capacity: usize,
    },

    /// Known and retained tagged fields could not be merged into one section.
    ///
    /// Reached when a field this build knows carries the same tag number as an
    /// unknown entry retained from a peer. Both claim the same slot in one
    /// ascending run, and no ordering of the two is correct.
    #[error("tagged-field section cannot be built: {0}")]
    TaggedFieldsInvalid(#[from] TaggedFieldsError),

    /// The sizing and writing targets observed different byte counts.
    #[error("encoded length predicted {predicted} bytes but wrote {actual}")]
    SizeMismatch {
        /// Length reported by `SizeTarget`.
        predicted: usize,
        /// Length written by `BufferTarget`.
        actual: usize,
    },

    /// A frame's body exceeds what its `int32` length prefix can describe.
    ///
    /// Kafka frames are length-delimited by a signed 32-bit count, so a body
    /// past `i32::MAX` has no wire representation at all. Named rather than
    /// truncated: silently writing a wrapped length would hand the peer a
    /// frame boundary in the middle of a message.
    #[error("frame body of {bytes} bytes exceeds the int32 length prefix")]
    FrameTooLarge {
        /// Bytes the header and body occupied.
        bytes: usize,
    },

    /// A frame's body exceeds the caller's configured outbound byte budget.
    #[error("frame body of {actual} bytes exceeds the outbound limit of {limit} bytes")]
    FrameLimitExceeded {
        /// Exact encoded header-and-message length from the sizing preflight.
        actual: usize,
        /// Caller-supplied maximum frame body length.
        limit: usize,
    },
}
