//! Malformed-input decoding failures.
//!
//! Variants retain offsets and violated limits so callers can diagnose hostile or
//! incompatible frames without collapsing everything into “invalid data.”

use thiserror::Error;

use crate::{ApiVersion, VersionRange};

/// Kafka wire decoding failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
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

    /// Input ended before the requested primitive or payload.
    #[error(
        "unexpected end of input at byte {offset}: needed {needed} bytes, only {remaining} remain"
    )]
    UnexpectedEnd {
        /// Byte offset from the beginning of the message.
        offset: usize,
        /// Number of bytes requested.
        needed: usize,
        /// Number of bytes available.
        remaining: usize,
    },

    /// A Kafka boolean used a byte other than zero or one.
    #[error("invalid boolean byte {value} at byte {offset}")]
    InvalidBoolean {
        /// Byte offset of the boolean.
        offset: usize,
        /// Invalid byte.
        value: u8,
    },

    /// A non-null length prefix was negative.
    #[error("negative {kind} length {length} at byte {offset}")]
    NegativeLength {
        /// Kind of length-prefixed value.
        kind: &'static str,
        /// Invalid signed length.
        length: i64,
        /// Byte offset of the prefix.
        offset: usize,
    },

    /// A compact non-null value used the null sentinel.
    #[error("null sentinel used for non-null {kind} at byte {offset}")]
    NullNotAllowed {
        /// Kind of compact value.
        kind: &'static str,
        /// Byte offset of the prefix.
        offset: usize,
    },

    /// A peer-controlled length exceeded its configured budget.
    #[error("{kind} length {length} exceeds configured limit {limit} at byte {offset}")]
    LimitExceeded {
        /// Kind of length-prefixed value.
        kind: &'static str,
        /// Claimed length.
        length: usize,
        /// Configured maximum.
        limit: usize,
        /// Byte offset of the prefix.
        offset: usize,
    },

    /// String bytes were not valid UTF-8.
    #[error("invalid UTF-8 string at byte {offset}; valid prefix length is {valid_up_to}")]
    InvalidUtf8 {
        /// Byte offset of the string payload.
        offset: usize,
        /// Valid UTF-8 prefix length.
        valid_up_to: usize,
    },

    /// An unsigned varint exceeded five bytes or overflowed `u32`.
    #[error("malformed unsigned varint at byte {offset}")]
    MalformedVarint {
        /// Byte offset of the first varint byte.
        offset: usize,
    },

    /// Tagged fields were not strictly increasing.
    #[error("tagged field {current} followed {previous} at byte {offset}")]
    TaggedFieldOrder {
        /// Previous tag.
        previous: u32,
        /// Current tag.
        current: u32,
        /// Byte offset of the current tag.
        offset: usize,
    },

    /// Checked conversion or addition overflowed the host representation.
    #[error("{kind} length overflow at byte {offset}")]
    LengthOverflow {
        /// Kind of value being converted.
        kind: &'static str,
        /// Byte offset of the prefix.
        offset: usize,
    },

    /// Complete-message decoding left unread bytes.
    #[error("{remaining} trailing bytes remain after decoding")]
    TrailingBytes {
        /// Remaining byte count.
        remaining: usize,
    },
}
