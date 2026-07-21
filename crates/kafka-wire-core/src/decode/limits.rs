//! Peer-controlled decoding budgets.
//!
//! Limits are explicit values passed to each decoder; this module owns no hidden
//! global policy.

/// Resource limits applied while decoding untrusted Kafka bytes.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodeLimits {
    /// Maximum bytes accepted for one complete message frame.
    pub max_frame_bytes: usize,
    /// Maximum UTF-8 byte length of one string.
    pub max_string_bytes: usize,
    /// Maximum payload length of one Kafka byte/blob field.
    pub max_bytes_bytes: usize,
    /// Maximum element count of one array.
    pub max_array_elements: usize,
    /// Maximum number of unknown tagged fields in one structure.
    pub max_tagged_fields: usize,
    /// Maximum byte length of one unknown tagged field.
    pub max_tag_bytes: usize,
    /// Maximum total unknown tagged-field bytes in one structure.
    pub max_total_tag_bytes: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 128 * 1024 * 1024,
            max_string_bytes: 16 * 1024 * 1024,
            max_bytes_bytes: 64 * 1024 * 1024,
            max_array_elements: 1_000_000,
            max_tagged_fields: 4_096,
            max_tag_bytes: 16 * 1024 * 1024,
            max_total_tag_bytes: 32 * 1024 * 1024,
        }
    }
}
