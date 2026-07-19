//! Lowering diagnostics for unsupported or malformed source semantics.

use std::path::PathBuf;

use thiserror::Error;

/// Failure while lowering raw Kafka source into the normalized IR.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum LowerError {
    /// The adapter encountered new message-level source properties.
    #[error("{path}: unmodeled message properties: {properties}")]
    MessageProperties {
        /// Source path.
        path: PathBuf,
        /// Sorted property names.
        properties: String,
    },
    /// The adapter encountered new field-level source properties.
    #[error("{path}: field {field} has unmodeled properties: {properties}")]
    FieldProperties {
        /// Source path.
        path: PathBuf,
        /// Protocol field name.
        field: String,
        /// Sorted property names.
        properties: String,
    },
    /// A request or response omitted its API key.
    #[error("{path}: message {message} is missing apiKey")]
    MissingApiKey {
        /// Source path.
        path: PathBuf,
        /// Protocol message name.
        message: String,
    },
    /// A version expression was invalid.
    #[error("{path}: invalid {role} versions `{value}` for {owner}: {reason}")]
    Versions {
        /// Source path.
        path: PathBuf,
        /// Semantic role of the expression.
        role: &'static str,
        /// Message or field name.
        owner: String,
        /// Source spelling.
        value: String,
        /// Parser diagnostic.
        reason: String,
    },
    /// A protocol default did not match its field type.
    #[error("{path}: invalid default for field {field}: {reason}")]
    Default {
        /// Source path.
        path: PathBuf,
        /// Protocol field name.
        field: String,
        /// Diagnostic.
        reason: String,
    },
}
