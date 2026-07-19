//! Lowering diagnostics for unsupported or malformed source semantics.

use std::path::PathBuf;

use thiserror::Error;

/// Failure while lowering raw Kafka source into the normalized IR.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
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
    /// The adapter encountered new `commonStructs` properties.
    #[error("{path}: common struct {declaration} has unmodeled properties: {properties}")]
    CommonStructProperties {
        /// Source path.
        path: PathBuf,
        /// Struct declaration name.
        declaration: String,
        /// Sorted property names.
        properties: String,
    },
    /// A field declared a type spelling the adapter cannot interpret.
    #[error("{path}: field {field} has an uninterpretable type: {reason}")]
    FieldType {
        /// Source path.
        path: PathBuf,
        /// Protocol field name.
        field: String,
        /// Type-parser diagnostic.
        reason: String,
    },
    /// A field named a domain entity the adapter does not model.
    #[error("{path}: field {field} has an unmodeled entityType: {reason}")]
    EntityType {
        /// Source path.
        path: PathBuf,
        /// Protocol field name.
        field: String,
        /// Entity-parser diagnostic.
        reason: String,
    },
    /// Inline field nesting exceeded the adapter's bound.
    #[error("{path}: field {field} nests deeper than {limit} levels")]
    NestingDepth {
        /// Source path.
        path: PathBuf,
        /// Protocol field name at the offending depth.
        field: String,
        /// The enforced nesting limit.
        limit: usize,
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
