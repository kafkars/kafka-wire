//! Collected semantic validation diagnostics.

use std::{error::Error, fmt, path::PathBuf};

/// One independent schema invariant violation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationError {
    /// Stable diagnostic code.
    pub code: &'static str,
    /// Source path.
    pub path: PathBuf,
    /// Optional message field.
    pub field: Option<String>,
    /// Human-readable diagnostic.
    pub message: String,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.field {
            Some(field) => write!(
                formatter,
                "{} [{}] field {field}: {}",
                self.path.display(),
                self.code,
                self.message
            ),
            None => write!(
                formatter,
                "{} [{}]: {}",
                self.path.display(),
                self.code,
                self.message
            ),
        }
    }
}

/// Non-empty collection of independent validation failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationErrors(pub Vec<ValidationError>);

impl fmt::Display for ValidationErrors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            formatter,
            "schema validation failed with {} error(s):",
            self.0.len()
        )?;
        for error in &self.0 {
            writeln!(formatter, "- {error}")?;
        }
        Ok(())
    }
}

impl Error for ValidationErrors {}
pub(super) fn diagnostic(
    message: &crate::Message,
    field: Option<&crate::Field>,
    code: &'static str,
    diagnostic: &str,
) -> ValidationError {
    ValidationError {
        code,
        path: message.source.clone(),
        field: field.map(|field| field.name.protocol().to_owned()),
        message: diagnostic.to_owned(),
    }
}
