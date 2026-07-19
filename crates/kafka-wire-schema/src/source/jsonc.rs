//! Position-preserving JSONC comment removal and deserialization.

use std::{io, path::PathBuf};

use thiserror::Error;

use crate::{RawMessage, SourceFile};

/// Failure while reading or parsing a source schema.
#[derive(Debug, Error)]
pub enum SourceError {
    /// The source file could not be read.
    #[error("could not read {path}: {source}")]
    Read {
        /// Source path.
        path: PathBuf,
        /// Filesystem failure.
        #[source]
        source: io::Error,
    },
    /// A block comment was not closed.
    #[error("unterminated block comment in {path} at line {line}, column {column}")]
    UnterminatedBlockComment {
        /// Source path.
        path: PathBuf,
        /// One-based line.
        line: usize,
        /// One-based column.
        column: usize,
    },
    /// Comment-free JSON was invalid.
    #[error("invalid Kafka message JSON in {path} at line {line}, column {column}: {source}")]
    Json {
        /// Source path.
        path: PathBuf,
        /// One-based line.
        line: usize,
        /// One-based column.
        column: usize,
        /// JSON parser failure.
        #[source]
        source: serde_json::Error,
    },
}

/// Parses one Apache Kafka JSONC message definition.
pub fn parse_jsonc(source: &SourceFile) -> Result<RawMessage, SourceError> {
    let stripped = strip_comments(source)?;
    serde_json::from_str(&stripped).map_err(|error| SourceError::Json {
        path: source.path().to_path_buf(),
        line: error.line(),
        column: error.column(),
        source: error,
    })
}

fn strip_comments(source: &SourceFile) -> Result<String, SourceError> {
    let bytes = source.contents().as_bytes();
    let mut output = bytes.to_vec();
    let mut index = 0_usize;
    let mut in_string = false;
    let mut escaped = false;

    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }

        if byte == b'"' {
            in_string = true;
            index += 1;
            continue;
        }

        if byte == b'/' && bytes.get(index + 1) == Some(&b'/') {
            output[index] = b' ';
            output[index + 1] = b' ';
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                output[index] = b' ';
                index += 1;
            }
            continue;
        }

        if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            let start = index;
            output[index] = b' ';
            output[index + 1] = b' ';
            index += 2;
            let mut closed = false;
            while index < bytes.len() {
                if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                    output[index] = b' ';
                    output[index + 1] = b' ';
                    index += 2;
                    closed = true;
                    break;
                }
                if bytes[index] != b'\n' && bytes[index] != b'\r' {
                    output[index] = b' ';
                }
                index += 1;
            }
            if !closed {
                let (line, column) = line_column(bytes, start);
                return Err(SourceError::UnterminatedBlockComment {
                    path: source.path().to_path_buf(),
                    line,
                    column,
                });
            }
            continue;
        }

        index += 1;
    }

    Ok(String::from_utf8_lossy(&output).into_owned())
}

fn line_column(bytes: &[u8], offset: usize) -> (usize, usize) {
    let prefix = &bytes[..offset];
    let line = prefix.split(|byte| *byte == b'\n').count();
    let column = prefix
        .iter()
        .rposition(|&byte| byte == b'\n')
        .map_or(offset + 1, |newline| offset - newline);
    (line, column)
}
