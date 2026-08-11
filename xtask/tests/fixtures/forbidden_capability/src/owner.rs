//! The declared owner of the filesystem capability in this fixture.
//!
//! The same `fs::` token here is legitimate, so the test must not report it.

use std::fs;

pub fn read_source(path: &str) -> std::io::Result<String> {
    fs::read_to_string(path)
}
