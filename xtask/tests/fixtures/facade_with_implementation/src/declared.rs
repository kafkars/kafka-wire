//! Implementation behind the fixture facade.
//!
//! This file is not a facade, so the same function here is legitimate and must
//! not be reported.

#[derive(Debug)]
pub struct VersionRange {
    pub lowest: u16,
    pub highest: u16,
}

pub fn effective_version() -> u16 {
    3
}
