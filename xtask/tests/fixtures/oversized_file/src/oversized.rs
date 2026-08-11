//! A fixture module deliberately longer than the budgets the test is given.
//!
//! The tests that use this fixture supply tiny limits, so the exact length
//! matters only in that it must exceed a target of 10, a soft limit of 15, and
//! a hard limit of 20. Every line below is here to make that true while still
//! reading as ordinary Rust.

#[derive(Debug)]
pub struct VersionRange {
    pub lowest: u16,
    pub highest: u16,
}

impl VersionRange {
    pub fn new(lowest: u16, highest: u16) -> Self {
        Self { lowest, highest }
    }

    pub fn contains(&self, version: u16) -> bool {
        version >= self.lowest && version <= self.highest
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.lowest <= other.highest && other.lowest <= self.highest
    }

    pub fn width(&self) -> u16 {
        self.highest - self.lowest + 1
    }
}
