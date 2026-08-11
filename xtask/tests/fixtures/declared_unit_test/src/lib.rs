//! A fixture facade that declares its sibling unit test correctly.
//!
//! This is the positive case: the test must stay quiet here, otherwise the
//! convention it enforces would be unusable.

mod limits;

#[cfg(test)]
mod limits_test;

pub use limits::DecodeLimits;
