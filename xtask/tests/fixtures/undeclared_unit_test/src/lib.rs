//! A fixture facade that wires up its sibling unit tests incorrectly.
//!
//! `declared_test` is declared correctly. `ungated_test` is declared without a
//! `#[cfg(test)]` gate, so its test code would reach a production build.
//! `orphan_test` is not declared at all, which is the silent failure this
//! test exists to close: it compiles to nothing and runs zero assertions.

mod limits;

#[cfg(test)]
mod declared_test;

mod ungated_test;

pub use limits::DecodeLimits;
