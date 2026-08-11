//! A module that merely writes `std::net` in prose, never in code.
//!
//! Substring matching rejected this file. Parsing must accept it: a test that
//! cannot tell a sentence from a socket teaches contributors to avoid the words
//! rather than avoid the capability.

/// Returns the diagnostic shown when a caller asks for `std::net` behaviour.
pub fn refusal() -> &'static str {
    // std::net is unavailable in this crate; see the capability rules.
    "std::net is not available here"
}
