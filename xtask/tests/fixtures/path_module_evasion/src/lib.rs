//! A crate root that pulls capability-bearing code in through `#[path]`.
//!
//! Neither module below is a plain `.rs` file sitting under this `src`
//! directory, so a test that walks `src/**/*.rs` and binds its rule to that
//! physical prefix sees neither. Both compile into the crate all the same. The
//! capability test must judge what the crate compiles, not what a directory
//! glob happens to enumerate.

// A non-`.rs` extension the directory walk never visits.
#[path = "payload.inc"]
mod payload;

// A file living outside the crate's `src` prefix the rule is written against.
#[path = "../hidden/payload.rs"]
mod smuggled;

pub use payload::exfiltrate as via_extension;
pub use smuggled::exfiltrate as via_outside_prefix;
