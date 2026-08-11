//! A fixture facade that breaks the declarative rule on purpose.
//!
//! A facade may declare children and curate re-exports. The function below is
//! implementation and must be rejected, while the `mod` and `pub use` lines
//! must not be. The include carries a second function, which must be rejected
//! too — an include the detector did not read through would be a way past it.

mod declared;

pub use declared::VersionRange;

include!("smuggled.rsi");

pub fn effective_version() -> u16 {
    3
}
