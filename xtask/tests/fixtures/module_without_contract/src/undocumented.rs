// A line comment is not a module contract: `//` documents the next item,
// while `//!` documents the module itself. The test must tell them apart.

pub fn effective_version() -> u16 {
    3
}
