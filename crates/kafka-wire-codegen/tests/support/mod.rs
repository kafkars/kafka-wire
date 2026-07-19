//! Shared inputs for the generation tests.
//!
//! This facade names the one thing the generation tests share: a synthetic
//! pinned workspace. It deliberately holds no claim of its own.

// Each integration test binary compiles this module separately and uses only
// what it needs, so an item unused by one of them is not an unused item.
#![allow(dead_code, unused_imports)]

mod workspace;

pub(crate) use workspace::{
    COMMIT, REFUSED, SUPPORTED, Workspace, hex_digest, read, repository_root, write,
};
