//! Location and loading of the miniature trees tests are proven against.
//!
//! This module owns how a negative fixture is found and the insistence that a
//! renamed or emptied fixture fails loudly. It deliberately owns no fixture
//! content and no judgement about what a fixture demonstrates.

use std::path::{Path, PathBuf};

use super::{WalkScope, rust_files_under};

pub(crate) fn fixture_root(case: &str) -> PathBuf {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(case);
    assert!(
        path.is_dir(),
        "test fixture `{case}` is missing at {}; \
         a deleted or renamed fixture must fail rather than quietly prove nothing",
        path.display()
    );
    path
}

/// Fixture root paired with every Rust file below it.
pub(crate) fn fixture_files(case: &str) -> (PathBuf, Vec<PathBuf>) {
    let root = fixture_root(case);
    let files = rust_files_under(&root, WalkScope::Fixture);
    assert!(
        !files.is_empty(),
        "test fixture `{case}` contains no Rust files, so it cannot demonstrate a rejection"
    );
    (root, files)
}
