//! Stable repository-root discovery from the xtask package location.

use std::path::PathBuf;

pub(crate) fn root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .map_or(manifest_dir.clone(), std::path::Path::to_path_buf)
}
