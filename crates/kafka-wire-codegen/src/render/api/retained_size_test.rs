//! Corpus proof that every generated structure exposes deep retained size.

use std::path::{Path, PathBuf};

use crate::{
    lockfile::ProtocolLock,
    render::{api::structs::render_declared_structs, text::RustText},
    source::load_sources,
};

use super::declarations::declared_structs;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

#[test]
fn every_declared_structure_recurses_through_each_retained_field() {
    let root = repository_root();
    let lock = ProtocolLock::read(&root.join("spec/protocol.lock"))
        .unwrap_or_else(|error| panic!("read protocol lock: {error}"));
    let sources =
        load_sources(&root, &lock).unwrap_or_else(|error| panic!("load pinned corpus: {error}"));

    for source in sources {
        let declarations = declared_structs(&source.message)
            .unwrap_or_else(|error| panic!("collect {}: {error}", source.filename));
        let expected_fields = declarations
            .iter()
            .map(|declaration| {
                declaration.fields.len() + usize::from(!declaration.flexible_versions.is_empty())
            })
            .sum::<usize>();
        let mut rust = RustText::default();
        render_declared_structs(&mut rust, &source.message)
            .unwrap_or_else(|error| panic!("render {}: {error}", source.filename));
        let rendered = rust.finish();

        assert_eq!(
            rendered.matches("impl RetainedSize for").count(),
            declarations.len(),
            "{} missed a retained-size implementation",
            source.filename,
        );
        assert_eq!(
            rendered.matches("RetainedSize::retained_size").count(),
            expected_fields,
            "{} missed a retained field",
            source.filename,
        );
    }
}
