//! Every nested version-gated field receives a generated preflight refusal.
//!
//! Scenario: load the complete pinned corpus, render only its declared
//! structures, and account for every non-ignorable field whose presence window
//! is narrower than its owner message. This is the class that once lost values
//! silently, so the proof is corpus-derived rather than a hand-picked list.

use std::path::{Path, PathBuf};

use crate::{
    lockfile::ProtocolLock,
    render::{field, text::RustText},
    source::load_sources,
};

use super::structs::{declared_structs, render_declared_structs};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

#[test]
fn every_nested_conditional_field_is_refused_before_encoding_can_skip_it() {
    let root = repository_root();
    let lock = ProtocolLock::read(&root.join("spec/protocol.lock"))
        .unwrap_or_else(|error| panic!("read protocol lock: {error}"));
    let sources =
        load_sources(&root, &lock).unwrap_or_else(|error| panic!("load pinned corpus: {error}"));

    let mut conditional_fields = 0_usize;
    let mut structures = 0_usize;
    for source in sources {
        let declarations = declared_structs(&source.message);
        let expected = declarations
            .iter()
            .flat_map(|(_, _, fields)| fields.iter())
            .filter(|field| {
                !field.ignorable && field::absence_condition(field, &source.message).is_some()
            })
            .count();

        let mut rust = RustText::default();
        render_declared_structs(&mut rust, &source.message)
            .unwrap_or_else(|error| panic!("render {}: {error}", source.filename));
        let rendered = rust.finish();

        assert_eq!(
            rendered.matches("fn validate_for_version").count(),
            declarations.len(),
            "{} did not give every declared structure one validation operation",
            source.filename,
        );
        assert_eq!(
            rendered.matches("::FieldNotRepresentable").count(),
            expected,
            "{} did not emit one refusal per conditional nested field",
            source.filename,
        );

        structures += declarations.len();
        conditional_fields += expected;
    }

    assert!(
        structures > 300,
        "the corpus exposed only {structures} nested structures; the proof became too narrow"
    );
    assert!(
        conditional_fields >= 35,
        "the corpus exposed only {conditional_fields} conditional nested fields; \
         the silent-loss regression proof became too narrow"
    );
}
