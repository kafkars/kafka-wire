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

use super::{declarations::declared_structs, structs::render_declared_structs};

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
        let declarations = declared_structs(&source.message)
            .unwrap_or_else(|error| panic!("collect {}: {error}", source.filename));
        let expected = declarations
            .iter()
            .map(|declaration| {
                let mut context = source.message.clone();
                context.valid_versions = declaration.versions.clone();
                context.flexible_versions = declaration.flexible_versions.clone();
                declaration
                    .fields
                    .iter()
                    .filter(|field| {
                        !field.ignorable && field::absence_condition(field, &context).is_some()
                    })
                    .count()
            })
            .sum::<usize>();

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
        conditional_fields >= 22,
        "the corpus exposed only {conditional_fields} conditional nested fields; \
         the silent-loss regression proof became too narrow"
    );
}

#[test]
fn every_nested_codec_uses_its_declarations_effective_window() {
    let root = repository_root();
    let lock = ProtocolLock::read(&root.join("spec/protocol.lock"))
        .unwrap_or_else(|error| panic!("read protocol lock: {error}"));
    let sources =
        load_sources(&root, &lock).unwrap_or_else(|error| panic!("load pinned corpus: {error}"));

    let mut narrowed = 0_usize;
    for source in sources {
        let declarations = declared_structs(&source.message)
            .unwrap_or_else(|error| panic!("collect {}: {error}", source.filename));
        let mut rust = RustText::default();
        render_declared_structs(&mut rust, &source.message)
            .unwrap_or_else(|error| panic!("render {}: {error}", source.filename));
        let rendered = rust.finish();

        assert_eq!(
            rendered.matches("const SUPPORTED_VERSIONS").count(),
            declarations.len(),
            "{} did not emit one supported range per declaration",
            source.filename,
        );
        assert_eq!(
            rendered
                .matches("if !Self::SUPPORTED_VERSIONS.contains(version)")
                .count(),
            declarations.len() * 2,
            "{} did not guard both direct encode and decode",
            source.filename,
        );
        assert_eq!(
            rendered.matches("fn encoded_len").count(),
            declarations.len(),
            "{} did not emit one checked sizing entry point per nested codec",
            source.filename,
        );
        assert_eq!(
            rendered.matches("fn encode_into").count(),
            declarations.len(),
            "{} did not emit one checked buffered entry point per nested codec",
            source.filename,
        );
        assert!(!rendered.contains("fn validate_encoding"));
        assert!(
            !rendered.contains(".encode(encoder, version)?;"),
            "{} re-entered checked encoding during validated nested descent",
            source.filename,
        );

        for declaration in declarations {
            let (start, end) = declaration.versions.single_bounded().unwrap_or_else(|| {
                panic!(
                    "{} {} has non-renderable effective versions {}",
                    source.filename,
                    declaration.name.declared(),
                    declaration.versions,
                )
            });
            let identity = format!(
                "impl {} {{\n    const SUPPORTED_VERSIONS: VersionRange = \
                 VersionRange::new({start}, {end});",
                declaration.name.rust_type(),
            );
            assert!(
                rendered.contains(&identity),
                "{} emitted the wrong supported window for {}",
                source.filename,
                declaration.name.declared(),
            );
            if *declaration.versions != source.message.valid_versions {
                narrowed += 1;
            }
        }
    }

    assert!(
        narrowed >= 42,
        "the corpus exposed only {narrowed} declaration windows narrower than their owners"
    );
}
