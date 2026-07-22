//! The delayed-tag boundary census covers every transition in the pinned IR.
//!
//! Scenario: independently count activation starts after flexibility begins and
//! require one ownership-phase assertion for each transition.

use std::path::{Path, PathBuf};

use kafka_wire_schema::{Field, Message};

use crate::{group::group_sources, lockfile::ProtocolLock, source::load_sources};

use super::{api::declared_structs, tag_boundaries::render_tag_boundaries};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

fn count_owner(fields: &[Field], message: &Message) -> usize {
    let flexible = message
        .effective_flexible_versions()
        .single_bounded()
        .map_or(i16::MAX, |(start, _)| start);
    fields
        .iter()
        .filter(|field| {
            field.tag.is_some()
                && field
                    .versions
                    .intersection(&message.valid_versions)
                    .single_bounded()
                    .is_some_and(|(start, _)| start > flexible)
        })
        .count()
}

fn count_message(message: &Message) -> usize {
    let nested = declared_structs(message)
        .unwrap_or_else(|error| panic!("declare structs: {error}"))
        .into_iter()
        .map(|declaration| {
            let mut context = message.clone();
            context.valid_versions = declaration.versions.clone();
            context.flexible_versions = declaration.flexible_versions;
            count_owner(declaration.fields, &context)
        })
        .sum::<usize>();
    count_owner(&message.fields, message) + nested
}

#[test]
fn every_delayed_tag_receives_its_exact_ownership_boundary() {
    let root = repository_root();
    let lock = ProtocolLock::read(&root.join("spec/protocol.lock"))
        .unwrap_or_else(|error| panic!("read protocol lock: {error}"));
    let grouped = group_sources(
        load_sources(&root, &lock).unwrap_or_else(|error| panic!("load pinned corpus: {error}")),
    )
    .unwrap_or_else(|error| panic!("group pinned corpus: {error}"));
    let expected = grouped
        .api
        .iter()
        .flat_map(crate::group::ApiGroup::messages)
        .chain(grouped.unkeyed.iter())
        .map(|source| count_message(&source.message))
        .sum::<usize>();
    let rendered = render_tag_boundaries(&grouped.api, &grouped.unkeyed, &lock.kafka.commit)
        .unwrap_or_else(|error| panic!("render tag boundaries: {error}"));

    assert_eq!(expected, 11, "the pinned delayed-tag census changed");
    assert_eq!(rendered.matches("assert_boundary(\n").count(), expected);
    assert!(rendered.contains(
        "&value.validate_known_tag_ownership(ApiVersion::new(0)),\n        \
         &value.validate_known_tag_ownership(ApiVersion::new(1)),"
    ));
}

#[test]
fn an_empty_boundary_census_emits_no_unused_verification_vocabulary() {
    let rendered = render_tag_boundaries(&[], &[], "commit")
        .unwrap_or_else(|error| panic!("render empty boundaries: {error}"));

    assert!(!rendered.contains("use kafka_wire_core"));
    assert!(!rendered.contains("fn retained_tag"));
    assert!(rendered.contains("fn assert_all_tag_activation_boundaries()"));
}
