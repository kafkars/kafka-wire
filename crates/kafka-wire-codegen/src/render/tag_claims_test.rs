//! The generated active-tag claim census covers the complete pinned IR.
//!
//! Scenario: render the verification program, count every tagged field in the
//! source model, and require one executable assertion per numeric claim.

use std::path::{Path, PathBuf};

use kafka_wire_schema::Field;

use crate::{group::group_sources, lockfile::ProtocolLock, source::load_sources};

use super::tag_claims::render_tag_claims;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

fn count_tags(fields: &[Field]) -> usize {
    fields
        .iter()
        .map(|field| usize::from(field.tag.is_some()) + count_tags(&field.fields))
        .sum()
}

#[test]
fn every_known_tag_receives_one_active_runtime_assertion() {
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
        .map(|source| {
            count_tags(&source.message.fields)
                + source
                    .message
                    .common_structs
                    .iter()
                    .map(|common| count_tags(&common.fields))
                    .sum::<usize>()
        })
        .sum::<usize>();
    let rendered = render_tag_claims(&grouped.api, &grouped.unkeyed, &lock.kafka.commit)
        .unwrap_or_else(|error| panic!("render tag claims: {error}"));

    assert!(
        expected > 20,
        "tag census unexpectedly shrank to {expected}"
    );
    assert_eq!(rendered.matches("assert_claim(\n").count(), expected);
    assert!(rendered.contains(
        "let value = crate::ApiVersionsResponse {\n        \
         unknown_tagged_fields: retained_tag(1),\n        ..Default::default()\n    };\n    \
         assert_claim(\n        &value.validate_known_tag_ownership(ApiVersion::new(3)),"
    ));
    assert!(rendered.contains(
        "let value = crate::BrokerHeartbeatRequest {\n        \
         unknown_tagged_fields: retained_tag(0),\n        ..Default::default()\n    };\n    \
         assert_claim(\n        &value.validate_known_tag_ownership(ApiVersion::new(1)),"
    ));
}

#[test]
fn an_empty_claim_census_emits_no_unused_verification_vocabulary() {
    let rendered = render_tag_claims(&[], &[], "commit")
        .unwrap_or_else(|error| panic!("render empty claims: {error}"));

    assert!(!rendered.contains("use kafka_wire_core"));
    assert!(!rendered.contains("fn retained_tag"));
    assert!(rendered.contains("fn assert_all_active_tag_claims()"));
}
