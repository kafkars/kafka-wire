//! The fuzz dispatch covers every generated message over its whole version range.
//!
//! Scenario: derive the dispatch from the complete enabled corpus and require
//! one typed arm carrying each message's exact normalized bounds.

use std::path::{Path, PathBuf};

use crate::{group::group_sources, lockfile::ProtocolLock, source::load_sources};

use super::fuzz_dispatch::render_fuzz_dispatch;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

#[test]
fn every_enabled_versioned_message_has_one_typed_dispatch_arm() {
    let root = repository_root();
    let lock = ProtocolLock::read(&root.join("spec/protocol.lock"))
        .unwrap_or_else(|error| panic!("read protocol lock: {error}"));
    let grouped = group_sources(
        load_sources(&root, &lock).unwrap_or_else(|error| panic!("load pinned corpus: {error}")),
    )
    .unwrap_or_else(|error| panic!("group pinned corpus: {error}"));
    let rendered = render_fuzz_dispatch(&grouped.api, &lock.kafka.commit)
        .unwrap_or_else(|error| panic!("render fuzz dispatch: {error}"));
    let mut expected = 0_usize;

    for source in grouped
        .api
        .iter()
        .flat_map(crate::group::ApiGroup::messages)
    {
        let message = &source.message;
        let Some((first, last)) = message.valid_versions.single_bounded() else {
            assert!(
                message.valid_versions.is_empty(),
                "{} has a non-renderable version set",
                message.name.protocol()
            );
            continue;
        };
        let arm = format!(
            "round_trip::<kafka_wire::{}>(body, select_version(version_selector, {first}, {last}))",
            message.name.rust_type()
        );
        assert_eq!(
            rendered.matches(&arm).count(),
            1,
            "{} did not receive exactly one dispatch arm",
            message.name.protocol()
        );
        expected += 1;
    }

    assert!(
        expected > 150,
        "the dispatch covered only {expected} messages"
    );
    assert_eq!(rendered.matches("=> round_trip::<").count(), expected);
}
