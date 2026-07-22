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
    let rendered = render_fuzz_dispatch(&grouped.api, &grouped.unkeyed, &lock.kafka.commit)
        .unwrap_or_else(|error| panic!("render fuzz dispatch: {error}"));
    let mut expected = 0_usize;

    for source in grouped
        .api
        .iter()
        .flat_map(crate::group::ApiGroup::messages)
        .chain(grouped.unkeyed.iter())
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
        let arm =
            format!("if let Some(version) = select_version(version_selector, {first}, {last})",);
        let call = format!(
            "round_trip::<kafka_wire::{}>(body, version)",
            message.name.rust_type()
        );
        assert!(
            rendered.contains(&arm),
            "{} did not receive its exact version bounds",
            message.name.protocol()
        );
        assert_eq!(
            rendered.matches(&call).count(),
            1,
            "{} did not receive exactly one typed dispatch call",
            message.name.protocol()
        );
        expected += 1;
    }

    assert_eq!(expected, 193, "the pinned enabled-schema census changed");
    assert_eq!(
        rendered.matches("round_trip::<kafka_wire::").count(),
        expected
    );
    assert_eq!(
        rendered
            .matches("if let Some(version) = select_version")
            .count(),
        expected
    );
}

#[test]
fn selector_arithmetic_represents_the_full_i16_version_domain() {
    let root = repository_root();
    let lock = ProtocolLock::read(&root.join("spec/protocol.lock"))
        .unwrap_or_else(|error| panic!("read protocol lock: {error}"));
    let grouped = group_sources(
        load_sources(&root, &lock).unwrap_or_else(|error| panic!("load pinned corpus: {error}")),
    )
    .unwrap_or_else(|error| panic!("group pinned corpus: {error}"));
    let rendered = render_fuzz_dispatch(&grouped.api, &grouped.unkeyed, &lock.kafka.commit)
        .unwrap_or_else(|error| panic!("render fuzz dispatch: {error}"));

    assert!(rendered.contains("i32::from(last).checked_sub(i32::from(first))"));
    assert!(!rendered.contains("unwrap_or"));
}
