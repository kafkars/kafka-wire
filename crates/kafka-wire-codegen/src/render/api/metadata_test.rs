//! Pair negotiation policy survives the complete IR-to-Rust rendering path.
//!
//! Scenario: render every enabled API pair, prove each request points to its
//! pair descriptor, compare that descriptor with normalized request policy,
//! and pin the one unstable API so the proof cannot pass over an empty set.

use std::path::{Path, PathBuf};

use crate::{group::group_sources, lockfile::ProtocolLock, source::load_sources};

use super::{
    descriptor::{api_descriptor_name, descriptor_name},
    file::render_api,
};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

#[test]
fn every_unstable_source_flag_reaches_pair_metadata() {
    let root = repository_root();
    let lock = ProtocolLock::read(&root.join("spec/protocol.lock"))
        .unwrap_or_else(|error| panic!("read protocol lock: {error}"));
    let grouped = group_sources(
        load_sources(&root, &lock).unwrap_or_else(|error| panic!("load pinned corpus: {error}")),
    )
    .unwrap_or_else(|error| panic!("group pinned corpus: {error}"));
    let mut unstable = Vec::new();

    for group in &grouped.api {
        let rendered = render_api(group, &lock.kafka.commit)
            .unwrap_or_else(|error| panic!("render API key {}: {error}", group.api_key));
        let pair_descriptor = api_descriptor_name(group);
        let marker = format!("pub const {pair_descriptor}: ApiDescriptor");
        let reflected = rendered.split_once(&marker).map_or_else(
            || panic!("{} has no pair descriptor", group.name.protocol_stem()),
            |(_, tail)| tail.split_once(");").map_or(tail, |(body, _)| body),
        );
        let reflected_flag = format!("{},", group.latest_version_unstable);
        assert!(
            reflected.trim_end().ends_with(&reflected_flag),
            "{} pair descriptor dropped latestVersionUnstable={}",
            group.name.protocol_stem(),
            group.latest_version_unstable,
        );

        for source in group.messages() {
            let message = &source.message;
            let descriptor = descriptor_name(message);
            let marker = format!("pub const {descriptor}: MessageDescriptor");
            assert!(
                rendered.contains(&marker),
                "{} has no directional descriptor",
                message.name.protocol()
            );
        }
        let typed =
            format!("const API_DESCRIPTOR: &'static ApiDescriptor = &super::{pair_descriptor};");
        assert!(
            rendered.contains(&typed),
            "{} request does not point to its pair descriptor",
            group.request.message.name.protocol(),
        );
        if group.latest_version_unstable {
            unstable.push(group.name.protocol_stem().to_owned());
        }
    }

    assert_eq!(unstable, ["InitProducerId"]);
}
