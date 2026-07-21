//! Request negotiation policy survives the complete IR-to-Rust rendering path.
//!
//! Scenario: render every enabled API pair, compare each request constant and
//! every descriptor with its normalized source flag, and pin the one unstable
//! request currently present so the proof cannot pass over an empty set.

use std::path::{Path, PathBuf};

use kafka_wire_schema::MessageKind;

use crate::{group::group_sources, lockfile::ProtocolLock, source::load_sources};

use super::{descriptor::descriptor_name, file::render_api};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

#[test]
fn every_unstable_source_flag_reaches_typed_and_reflected_metadata() {
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
        for source in group.messages() {
            let message = &source.message;
            let descriptor = descriptor_name(message);
            let marker = format!("pub const {descriptor}: MessageDescriptor");
            let reflected = rendered.split_once(&marker).map_or_else(
                || panic!("{} has no descriptor", message.name.protocol()),
                |(_, tail)| tail.split_once(");").map_or(tail, |(body, _)| body),
            );
            let reflected_flag = format!("{},", message.latest_version_unstable);
            assert!(
                reflected.trim_end().ends_with(&reflected_flag),
                "{} descriptor dropped latestVersionUnstable={}",
                message.name.protocol(),
                message.latest_version_unstable,
            );

            if message.kind == MessageKind::Request {
                let typed = format!(
                    "const LATEST_VERSION_UNSTABLE: bool = {};",
                    message.latest_version_unstable
                );
                assert!(
                    rendered.contains(&typed),
                    "{} request impl dropped latestVersionUnstable={}",
                    message.name.protocol(),
                    message.latest_version_unstable,
                );
                if message.latest_version_unstable {
                    unstable.push(message.name.protocol().to_owned());
                }
            }
        }
    }

    assert_eq!(unstable, ["InitProducerIdRequest"]);
}
