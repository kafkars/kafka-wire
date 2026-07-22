//! Generated namespace collision scenarios.
//!
//! These tests prove fixed generated modules and handwritten public symbols
//! participate in the same claims as schema-produced exports.

#![allow(clippy::expect_used)]

use kafka_wire_schema::{ApiName, MessageName};

use crate::{
    GenerationError, group::group_sources, lockfile::ProtocolLock,
    namespace::validate_generated_namespace, source::load_sources,
};

#[test]
fn a_generated_module_collision_reports_both_producers() {
    let (mut groups, unkeyed) = corpus();
    groups[0].name = ApiName::try_new("registry").expect("valid fixture name");

    let error = validate_generated_namespace(&groups, &unkeyed)
        .expect_err("fixed module collision must fail");
    assert!(matches!(
        error,
        GenerationError::GeneratedSymbolCollision {
            symbol,
            first,
            second,
            ..
        } if symbol == "registry"
            && first.contains("fixed API registry")
            && second.contains("API key")
    ));
}

#[test]
fn a_generated_export_cannot_shadow_the_handwritten_facade() {
    let (mut groups, unkeyed) = corpus();
    groups[0].request.message.name =
        MessageName::try_new("KafkaMessage").expect("valid fixture message name");

    let error = validate_generated_namespace(&groups, &unkeyed)
        .expect_err("handwritten symbol collision must fail");
    assert!(matches!(
        error,
        GenerationError::GeneratedSymbolCollision {
            symbol,
            first,
            second,
            ..
        } if symbol == "KafkaMessage"
            && first.contains("handwritten crate facade")
            && second.contains("message KafkaMessage")
    ));
}

#[test]
fn nested_structs_participate_in_their_actual_module_namespace() {
    let (mut groups, unkeyed) = corpus();
    let mut collision = None;
    'groups: for group in &mut groups {
        for source in [&mut group.request, &mut group.response] {
            if let Some(declaration) = source.message.structs.declarations().first() {
                let name = declaration.name.declared().to_owned();
                source.message.name = MessageName::try_new(&name).expect("valid nested type name");
                collision = Some(name);
                break 'groups;
            }
        }
    }
    let collision = collision.expect("pinned corpus must declare a nested struct");

    let error = validate_generated_namespace(&groups, &unkeyed)
        .expect_err("message and nested type collision must fail");
    assert!(matches!(
        error,
        GenerationError::GeneratedSymbolCollision {
            symbol,
            first,
            second,
            ..
        } if symbol == collision
            && first.contains("message")
            && second.contains("nested struct")
    ));
}

fn corpus() -> (
    Vec<crate::group::ApiGroup>,
    Vec<crate::source::MessageSource>,
) {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("crate must live in workspace");
    let lock = ProtocolLock::read(&workspace.join("spec/protocol.lock")).expect("read lock");
    let sources = load_sources(workspace, &lock).expect("load corpus");
    let grouped = group_sources(sources).expect("group corpus");
    (grouped.api, grouped.unkeyed)
}
