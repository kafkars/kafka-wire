//! Generated namespace collision scenarios.
//!
//! These tests prove fixed generated modules and handwritten public symbols
//! participate in the same claims as schema-produced exports.

#![allow(clippy::expect_used)]

use std::collections::BTreeSet;

use kafka_wire_schema::{ApiName, FieldName, MessageName};

use crate::{
    GenerationError,
    group::group_sources,
    lockfile::ProtocolLock,
    namespace::{handwritten_root_types, validate_generated_namespace},
    source::load_sources,
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

#[test]
fn flexible_owners_claim_the_synthesized_unknown_tagged_member() {
    let (mut groups, unkeyed) = corpus();
    let source = groups
        .iter_mut()
        .flat_map(|group| [&mut group.request, &mut group.response])
        .find(|source| {
            !source.message.effective_flexible_versions().is_empty()
                && !source.message.fields.is_empty()
        })
        .expect("pinned corpus must contain a flexible message with a field");
    source.message.fields[0].name = FieldName::new("UnknownTaggedFields");

    let error = validate_generated_namespace(&groups, &unkeyed)
        .expect_err("a schema field cannot duplicate synthesized storage");
    assert!(matches!(
        error,
        GenerationError::GeneratedSymbolCollision {
            symbol,
            first,
            second,
            ..
        } if symbol == "unknown_tagged_fields"
            && first.contains("schema field")
            && second.contains("compiler-synthesized")
    ));
}

#[test]
fn generated_exports_cannot_shadow_private_crate_root_modules() {
    let (groups, mut unkeyed) = corpus();
    let source = unkeyed
        .first_mut()
        .expect("pinned corpus must contain an unkeyed schema");
    source.message.name = MessageName::new("Message");

    let error = validate_generated_namespace(&groups, &unkeyed)
        .expect_err("a generated root module cannot shadow a private module");
    assert!(matches!(
        error,
        GenerationError::GeneratedSymbolCollision {
            symbol,
            first,
            second,
            ..
        } if symbol == "message"
            && first.contains("handwritten private crate-root module")
            && second.contains("generated struct module")
    ));
}

#[test]
fn a_generated_export_cannot_shadow_the_test_only_root_module() {
    let (groups, mut unkeyed) = corpus();
    let source = unkeyed
        .first_mut()
        .expect("pinned corpus must contain an unkeyed schema");
    source.message.name = MessageName::new("TaggedClaimsTest");

    let error = validate_generated_namespace(&groups, &unkeyed)
        .expect_err("a generated root module cannot shadow the test module");
    assert!(matches!(
        error,
        GenerationError::GeneratedSymbolCollision {
            symbol,
            first,
            second,
            ..
        } if symbol == "tagged_claims_test"
            && first.contains("handwritten private crate-root module")
            && second.contains("generated struct module")
    ));
}

#[test]
fn every_crate_root_module_is_reserved_before_generation() {
    let facade = syn::parse_file(include_str!("../../kafka-wire/src/lib.rs"))
        .expect("kafka-wire's crate facade must parse as Rust");
    let declared = facade
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Mod(module) => Some(module.ident.to_string()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let reserved = handwritten_root_types()
        .into_iter()
        .filter_map(|(symbol, producer)| {
            (producer == "handwritten private crate-root module").then_some(symbol)
        })
        .collect::<BTreeSet<_>>();

    assert_eq!(reserved, declared);
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
