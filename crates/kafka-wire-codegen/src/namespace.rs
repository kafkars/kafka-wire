//! Complete generated and crate-root Rust namespace claims: modules, re-exports,
//! descriptors, fixed vocabulary, and the handwritten crate facade.
//! Per-message struct collisions remain schema-front-end invariants.

use std::collections::BTreeMap;

use crate::{
    GenerationError,
    group::ApiGroup,
    namespace_members::validate_synthesized_members,
    render::{api_descriptor_name, descriptor_name},
    source::MessageSource,
};

const GENERATED_TYPE_NAMESPACE: &str = "the generated module type namespace";
const GENERATED_VALUE_NAMESPACE: &str = "the generated module value namespace";
const ROOT_TYPE_NAMESPACE: &str = "the kafka-wire crate-root type namespace";
const ROOT_VALUE_NAMESPACE: &str = "the kafka-wire crate-root value namespace";
const PRIVATE_ROOT_MODULE: &str = "handwritten private crate-root module";

/// Proves every emitted symbol has one producer in each scope it enters.
pub(crate) fn validate_generated_namespace(
    groups: &[ApiGroup],
    unkeyed: &[MessageSource],
) -> Result<(), GenerationError> {
    let mut generated_types = BTreeMap::new();
    let mut generated_values = BTreeMap::new();
    let mut root_types = handwritten_root_types();
    let mut root_values = handwritten_root_values();

    for (module, producer) in [
        ("registry", "fixed API registry module"),
        ("header_version", "fixed header-version policy module"),
    ] {
        claim(
            &mut generated_types,
            GENERATED_TYPE_NAMESPACE,
            module,
            producer,
        )?;
    }
    if !unkeyed.is_empty() {
        claim(
            &mut generated_types,
            GENERATED_TYPE_NAMESPACE,
            "framing",
            "fixed framing module",
        )?;
    }

    for group in groups {
        let producer = format!(
            "API key {} pair {}",
            group.api_key,
            group.name.protocol_stem()
        );
        claim(
            &mut generated_types,
            GENERATED_TYPE_NAMESPACE,
            group.module_name(),
            &producer,
        )?;
        for source in group.messages() {
            validate_message_module(source)?;
            claim_message_exports(
                source,
                &mut generated_types,
                &mut generated_values,
                &mut root_types,
                &mut root_values,
            )?;
        }
        claim_value_export(
            &mut generated_values,
            &mut root_values,
            &api_descriptor_name(group),
            &format!("pair descriptor for {}", group.name.protocol_stem()),
        )?;
    }

    for source in unkeyed {
        validate_message_module(source)?;
        claim_type_export(
            &mut generated_types,
            &mut root_types,
            source.message.name.rust_type(),
            &format!("message {}", source.message.name.protocol()),
        )?;
        claim_type_export(
            &mut generated_types,
            &mut root_types,
            source.message.name.rust_module(),
            &format!(
                "generated struct module for {}",
                source.message.name.protocol()
            ),
        )?;
    }

    for (symbol, producer) in [
        ("API_DESCRIPTORS", "fixed API-pair registry"),
        ("MESSAGE_DESCRIPTORS", "fixed directional-message registry"),
        (
            "request_header_version",
            "fixed request-header policy function",
        ),
        (
            "response_header_version",
            "fixed response-header policy function",
        ),
    ] {
        claim_value_export(&mut generated_values, &mut root_values, symbol, producer)?;
    }
    Ok(())
}

fn validate_message_module(source: &MessageSource) -> Result<(), GenerationError> {
    let message = &source.message;
    let namespace = format!(
        "the generated `{}` module type namespace",
        message.name.rust_module()
    );
    let mut claimed = BTreeMap::new();
    claim(
        &mut claimed,
        &namespace,
        message.name.rust_type(),
        &format!("message {}", message.name.protocol()),
    )?;
    for declaration in message.structs.declarations() {
        claim(
            &mut claimed,
            &namespace,
            declaration.name.rust_type(),
            &format!(
                "nested struct {} declared {} by {}",
                declaration.name.declared(),
                declaration.origin.describe(),
                declaration.name.owner()
            ),
        )?;
    }
    validate_synthesized_members(source)?;
    Ok(())
}

fn claim_message_exports(
    source: &MessageSource,
    generated_types: &mut BTreeMap<String, String>,
    generated_values: &mut BTreeMap<String, String>,
    root_types: &mut BTreeMap<String, String>,
    root_values: &mut BTreeMap<String, String>,
) -> Result<(), GenerationError> {
    let protocol = source.message.name.protocol();
    claim_type_export(
        generated_types,
        root_types,
        source.message.name.rust_type(),
        &format!("message {protocol}"),
    )?;
    claim_type_export(
        generated_types,
        root_types,
        source.message.name.rust_module(),
        &format!("generated struct module for {protocol}"),
    )?;
    claim_value_export(
        generated_values,
        root_values,
        &descriptor_name(&source.message),
        &format!("descriptor for message {protocol}"),
    )
}

fn claim_type_export(
    generated: &mut BTreeMap<String, String>,
    root: &mut BTreeMap<String, String>,
    symbol: &str,
    producer: &str,
) -> Result<(), GenerationError> {
    claim(generated, GENERATED_TYPE_NAMESPACE, symbol, producer)?;
    claim(root, ROOT_TYPE_NAMESPACE, symbol, producer)
}

fn claim_value_export(
    generated: &mut BTreeMap<String, String>,
    root: &mut BTreeMap<String, String>,
    symbol: &str,
    producer: &str,
) -> Result<(), GenerationError> {
    claim(generated, GENERATED_VALUE_NAMESPACE, symbol, producer)?;
    claim(root, ROOT_VALUE_NAMESPACE, symbol, producer)
}

fn claim(
    claimed: &mut BTreeMap<String, String>,
    namespace: &str,
    symbol: &str,
    producer: &str,
) -> Result<(), GenerationError> {
    if let Some(first) = claimed.get(symbol) {
        return Err(GenerationError::GeneratedSymbolCollision {
            namespace: namespace.to_owned(),
            symbol: symbol.to_owned(),
            first: first.clone(),
            second: producer.to_owned(),
        });
    }
    claimed.insert(symbol.to_owned(), producer.to_owned());
    Ok(())
}

pub(crate) fn handwritten_root_types() -> BTreeMap<String, String> {
    [
        ("ApiDescriptor", "handwritten crate facade"),
        ("KafkaMessage", "handwritten crate facade"),
        ("KafkaRequest", "handwritten crate facade"),
        ("KafkaResponse", "handwritten crate facade"),
        ("MessageDescriptor", "handwritten crate facade"),
        ("MessageDirection", "handwritten crate facade"),
        ("OutboundFrameLimits", "handwritten crate facade"),
        ("ProtocolEq", "handwritten crate facade"),
        ("RequestResponsePair", "handwritten crate facade"),
        ("descriptor", PRIVATE_ROOT_MODULE),
        ("frame", PRIVATE_ROOT_MODULE),
        ("generated", PRIVATE_ROOT_MODULE),
        ("message", PRIVATE_ROOT_MODULE),
        ("tagged_claims_test", PRIVATE_ROOT_MODULE),
    ]
    .into_iter()
    .map(|(symbol, producer)| (symbol.to_owned(), producer.to_owned()))
    .collect()
}

fn handwritten_root_values() -> BTreeMap<String, String> {
    ["encode_request", "response_header_version_for"]
        .into_iter()
        .map(|symbol| (symbol.to_owned(), "handwritten crate facade".to_owned()))
        .collect()
}
