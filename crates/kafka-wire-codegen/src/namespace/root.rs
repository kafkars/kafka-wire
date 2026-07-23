//! Handwritten crate-root claims reserved before generation.

use std::collections::BTreeMap;

const PRIVATE_ROOT_MODULE: &str = "handwritten private crate-root module";

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
        ("RetainedFootprint", "handwritten crate facade"),
        ("RetainedSize", "handwritten crate facade"),
        ("descriptor", PRIVATE_ROOT_MODULE),
        ("frame", PRIVATE_ROOT_MODULE),
        ("generated", PRIVATE_ROOT_MODULE),
        ("message", PRIVATE_ROOT_MODULE),
        ("retained", PRIVATE_ROOT_MODULE),
        ("retained_test", PRIVATE_ROOT_MODULE),
        ("tagged_claims_test", PRIVATE_ROOT_MODULE),
    ]
    .into_iter()
    .map(|(symbol, producer)| (symbol.to_owned(), producer.to_owned()))
    .collect()
}

pub(super) fn handwritten_root_values() -> BTreeMap<String, String> {
    ["encode_request", "response_header_version_for"]
        .into_iter()
        .map(|symbol| (symbol.to_owned(), "handwritten crate facade".to_owned()))
        .collect()
}
