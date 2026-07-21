//! Override documents reject drift instead of silently changing compiler policy.
//!
//! Scenario: load the real pinned API inventory, then vary one override schema
//! key, version expression, source identity, or duplicate entry and require the
//! strict reader to fail before rendering.

use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{
    GenerationError,
    group::{ApiGroup, group_sources},
    lockfile::ProtocolLock,
    overrides::{HeaderOverrides, SchemaExceptionOverrides},
    source::load_sources,
};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

fn corpus() -> (ProtocolLock, Vec<ApiGroup>) {
    let root = repository_root();
    let lock = ProtocolLock::read(&root.join("spec/protocol.lock"))
        .unwrap_or_else(|error| panic!("read protocol lock: {error}"));
    let sources =
        load_sources(&root, &lock).unwrap_or_else(|error| panic!("load pinned sources: {error}"));
    let groups = group_sources(sources)
        .unwrap_or_else(|error| panic!("group pinned sources: {error}"))
        .api;
    (lock, groups)
}

fn workspace(name: &str, file: &str, source: &str) -> PathBuf {
    let root = repository_root()
        .join("target/codegen-overrides")
        .join(name);
    if root.exists() {
        fs::remove_dir_all(&root)
            .unwrap_or_else(|error| panic!("clear {}: {error}", root.display()));
    }
    let path = root.join("spec/overrides").join(file);
    fs::create_dir_all(path.parent().unwrap_or_else(|| Path::new(".")))
        .unwrap_or_else(|error| panic!("create override fixture: {error}"));
    fs::write(&path, source).unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
    root
}

fn header(versions: &str, api_key: i16, source: &str) -> String {
    format!(
        "schema = 1\n\n\
         [[response_header_exceptions]]\n\
         api_key = {api_key}\n\
         versions = \"{versions}\"\n\
         header_version = 0\n\
         reason = \"compatibility\"\n\
         source = \"{source}\"\n"
    )
}

#[test]
fn header_versions_are_strict_open_ranges_tied_to_a_real_response() {
    let (lock, groups) = corpus();
    let source = "clients/src/main/resources/common/message/ApiVersionsResponse.json";

    let valid = workspace("header-valid", "headers.toml", &header("3+", 18, source));
    HeaderOverrides::read(&valid, &lock, &groups)
        .unwrap_or_else(|error| panic!("valid header override rejected: {error}"));

    for (name, versions) in [("closed", "3-5"), ("repeated-plus", "3++")] {
        let root = workspace(name, "headers.toml", &header(versions, 18, source));
        assert!(
            HeaderOverrides::read(&root, &lock, &groups).is_err(),
            "malformed header versions `{versions}` were accepted"
        );
    }

    let missing = workspace("missing-api", "headers.toml", &header("3+", 9_000, source));
    assert!(
        matches!(
            HeaderOverrides::read(&missing, &lock, &groups),
            Err(GenerationError::InvalidOverride { .. })
        ),
        "an exception for an unknown API key was accepted"
    );
}

#[test]
fn header_sources_unknown_keys_and_overlaps_are_rejected() {
    let (lock, groups) = corpus();
    let source = "clients/src/main/resources/common/message/ApiVersionsResponse.json";
    let wrong = workspace(
        "wrong-source",
        "headers.toml",
        &header("3+", 18, "message/MetadataResponse.json"),
    );
    assert!(HeaderOverrides::read(&wrong, &lock, &groups).is_err());

    let unknown = workspace(
        "unknown-key",
        "headers.toml",
        &header("3+", 18, source).replace("schema = 1", "schema = 1\nmystery = true"),
    );
    assert!(
        matches!(
            HeaderOverrides::read(&unknown, &lock, &groups),
            Err(GenerationError::Override { .. })
        ),
        "an unknown override key was ignored"
    );

    let entry = header("3+", 18, source);
    let duplicate = entry.replacen("schema = 1\n\n", "", 1);
    let overlap = workspace("overlap", "headers.toml", &format!("{entry}\n{duplicate}"));
    assert!(
        matches!(
            HeaderOverrides::read(&overlap, &lock, &groups),
            Err(GenerationError::InvalidOverride { .. })
        ),
        "overlapping header exceptions were accepted"
    );
}

#[test]
fn schema_exceptions_are_versioned_unique_and_tied_to_pinned_sources() {
    let (lock, _) = corpus();
    let valid = "schema = 1\n\n\
        [[accepted]]\n\
        message = \"ShareFetchRequest\"\n\
        field = \"PartitionMaxBytes\"\n\
        code = \"KAFKA_SCHEMA_UNUSED_FIELD\"\n\
        reason = \"reviewed upstream defect\"\n\
        upstream = \"clients/src/main/resources/common/message/ShareFetchRequest.json\"\n";
    let root = workspace("schema-valid", "schema_exceptions.toml", valid);
    SchemaExceptionOverrides::read(&root, &lock)
        .unwrap_or_else(|error| panic!("valid schema exception rejected: {error}"));

    let unknown = workspace(
        "schema-unknown-key",
        "schema_exceptions.toml",
        &valid.replace("schema = 1", "schema = 1\nmystery = true"),
    );
    assert!(matches!(
        SchemaExceptionOverrides::read(&unknown, &lock),
        Err(GenerationError::Override { .. })
    ));

    let wrong_source = workspace(
        "schema-wrong-source",
        "schema_exceptions.toml",
        &valid.replace("ShareFetchRequest.json", "MetadataRequest.json"),
    );
    assert!(matches!(
        SchemaExceptionOverrides::read(&wrong_source, &lock),
        Err(GenerationError::InvalidOverride { .. })
    ));

    let duplicate = valid.replacen("schema = 1\n\n", "", 1);
    let duplicate = workspace(
        "schema-duplicate",
        "schema_exceptions.toml",
        &format!("{valid}\n{duplicate}"),
    );
    assert!(matches!(
        SchemaExceptionOverrides::read(&duplicate, &lock),
        Err(GenerationError::InvalidOverride { .. })
    ));
}
