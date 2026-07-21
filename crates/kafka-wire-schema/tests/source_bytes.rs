//! In-memory source loading never reopens the provenance path.
//!
//! Scenario: construct a source from valid pinned bytes, replace the path with
//! invalid content, then compile the owned source successfully. The front end
//! must parse the verified object it received rather than whatever the path
//! happens to contain later.

use std::{
    fs,
    path::{Path, PathBuf},
};

use kafka_wire_schema::{SourceFile, load_source};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

#[test]
fn parsing_uses_the_exact_owned_bytes_instead_of_reading_the_path_again() {
    let root = repository_root();
    let original = root
        .join("spec/upstream/apache-kafka")
        .join("678c0e07e4733c5a592e52046dc2c4e1625587f1")
        .join("message/SaslHandshakeRequest.json");
    let bytes =
        fs::read(&original).unwrap_or_else(|error| panic!("read {}: {error}", original.display()));

    let path = root.join("target/schema-source-bytes/SaslHandshakeRequest.json");
    fs::create_dir_all(path.parent().unwrap_or_else(|| Path::new(".")))
        .unwrap_or_else(|error| panic!("create fixture directory: {error}"));
    let source = SourceFile::from_bytes(path.clone(), bytes)
        .unwrap_or_else(|error| panic!("build exact source: {error}"));
    fs::write(&path, b"this is not JSON")
        .unwrap_or_else(|error| panic!("replace fixture path: {error}"));

    let message = load_source(source)
        .unwrap_or_else(|error| panic!("owned verified bytes were not parsed: {error}"));
    assert_eq!(message.name.protocol(), "SaslHandshakeRequest");
}
