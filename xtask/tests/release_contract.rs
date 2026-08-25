//! Public wire packages carry exact and archive-visible release metadata.

use std::{
    fs,
    path::{Path, PathBuf},
};

use toml::Value;

const PUBLIC_PACKAGES: [(&str, &str); 3] = [
    ("kafka-wire", "crates/kafka-wire/Cargo.toml"),
    ("kafka-wire-core", "crates/kafka-wire-core/Cargo.toml"),
    ("kafka-wire-records", "crates/kafka-wire-records/Cargo.toml"),
];
const RELEASE_VERSION: &str = "0.1.0-rc.3";
const REPOSITORY: &str = "https://github.com/kafkars/kafka-wire";

#[test]
fn public_package_metadata_and_policy_files_are_complete() {
    let root = workspace_root();
    let repository_license = read(&root.join("LICENSE"));
    let workspace = parse(&root.join("Cargo.toml"));
    assert_eq!(
        workspace["workspace"]["package"]["version"].as_str(),
        Some(RELEASE_VERSION)
    );
    assert_eq!(
        workspace["workspace"]["package"]["license"].as_str(),
        Some("Apache-2.0")
    );
    assert_eq!(
        workspace["workspace"]["package"]["repository"].as_str(),
        Some(REPOSITORY)
    );

    for (name, manifest_path) in PUBLIC_PACKAGES {
        let manifest_path = root.join(manifest_path);
        let manifest = parse(&manifest_path);
        let package = &manifest["package"];
        assert_eq!(package["name"].as_str(), Some(name));
        assert_eq!(package["version"]["workspace"].as_bool(), Some(true));
        assert_eq!(package["license"]["workspace"].as_bool(), Some(true));
        assert_eq!(package["repository"]["workspace"].as_bool(), Some(true));
        assert_eq!(package["readme"].as_str(), Some("README.md"));
        assert_eq!(
            package["publish"]
                .as_array()
                .and_then(|values| { (values.len() == 1).then(|| values[0].as_str()).flatten() }),
            Some("crates-io")
        );
        assert!(
            package["description"]
                .as_str()
                .is_some_and(|value| !value.trim().is_empty()),
            "{name} description must be nonempty"
        );
        let Some(package_root) = manifest_path.parent() else {
            panic!("public package manifest must have a parent: {name}");
        };
        assert_eq!(
            read(&package_root.join("LICENSE")),
            repository_license,
            "{name} package LICENSE differs from the repository license"
        );
    }

    for policy in [
        "CHANGELOG.md",
        "CODE_OF_CONDUCT.md",
        "CONTRIBUTING.md",
        "LICENSE",
        "SECURITY.md",
    ] {
        assert!(root.join(policy).is_file(), "missing policy file: {policy}");
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn parse(path: &Path) -> Value {
    read(path)
        .parse::<Value>()
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}
