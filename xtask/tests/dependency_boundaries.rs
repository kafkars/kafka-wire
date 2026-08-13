//! Internal crate dependencies follow the compiler/runtime direction exactly.
//!
//! Scenario: read every workspace manifest and reject an internal dependency
//! the architecture map does not permit. A fixture in which the wire kernel
//! depends on the protocol crate — the direction reversed — must be rejected.
//!
//! The build-script search below is bounded. An unfiltered walk from the
//! workspace root descends into `target/`, which already holds five figures of
//! files and grows without limit.

#![allow(clippy::unwrap_used)]

mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use serde::Deserialize;
use support::{DependencyRule, fixture_root, load_policy, tracked_files, workspace_root};

#[derive(Debug, Deserialize)]
struct Manifest {
    package: Option<Package>,
    #[serde(default)]
    dependencies: BTreeMap<String, toml::Value>,
    #[serde(rename = "dev-dependencies", default)]
    dev_dependencies: BTreeMap<String, toml::Value>,
    #[serde(rename = "build-dependencies", default)]
    build_dependencies: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Deserialize)]
struct Package {
    name: String,
    publish: Option<toml::Value>,
}

const PUBLIC_PACKAGES: [&str; 3] = ["kafka-wire", "kafka-wire-core", "kafka-wire-records"];

/// Dependency edges that contradict the configured architecture map.
///
/// Two directions are judged. Every internal edge must appear in the package's
/// `allowed_internal` list, which keeps the compiler/runtime direction one-way.
/// A package that also carries an `allowed_external` list is held to an
/// allowlist for third-party crates too, so a core crate cannot quietly take on
/// a networking or process crate (`socket2`, `mio`, `libc`) that would hand it
/// the very capability the source test forbids.
fn dependency_violations(
    root: &Path,
    rules: &[DependencyRule],
    versioned_path_packages: &[&str],
) -> Vec<String> {
    let package_version = env!("CARGO_PKG_VERSION");
    let rules_by_package = rules
        .iter()
        .map(|rule| (rule.package.as_str(), rule))
        .collect::<BTreeMap<_, _>>();
    let versioned_path_packages = versioned_path_packages
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let manifests = crate_manifests(root);
    let internal = manifests
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut violations = Vec::new();

    for package in rules_by_package.keys() {
        if !internal.contains(package) {
            violations.push(format!(
                "architecture.toml has a dependency rule for missing package {package}"
            ));
        }
    }

    for (package, manifest) in &manifests {
        let Some(rule) = rules_by_package.get(package.as_str()) else {
            violations.push(format!(
                "{package}: missing dependency rule in architecture.toml"
            ));
            continue;
        };

        let permitted_internal = rule
            .allowed_internal
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let permitted_external = rule
            .allowed_external
            .as_ref()
            .map(|allowed| allowed.iter().map(String::as_str).collect::<BTreeSet<_>>());

        for (dependency, specification) in manifest
            .dependencies
            .iter()
            .chain(manifest.dev_dependencies.iter())
            .chain(manifest.build_dependencies.iter())
        {
            let dependency = dependency.as_str();
            if internal.contains(dependency) {
                let expected_path = format!("../{dependency}");
                if versioned_path_packages.contains(package.as_str())
                    && !is_exact_versioned_path(specification, &expected_path, package_version)
                {
                    violations.push(format!(
                        "{package} dependency {dependency} must be exactly \
                         {{ version = \"{package_version}\", path = \"{expected_path}\" }}"
                    ));
                }
                if !permitted_internal.contains(dependency) {
                    violations.push(format!(
                        "{package} may not depend on internal crate {dependency}; \
                         allowed: {permitted_internal:?}"
                    ));
                }
            } else if let Some(permitted_external) = &permitted_external {
                if !permitted_external.contains(dependency) {
                    violations.push(format!(
                        "{package} may not depend on external crate {dependency}; \
                         its dependencies are an allowlist: {permitted_external:?}"
                    ));
                }
            }
        }
    }

    violations
}

fn is_exact_versioned_path(specification: &toml::Value, path: &str, version: &str) -> bool {
    specification.as_table().is_some_and(|table| {
        table.len() == 2
            && table.get("path").and_then(toml::Value::as_str) == Some(path)
            && table.get("version").and_then(toml::Value::as_str) == Some(version)
    })
}

fn publication_violations(manifests: &BTreeMap<String, Manifest>) -> Vec<String> {
    let mut violations = Vec::new();
    for (name, manifest) in manifests {
        let publish = manifest
            .package
            .as_ref()
            .unwrap_or_else(|| unreachable!("package map contains only named manifests"))
            .publish
            .as_ref();
        if PUBLIC_PACKAGES.contains(&name.as_str()) {
            if !publish
                .and_then(toml::Value::as_array)
                .is_some_and(|registries| {
                    registries.len() == 1
                        && registries.first().and_then(toml::Value::as_str) == Some("crates-io")
                })
            {
                violations.push(format!("{name} must publish only to crates-io"));
            }
        } else if publish.and_then(toml::Value::as_bool) != Some(false) {
            violations.push(format!("{name} must remain unpublished"));
        }
    }
    violations
}

#[test]
fn internal_dependencies_match_the_architecture_map() {
    let workspace = workspace_root();
    let config = load_policy(&workspace);
    let violations = dependency_violations(
        &workspace,
        &config.dependency_rules,
        &["kafka-wire", "kafka-wire-records"],
    );

    assert!(
        violations.is_empty(),
        "dependency direction violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn only_runtime_wire_packages_are_registry_publishable() {
    let workspace = workspace_root();
    let violations = publication_violations(&crate_manifests(&workspace));

    assert!(
        violations.is_empty(),
        "package publication violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn publication_role_drift_is_rejected() {
    let public = r#"
[package]
name = "kafka-wire"
publish = false
"#;
    let private = r#"
[package]
name = "kafka-wire-schema"
publish = ["crates-io"]
"#;
    let manifests = [public, private]
        .into_iter()
        .map(|source| {
            let manifest = toml::from_str::<Manifest>(source)
                .unwrap_or_else(|error| panic!("parse publication fixture: {error}"));
            let name = manifest
                .package
                .as_ref()
                .unwrap_or_else(|| panic!("publication fixture has no package"))
                .name
                .clone();
            (name, manifest)
        })
        .collect();

    assert_eq!(
        publication_violations(&manifests),
        [
            "kafka-wire must publish only to crates-io",
            "kafka-wire-schema must remain unpublished",
        ]
    );
}

#[test]
fn a_reversed_internal_dependency_is_rejected() {
    let root = fixture_root("reversed_dependency");
    let rules = vec![
        DependencyRule {
            package: "fixture-wire".to_owned(),
            allowed_internal: Vec::new(),
            allowed_external: None,
        },
        DependencyRule {
            package: "fixture-protocol".to_owned(),
            allowed_internal: vec!["fixture-wire".to_owned()],
            allowed_external: None,
        },
    ];
    let violations = dependency_violations(&root, &rules, &[]);

    assert!(
        violations.iter().any(|violation| {
            violation.contains("fixture-wire may not depend on internal crate fixture-protocol")
        }),
        "the dependency detector accepted a reversed dependency edge: {violations:?}"
    );
    assert!(
        !violations
            .iter()
            .any(|violation| violation.starts_with("fixture-protocol may not")),
        "the dependency detector rejected a permitted dependency edge: {violations:?}"
    );
}

#[test]
fn a_package_with_no_dependency_rule_is_rejected() {
    let root = fixture_root("reversed_dependency");
    let violations = dependency_violations(&root, &[], &[]);

    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("missing dependency rule in architecture.toml")),
        "the dependency detector accepted an unruled package: {violations:?}"
    );
}

#[test]
fn a_core_crate_taking_a_networking_crate_is_rejected() {
    let root = fixture_root("networking_dependency");
    let rules = vec![DependencyRule {
        package: "fixture-wire".to_owned(),
        allowed_internal: Vec::new(),
        // The wire kernel may take `bytes`, and nothing else. A third-party
        // socket crate is exactly what the allowlist exists to reject.
        allowed_external: Some(vec!["bytes".to_owned()]),
    }];
    let violations = dependency_violations(&root, &rules, &[]);

    assert!(
        violations.iter().any(|violation| {
            violation.contains("fixture-wire may not depend on external crate socket2")
        }),
        "the dependency detector let a networking crate into a core crate: {violations:?}"
    );
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("external crate bytes")),
        "the dependency detector rejected an allowlisted external dependency: {violations:?}"
    );
}

#[test]
fn a_packaged_internal_dependency_without_a_version_is_rejected() {
    let root = fixture_root("reversed_dependency");
    let rules = vec![
        DependencyRule {
            package: "fixture-wire".to_owned(),
            allowed_internal: vec!["fixture-protocol".to_owned()],
            allowed_external: None,
        },
        DependencyRule {
            package: "fixture-protocol".to_owned(),
            allowed_internal: vec!["fixture-wire".to_owned()],
            allowed_external: None,
        },
    ];
    let violations = dependency_violations(&root, &rules, &["fixture-wire"]);

    assert!(
        violations.iter().any(|violation| violation
            .contains("fixture-wire dependency fixture-protocol must be exactly")),
        "the dependency detector accepted a packaged path-only edge: {violations:?}"
    );
}

#[test]
fn workspace_contains_no_build_scripts() {
    let workspace = workspace_root();
    let violations = tracked_files(&workspace)
        .into_iter()
        .filter(|path| path.file_name().is_some_and(|name| name == "build.rs"))
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "build-time code generation is forbidden:\n{}",
        violations.join("\n")
    );
}

fn crate_manifests(root: &Path) -> BTreeMap<String, Manifest> {
    let crate_manifests = walkdir::WalkDir::new(root.join("crates"))
        .max_depth(3)
        .into_iter()
        .filter_map(Result::ok);
    let xtask_manifests = walkdir::WalkDir::new(root.join("xtask"))
        .max_depth(2)
        .into_iter()
        .filter_map(Result::ok);

    let mut manifests = BTreeMap::new();
    for entry in crate_manifests.chain(xtask_manifests) {
        if !entry.file_type().is_file() || entry.file_name() != "Cargo.toml" {
            continue;
        }

        let source = fs::read_to_string(entry.path()).unwrap();
        let manifest: Manifest = toml::from_str(&source).unwrap();
        if let Some(package) = &manifest.package {
            manifests.insert(package.name.clone(), manifest);
        }
    }

    assert!(
        !manifests.is_empty(),
        "no crate manifests found below {}; \
         a dependency test that reads nothing would pass over an empty graph",
        root.display()
    );
    manifests
}
