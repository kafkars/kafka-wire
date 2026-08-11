//! `lib.rs` and `mod.rs` files remain declarative navigation surfaces.
//!
//! Scenario: parse every facade in a tree and assert it holds only module
//! declarations and re-exports. The live workspace must be clean, and a fixture
//! facade carrying a function must be rejected by name and kind.
//!
//! `include!` is resolved rather than refused. A flat facade over generated
//! output has to name every generated item at the crate root, and an include of
//! a machine-written `pub use` list is that naming — not behavior smuggled into
//! a facade. So the detector reads the included file and applies the identical
//! rule to what it brings in, which is strictly more than it saw before: an
//! include hiding a function is rejected exactly as a function would be.

#![allow(clippy::unwrap_used)]

mod support;

use std::path::{Path, PathBuf};

use support::{
    display_path, fixture_files, is_facade, load_policy, read, rust_files, workspace_root,
};
use syn::{Item, Macro};

/// Facades holding anything beyond `mod`, `use`, and `pub use`.
fn facades_with_implementation(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();

    for path in files.iter().filter(|path| is_facade(path)) {
        let source = read(path);
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse facade {}: {error}", display_path(root, path)));
        check_items(root, path, &syntax.items, &mut violations);
    }

    violations
}

fn check_items(root: &Path, path: &Path, items: &[Item], violations: &mut Vec<String>) {
    for item in items {
        if let Item::Macro(invocation) = item
            && let Some(included) = include_target(path, &invocation.mac)
        {
            let source = read(&included);
            let syntax = syn::parse_file(&source).unwrap_or_else(|error| {
                panic!("parse included fragment {}: {error}", included.display())
            });
            check_items(root, &included, &syntax.items, violations);
            continue;
        }
        if !matches!(item, Item::Mod(_) | Item::Use(_)) {
            violations.push(format!(
                "{} contains implementation item {}; move it behind the facade",
                display_path(root, path),
                item_kind(item)
            ));
        }
    }
}

/// The file an `include!` names, resolved beside the file that includes it.
///
/// Anything that is not a literal `include!("...")` returns `None` and is then
/// reported as a macro, which is what keeps this from becoming a hole: a facade
/// cannot reach a computed path or a different macro through here.
fn include_target(path: &Path, invocation: &Macro) -> Option<PathBuf> {
    if !invocation.path.is_ident("include") {
        return None;
    }
    let literal: syn::LitStr = invocation.parse_body().ok()?;
    Some(path.parent()?.join(literal.value()))
}

#[test]
fn facades_declare_modules_and_reexports_only() {
    let workspace = workspace_root();
    let config = load_policy(&workspace);
    let violations = facades_with_implementation(&workspace, &rust_files(&workspace, &config));

    assert!(
        violations.is_empty(),
        "facade architecture violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn a_facade_carrying_a_function_is_rejected() {
    let (root, files) = fixture_files("facade_with_implementation");
    let violations = facades_with_implementation(&root, &files);

    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("lib.rs") && violation.contains("function")),
        "the facade detector accepted a function declared in `lib.rs`: {violations:?}"
    );
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("declared.rs")),
        "the facade detector rejected a non-facade implementation file: {violations:?}"
    );
}

#[test]
fn a_facade_including_a_function_is_rejected_at_the_fragment() {
    // Resolving `include!` is only honest if what it resolves to is judged.
    // A detector that skipped the fragment would turn one include into a way
    // around the whole rule.
    let (root, files) = fixture_files("facade_with_implementation");
    let violations = facades_with_implementation(&root, &files);

    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("smuggled.rsi") && violation.contains("function")),
        "the facade detector accepted a function reached through `include!`: {violations:?}"
    );
}

fn item_kind(item: &Item) -> &'static str {
    match item {
        Item::Const(_) => "const",
        Item::Enum(_) => "enum",
        Item::ExternCrate(_) => "extern crate",
        Item::Fn(_) => "function",
        Item::ForeignMod(_) => "foreign module",
        Item::Impl(_) => "impl",
        Item::Macro(_) => "macro",
        Item::Static(_) => "static",
        Item::Struct(_) => "struct",
        Item::Trait(_) => "trait",
        Item::TraitAlias(_) => "trait alias",
        Item::Type(_) => "type alias",
        Item::Union(_) => "union",
        Item::Verbatim(_) => "verbatim item",
        Item::Mod(_) | Item::Use(_) => "declaration",
        _ => "unsupported item",
    }
}
