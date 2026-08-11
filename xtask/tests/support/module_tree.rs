//! Resolution of the set of source files that actually compile into a crate,
//! by following `mod` declarations and `#[path]` attributes from its entry files.
//!
//! This module owns the question "which files does this crate compile", which
//! is strictly stronger than "which `.rs` files sit under this directory". A
//! `#[path]` attribute can pull in a file with a non-`.rs` extension, or one
//! living outside the crate's source directory; a directory glob misses the
//! former and misattributes the latter. Binding a capability rule to this tree
//! rather than to a physical prefix closes both evasions.
//!
//! It deliberately owns no policy and makes no capability judgement: it reports
//! files, never verdicts. Resolution follows the Rust reference for non-inline
//! `mod` statements exactly — a `#[path]` there is relative to the directory of
//! the file that writes it, while a plain `mod name;` resolves against the
//! module directory. Deeply nested `#[path]` inside inline `mod { .. }` blocks
//! is approximated rather than modelled to the letter, because the attack this
//! closes lives at file scope, not inside an inline block.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use syn::{Attribute, Expr, Item, ItemMod, Lit, Meta};

/// Every source file that compiles into the crate whose sources live under
/// `crate_root`, discovered by following module declarations from its entry
/// files.
///
/// `crate_root` may name the source directory directly (`.../src`) or the crate
/// directory (`.../kafka-wire-conformance`); both `lib.rs`/`main.rs` and their
/// `src/` counterparts are probed so a rule bound to either shape resolves the
/// same tree.
pub(crate) fn compiled_files(crate_root: &Path) -> BTreeSet<PathBuf> {
    let mut files = BTreeSet::new();
    let mut visited = BTreeSet::new();

    for entry in entry_files(crate_root) {
        let mod_dir = entry
            .parent()
            .map_or_else(|| crate_root.to_path_buf(), Path::to_path_buf);
        walk_file(&entry, &mod_dir, &mut files, &mut visited);
    }

    files
}

/// Record `file` and recurse into every module it declares.
///
/// `mod_dir` is the directory against which this file's plain `mod name;`
/// children resolve; a `#[path]` on those children instead resolves against the
/// file's own directory, which is why the two are tracked separately below. An
/// unreadable or unparseable file is still recorded — a caller judging the tree
/// must see it and refuse to vouch for it — but its children cannot be found.
fn walk_file(
    file: &Path,
    mod_dir: &Path,
    files: &mut BTreeSet<PathBuf>,
    visited: &mut BTreeSet<PathBuf>,
) {
    if !visited.insert(file.to_path_buf()) || !file.is_file() {
        return;
    }
    files.insert(file.to_path_buf());

    let Ok(source) = fs::read_to_string(file) else {
        return;
    };
    let Ok(ast) = syn::parse_file(&source) else {
        return;
    };

    let file_dir = file.parent().unwrap_or(mod_dir);
    walk_items(&ast.items, file_dir, mod_dir, files, visited);
}

fn walk_items(
    items: &[Item],
    file_dir: &Path,
    mod_dir: &Path,
    files: &mut BTreeSet<PathBuf>,
    visited: &mut BTreeSet<PathBuf>,
) {
    for item in items {
        let Item::Mod(module) = item else {
            continue;
        };

        if let Some((_, inner)) = &module.content {
            // An inline `mod name { .. }` adds no file, but its children resolve
            // one directory deeper.
            let inner_dir = mod_dir.join(module.ident.to_string());
            walk_items(inner, file_dir, &inner_dir, files, visited);
        } else {
            // A bodyless `mod name;` names a file to load and recurse into.
            let (child, child_mod_dir) = resolve_child(module, file_dir, mod_dir);
            walk_file(&child, &child_mod_dir, files, visited);
        }
    }
}

/// Resolve a `mod name;` declaration to the file it loads and the directory its
/// own children will resolve against.
fn resolve_child(module: &ItemMod, file_dir: &Path, mod_dir: &Path) -> (PathBuf, PathBuf) {
    let name = module.ident.to_string();

    // A `#[path]` on a non-inline `mod` is relative to the directory of the file
    // that writes it, not the module directory; the loaded file is then a
    // "mod-rs" file, so its children resolve against its own directory.
    if let Some(relative) = path_attribute(&module.attrs) {
        let child = file_dir.join(relative);
        let child_dir = child
            .parent()
            .map_or_else(|| file_dir.to_path_buf(), Path::to_path_buf);
        return (child, child_dir);
    }

    let flat = mod_dir.join(format!("{name}.rs"));
    let nested_dir = mod_dir.join(&name);
    if flat.is_file() {
        (flat, nested_dir)
    } else {
        (nested_dir.join("mod.rs"), nested_dir)
    }
}

/// The string value of a `#[path = "..."]` attribute, if one is present.
fn path_attribute(attrs: &[Attribute]) -> Option<String> {
    attrs.iter().find_map(|attr| {
        let Meta::NameValue(name_value) = &attr.meta else {
            return None;
        };
        if !name_value.path.is_ident("path") {
            return None;
        }
        match &name_value.value {
            Expr::Lit(literal) => match &literal.lit {
                Lit::Str(string) => Some(string.value()),
                _ => None,
            },
            _ => None,
        }
    })
}

/// Candidate crate entry files that exist below `crate_root`.
fn entry_files(crate_root: &Path) -> Vec<PathBuf> {
    let mut entries = Vec::new();
    for name in ["lib.rs", "main.rs"] {
        for base in [crate_root.to_path_buf(), crate_root.join("src")] {
            let candidate = base.join(name);
            if candidate.is_file() {
                entries.push(candidate);
            }
        }
    }
    entries
}
