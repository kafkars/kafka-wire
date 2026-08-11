//! Resolution of one Rust file's `use` trees and path expressions into
//! fully-qualified paths.
//!
//! This module owns the syntactic question "which absolute paths does this file
//! name", and therefore owns the alias bookkeeping that makes
//! `use std::net as network;` followed by `network::TcpStream` read as
//! `std::net::TcpStream`. Nested use groups, renamed imports, globs, `extern
//! crate`, and paths written inline with no `use` at all all resolve here.
//!
//! It deliberately owns no policy: it does not know which paths are forbidden,
//! and it never opens a second file, so cross-module and cross-crate name
//! resolution are out of scope by construction. A caller that needs to reason
//! about a re-export must inspect the file that writes the re-export. The
//! lexical fallback for macro bodies that do not parse as Rust is owned by
//! `macro_tokens`; this module reaches for it only when structural reading
//! fails.

use std::collections::BTreeMap;

use syn::{
    Block, Expr, ItemExternCrate, ItemUse, Macro, Path, Stmt, Token, UseTree,
    punctuated::Punctuated,
    visit::{self, Visit},
};

use super::macro_tokens;

/// What a static read of one file established about the paths it names.
#[derive(Clone, Debug)]
pub(crate) enum PathReach {
    /// Fully-qualified paths, after use-tree and alias resolution.
    Named(Vec<String>),
    /// Not parseable as Rust, so no capability claim can be made about it.
    Unparseable,
}

impl PathReach {
    /// Resolved paths, or nothing at all when the file could not be read as Rust.
    ///
    /// An unreadable file yields no evidence of safety; callers must report it
    /// rather than treat an empty reach as a clean one.
    pub(crate) fn named(&self) -> &[String] {
        match self {
            Self::Named(named) => named,
            Self::Unparseable => &[],
        }
    }
}

/// Path roots that are already absolute and never resolved through a glob.
///
/// A bare `net::TcpStream` under `use std::*;` must be tried as `std::net`, but
/// a path that already starts at a crate root is complete on its own; expanding
/// it through the glob would only manufacture nonsense like `std::net::std::io`.
const ROOTED_HEADS: [&str; 7] = ["std", "core", "alloc", "crate", "self", "super", "Self"];

/// Resolve every path `source` names into its fully-qualified form.
pub(crate) fn path_reach(source: &str) -> PathReach {
    let Ok(file) = syn::parse_file(source) else {
        return PathReach::Unparseable;
    };

    let mut reader = PathReader::default();
    reader.visit_file(&file);

    // Aliases bind file-wide, and a `use` may follow the code that relies on it,
    // so resolution runs only once the whole file has been read.
    let mut named = Vec::new();
    for written in &reader.written {
        named.push(resolve(written, &reader.aliases));

        // A path whose head is neither an alias nor an absolute root may have
        // been brought into scope by a parent glob (`use std::*;` then
        // `net::TcpStream`). Try it under each recorded glob prefix so the child
        // segment is tied back to a forbidden capability instead of left bare.
        let head = written.split("::").next().unwrap_or(written);
        if !reader.aliases.contains_key(head) && !ROOTED_HEADS.contains(&head) {
            for glob in &reader.globs {
                named.push(format!("{glob}::{written}"));
            }
        }
    }
    named.sort();
    named.dedup();

    PathReach::Named(named)
}

/// Whether `path` is `capability` itself or lives beneath it.
pub(crate) fn lies_under(path: &str, capability: &str) -> bool {
    path == capability
        || path
            .strip_prefix(capability)
            .is_some_and(|rest| rest.starts_with("::"))
}

/// One file's imported names and every path it writes, before resolution.
#[derive(Default)]
struct PathReader {
    /// Local name to the fully-qualified path it stands for.
    aliases: BTreeMap<String, String>,
    /// Paths exactly as written; the head segment may still be an alias.
    written: Vec<String>,
    /// Prefixes of glob imports (`use std::*;` records `std`), against which a
    /// bare child path may later be resolved.
    globs: Vec<String>,
}

impl PathReader {
    /// Flatten one `use` tree, recording both the path it imports and the local
    /// name it binds.
    ///
    /// Both halves matter. The imported path is direct evidence of a capability
    /// even when the import is never used, and the bound name is what lets a
    /// later `network::TcpStream` be traced back to `std::net`.
    fn read_use_tree(&mut self, tree: &UseTree, prefix: &mut Vec<String>) {
        match tree {
            UseTree::Path(segment) => {
                prefix.push(segment.ident.to_string());
                self.read_use_tree(&segment.tree, prefix);
                prefix.pop();
            }
            UseTree::Name(name) => {
                let local = name.ident.to_string();
                let full = joined(prefix, &local);
                self.bind(&local, &full);
                self.written.push(full);
            }
            UseTree::Rename(rename) => {
                let full = joined(prefix, &rename.ident.to_string());
                self.bind(&rename.rename.to_string(), &full);
                self.written.push(full);
            }
            UseTree::Glob(_) => {
                // Which names a glob imports cannot be known without reading the
                // imported module, so the glob's own prefix is the evidence, and
                // is also recorded so a later bare child path can be resolved
                // through it.
                let prefix = prefix.join("::");
                self.written.push(prefix.clone());
                self.globs.push(prefix);
            }
            UseTree::Group(group) => {
                for item in &group.items {
                    self.read_use_tree(item, prefix);
                }
            }
        }
    }

    fn bind(&mut self, local: &str, full: &str) {
        // `use std::io::Write as _;` imports the trait without naming it.
        if local != "_" {
            self.aliases.insert(local.to_owned(), full.to_owned());
        }
    }

    /// Read a macro body, which the parser hands over as unstructured tokens.
    ///
    /// Re-parsing as Rust keeps string literals literal, so a diagnostic that
    /// merely names a path is not mistaken for a use of it. Where the body is
    /// not Rust — a `macro_rules!` definition, or any custom syntax — the
    /// lexical fallback in `macro_tokens` over-reports rather than letting a
    /// capability hide behind a delimiter.
    fn read_macro_tokens(&mut self, node: &Macro) {
        if let Ok(arguments) = node.parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated)
        {
            for argument in &arguments {
                self.visit_expr(argument);
            }
            return;
        }

        if let Ok(statements) = node.parse_body_with(Block::parse_within) {
            for statement in &statements {
                self.visit_stmt(statement);
            }
            return;
        }

        self.written
            .extend(macro_tokens::path_like_sequences(&node.tokens.to_string()));
    }
}

impl<'ast> Visit<'ast> for PathReader {
    fn visit_item_use(&mut self, node: &'ast ItemUse) {
        self.read_use_tree(&node.tree, &mut Vec::new());
    }

    fn visit_item_extern_crate(&mut self, node: &'ast ItemExternCrate) {
        let name = node.ident.to_string();
        if let Some((_, rename)) = &node.rename {
            self.bind(&rename.to_string(), &name);
        }
        self.written.push(name);
    }

    fn visit_path(&mut self, node: &'ast Path) {
        self.written.push(
            node.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::"),
        );
        // Generic arguments carry paths of their own, as in `Vec<std::net::Ipv4Addr>`.
        visit::visit_path(self, node);
    }

    fn visit_stmt(&mut self, node: &'ast Stmt) {
        visit::visit_stmt(self, node);
    }

    fn visit_macro(&mut self, node: &'ast Macro) {
        visit::visit_macro(self, node);
        self.read_macro_tokens(node);
    }
}

/// Rewrite a written path's head segment through the file's alias bindings.
fn resolve(written: &str, aliases: &BTreeMap<String, String>) -> String {
    let (head, rest) = match written.split_once("::") {
        Some((head, rest)) => (head, Some(rest)),
        None => (written, None),
    };

    let Some(bound) = aliases.get(head) else {
        return written.to_owned();
    };

    match rest {
        Some(rest) => format!("{bound}::{rest}"),
        None => bound.clone(),
    }
}

fn joined(prefix: &[String], last: &str) -> String {
    if prefix.is_empty() {
        last.to_owned()
    } else {
        format!("{}::{last}", prefix.join("::"))
    }
}
