//! Generated facade rendering for all API-pair modules.

use crate::{group::ApiGroup, provenance::generated_banner, source::MessageSource};

use super::{
    api::{api_descriptor_name, descriptor_name},
    text::RustText,
};

pub(crate) fn render_module_file(
    groups: &[ApiGroup],
    unkeyed: &[MessageSource],
    commit: &str,
) -> String {
    let mut rust = RustText::default();
    rust.line(generated_banner());
    rust.line("//!");
    rust.line("//! Generated module facade for Apache Kafka commit");
    rust.line(format!("//! `{commit}`."));
    rust.blank();

    // `too_many_lines` asks a human to find a seam. A decode body is one `let`
    // per declared field, so its length is the message's field count and there
    // is no seam to find — `StreamsGroupHeartbeatRequest` crosses 100 lines by
    // having that many fields. Scope the exception to generated modules so the
    // lint keeps its force everywhere a human writes code, and emit it here so
    // it stays part of the reviewed, hashed output.
    rust.line("#![allow(clippy::too_many_lines)]");
    rust.blank();

    let mut modules = groups.iter().map(ApiGroup::module_name).collect::<Vec<_>>();
    modules.push("registry");
    modules.push("header_version");
    if !unkeyed.is_empty() {
        modules.push("framing");
    }
    modules.sort_unstable();
    for module in modules {
        rust.line(format!("mod {module};"));
    }
    rust.blank();

    for (module, items) in module_exports(groups, unkeyed) {
        // Brace collapsing and line breaking are rustfmt's, not the emitter's.
        rust.line(format!("pub use {module}::{{{}}};", items.join(", ")));
    }
    rust.finish()
}

/// Every name this facade re-exports, grouped by the module that declares it.
///
/// Shared with the crate-root export list rather than recomputed there. The two
/// files must name exactly the same set — the root list is what makes those
/// names public — and a second traversal is a second chance to disagree.
///
/// Each message contributes three names: its directional descriptor, its type,
/// and the module the module-scoped naming rule scopes its nested structs to. Each group adds its
/// pair descriptor. The message module has to be re-exported, not merely
/// emitted — a nested struct is reachable only through it now, and a `pub mod`
/// nothing re-exports is both unreachable to a caller and an `unreachable_pub`
/// warning on checked-in output.
pub(crate) fn module_exports(
    groups: &[ApiGroup],
    unkeyed: &[MessageSource],
) -> Vec<(String, Vec<String>)> {
    let mut exports = groups
        .iter()
        .map(|group| {
            let mut items = group
                .messages()
                .flat_map(|source| {
                    [
                        descriptor_name(&source.message),
                        source.message.name.rust_type().to_owned(),
                        source.message.name.rust_module().to_owned(),
                    ]
                })
                .collect::<Vec<_>>();
            items.push(api_descriptor_name(group));
            items.sort_unstable();
            (group.module_name().to_owned(), items)
        })
        .collect::<Vec<_>>();
    exports.push((
        "registry".to_owned(),
        vec![
            "API_DESCRIPTORS".to_owned(),
            "MESSAGE_DESCRIPTORS".to_owned(),
        ],
    ));
    exports.push((
        "header_version".to_owned(),
        vec![
            "request_header_version".to_owned(),
            "response_header_version".to_owned(),
        ],
    ));
    // A framing schema carries no descriptor, so it exports its type and the
    // module its own structs are scoped to, and nothing else.
    if !unkeyed.is_empty() {
        let mut items = unkeyed
            .iter()
            .flat_map(|source| {
                [
                    source.message.name.rust_type().to_owned(),
                    source.message.name.rust_module().to_owned(),
                ]
            })
            .collect::<Vec<_>>();
        items.sort_unstable();
        exports.push(("framing".to_owned(), items));
    }
    exports.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    exports
}
