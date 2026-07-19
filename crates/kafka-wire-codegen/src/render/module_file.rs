//! Generated facade rendering for all API-pair modules.

use crate::{group::ApiGroup, provenance::generated_banner};

use super::{api::descriptor_name, text::RustText};

pub(crate) fn render_module_file(groups: &[ApiGroup], commit: &str) -> String {
    let mut rust = RustText::default();
    rust.line(generated_banner());
    rust.line("//!");
    rust.line("//! Generated module facade for Apache Kafka commit");
    rust.line(format!("//! `{commit}`."));
    rust.blank();

    let mut modules = groups
        .iter()
        .map(|group| group.module_name.as_str())
        .collect::<Vec<_>>();
    modules.push("registry");
    modules.sort_unstable();
    for module in modules {
        rust.line(format!("mod {module};"));
    }
    rust.blank();

    let mut exports = groups
        .iter()
        .map(|group| {
            let mut items = group
                .messages()
                .flat_map(|source| {
                    [
                        descriptor_name(&source.message),
                        source.message.name.rust_type().to_owned(),
                    ]
                })
                .collect::<Vec<_>>();
            items.sort_unstable();
            (group.module_name.clone(), items)
        })
        .collect::<Vec<_>>();
    exports.push((
        "registry".to_owned(),
        vec!["MESSAGE_DESCRIPTORS".to_owned()],
    ));
    exports.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    for (module, items) in exports {
        // Brace collapsing and line breaking are rustfmt's, not the emitter's.
        rust.line(format!("pub use {module}::{{{}}};", items.join(", ")));
    }
    rust.finish()
}
