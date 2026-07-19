//! Static protocol registry rendering.

use crate::{group::ApiGroup, provenance::generated_banner};

use super::{api::descriptor_name, text::RustText};

pub(crate) fn render_registry(groups: &[ApiGroup], commit: &str) -> String {
    let mut rust = RustText::default();
    rust.line(generated_banner());
    rust.line("//!");
    rust.line("//! Static message registry for Apache Kafka commit");
    rust.line(format!("//! `{commit}`."));
    rust.blank();
    rust.line("use crate::MessageDescriptor;");
    rust.blank();

    let constants = groups
        .iter()
        .flat_map(ApiGroup::messages)
        .map(|source| descriptor_name(&source.message))
        .collect::<Vec<_>>();
    let mut imports = constants.clone();
    imports.sort_unstable();
    // Brace collapsing and line breaking are rustfmt's, not the emitter's.
    rust.line(format!("use super::{{{}}};", imports.join(", ")));
    rust.blank();

    rust.line(
        "/// All messages emitted by this pinned protocol slice, sorted by API key and direction.",
    );
    rust.line("pub const MESSAGE_DESCRIPTORS: &[MessageDescriptor] = &[");
    for constant in constants {
        rust.line(format!("    {constant},"));
    }
    rust.line("];");
    rust.finish()
}
