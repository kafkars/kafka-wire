//! Static protocol registry rendering.

use crate::{group::ApiGroup, provenance::generated_banner};

use super::{
    api::{api_descriptor_name, descriptor_name},
    text::RustText,
};

pub(crate) fn render_registry(groups: &[ApiGroup], commit: &str) -> String {
    let mut rust = RustText::default();
    rust.line(generated_banner());
    rust.line("//!");
    rust.line("//! Static message registry for Apache Kafka commit");
    rust.line(format!("//! `{commit}`."));
    rust.blank();
    rust.line("use crate::{ApiDescriptor, MessageDescriptor};");
    rust.blank();

    // Each descriptor is named where it is used rather than imported first. The
    // import list was a third of this file — 92 lines naming every constant
    // twice — and bought nothing: the table below reads the same either way, and
    // `super::` says where a descriptor comes from at the point a reader asks.
    rust.line(
        "/// All messages emitted by this pinned protocol slice, sorted by API key and direction.",
    );
    rust.line("pub const MESSAGE_DESCRIPTORS: &[MessageDescriptor] = &[");
    for source in groups.iter().flat_map(ApiGroup::messages) {
        rust.line(format!("    super::{},", descriptor_name(&source.message)));
    }
    rust.line("];");
    rust.blank();
    rust.line("/// All validated API pairs, sorted by API key.");
    rust.line("pub const API_DESCRIPTORS: &[ApiDescriptor] = &[");
    for group in groups {
        rust.line(format!("    super::{},", api_descriptor_name(group)));
    }
    rust.line("];");
    rust.finish()
}
