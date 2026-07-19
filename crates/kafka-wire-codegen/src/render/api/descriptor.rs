//! Static reflection descriptor rendering for one generated message.

use kafka_wire_schema::{Message, MessageKind};

use crate::render::text::RustText;

pub(super) fn render_descriptor(rust: &mut RustText, message: &Message) {
    let direction = match message.kind {
        MessageKind::Request => "Request",
        MessageKind::Response => "Response",
    };
    let constant = descriptor_name(message);
    let (start, end) = message.valid_versions.single_bounded().unwrap_or((0, 0));
    rust.line(format!(
        "/// Static metadata for [`{}`].",
        message.name.rust_type()
    ));
    rust.line(format!(
        "pub const {constant}: MessageDescriptor = MessageDescriptor::new("
    ));
    rust.line(format!("    {},", message.api_key));
    rust.line(format!("    {:?},", message.name.protocol()));
    rust.line(format!("    MessageDirection::{direction},"));
    rust.line(format!("    VersionRange::new({start}, {end}),"));
    rust.line(format!(
        "    {},",
        option_range(&message.effective_flexible_versions())
    ));
    rust.line(");");
    rust.blank();
}

pub(crate) fn descriptor_name(message: &Message) -> String {
    format!(
        "{}_DESCRIPTOR",
        message.name.rust_module().to_ascii_uppercase()
    )
}

fn option_range(versions: &kafka_wire_schema::VersionSet) -> String {
    versions.single_bounded().map_or_else(
        || "None".to_owned(),
        |(start, end)| format!("Some(VersionRange::new({start}, {end}))"),
    )
}
