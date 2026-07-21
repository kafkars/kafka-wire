//! Static reflection descriptor rendering for one generated message.

use kafka_wire_schema::{Message, MessageKind};

use crate::{
    GenerationError,
    render::{invariant, text::RustText},
};

pub(super) fn render_descriptor(
    rust: &mut RustText,
    message: &Message,
    api_key: i16,
) -> Result<(), GenerationError> {
    // Grouping already rejected every kind without a direction and a key, so
    // this is a totality guard rather than a policy decision.
    let direction = match message.kind {
        MessageKind::Request => "Request",
        MessageKind::Response => "Response",
        MessageKind::Header | MessageKind::Data => return Ok(()),
    };
    let constant = descriptor_name(message);
    let (start, end) = invariant::bounded(message, &message.valid_versions, "valid versions")?;
    rust.line(format!(
        "/// Static metadata for [`{}`].",
        message.name.rust_type()
    ));
    rust.line(format!(
        "pub const {constant}: MessageDescriptor = MessageDescriptor::new("
    ));
    rust.line(format!("    {api_key},"));
    rust.line(format!("    {:?},", message.name.protocol()));
    rust.line(format!("    MessageDirection::{direction},"));
    rust.line(format!("    VersionRange::new({start}, {end}),"));
    rust.line(format!(
        "    {},",
        option_range(message, &message.effective_flexible_versions())?
    ));
    rust.line(");");
    rust.blank();
    Ok(())
}

pub(crate) fn descriptor_name(message: &Message) -> String {
    format!("{}_DESCRIPTOR", message.name.descriptor_symbol())
}

fn option_range(
    message: &Message,
    versions: &kafka_wire_schema::VersionSet,
) -> Result<String, GenerationError> {
    Ok(
        invariant::optional_bounded(message, versions, "effective flexible versions")?.map_or_else(
            || "None".to_owned(),
            |(start, end)| format!("Some(VersionRange::new({start}, {end}))"),
        ),
    )
}
