//! Runtime assertions for every known tag that activates after flexibility begins.
//!
//! These assertions isolate the generated ownership phase, so unrelated field
//! defaults cannot hide whether a retained tag crosses its exact version edge.

use kafka_wire_schema::{Field, Message};

use crate::{
    GenerationError, group::ApiGroup, provenance::generated_banner, source::MessageSource,
};

use super::{api::declared_structs, field, invariant, tag_plan::known_tag_plans, text::RustText};

struct TagBoundary {
    rust_type: String,
    owner: String,
    before: i16,
    active: i16,
    tag: u32,
}

pub(crate) fn render_tag_boundaries(
    groups: &[ApiGroup],
    unkeyed: &[MessageSource],
    commit: &str,
) -> Result<String, GenerationError> {
    let mut boundaries = Vec::new();
    for source in groups
        .iter()
        .flat_map(ApiGroup::messages)
        .chain(unkeyed.iter())
    {
        collect_message_boundaries(&source.message, &mut boundaries)?;
    }
    boundaries.sort_unstable_by(|left, right| {
        (&left.rust_type, left.active, left.tag).cmp(&(&right.rust_type, right.active, right.tag))
    });

    let mut rust = RustText::default();
    rust.line(generated_banner());
    rust.line("//!");
    rust.line("//! Ownership boundaries for tags introduced after flexible encoding at");
    rust.line(format!("//! Apache Kafka commit {commit}."));
    rust.blank();
    if boundaries.is_empty() {
        rust.open("pub(super) fn assert_all_tag_activation_boundaries()");
        rust.close("");
        return Ok(rust.finish());
    }
    rust.line("use kafka_wire_core::{ApiVersion, Bytes, EncodeError, TaggedField, TaggedFields};");
    rust.blank();
    render_helpers(&mut rust);
    rust.open("pub(super) fn assert_all_tag_activation_boundaries()");
    let owners = boundaries
        .chunk_by(|left, right| left.rust_type == right.rust_type)
        .collect::<Vec<_>>();
    for index in 0..owners.len() {
        rust.line(format!("assert_boundary_group_{index}();"));
    }
    rust.close("");
    for (index, owner) in owners.into_iter().enumerate() {
        rust.blank();
        rust.line(format!("// {}", owner[0].rust_type));
        rust.open(format!("fn assert_boundary_group_{index}()"));
        for boundary in owner {
            render_boundary(&mut rust, boundary);
        }
        rust.close("");
    }
    Ok(rust.finish())
}

fn render_boundary(rust: &mut RustText, boundary: &TagBoundary) {
    rust.open(format!("let value = {}", boundary.rust_type));
    rust.line(format!(
        "unknown_tagged_fields: retained_tag({}),",
        boundary.tag
    ));
    rust.line("..Default::default()");
    rust.close(";");
    rust.line("assert_boundary(");
    rust.line(format!(
        "    &value.validate_known_tag_ownership(ApiVersion::new({})),",
        boundary.before
    ));
    rust.line(format!(
        "    &value.validate_known_tag_ownership(ApiVersion::new({})),",
        boundary.active
    ));
    rust.line(format!("    {:?},", boundary.owner));
    rust.line(format!("    ApiVersion::new({}),", boundary.before));
    rust.line(format!("    ApiVersion::new({}),", boundary.active));
    rust.line(format!("    {},", boundary.tag));
    rust.line(");");
}

fn collect_message_boundaries(
    message: &Message,
    boundaries: &mut Vec<TagBoundary>,
) -> Result<(), GenerationError> {
    field::validate_supported(message)?;
    let rust_type = format!("crate::{}", message.name.rust_type());
    collect_owner_boundaries(
        &rust_type,
        message.name.protocol(),
        &message.fields,
        message,
        boundaries,
    )?;
    for declaration in declared_structs(message)? {
        let mut context = message.clone();
        context.valid_versions = declaration.versions.clone();
        context.flexible_versions = declaration.flexible_versions;
        let rust_type = format!(
            "crate::{}::{}",
            message.name.rust_module(),
            declaration.name.rust_type()
        );
        collect_owner_boundaries(
            &rust_type,
            declaration.name.rust_type(),
            declaration.fields,
            &context,
            boundaries,
        )?;
    }
    Ok(())
}

fn collect_owner_boundaries(
    rust_type: &str,
    owner: &str,
    fields: &[Field],
    message: &Message,
    boundaries: &mut Vec<TagBoundary>,
) -> Result<(), GenerationError> {
    let flexible = message.effective_flexible_versions();
    let Some((flexible_start, _)) =
        invariant::optional_bounded(message, &flexible, "flexible tag boundary")?
    else {
        return Ok(());
    };
    for plan in known_tag_plans(fields, message) {
        let (active_start, _) =
            invariant::bounded(message, plan.active_versions(), "known tag versions")?;
        if active_start > flexible_start {
            boundaries.push(TagBoundary {
                rust_type: rust_type.to_owned(),
                owner: owner.to_owned(),
                before: active_start - 1,
                active: active_start,
                tag: plan.tag(),
            });
        }
    }
    Ok(())
}

fn render_helpers(rust: &mut RustText) {
    rust.open("fn retained_tag(tag: u32) -> TaggedFields");
    rust.line(
        "TaggedFields::from_sorted(vec![TaggedField::new(tag, Bytes::from_static(&[0xaa]))])",
    );
    rust.line("    .unwrap_or_else(|error| panic!(\"one retained tag must be valid: {error}\"))");
    rust.close("");
    rust.blank();
    rust.open(
        "fn assert_boundary(\
         before_outcome: &Result<(), EncodeError>, active_outcome: &Result<(), EncodeError>, \
         owner: &'static str, before: ApiVersion, active: ApiVersion, tag: u32)",
    );
    rust.line("assert_eq!(before_outcome, &Ok(()), \"{owner} claimed tag {tag} in {before}\");");
    rust.line("assert_eq!(");
    rust.line("    active_outcome,");
    rust.line("    &Err(EncodeError::KnownTagConflict {");
    rust.line("        message: owner,");
    rust.line("        tag,");
    rust.line("        version: active,");
    rust.line("    }),");
    rust.line("    \"{owner} did not claim tag {tag} at activation version {active}\",");
    rust.line(");");
    rust.close("");
    rust.blank();
}
