//! Exhaustive runtime assertions for active generated tagged-field ownership.
//!
//! This renderer owns the claim census and nothing about message codecs. Each
//! assertion constructs the invalid dual representation and directly exercises
//! the crate-private ownership phase that ordinary decode paths cannot reach.

use kafka_wire_schema::{Field, Message};

use crate::{
    GenerationError, group::ApiGroup, provenance::generated_banner, source::MessageSource,
};

use super::{api::declared_structs, field, invariant, tag_plan::known_tag_plans, text::RustText};

struct TagClaim {
    rust_type: String,
    owner: String,
    version: i16,
    tag: u32,
}

pub(crate) fn render_tag_claims(
    groups: &[ApiGroup],
    unkeyed: &[MessageSource],
    commit: &str,
) -> Result<String, GenerationError> {
    let mut claims = Vec::new();
    for source in groups
        .iter()
        .flat_map(ApiGroup::messages)
        .chain(unkeyed.iter())
    {
        collect_message_claims(&source.message, &mut claims)?;
    }
    claims.sort_unstable_by(|left, right| {
        (&left.rust_type, left.version, left.tag).cmp(&(&right.rust_type, right.version, right.tag))
    });

    let mut rust = RustText::default();
    rust.line(generated_banner());
    rust.line("//!");
    rust.line("//! Runtime ownership assertions for every known tagged field at");
    rust.line(format!("//! Apache Kafka commit {commit}."));
    rust.blank();
    if claims.is_empty() {
        rust.open("pub(super) fn assert_all_active_tag_claims()");
        rust.close("");
        return Ok(rust.finish());
    }
    rust.line("use kafka_wire_core::{ApiVersion, Bytes, EncodeError, TaggedField, TaggedFields};");
    rust.blank();
    render_helpers(&mut rust);
    rust.open("pub(super) fn assert_all_active_tag_claims()");
    let owners = claims
        .chunk_by(|left, right| left.rust_type == right.rust_type)
        .collect::<Vec<_>>();
    for index in 0..owners.len() {
        rust.line(format!("assert_claim_group_{index}();"));
    }
    rust.close("");
    for (index, owner) in owners.into_iter().enumerate() {
        rust.blank();
        rust.line(format!("// {}", owner[0].rust_type));
        rust.open(format!("fn assert_claim_group_{index}()"));
        for claim in owner {
            render_claim(&mut rust, claim);
        }
        rust.close("");
    }
    Ok(rust.finish())
}

fn render_claim(rust: &mut RustText, claim: &TagClaim) {
    rust.open(format!("let value = {}", claim.rust_type));
    rust.line(format!(
        "unknown_tagged_fields: retained_tag({}),",
        claim.tag
    ));
    rust.line("..Default::default()");
    rust.close(";");
    rust.line("assert_claim(");
    rust.line(format!(
        "    &value.validate_known_tag_ownership(ApiVersion::new({})),",
        claim.version
    ));
    rust.line(format!(
        "    ({:?}, ApiVersion::new({}), {}),",
        claim.owner, claim.version, claim.tag
    ));
    rust.line(");");
}

fn collect_message_claims(
    message: &Message,
    claims: &mut Vec<TagClaim>,
) -> Result<(), GenerationError> {
    field::validate_supported(message)?;
    let rust_type = format!("crate::{}", message.name.rust_type());
    collect_owner_claims(
        &rust_type,
        message.name.protocol(),
        &message.fields,
        message,
        claims,
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
        collect_owner_claims(
            &rust_type,
            declaration.name.rust_type(),
            declaration.fields,
            &context,
            claims,
        )?;
    }
    Ok(())
}

fn collect_owner_claims(
    rust_type: &str,
    owner: &str,
    fields: &[Field],
    message: &Message,
    claims: &mut Vec<TagClaim>,
) -> Result<(), GenerationError> {
    for plan in known_tag_plans(fields, message) {
        let (version, _) =
            invariant::bounded(message, plan.active_versions(), "known tag versions")?;
        claims.push(TagClaim {
            rust_type: rust_type.to_owned(),
            owner: owner.to_owned(),
            version,
            tag: plan.tag(),
        });
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
        "fn assert_claim(\
         outcome: &Result<(), EncodeError>, expected: (&'static str, ApiVersion, u32))",
    );
    rust.line("let (owner, version, tag) = expected;");
    rust.line("assert_eq!(");
    rust.line("    outcome,");
    rust.line("    &Err(EncodeError::KnownTagConflict { message: owner, tag, version }),");
    rust.line("    \"{owner} did not validate active tag {tag} in version {version}\",");
    rust.line(");");
    rust.close("");
    rust.blank();
}
