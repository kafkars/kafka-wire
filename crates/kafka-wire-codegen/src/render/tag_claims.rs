//! Exhaustive runtime assertions for active generated tagged-field ownership.
//!
//! This renderer owns the claim census and nothing about message codecs. Each
//! assertion publicly constructs the invalid dual representation that ordinary
//! same-version decode paths cannot produce.

use kafka_wire_schema::{Field, Message};

use crate::{
    GenerationError, group::ApiGroup, provenance::generated_banner, source::MessageSource,
};

use super::{api::declared_structs, field, invariant, text::RustText};

struct TagClaim {
    rust_type: String,
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
    rust.line(
        "use kafka_wire_core::{ApiVersion, Bytes, BytesMut, EncodeError, KafkaEncode, \
         TaggedField, TaggedFields, TaggedFieldsError};",
    );
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
    rust.line("assert_claim(");
    rust.line(format!("    {}::default(),", claim.rust_type));
    rust.line("    |value, fields| value.unknown_tagged_fields = fields,");
    rust.line(format!("    ApiVersion::new({}),", claim.version));
    rust.line(format!("    {},", claim.tag));
    rust.line(");");
}

fn collect_message_claims(
    message: &Message,
    claims: &mut Vec<TagClaim>,
) -> Result<(), GenerationError> {
    field::validate_supported(message)?;
    let rust_type = format!("kafka_wire::{}", message.name.rust_type());
    collect_owner_claims(&rust_type, &message.fields, message, claims)?;
    for declaration in declared_structs(message)? {
        let mut context = message.clone();
        context.valid_versions = declaration.versions.clone();
        context.flexible_versions = declaration.flexible_versions;
        let rust_type = format!(
            "kafka_wire::{}::{}",
            message.name.rust_module(),
            declaration.name.rust_type()
        );
        collect_owner_claims(&rust_type, declaration.fields, &context, claims)?;
    }
    Ok(())
}

fn collect_owner_claims(
    rust_type: &str,
    fields: &[Field],
    message: &Message,
    claims: &mut Vec<TagClaim>,
) -> Result<(), GenerationError> {
    for field in fields.iter().filter(|field| field.tag.is_some()) {
        let tag = field
            .tag
            .ok_or_else(|| GenerationError::InternalInvariant {
                message: message.name.protocol().to_owned(),
                invariant: format!("tagged field {} lost its tag", field.name.protocol()),
            })?;
        let active = field.versions.intersection(&message.valid_versions);
        let (version, _) = invariant::bounded(message, &active, "known tag versions")?;
        claims.push(TagClaim {
            rust_type: rust_type.to_owned(),
            version,
            tag,
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
        "fn assert_claim<T: KafkaEncode>(\
         mut value: T, assign: impl FnOnce(&mut T, TaggedFields), version: ApiVersion, tag: u32)",
    );
    rust.line("assign(&mut value, retained_tag(tag));");
    rust.line("let mut output = BytesMut::from(&b\"prior output\"[..]);");
    rust.line("let before = output.clone();");
    rust.line("let outcome = value.encode_into(&mut output, version);");
    rust.line("assert_eq!(");
    rust.line("    outcome,");
    rust.line("    Err(EncodeError::TaggedFieldsInvalid(TaggedFieldsError::Duplicate { tag })),");
    rust.line("    \"{} did not claim active tag {tag}\",");
    rust.line("    ::core::any::type_name::<T>(),");
    rust.line(");");
    rust.line("assert_eq!(output, before, \"tag conflict wrote partial output\");");
    rust.close("");
    rust.blank();
}
