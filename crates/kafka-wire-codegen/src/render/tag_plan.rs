//! Typed rendering plan for one owner's known tagged fields.
//!
//! This module derives tag identity, field binding, and active versions once
//! from validated IR. Individual emitters consume the plan but own their Rust.

use kafka_wire_schema::{Field, Message, VersionSet};

use super::field;

/// One known tagged field as every generated phase must understand it.
#[derive(Debug)]
pub(crate) struct KnownTagPlan<'a> {
    tag: u32,
    field_index: usize,
    active_versions: VersionSet,
    field: &'a Field,
}

impl<'a> KnownTagPlan<'a> {
    pub(crate) fn tag(&self) -> u32 {
        self.tag
    }

    pub(crate) fn field_index(&self) -> usize {
        self.field_index
    }

    pub(crate) fn active_versions(&self) -> &VersionSet {
        &self.active_versions
    }

    pub(crate) fn field(&self) -> &'a Field {
        self.field
    }

    /// Version gate when the phase can run across the whole owner range.
    pub(crate) fn owner_condition(&self, message: &Message) -> Option<String> {
        (self.active_versions != message.valid_versions)
            .then(|| field::condition_for(&self.active_versions, message))
    }

    /// Version gate inside the owner's already-established flexible section.
    pub(crate) fn section_condition(&self, message: &Message) -> Option<String> {
        let flexible = message
            .effective_flexible_versions()
            .intersection(&message.valid_versions);
        (!flexible.is_subset_of(&self.active_versions))
            .then(|| field::condition_for(&self.active_versions, message))
    }
}

/// Derives one ascending plan from the normalized tag declarations.
pub(crate) fn known_tag_plans<'a>(fields: &'a [Field], message: &Message) -> Vec<KnownTagPlan<'a>> {
    let mut plans = fields
        .iter()
        .enumerate()
        .filter_map(|(field_index, field)| {
            field.tag.map(|tag| KnownTagPlan {
                tag,
                field_index,
                active_versions: field.tagged_versions.intersection(&message.valid_versions),
                field,
            })
        })
        .collect::<Vec<_>>();
    plans.sort_unstable_by_key(KnownTagPlan::tag);
    plans
}
