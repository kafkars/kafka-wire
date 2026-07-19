//! Version predicates emitted into generated encode and decode implementations.

use kafka_wire_schema::{Field, Message, VersionSet};

pub(crate) fn presence_condition(field: &Field, message: &Message) -> Option<String> {
    let effective = field.versions.intersection(&message.valid_versions);
    if effective == message.valid_versions {
        None
    } else {
        Some(render_condition(&effective, message))
    }
}

/// The predicate that is true exactly where the field is absent.
///
/// Built from the version bounds rather than by wrapping `presence_condition`
/// in `!(...)`: the negation of `>=` is `<`, and emitting the double negative
/// produces a non-minimal boolean that the lints on checked-in output reject.
pub(crate) fn absence_condition(field: &Field, message: &Message) -> Option<String> {
    let effective = field.versions.intersection(&message.valid_versions);
    if effective == message.valid_versions {
        return None;
    }
    let valid = message.valid_versions.single_bounded();
    let ranges = effective.ranges();
    if let (Some((valid_start, valid_end)), [range]) = (valid, ranges) {
        let start = range.start();
        let end = range.end().unwrap_or(valid_end);
        if start == valid_start && end < valid_end {
            return Some(format!("version.value() > {end}"));
        }
        if start > valid_start && end == valid_end {
            return Some(format!("version.value() < {start}"));
        }
        if start == end {
            return Some(format!("version.value() != {start}"));
        }
        return Some(format!(
            "version.value() < {start} || version.value() > {end}"
        ));
    }
    // A disjoint presence set has no single-interval complement worth spelling
    // out, so the general negation stands.
    Some(format!("!({})", render_condition(&effective, message)))
}

fn render_condition(versions: &VersionSet, message: &Message) -> String {
    let valid = message.valid_versions.single_bounded();
    let ranges = versions.ranges();
    if let (Some((valid_start, valid_end)), [range]) = (valid, ranges) {
        let start = range.start();
        let end = range.end().unwrap_or(valid_end);
        if start == valid_start && end < valid_end {
            return format!("version.value() <= {end}");
        }
        if start > valid_start && end == valid_end {
            return format!("version.value() >= {start}");
        }
        if start == end {
            return format!("version.value() == {start}");
        }
        return format!("version.value() >= {start} && version.value() <= {end}");
    }

    ranges
        .iter()
        .map(|range| match range.end() {
            Some(end) if end == range.start() => format!("version.value() == {}", range.start()),
            Some(end) => format!(
                "(version.value() >= {} && version.value() <= {end})",
                range.start()
            ),
            None => format!("version.value() >= {}", range.start()),
        })
        .collect::<Vec<_>>()
        .join(" || ")
}
