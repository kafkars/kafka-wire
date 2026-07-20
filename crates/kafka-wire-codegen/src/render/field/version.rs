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

/// The presence gate a known tagged field needs inside its section.
///
/// A tagged field is only ever read or written where the tagged-field section
/// exists at all, so the universe here is the flexible window rather than every
/// supported version. `ApiVersionsResponse` declares all four of its tags across
/// exactly its flexible window, which needs no gate; `FetchRequest.ReplicaState`
/// arrives at v15 in a message flexible from v12, which does.
pub(crate) fn tagged_presence_condition(field: &Field, message: &Message) -> Option<String> {
    let present = field.versions.intersection(&message.valid_versions);
    let flexible = message
        .effective_flexible_versions()
        .intersection(&message.valid_versions);
    if flexible.is_subset_of(&present) {
        None
    } else {
        Some(render_condition(&present, message))
    }
}

/// The predicate that is true exactly where the field is absent.
///
/// Built from the version bounds rather than by wrapping `presence_condition`
/// in `!(...)`: the negation of `>=` is `<`, and emitting the double negative
/// produces a non-minimal boolean that the lints on checked-in output reject.
pub(crate) fn absence_condition(field: &Field, message: &Message) -> Option<String> {
    let effective = field.versions.intersection(&message.valid_versions);
    negated_condition(&effective, message)
}

/// The predicate true exactly where `versions` is true.
pub(crate) fn condition_for(versions: &VersionSet, message: &Message) -> String {
    render_condition(versions, message)
}

/// One rendered condition, parenthesized only if it needs to be to survive
/// being `&&`-ed to another.
///
/// `&&` binds tighter than `||`, so splicing a disjunction into a conjunction
/// silently reassociates it: `a || b && c` means `a || (b && c)`. A field
/// present in versions 8 through 10 of a 0-13 message renders exactly that
/// disjunction, and the guard built from it refused every version below 8
/// whatever the field actually held — a message that a real broker accepts,
/// rejected by this library, with a diagnostic naming the wrong cause.
///
/// Applied only where a condition is composed, and only when it contains a
/// disjunction, because the lints on checked-in output reject the redundant
/// parentheses that wrapping everything would produce.
pub(crate) fn as_conjunct(condition: &str) -> String {
    if condition.contains("||") {
        return format!("({condition})");
    }
    condition.to_owned()
}

/// The predicate true exactly where `versions` is false, or `None` when it
/// covers every supported version and so has no complement worth emitting.
///
/// Built from the version bounds for the same reason `absence_condition` is:
/// the negation of `>=` is `<`, and a rendered double negative is a non-minimal
/// boolean that the lints on checked-in output reject.
pub(crate) fn negated_condition(effective: &VersionSet, message: &Message) -> Option<String> {
    if *effective == message.valid_versions {
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
    Some(format!("!({})", render_condition(effective, message)))
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
