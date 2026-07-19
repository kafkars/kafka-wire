//! Protocol prose turned into rustdoc.
//!
//! Upstream `about` text is written for a spec reader, not for rustdoc. This
//! file owns the transformations applied to it: terminating the sentence,
//! marking bare protocol identifiers as code so they render as such, and
//! escaping the punctuation rustdoc would otherwise read as markup. It
//! deliberately owns no emission of its own.

/// Renders one upstream `about` string as a rustdoc sentence.
pub(super) fn sentence(source: &str) -> String {
    let source = source
        .split_whitespace()
        .map(mark_protocol_identifier)
        .collect::<Vec<_>>()
        .join(" ");
    let source = escape_link_brackets(&source);
    if source.ends_with('.') || source.ends_with('!') || source.ends_with('?') {
        source
    } else {
        format!("{source}.")
    }
}

/// Escapes square brackets that rustdoc would read as an intra-doc link.
///
/// Upstream prose indexes things: `GetTelemetrySubscriptionsRequest` explains
/// its field by writing `Array[0] empty string`, and rustdoc reads `[0]` as a
/// link to an item named `0`, which does not exist and which the workspace's
/// `-D warnings` turns into a failed build.
///
/// Run after identifier marking and skipped inside code spans, because a marked
/// identifier is already protected by its backticks and a backslash there would
/// render literally.
fn escape_link_brackets(source: &str) -> String {
    let mut escaped = String::with_capacity(source.len());
    let mut in_code_span = false;
    for character in source.chars() {
        match character {
            '`' => in_code_span = !in_code_span,
            '[' | ']' if !in_code_span => escaped.push('\\'),
            _ => {}
        }
        escaped.push(character);
    }
    escaped
}

fn mark_protocol_identifier(token: &str) -> String {
    if token.contains('`') {
        return token.to_owned();
    }
    let is_identifier = |character: char| character.is_alphanumeric() || character == '_';
    let Some(start) = token.find(is_identifier) else {
        return token.to_owned();
    };
    let Some(last) = token.rfind(is_identifier) else {
        return token.to_owned();
    };
    let end = last + token[last..].chars().next().map_or(0, char::len_utf8);
    let identifier = &token[start..end];
    let mut characters = identifier.chars();
    let _ = characters.next();
    let has_internal_uppercase = characters.any(char::is_uppercase);
    let has_lowercase = identifier.chars().any(char::is_lowercase);
    let is_plain_identifier = identifier.chars().all(is_identifier);
    // An underscored word is an item too, in either case: rustdoc's lint reads
    // READ_UNCOMMITTED and isolation_level alike as identifiers, and upstream
    // prose names both.
    let is_snake_cased = identifier.contains('_') && identifier.chars().any(char::is_alphabetic);
    if is_snake_cased && is_plain_identifier {
        return format!("{}`{identifier}`{}", &token[..start], &token[end..]);
    }
    if has_internal_uppercase && has_lowercase && is_plain_identifier {
        format!("{}`{identifier}`{}", &token[..start], &token[end..])
    } else {
        token.to_owned()
    }
}
