//! Protocol prose turned into rustdoc.
//!
//! Upstream `about` text is written for a spec reader, not for rustdoc. This
//! file owns the one transformation applied to it: terminating the sentence and
//! marking bare protocol identifiers as code so they render as such. It
//! deliberately owns no emission of its own.

/// Renders one upstream `about` string as a rustdoc sentence.
pub(super) fn sentence(source: &str) -> String {
    let source = source
        .split_whitespace()
        .map(mark_protocol_identifier)
        .collect::<Vec<_>>()
        .join(" ");
    if source.ends_with('.') || source.ends_with('!') || source.ends_with('?') {
        source
    } else {
        format!("{source}.")
    }
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
    if has_internal_uppercase && has_lowercase && is_plain_identifier {
        format!("{}`{identifier}`{}", &token[..start], &token[end..])
    } else {
        token.to_owned()
    }
}
