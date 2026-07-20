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

fn is_identifier_character(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

/// Marks every bare protocol identifier inside one whitespace-delimited token.
///
/// Walks the token's runs of identifier characters rather than the single span
/// between its first and last one. Upstream writes `0=NOT_REQUESTED` to
/// enumerate a status, and a span-based reading takes the whole thing —
/// punctuation included — decides it is not a plain identifier, and leaves
/// `NOT_REQUESTED` bare for the lints on checked-in output to reject.
fn mark_protocol_identifier(token: &str) -> String {
    if token.contains('`') {
        return token.to_owned();
    }
    let mut marked = String::with_capacity(token.len());
    let mut rest = token;
    while !rest.is_empty() {
        let Some(start) = rest.find(is_identifier_character) else {
            marked.push_str(rest);
            break;
        };
        marked.push_str(&rest[..start]);
        let tail = &rest[start..];
        let end = tail
            .find(|character| !is_identifier_character(character))
            .unwrap_or(tail.len());
        let (run, remainder) = tail.split_at(end);
        if is_protocol_identifier(run) {
            marked.push('`');
            marked.push_str(run);
            marked.push('`');
        } else {
            marked.push_str(run);
        }
        rest = remainder;
    }
    marked
}

/// Whether one unbroken run of identifier characters names a protocol item.
fn is_protocol_identifier(run: &str) -> bool {
    // An underscored word is an item too, in either case: rustdoc's lint reads
    // READ_UNCOMMITTED and isolation_level alike as identifiers, and upstream
    // prose names both. A run of digits joined by underscores is not one.
    if run.contains('_') && run.chars().any(char::is_alphabetic) {
        return true;
    }
    let mut characters = run.chars();
    let _ = characters.next();
    let has_internal_uppercase = characters.any(char::is_uppercase);
    has_internal_uppercase && run.chars().any(char::is_lowercase)
}
