//! Lexical fallback for macro token streams that do not parse as Rust.
//!
//! This module owns the last resort the path resolver reaches for when a macro
//! body is a `macro_rules!` definition or any custom syntax: scanning the
//! rendered tokens for `ident::ident` sequences that could name a capability.
//! It treats string and character literals as opaque, so a path merely printed
//! inside a literal is not mistaken for one that is used.
//!
//! It deliberately owns no AST knowledge and no policy. A body that does parse
//! as Rust is resolved structurally by `paths`, never here; this scanner exists
//! precisely because that structural read is unavailable.

/// Extract `ident::ident` sequences from macro tokens that are not Rust.
pub(crate) fn path_like_sequences(tokens: &str) -> Vec<String> {
    let text = join_path_separators(&without_literals(tokens));

    text.split(|character: char| {
        !(character.is_alphanumeric() || character == '_' || character == ':')
    })
    .map(|chunk| chunk.trim_matches(':'))
    .filter(|chunk| !chunk.is_empty() && !chunk.starts_with(char::is_numeric))
    .map(str::to_owned)
    .collect()
}

/// Collapse whitespace around `::` so rendered tokens read as one path again.
///
/// A token stream prints `std :: net :: TcpStream`; the separators must be
/// rejoined before the text can be split into candidate paths.
fn join_path_separators(tokens: &str) -> String {
    tokens
        .replace(" :: ", "::")
        .replace(":: ", "::")
        .replace(" ::", "::")
}

/// Blank out string and character literals before scanning unparsed tokens.
///
/// A character literal must be consumed as a unit: a naive stripper that only
/// knew `"` as a delimiter treated the quote inside `'"'` as the start of a
/// string and blanked every real path that followed it. Lifetimes such as `'a`
/// share the leading quote but have no closing one, so they are left in place.
fn without_literals(tokens: &str) -> String {
    let characters = tokens.chars().collect::<Vec<_>>();
    let mut kept = String::with_capacity(tokens.len());
    let mut index = 0;

    while let Some(&character) = characters.get(index) {
        if character == '"' {
            kept.push(' ');
            index = end_of_string(&characters, index);
            continue;
        }
        if character == '\'' {
            if let Some(after) = end_of_char_literal(&characters, index) {
                kept.push(' ');
                index = after;
                continue;
            }
        }
        kept.push(character);
        index += 1;
    }

    kept
}

/// Index just past the closing `"` of the string literal opening at `start`.
fn end_of_string(characters: &[char], start: usize) -> usize {
    let mut index = start + 1;
    let mut escaped = false;
    while let Some(&character) = characters.get(index) {
        index += 1;
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '"' {
            break;
        }
    }
    index
}

/// Index just past a character literal opening at `start`, or `None` when the
/// quote begins a lifetime or label rather than a literal.
fn end_of_char_literal(characters: &[char], start: usize) -> Option<usize> {
    match characters.get(start + 1)? {
        // An escape (`'\n'`, `'\''`, `'\x41'`, `'\u{7d}'`): the escaped content
        // always follows the backslash, then the literal runs to its closing
        // quote.
        '\\' => {
            let mut index = start + 3;
            while *characters.get(index)? != '\'' {
                index += 1;
            }
            Some(index + 1)
        }
        // An empty `''` is not a literal; a single char followed by a closing
        // quote is `'x'`; anything else is a lifetime like `'static`.
        '\'' => None,
        _ if characters.get(start + 2) == Some(&'\'') => Some(start + 3),
        _ => None,
    }
}
