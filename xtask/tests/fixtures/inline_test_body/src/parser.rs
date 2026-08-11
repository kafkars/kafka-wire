//! A fixture production module carrying both banned constructs.
//!
//! It embeds a test body and reaches for a placeholder macro, so the hygiene
//! test must report it twice for two distinct reasons.

pub fn parse_version(source: &str) -> u16 {
    let _ = source;
    todo!("version parsing is not written yet")
}

#[cfg(test)]
mod tests {
    use super::parse_version;

    #[test]
    fn parses_a_version() {
        assert_eq!(parse_version("3"), 3);
    }
}
