//! Doc-comment containment for source-controlled prose.
//!
//! Scenario: every Rust and Unicode line boundary starts another documented
//! line, so source text can never inject an attribute or item into generated
//! output.

use super::text::RustText;

#[test]
fn every_physical_prose_line_keeps_the_doc_prefix() {
    let mut rust = RustText::default();
    rust.doc_line("safe\r\n#[allow(unsafe_code)]\nfn escaped() {}\u{2028}pub mod escaped\u{2029}");

    assert_eq!(
        rust.finish(),
        "/// safe\n/// #[allow(unsafe_code)]\n/// fn escaped() {}\n/// pub mod escaped\n/// \n"
    );
}
