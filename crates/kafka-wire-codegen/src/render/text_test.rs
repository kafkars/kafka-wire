//! Source-containment and scope-balance scenarios for generated Rust text.
//!
//! Every physical prose line stays documented, while over-closing or leaving a
//! scope unfinished fails at the builder rather than later in rustfmt.

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

#[test]
#[should_panic(expected = "RustText::close cannot close a scope because none is open")]
fn close_rejects_an_unopened_scope() {
    RustText::default().close("");
}

#[test]
#[should_panic(expected = "RustText::reopen cannot close a scope because none is open")]
fn reopen_rejects_an_unopened_scope() {
    RustText::default().reopen("} else {");
}

#[test]
#[should_panic(expected = "RustText::finish found 1 unclosed scope(s)")]
fn finish_rejects_an_unclosed_scope() {
    let mut rust = RustText::default();
    rust.open("fn unfinished()");
    rust.finish();
}
