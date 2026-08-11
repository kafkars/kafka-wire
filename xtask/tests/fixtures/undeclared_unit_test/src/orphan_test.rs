//! Scenario: a sibling unit test no facade declares.
//!
//! Nothing compiles this file, so the assertion below never runs. It is
//! deliberately false: if this fixture were ever wired into a build, the
//! failure would be loud.

#[test]
fn never_runs_because_nothing_declares_this_file() {
    assert_eq!(1, 2, "this file is never compiled");
}
