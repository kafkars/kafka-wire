//! Every command surface is reachable and every mistake is named.
//!
//! Scenario: parse each supported argument list and assert the command it
//! selects, then parse the plausible mistakes and assert the message says what
//! to do instead. The parser is hand-written and dependency-free, so nothing
//! else checks that a command added to the enum is actually reachable from the
//! command line.
//!
//! The capability-bearing commands matter most: `vendor` reaches the network
//! and `generate-all` needs an explicit `--check-only`. Neither may become
//! reachable by accident, and neither may stop being reachable in silence.

use crate::cli::{Command, CorpusMode, VectorsMode};

fn parse(arguments: &[&str]) -> Result<Command, String> {
    Command::parse(arguments.iter().map(|argument| (*argument).to_owned()))
}

fn rejection(arguments: &[&str]) -> String {
    parse(arguments)
        .err()
        .unwrap_or_else(|| panic!("`cargo xtask {}` was accepted", arguments.join(" ")))
}

#[test]
fn every_command_is_reachable_from_the_command_line() {
    for (arguments, expected) in [
        (vec!["vendor"], Command::Vendor),
        (vec!["generate"], Command::Generate),
        (vec!["generated-check"], Command::GeneratedCheck),
        (vec!["verify"], Command::Verify),
        (vec!["defaults"], Command::Defaults),
        (vec!["doctor"], Command::Doctor),
        (
            vec!["generate-all", "--check-only"],
            Command::GenerateAll(CorpusMode::CheckOnly),
        ),
        (
            vec!["vectors", "--check"],
            Command::Vectors(VectorsMode::Check),
        ),
        (
            vec!["vectors", "--refresh"],
            Command::Vectors(VectorsMode::Refresh),
        ),
    ] {
        assert_eq!(
            parse(&arguments),
            Ok(expected),
            "`cargo xtask {}` does not select its command",
            arguments.join(" ")
        );
    }
}

#[test]
fn no_arguments_prints_the_command_map() {
    let usage = rejection(&[]);

    for command in [
        "vendor",
        "generate",
        "generated-check",
        "generate-all --check-only",
        "verify",
        "vectors --check",
        "defaults",
        "doctor",
    ] {
        assert!(
            usage.contains(command),
            "the usage text omits `{command}`: {usage}"
        );
    }
}

#[test]
fn an_unknown_command_is_named_and_followed_by_the_command_map() {
    let error = rejection(&["genrate"]);

    assert!(
        error.contains("unknown command `genrate`"),
        "the refusal must quote what was typed: {error}"
    );
    assert!(
        error.contains("usage: cargo xtask"),
        "the refusal must show what is available instead: {error}"
    );
}

#[test]
fn generate_all_refuses_to_default_to_writing_the_checked_in_tree() {
    // The absent flag is the interesting case: `generate-all` on its own could
    // plausibly mean "render everything into the repository", which is a
    // decision about the protocol slice, not an invocation detail.
    for arguments in [vec!["generate-all"], vec!["generate-all", "--write"]] {
        let error = rejection(&arguments);
        assert!(
            error.contains("--check-only"),
            "`cargo xtask {}` must name the flag it needs: {error}",
            arguments.join(" ")
        );
    }
}

#[test]
fn a_flagless_command_rejects_a_flag_rather_than_ignoring_it() {
    // Silently discarding an option is how `generated-check --write` turns into
    // a command that quietly did not do what was asked.
    for arguments in [
        vec!["generated-check", "--write"],
        vec!["doctor", "--verbose"],
        vec!["vendor", "--offline"],
        vec!["defaults", "--refresh"],
    ] {
        let error = rejection(&arguments);
        assert!(
            error.contains("unexpected extra arguments"),
            "`cargo xtask {}` silently ignored an option: {error}",
            arguments.join(" ")
        );
    }
}

#[test]
fn a_third_argument_is_rejected_for_every_command() {
    for arguments in [
        vec!["generate-all", "--check-only", "extra"],
        vec!["vectors", "--check", "extra"],
        vec!["doctor", "extra", "more"],
    ] {
        let error = rejection(&arguments);
        assert!(
            error.contains("unexpected extra arguments"),
            "`cargo xtask {}` accepted a third argument: {error}",
            arguments.join(" ")
        );
    }
}
