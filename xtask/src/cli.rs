//! Minimal dependency-free command parsing.

/// Supported repository maintenance command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Command {
    /// Regenerate checked-in protocol Rust.
    Generate,
    /// Verify generated Rust without writing.
    GeneratedCheck,
    /// Run generation verification and architecture guards.
    Verify,
    /// Print the pinned source and command map.
    Doctor,
}

impl Command {
    pub(crate) fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut arguments = arguments.into_iter();
        let Some(command) = arguments.next() else {
            return Err(usage());
        };
        if arguments.next().is_some() {
            return Err(format!("unexpected extra arguments\n\n{}", usage()));
        }

        match command.as_str() {
            "generate" => Ok(Self::Generate),
            "generated-check" => Ok(Self::GeneratedCheck),
            "verify" => Ok(Self::Verify),
            "doctor" => Ok(Self::Doctor),
            _ => Err(format!("unknown command `{command}`\n\n{}", usage())),
        }
    }
}

fn usage() -> String {
    [
        "usage: cargo xtask <command>",
        "",
        "commands:",
        "  generate         replace checked-in generated protocol files",
        "  generated-check  verify generated files without modifying them",
        "  verify           run generated-check and repository guards",
        "  doctor           print pinned inputs and common commands",
    ]
    .join("\n")
}
