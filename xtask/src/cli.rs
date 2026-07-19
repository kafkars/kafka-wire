//! Minimal dependency-free command parsing.

/// Supported repository maintenance command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Command {
    /// Refresh the vendored upstream corpus for the pinned commit.
    ///
    /// This is the only command that reaches the network, and it is run by a
    /// human on purpose. Building, testing, and generating stay offline.
    Vendor,
    /// Regenerate checked-in protocol Rust.
    Generate,
    /// Verify generated Rust without writing.
    GeneratedCheck,
    /// Render every loadable schema into a scratch crate and compile it.
    GenerateAll(CorpusMode),
    /// Run generation verification and architecture guards.
    Verify,
    /// Author or verify the broker-authored byte-vector corpus.
    Vectors(VectorsMode),
    /// Print the pinned source and command map.
    Doctor,
}

/// How far `generate-all` is allowed to go.
///
/// Named rather than assumed because the two answers differ in what they touch:
/// the probe writes only under `target/`, while rendering the whole corpus into
/// the checked-in tree is a decision no one has made yet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CorpusMode {
    /// Render into `target/` and run `cargo check`. Nothing checked in changes.
    CheckOnly,
}

/// Which half of the vector corpus command to run.
///
/// The two halves have different capabilities, so they are named rather than
/// passed as a boolean: refresh reaches a Java toolchain, check never does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VectorsMode {
    /// Re-ask Apache Kafka's own writer for every vector. Needs the pinned jar.
    Refresh,
    /// Verify the checked-in corpus in pure Rust. This is what CI runs.
    Check,
}

impl Command {
    pub(crate) fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut arguments = arguments.into_iter();
        let Some(command) = arguments.next() else {
            return Err(usage());
        };
        let option = arguments.next();
        if arguments.next().is_some() {
            return Err(format!("unexpected extra arguments\n\n{}", usage()));
        }

        if command.as_str() == "generate-all" {
            return match option.as_deref() {
                Some("--check-only") => Ok(Self::GenerateAll(CorpusMode::CheckOnly)),
                _ => Err(format!(
                    "`generate-all` needs --check-only; rendering the whole corpus \
                     into the checked-in tree is not a decision this command makes\n\n{}",
                    usage()
                )),
            };
        }
        if command.as_str() == "vectors" {
            return match option.as_deref() {
                Some("--refresh") => Ok(Self::Vectors(VectorsMode::Refresh)),
                Some("--check") => Ok(Self::Vectors(VectorsMode::Check)),
                _ => Err(format!(
                    "`vectors` needs --refresh or --check\n\n{}",
                    usage()
                )),
            };
        }
        if option.is_some() {
            return Err(format!("unexpected extra arguments\n\n{}", usage()));
        }

        match command.as_str() {
            "vendor" => Ok(Self::Vendor),
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
        "  vendor             re-download the pinned upstream corpus (network)",
        "  generate           replace checked-in generated protocol files",
        "  generated-check    verify generated files without modifying them",
        "  generate-all --check-only",
        "                     render every loadable schema under target/ and compile it",
        "  verify             run generated-check and repository guards",
        "  vectors --check    verify the checked-in byte-vector corpus (offline)",
        "  vectors --refresh  re-author it from the pinned Kafka jar (needs Java)",
        "  doctor             print pinned inputs and common commands",
    ]
    .join("\n")
}
