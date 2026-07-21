//! A throwaway checkout holding exactly the inputs one generation needs.
//!
//! This module owns the synthetic pinned workspace the generation tests drive:
//! a lockfile it writes, vendored schemas it copies verbatim from the real
//! corpus, and the output directory it reads back. Real upstream bytes are used
//! on purpose — a determinism or transactionality claim proved against a
//! hand-written toy schema says nothing about the corpus actually compiled.
//!
//! It deliberately owns no assertion. What a generation run must be true of
//! belongs to the test file making the claim.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use kafka_wire_codegen::{GenerationError, GenerationMode, GeneratorConfig};
use sha2::{Digest, Sha256};

/// Commit the synthetic lockfile pins, and the directory the corpus lives in.
pub(crate) const COMMIT: &str = "678c0e07e4733c5a592e52046dc2c4e1625587f1";

/// One request and response pair the backend is known to render.
pub(crate) const SUPPORTED: [&str; 2] = ["SaslHandshakeRequest.json", "SaslHandshakeResponse.json"];

/// A schema outside the backend's slice, used to prove a refusal writes nothing.
///
/// It has been three different schemas as the backend grew: an array of structs,
/// then partial-version nullability, both of which are now emitted. This one is
/// a message upstream RETIRED — `validVersions: "none"` — so it is refused by
/// choice rather than by limitation, and unlike its predecessors it will not
/// stop being refused. Apache Kafka generates no code for it either.
pub(crate) const REFUSED: &str = "LeaderAndIsrRequest.json";

pub(crate) fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

pub(crate) fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

pub(crate) fn write(path: &Path, source: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .unwrap_or_else(|error| panic!("create {}: {error}", parent.display()));
    }
    fs::write(path, source).unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
}

pub(crate) fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut output = String::new();
    for byte in Sha256::digest(bytes) {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

/// A throwaway checkout holding exactly the inputs one generation needs.
pub(crate) struct Workspace {
    pub(crate) root: PathBuf,
}

impl Workspace {
    /// Pins `enabled` schemas, copied verbatim from the real vendored corpus.
    ///
    /// Copying real upstream bytes rather than authoring a fixture schema is
    /// deliberate: a determinism claim proved against a hand-written toy says
    /// nothing about the corpus the generator actually compiles.
    pub(crate) fn pinning(name: &str, enabled: &[&str]) -> Self {
        let repository = repository_root();
        let root = repository
            .join("target")
            .join("codegen-pipeline")
            .join(name);
        if root.exists() {
            fs::remove_dir_all(&root)
                .unwrap_or_else(|error| panic!("clear {}: {error}", root.display()));
        }

        // rustfmt reads its configuration from the process working directory,
        // which is the workspace root. Without this the probe would be laid out
        // by rustfmt's defaults instead of by this repository's rules.
        write(
            &root.join("rustfmt.toml"),
            &read(&repository.join("rustfmt.toml")),
        );

        let source_root = repository
            .join("spec/upstream/apache-kafka")
            .join(COMMIT)
            .join("message");
        let mut entries = Vec::new();
        for filename in enabled {
            let bytes = fs::read(source_root.join(filename))
                .unwrap_or_else(|error| panic!("read vendored {filename}: {error}"));
            write(
                &root
                    .join("spec/upstream/apache-kafka")
                    .join(COMMIT)
                    .join("message")
                    .join(filename),
                &String::from_utf8_lossy(&bytes),
            );
            entries.push(format!(
                "[[kafka.files]]\npath = \"{filename}\"\nsha256 = \"{}\"\nstatus = \"enabled\"",
                hex_digest(&bytes)
            ));
        }

        let lock = format!(
            "schema = 1\n\n\
             [kafka]\n\
             repository = \"https://github.com/apache/kafka\"\n\
             commit = \"{COMMIT}\"\n\
             upstream_message_root = \"clients/src/main/resources/common/message\"\n\
             vendored_root = \"spec/upstream/apache-kafka\"\n\n\
             {}\n\n\
             [generator]\n\
             ir_version = 1\n\
             output = \"generated\"\n",
            entries.join("\n\n")
        );
        write(&root.join("spec/protocol.lock"), &lock);
        // Override documents are strict inputs tied to the corpus they govern.
        // This tiny lock contains no API or schema needing an exception, so its
        // matching policy is the versioned empty set rather than production
        // entries for sources the fixture deliberately did not pin.
        for overrides in ["headers.toml", "schema_exceptions.toml"] {
            write(&root.join("spec/overrides").join(overrides), "schema = 1\n");
        }

        Self { root }
    }

    pub(crate) fn generate(
        &self,
        mode: GenerationMode,
    ) -> Result<kafka_wire_codegen::GenerationReport, GenerationError> {
        kafka_wire_codegen::generate(&GeneratorConfig::new(&self.root, mode))
    }

    pub(crate) fn output_root(&self) -> PathBuf {
        self.root.join("generated")
    }

    /// Every file currently in the output tree, by relative path.
    pub(crate) fn tree(&self) -> BTreeMap<String, String> {
        let root = self.output_root();
        if !root.exists() {
            return BTreeMap::new();
        }
        let mut files = BTreeMap::new();
        let entries =
            fs::read_dir(&root).unwrap_or_else(|error| panic!("read {}: {error}", root.display()));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|error| panic!("read entry: {error}"))
                .path();
            if path.is_file() {
                let name = path
                    .file_name()
                    .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
                files.insert(name, read(&path));
            }
        }
        files
    }
}
