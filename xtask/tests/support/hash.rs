//! Content hashing shared by generated-output tests.

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

pub(crate) fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(output, "{byte:02x}")
            .unwrap_or_else(|error| panic!("writing a SHA-256 digest to String failed: {error}"));
    }
    output
}
