//! Fixed compiler-authored verification files.
//!
//! Individual renderers own their proofs; this module gives pipeline
//! orchestration one named batch with stable paths and producer identities.

use crate::{GenerationError, group::ApiGroup, source::MessageSource};

use super::{render_fuzz_dispatch, render_tag_boundaries, render_tag_claims};

/// One complete generated verification file and its ownership label.
#[derive(Debug)]
pub(crate) struct VerificationFile {
    pub(crate) path: &'static str,
    pub(crate) source: String,
    pub(crate) producer: &'static str,
}

pub(crate) fn render_verification_files(
    groups: &[ApiGroup],
    unkeyed: &[MessageSource],
    commit: &str,
) -> Result<Vec<VerificationFile>, GenerationError> {
    Ok(vec![
        VerificationFile {
            path: "fuzz_roundtrip.rs",
            source: render_fuzz_dispatch(groups, unkeyed, commit)?,
            producer: "fixed fuzz dispatch",
        },
        VerificationFile {
            path: "tag_boundaries.rs",
            source: render_tag_boundaries(groups, unkeyed, commit)?,
            producer: "fixed known-tag boundary assertions",
        },
        VerificationFile {
            path: "tag_claims.rs",
            source: render_tag_claims(groups, unkeyed, commit)?,
            producer: "fixed known-tag claim assertions",
        },
    ])
}
