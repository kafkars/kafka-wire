//! Vendoring refuses a listing it cannot vouch for, and keeps reviewed status.
//!
//! Scenario: hand the listing reader synthetic GitHub responses and assert what
//! it accepts. Every refusal here is a way the vendored corpus could end up
//! recording something untrue about upstream: a partial tree presented as
//! complete, an empty tree presented as a corpus, or a listing quietly shortened
//! by dropping the one entry that could not be used.
//!
//! None of this reaches the network. The one line that does is the fetch inside
//! `discover`, which is why the judgement was split out from it. Whether a
//! single name is usable at all belongs to `upstream_name`, and is proved there.

use std::collections::BTreeMap;

use crate::{
    protocol_lock::{SourceStatus, digest},
    vendor::{relock, schema_filenames},
};

const COMMIT: &str = "678c0e07e4733c5a592e52046dc2c4e1625587f1";

/// One git-tree listing, as GitHub returns it.
fn listing(truncated: bool, entries: &[(&str, &str)]) -> Vec<u8> {
    let tree = entries
        .iter()
        .map(|(path, kind)| format!("{{\"path\":\"{path}\",\"type\":\"{kind}\"}}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{\"truncated\":{truncated},\"tree\":[{tree}]}}").into_bytes()
}

fn rejection(listing: &[u8], situation: &str) -> String {
    schema_filenames(listing, COMMIT)
        .err()
        .unwrap_or_else(|| panic!("{situation} was accepted"))
}

#[test]
fn a_complete_listing_yields_its_schema_files_in_sorted_order() {
    // Without this, every refusal below could pass because the reader refuses
    // everything. Sorting matters too: the lockfile records files in this
    // order, so an unsorted listing would rewrite the whole document.
    let filenames = schema_filenames(
        &listing(
            false,
            &[
                ("MetadataRequest.json", "blob"),
                ("ApiVersionsRequest.json", "blob"),
                ("notes.txt", "blob"),
                ("subdirectory", "tree"),
            ],
        ),
        COMMIT,
    )
    .unwrap_or_else(|error| panic!("a complete listing was rejected: {error}"));

    assert_eq!(
        filenames,
        vec![
            "ApiVersionsRequest.json".to_owned(),
            "MetadataRequest.json".to_owned()
        ]
    );
}

#[test]
fn a_truncated_listing_is_a_hard_error() {
    // GitHub sets `truncated` when it dropped entries. Vendoring what arrived
    // would write a partial corpus and a lockfile asserting it is the whole
    // pinned commit, which no later check could tell apart from the truth.
    let error = rejection(
        &listing(true, &[("ApiVersionsRequest.json", "blob")]),
        "a truncated listing",
    );

    assert!(
        error.contains("truncated") && error.contains(COMMIT),
        "the refusal must name the truncation and the commit: {error}"
    );
}

#[test]
fn an_empty_listing_is_a_hard_error() {
    for (situation, entries) in [
        ("a listing with no entries at all", Vec::new()),
        (
            "a listing naming only non-schema files",
            vec![("notes.txt", "blob"), ("message", "tree")],
        ),
    ] {
        let error = rejection(&listing(false, &entries), situation);
        assert!(
            error.contains("refusing to record an empty corpus"),
            "the refusal for {situation} must say why: {error}"
        );
    }
}

#[test]
fn a_traversing_listing_entry_fails_the_whole_listing() {
    // One bad name must not simply be skipped: silently dropping it would
    // vendor a corpus smaller than upstream's and record it as complete.
    let error = rejection(
        &listing(
            false,
            &[
                ("ApiVersionsRequest.json", "blob"),
                ("../../../etc/passwd.json", "blob"),
            ],
        ),
        "a listing containing a traversal",
    );

    assert!(
        error.contains("unusable schema filename"),
        "the refusal must name the offending entry: {error}"
    );
}

#[test]
fn a_reviewed_status_survives_re_vendoring_and_a_new_file_does_not_inherit_one() {
    // This is what makes re-vendoring reproduce the lockfile: the digests come
    // from the bytes, and the statuses come from the document being replaced.
    let bytes = b"{\"name\": \"ApiVersionsRequest\"}";
    let recorded = BTreeMap::from([
        ("ApiVersionsRequest.json", SourceStatus::Enabled),
        ("MetadataRequest.json", SourceStatus::Pending),
    ]);

    let enabled = relock("ApiVersionsRequest.json", bytes, &recorded);
    assert_eq!(enabled.status, SourceStatus::Enabled);
    assert_eq!(enabled.path, "ApiVersionsRequest.json");
    assert_eq!(enabled.sha256, digest(bytes));

    assert_eq!(
        relock("MetadataRequest.json", bytes, &recorded).status,
        SourceStatus::Pending
    );
    assert_eq!(
        relock("BrandNewRequest.json", bytes, &recorded).status,
        SourceStatus::Pending,
        "a message upstream added since the last run must not join the compiled set"
    );
}
