//! An upstream name is refused at intake or it is never questioned again.
//!
//! Scenario: offer the naming rules every shape a hostile or careless upstream
//! listing could contain, and assert what survives. Each accepted name becomes
//! a path component under the vendored commit directory and a quoted string in
//! `spec/protocol.lock`, so anything refused here would otherwise have to be
//! escaped correctly at two later places that do not know it is dangerous.
//!
//! Every rule is asserted in both directions. A validator that rejects
//! everything passes an all-negative suite while vendoring nothing at all.

use crate::upstream_name::{plain_filename, repository_slug};

#[test]
fn a_listing_entry_that_is_not_one_plain_filename_is_rejected() {
    // Each name becomes a path component under the vendored directory and a
    // TOML string in the lockfile, so a separator, a traversal segment, or a
    // quote is refused at intake rather than escaped at every later use.
    for candidate in [
        "../escape.json",
        "..",
        ".",
        "nested/Request.json",
        "/absolute/Request.json",
        "quote\".json",
        "back\\slash.json",
        "with space.json",
        "",
    ] {
        assert!(
            plain_filename(candidate).is_err(),
            "`{candidate}` was accepted as a vendorable filename"
        );
    }

    for candidate in ["ApiVersionsRequest.json", "T.json", "with-dash_and.json"] {
        assert_eq!(
            plain_filename(candidate).as_deref(),
            Ok(candidate),
            "`{candidate}` is an ordinary filename and must be accepted"
        );
    }
}

#[test]
fn only_a_github_owner_repo_url_names_a_vendorable_repository() {
    assert_eq!(
        repository_slug("https://github.com/apache/kafka").as_deref(),
        Ok("apache/kafka")
    );
    assert_eq!(
        repository_slug("https://github.com/apache/kafka/").as_deref(),
        Ok("apache/kafka"),
        "a trailing slash is cosmetic and must not change the slug"
    );

    for candidate in [
        "https://gitlab.com/apache/kafka",
        "git@github.com:apache/kafka.git",
        "https://github.com/apache",
        "https://github.com/apache/kafka/tree/trunk",
        "",
    ] {
        assert!(
            repository_slug(candidate).is_err(),
            "`{candidate}` was accepted as a vendorable repository"
        );
    }
}
