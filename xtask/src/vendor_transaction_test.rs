//! Corpus and lock installation is coordinated and recoverable.
//!
//! These scenarios install a complete staged pair, then force the second rename
//! to fail and prove the prior corpus and lock both return byte-for-byte.

#![allow(clippy::unwrap_used)]

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use crate::{protocol_lock::digest, vendor_cleanup, vendor_transaction::StagedVendor};

const COMMIT: &str = "678c0e07e4733c5a592e52046dc2c4e1625587f1";

#[test]
fn a_complete_staged_pair_replaces_both_targets() {
    let root = fresh_workspace("success");
    let (destination, lock_path, old_lock) = old_pair(&root);
    let corpus = BTreeMap::from([("NewRequest.json".to_owned(), b"new bytes".to_vec())]);
    let new_lock = lock_document("NewRequest.json", b"new bytes");

    StagedVendor::new(&destination, &lock_path, &corpus, new_lock.as_bytes())
        .unwrap_or_else(|error| panic!("stage vendor pair: {error}"))
        .commit()
        .unwrap_or_else(|error| panic!("commit vendor pair: {error}"));

    assert_eq!(
        fs::read(destination.join("NewRequest.json")).unwrap(),
        b"new bytes"
    );
    assert!(!destination.join("OldRequest.json").exists());
    assert_eq!(fs::read_to_string(lock_path).unwrap(), new_lock);
    assert_ne!(new_lock, old_lock);
}

#[test]
fn a_failed_second_install_restores_the_prior_pair() {
    let root = fresh_workspace("rollback");
    let (destination, lock_path, old_lock) = old_pair(&root);
    let corpus = BTreeMap::from([("NewRequest.json".to_owned(), b"new bytes".to_vec())]);
    let new_lock = lock_document("NewRequest.json", b"new bytes");
    let staged = StagedVendor::new(&destination, &lock_path, &corpus, new_lock.as_bytes())
        .unwrap_or_else(|error| panic!("stage vendor pair: {error}"));

    staged.remove_staged_lock_for_test();
    let error = staged
        .commit()
        .err()
        .unwrap_or_else(|| panic!("missing staged lock unexpectedly committed"));

    assert!(
        error.contains("rollback succeeded"),
        "rollback was not reported: {error}"
    );
    assert_eq!(
        fs::read(destination.join("OldRequest.json")).unwrap(),
        b"old bytes"
    );
    assert!(!destination.join("NewRequest.json").exists());
    assert_eq!(fs::read_to_string(lock_path).unwrap(), old_lock);
}

#[test]
fn staging_rejects_a_lock_that_does_not_describe_the_corpus() {
    let root = fresh_workspace("digest-mismatch");
    let destination = root.join("vendor").join(COMMIT).join("message");
    let lock_path = root.join("protocol.lock");
    let corpus = BTreeMap::from([("NewRequest.json".to_owned(), b"new bytes".to_vec())]);
    let wrong_lock = lock_document("NewRequest.json", b"different bytes");

    let error = StagedVendor::new(&destination, &lock_path, &corpus, wrong_lock.as_bytes())
        .err()
        .unwrap_or_else(|| panic!("a mismatched lock unexpectedly staged"));

    assert!(
        error.contains("digest does not match NewRequest.json"),
        "unexpected staging error: {error}"
    );
    assert!(!destination.exists());
    assert!(!lock_path.exists());
}

#[test]
fn staging_cleanup_preserves_the_cause_and_removes_partial_artifacts() {
    let root = fresh_workspace("staging-cleanup");
    let directory = root.join("partial-directory");
    let file = root.join("partial-lock");
    fs::create_dir(&directory).unwrap();
    fs::write(&file, b"partial").unwrap();

    assert_eq!(
        vendor_cleanup::directory_after_error(&directory, "lock staging failed".to_owned()),
        "lock staging failed"
    );
    assert_eq!(
        vendor_cleanup::file_after_error(&file, "lock write failed".to_owned()),
        "lock write failed"
    );
    assert!(!directory.exists());
    assert!(!file.exists());
}

#[test]
fn failed_cleanup_is_reported_at_the_right_side_of_commit() {
    let root = fresh_workspace("cleanup-reporting");
    let not_a_directory = root.join("corpus-backup");
    let not_a_file = root.join("lock-backup");
    fs::write(&not_a_directory, b"obstacle").unwrap();
    fs::create_dir(&not_a_file).unwrap();

    let staged_error =
        vendor_cleanup::directory_after_error(&not_a_directory, "staging failed".to_owned());
    assert!(staged_error.contains("staging failed; cleanup failed"));

    let warnings = vendor_cleanup::installed_backups(&not_a_directory, &not_a_file, true);
    assert_eq!(warnings.len(), 2);
    assert!(
        warnings
            .iter()
            .all(|warning| warning.contains("vendor pair was installed"))
    );
}

fn fresh_workspace(name: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("target/vendor-transaction")
        .join(name);
    if root.exists() {
        fs::remove_dir_all(&root)
            .unwrap_or_else(|error| panic!("clear {}: {error}", root.display()));
    }
    fs::create_dir_all(&root).unwrap_or_else(|error| panic!("create {}: {error}", root.display()));
    root
}

fn old_pair(root: &Path) -> (PathBuf, PathBuf, String) {
    let destination = root.join("vendor").join(COMMIT).join("message");
    fs::create_dir_all(&destination).unwrap();
    fs::write(destination.join("OldRequest.json"), b"old bytes").unwrap();
    let lock_path = root.join("protocol.lock");
    let lock = lock_document("OldRequest.json", b"old bytes");
    fs::write(&lock_path, &lock).unwrap();
    (destination, lock_path, lock)
}

fn lock_document(filename: &str, bytes: &[u8]) -> String {
    format!(
        "schema = 1\n\n\
         [kafka]\n\
         repository = \"https://github.com/apache/kafka\"\n\
         commit = \"{COMMIT}\"\n\
         upstream_message_root = \"clients/src/main/resources/common/message\"\n\
         vendored_root = \"vendor\"\n\n\
         [[kafka.files]]\n\
         path = \"{filename}\"\n\
         sha256 = \"{}\"\n\
         status = \"pending\"\n\n\
         [generator]\n\
         ir_version = 1\n\
         output = \"generated\"\n",
        digest(bytes)
    )
}
