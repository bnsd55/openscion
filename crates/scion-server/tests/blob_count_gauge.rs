//! The blob-count gauge's refresh: it reads the store's metadata in one
//! bounded statement per tick, and counts what the metadata names, not
//! what the blob folder holds (an orphan file or a missing file must
//! not move the gauge).

use scion_core::BlobId;
use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use std::sync::{Mutex, MutexGuard};

/// The statement log these tests read is one process-wide seam: the
/// tests run in parallel, and the counting test's window must hold
/// only its own statements. Every test that resets or reads the log
/// holds the lock for its whole body.
static LOG_LOCK: Mutex<()> = Mutex::new(());

fn log_guard() -> MutexGuard<'static, ()> {
    LOG_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A store whose metadata names exactly one blob, while its folder
/// holds one file that no row names: an orphan the collector has not
/// taken yet. The gauge must read the metadata's 1, not the folder's 2.
fn store_with_orphan() -> (tempfile::TempDir, Backend) {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    store.blobs().put(b"orphan\n").unwrap();
    store
        .write(|c| {
            let repo = c.create_repo("main")?;
            c.add_repo_blob(repo, BlobId::of(b"named\n"))?;
            Ok(())
        })
        .unwrap();
    (dir, store)
}

/// A store with no repository holds no named blobs: the count is zero,
/// though its folder may hold files.
#[test]
fn an_empty_store_counts_zero_blobs() {
    let _log = log_guard();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    store.blobs().put(b"unrelated\n").unwrap();
    let count = store.read(|c| c.blob_count()).unwrap();
    assert_eq!(count, 0, "the gauge counts named blobs, not files");
}

/// The count comes from the metadata: one row names one blob, a file
/// no row names counts nothing.
#[test]
fn the_count_is_what_the_metadata_names_not_what_the_folder_holds() {
    let _log = log_guard();
    let (_dir, store) = store_with_orphan();
    let count = store.read(|c| c.blob_count()).unwrap();
    assert_eq!(
        count, 1,
        "one row names one blob; the orphan file must not count"
    );
}

/// The refresh is one bounded statement: one tick of the poll costs one
/// SELECT over `repo_blobs` and nothing else.
#[test]
fn one_refresh_is_one_statement_over_repo_blobs() {
    let _log = log_guard();
    let (_dir, store) = store_with_orphan();
    let _ = store.read(|c| c.blob_count()).unwrap();
    Backend::test_reset_statement_count();
    let _ = store.read(|c| c.blob_count()).unwrap();
    let log = Backend::test_statement_log();
    let selects: Vec<&String> = log.iter().filter(|s| s.contains("SELECT")).collect();
    assert_eq!(
        selects.len(),
        1,
        "one refresh is one SELECT, not a walk: {log:?}"
    );
    assert!(
        selects[0].contains("repo_blobs"),
        "the statement reads the blob rows: {}",
        selects[0]
    );
    assert!(
        log.iter().all(|s| {
            s.contains("repo_blobs") || s.starts_with("BEGIN") || s.starts_with("ROLLBACK")
        }),
        "the read speaks only its transaction wrapper and the one          SELECT: {log:?}"
    );
}
