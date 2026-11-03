//! The write path's derived reads and deletes ride the transaction's own
//! memory: one preloaded statement answers the per-value reads, and the
//! old-row deletes wait for the commit and land as one statement. These
//! tests prove the end state, not the round trips: a row the write path
//! deleted is gone after the transaction commits, and a write that
//! disagrees with a stored row still refuses, whether the read that
//! checks it answers from the transaction's memory or the database.

use std::sync::{Mutex, MutexGuard};

use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};

use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::tree;

const REPO: &str = "write-flush";

/// The statement log these tests read is one process-wide seam: the
/// tests run in parallel, and a counting window that only this file
/// held would catch another test's statements and count the wrong
/// shapes. Every test in the file holds the lock for its whole body.
static LOG_LOCK: Mutex<()> = Mutex::new(());

fn log_guard() -> MutexGuard<'static, ()> {
    LOG_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn changeset(message: &str, parent: Option<scion_core::ChangesetId>) -> Changeset {
    Changeset::new(
        parent.map(|parent| vec![parent]).unwrap_or_default(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        message.as_bytes().to_vec(),
        std::collections::BTreeMap::from([(
            RepoPath::new("a.txt").unwrap(),
            Some(FileState::Regular {
                content: BlobId::of(message.as_bytes()),
                executable: false,
            }),
        )]),
    )
    .expect("changeset fits the format")
}

#[test]
fn the_old_rows_a_write_path_deletes_are_gone_after_its_commit() {
    let _log = log_guard();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    let parent = changeset("parent\n", None);
    let parent_id = store
        .write(|c| Ok(tree::store_changeset(c, repo, &parent, &mut |_| Ok(0))?.0))
        .unwrap();

    // A row of an older version of the root-tree kind, as a version
    // bump left behind would hold.
    store
        .write(|c| c.put_derived("root_tree", 0, parent_id, b"old-version-value"))
        .unwrap();
    let before = store
        .read(|c| c.get_derived("root_tree", 0, parent_id))
        .unwrap();
    assert_eq!(before.as_deref(), Some(&b"old-version-value"[..]));

    // The write path deletes below the kind's version and defers the
    // delete to its commit. After the transaction commits, the row is
    // gone: the flush ran inside it, so a rollback would have kept it.
    let child = changeset("child\n", Some(parent_id));
    store
        .write(|c| tree::store_changeset(c, repo, &child, &mut |_| Ok(0)))
        .unwrap();
    let after = store
        .read(|c| c.get_derived("root_tree", 0, parent_id))
        .unwrap();
    assert_eq!(
        after, None,
        "the deferred delete must have run at the commit"
    );
}

#[test]
fn a_write_that_disagrees_with_a_stored_row_still_refuses() {
    let _log = log_guard();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    let cs = changeset("root\n", None);
    let id = store
        .write(|c| Ok(tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0))
        .unwrap();

    // The second write of the same row in one transaction holds a value
    // the stored row disagrees with: corruption, whether the check's
    // read answers from the transaction's memory or the database.
    let err = store
        .write(|c| {
            c.put_derived("root_tree", 1, id, b"first")?;
            c.put_derived("root_tree", 1, id, b"second")
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("differs from the stored value"),
        "expected the stored-value conflict, got {err:?}"
    );
}

#[test]
fn the_write_paths_derived_reads_ride_one_preload() {
    let _log = log_guard();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    let parent = changeset("parent\n", None);
    let parent_id = store
        .write(|c| Ok(tree::store_changeset(c, repo, &parent, &mut |_| Ok(0))?.0))
        .unwrap();
    let child = changeset("child\n", Some(parent_id));
    Backend::test_reset_statement_count();
    store
        .write(|c| tree::store_changeset(c, repo, &child, &mut |_| Ok(0)))
        .unwrap();
    let log = Backend::test_statement_log();
    let reads = log
        .iter()
        .filter(|sql| sql.contains("SELECT value FROM derived"))
        .count();
    let deletes = log
        .iter()
        .filter(|sql| sql.starts_with("DELETE FROM derived"))
        .count();
    let preloads = log
        .iter()
        .filter(|sql| sql.starts_with("SELECT kind, version, cs, value FROM derived"))
        .count();
    assert_eq!(
        reads, 0,
        "the build's per-value reads must answer from the preload"
    );
    assert_eq!(
        deletes, 0,
        "the write path's deletes must wait for the commit flush"
    );
    assert_eq!(
        preloads, 1,
        "the build's derived reads ride one preload statement"
    );
}
