//! The write path's history-node buffer holds a bound, not the whole
//! transaction's nodes (#827): a node the buffer already holds pays for
//! its bytes once, and the flush runs strictly over the bound. These
//! tests read the buffer's peak and the flush's one statement shape
//! through the test seams, under a bound this file sets. The seams are
//! process-wide, so every test here holds one lock for its whole body,
//! and this file is its own process: the import suites' batching lane
//! buffers history nodes too, and its tests cannot reach this file's
//! override, peak or log.

use std::sync::{Mutex, MutexGuard};

use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};
use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::pg::{
    test_history_pending_peak_bytes, test_set_history_pending_bound_for_tests,
};
use scion_server::test_support::{private_tempdir, tree};

static SEAM: Mutex<()> = Mutex::new(());

fn seam() -> MutexGuard<'static, ()> {
    SEAM.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn changeset(name: &str) -> Changeset {
    // One file change: the changeset's history holds a leaf and a root
    // index with real bodies, so the buffer's bytes are a body's size,
    // not the one byte of an empty changeset's node.
    Changeset::new(
        vec![],
        IdentityTime::new(b"Ada".to_vec(), b"ada@example.com".to_vec(), 0, 0)
            .expect("identity fits the format"),
        None,
        name.as_bytes().to_vec(),
        [(
            RepoPath::new("a.txt").unwrap(),
            Some(FileState::Regular {
                content: BlobId::of(name.as_bytes()),
                executable: false,
            }),
        )]
        .into_iter()
        .collect(),
    )
    .expect("changeset fits the format")
}

/// A store whose one repo can take the changeset's history nodes
/// through the batching lane, and the lane's one buffered put: the
/// changeset stored once buffers its nodes, stored again re-puts the
/// same ids.
fn store() -> (tempfile::TempDir, Backend) {
    let data = private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    store
        .write(|c| {
            let repo = c.create_repo("r")?;
            let cs = changeset("one\n");
            c.insert_changeset(repo, &cs)?;
            Ok(())
        })
        .unwrap();
    (data, store)
}

/// One lane put of the changeset, under `bound`, ending in a rollback:
/// the statements stay in the log, the buffered rows never reach a
/// commit flush, so the log's one history flush shape is the mid-put
/// flush and nothing else.
fn lane_put_bytes(bound: usize) -> (tempfile::TempDir, Backend, usize, Vec<String>) {
    let (data, store) = store();
    let cs = changeset("one\n");
    let _cap = test_set_history_pending_bound_for_tests(bound);
    Backend::test_reset_statement_count();
    let _ = store.write(|c| {
        let repo = c.repo_id("r")?;
        c.set_changeset_batching(true);
        tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?;
        c.set_changeset_batching(false);
        Err::<(), _>(scion_server::Error::Corrupt("the test's rollback".into()))
    });
    let log = Backend::test_statement_log();
    (data, store, test_history_pending_peak_bytes(), log)
}

fn history_flushes(log: &[String]) -> usize {
    log.iter()
        .filter(|sql| sql.contains("INSERT INTO history_nodes") && sql.contains("unnest"))
        .count()
}

/// A history node the transaction puts twice pays for its bytes once:
/// the row the buffer already holds is replaced, the old body's bytes
/// come off the count before the new body's go on, and the same
/// content address means the same body, so the count is unchanged. A
/// count that adds the old length back, or divides it, answers a
/// buffer bigger than the truth.
#[test]
fn a_history_reput_pays_no_extra_bytes() {
    let _seam = seam();
    // Calibrate: one lane put's buffered bytes, unbounded.
    let (_data, store, bytes, _) = lane_put_bytes(usize::MAX);
    assert!(bytes > 0, "the put buffered its nodes: {bytes}");
    // The same put again under its own size as the bound: the re-put
    // replaces the row, the count comes back to `bytes`, and no flush
    // fires.
    let cs = changeset("one\n");
    let _cap = test_set_history_pending_bound_for_tests(bytes);
    let rolled_back = store
        .write(|c| {
            let repo = c.repo_id("r")?;
            c.set_changeset_batching(true);
            tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?;
            tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?;
            c.set_changeset_batching(false);
            Err::<(), _>(scion_server::Error::Corrupt("the test's rollback".into()))
        })
        .is_err();
    assert!(rolled_back, "the write rolled back");
    let peak = test_history_pending_peak_bytes();
    assert_eq!(
        peak, bytes,
        "the buffer held its one put's bytes, twice put: the peak was {peak}"
    );
}

/// The buffer's flush runs strictly over its bound: at the bound
/// exactly the buffer keeps holding, one byte over it the flush runs
/// inside the transaction. A comparison that fires at the bound
/// flushes a buffer the caller can still fill; one that fires under it
/// never flushes at all and the buffer rides to the commit.
#[test]
fn the_history_buffer_flushes_strictly_over_its_bound() {
    let _seam = seam();
    // Calibrate: one lane put's buffered bytes, unbounded.
    let (_data, _, bytes, _) = lane_put_bytes(usize::MAX);
    assert!(bytes > 0, "the put buffered its nodes: {bytes}");
    // At the bound exactly: no mid-put flush - the rollback's log
    // holds no history flush.
    let (_data, _, _, log) = lane_put_bytes(bytes);
    assert_eq!(
        history_flushes(&log),
        0,
        "a buffer at its bound exactly keeps holding: no flush ran"
    );
    // One byte under the bound as the limit: the put is over it, and
    // the flush runs inside the transaction.
    let (_data, _, _, log) = lane_put_bytes(bytes - 1);
    assert_eq!(
        history_flushes(&log),
        1,
        "a buffer over its bound flushes inside the transaction"
    );
}
