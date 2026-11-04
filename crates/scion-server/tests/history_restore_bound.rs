//! The restore's history-node buffer holds a bound, not the whole
//! history (#827): a restore that packs many commits into one
//! transaction - the replay packs up to 64 MiB of changeset bodies -
// would otherwise hold every commit's history nodes in RAM until the
//! commit. At the bound the flush runs inside the transaction, the rows
//! land in the table, and the transaction's own reads of them read the
//! table. The test reads the buffer's peak bytes through the test seam,
//! under a small bound: without the cap the same restore's peak is the
//! whole replay's nodes, many times the bound, and the test fails.

use std::path::Path;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::pg::{
    test_history_pending_peak_bytes, test_set_history_pending_bound_for_tests,
};
use scion_server::test_support::tree;
use scion_server::{Backend, archive};

fn path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

fn regular(content: &str) -> FileState {
    FileState::Regular {
        content: BlobId::of(content.as_bytes()),
        executable: false,
    }
}

/// A first-parent chain of `n` changesets, all changing the same file:
/// every changeset's history holds a new leaf and a new root index, a
/// small body each, so the replay's buffer grows a little with every
/// commit and the cap's flush runs between them.
fn chain(parents: Vec<ChangesetId>, name: &str, content: &str) -> Changeset {
    Changeset::new(
        parents,
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"change\n".to_vec(),
        [(path(name), Some(regular(content)))].into_iter().collect(),
    )
    .expect("changeset fits the format")
}

/// A store whose one repo holds `n` changesets in one first-parent
/// chain, each changing its own file at the root.
fn source(data: &Path, n: usize) -> Backend {
    let store = Backend::open_test(data).unwrap();
    store.blobs().put(b"change\n").unwrap();
    store
        .write(|c| {
            let repo = c.create_repo("chain")?;
            let mut parent = None;
            for i in 0..n {
                let content = format!("change-{i}\n");
                store.blobs().put(content.as_bytes()).unwrap();
                let cs = chain(parent.into_iter().collect(), "f.txt", &content);
                parent = Some(tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0);
            }
            assert!(c.cas_ref(repo, "main", None, parent.expect("depth > 0"))?);
            Ok(())
        })
        .unwrap();
    store
}

#[test]
fn a_restore_of_many_commits_flushes_its_history_buffer_at_the_bound() {
    let commits = 400;
    let data = scion_server::test_support::private_tempdir();
    let store = source(data.path(), commits);
    let parent = scion_server::test_support::private_tempdir();
    let out = parent.path().join("archive");
    archive::export(&store, data.path(), None, true, &out).unwrap();
    drop(store);

    // One transaction for the whole replay: every node the replay
    // writes would ride one buffer to the commit.
    let whole = usize::MAX;
    // A bound a handful of commits passes: without the cap the replay's
    // four hundred commits hold all their nodes, far over it.
    let bound = 4 << 10;
    let _cap = test_set_history_pending_bound_for_tests(bound);
    let empty = scion_server::test_support::private_tempdir();
    let report = archive::restore_with_budget(
        &out,
        empty.path(),
        &Backend::test_postgres_url(empty.path()).unwrap(),
        None,
        whole,
        Backend::open,
    )
    .unwrap();
    assert_eq!(report.repos, vec!["chain".to_owned()], "the repo replayed");
    assert_eq!(
        report.refs, 1,
        "the chain's ref came back: all {commits} commits replayed"
    );
    let peak = test_history_pending_peak_bytes();
    // The buffer may pass the bound by the bodies of one commit's
    // derive - two small nodes here - never by the whole replay's
    // nodes.
    assert!(
        peak <= bound + (2 << 10),
        "the restore held {peak} bytes of history nodes, bound {bound}: \\
         the buffer is not flushing at its bound"
    );
}
