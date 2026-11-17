//! The gc sweep walks the changesets it may prune through
//! `repo_changesets_after`: the read returns the stored rows strictly
//! after the `(repo, cs)` cursor, at most `limit` of them, each with
//! its body. This guard pins that round-trip: whatever the sweep later
//! deletes, the read it walks must answer the stored rows, so an empty
//! or short answer cannot pass for "nothing to prune".

use std::collections::BTreeMap;

use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};
use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::private_tempdir;

fn changeset(name: &str) -> Changeset {
    Changeset::new(
        vec![],
        IdentityTime::new(b"Ada".to_vec(), b"ada@example.com".to_vec(), 0, 0)
            .expect("identity fits the format"),
        None,
        name.as_bytes().to_vec(),
        BTreeMap::from([(
            RepoPath::new("a.txt").unwrap(),
            Some(FileState::Regular {
                content: BlobId::of(name.as_bytes()),
                executable: false,
            }),
        )]),
    )
    .expect("changeset fits the format")
}

/// Two changesets in one repo: the store's data directory outlives the
/// backend, and the ids come back in the order the repo stored them.
fn store_with_two_changesets() -> (
    tempfile::TempDir,
    scion_server::Backend,
    scion_server::test_support::meta::RepoId,
    Vec<scion_core::ChangesetId>,
) {
    let data = private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let repo = store.write(|c| c.create_repo("r")).unwrap();
    let mut ids = Vec::new();
    for name in ["alpha\n", "beta\n"] {
        let cs = changeset(name);
        let id = store.write(|c| c.insert_changeset(repo, &cs)).unwrap();
        ids.push(id);
    }
    (data, store, repo, ids)
}

/// The sweep's read answers the rows after its cursor: from the start
/// it returns both rows with their bodies, and from the first row's
/// key it returns exactly the second. A read that answers nothing, or
/// answers rows that are not the stored ones, sends the sweep home
/// with the wrong prune list.
#[test]
fn the_sweep_read_answers_the_rows_after_its_cursor() {
    let (_data, store, repo, ids) = store_with_two_changesets();
    let all = store
        .write(|c| c.repo_changesets_after(repo, &[], 10))
        .unwrap();
    assert_eq!(all.len(), 2, "both stored rows come back");
    assert!(all.iter().all(|row| row.0 == repo));
    // The read orders by the changeset's key bytes, not by insert order.
    let stored: Vec<Vec<u8>> = ids.iter().map(|id| id.as_bytes().to_vec()).collect();
    for row in all.iter() {
        assert!(
            stored.contains(&row.1),
            "the row names a stored changeset"
        );
        assert!(!row.2.is_empty(), "the row carries its body");
    }
    assert!(
        all[0].1 < all[1].1,
        "the read orders by the changeset key"
    );
    let rest = store
        .write(|c| c.repo_changesets_after(repo, &all[0].1, 10))
        .unwrap();
    assert_eq!(rest.len(), 1, "one row sits after the first");
    assert_eq!(rest[0].1, all[1].1, "the row after the cursor is the other one");
}

/// The limit is a bound, not a target: with the cursor at the start
/// and one row allowed, exactly the first row comes back, so the
/// sweep's paging cannot read past its budget.
#[test]
fn the_sweep_read_stops_at_its_limit() {
    let (_data, store, repo, _ids) = store_with_two_changesets();
    let page = store
        .write(|c| c.repo_changesets_after(repo, &[], 1))
        .unwrap();
    assert_eq!(page.len(), 1, "the limit holds");
}
