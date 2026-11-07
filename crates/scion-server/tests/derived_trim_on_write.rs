//! A store's write trims the derived rows of older versions: one create
//! deletes up to `STEP` rows below each kind's version, so a version
//! bump's leftovers clear as the store is written, not only when a fill
//! or a backfill walks their changesets (#949). The red proof on main
//! kept an old-version row of each kind through one create.

use scion_core::{Changeset, FileState, RepoPath};
use scion_server::Backend;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::write;
use std::collections::BTreeMap;

const REPO_NAME: &str = "trim";

fn identity() -> scion_core::IdentityTime {
    scion_core::IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

fn later() -> Deadline {
    Deadline::new(std::time::Instant::now() + std::time::Duration::from_secs(600))
}

/// A store with repo `trim`, a root changeset on `main` that holds no
/// files, and an old-version derived row (version 0) of each kind the
/// write path touches: `commit_graph` and `path_history`. `RootTree` already
/// trims on main; the two other kinds are the ones whose trim #935
/// dropped.
fn store_with_old_rows() -> (tempfile::TempDir, Backend) {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let root = Changeset::new(
        Vec::new(),
        identity(),
        None,
        b"root\n".to_vec(),
        BTreeMap::new(),
    )
    .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo("trim")?;
            let (root, _) =
                scion_server::test_support::tree::store_changeset(c, repo, &root, &mut |_| Ok(0))?;
            assert!(c.cas_ref(repo, "main", None, root)?);
            // One old-version row per kind: version 0 sits below each
            // kind's current version, exactly what the trim deletes.
            c.put_derived("commit_graph", 0, root, b"old-graph")?;
            c.put_derived("path_history", 0, root, b"old-history")?;
            Ok(())
        })
        .unwrap();
    (dir, store)
}

#[test]
fn one_create_trims_the_old_version_rows_of_every_kind_it_writes() {
    let (_dir, store) = store_with_old_rows();
    let repo = store.read(|c| c.repo_id("trim")).unwrap();
    let base = store
        .read(|c| {
            Ok(c.get_ref(repo, "main")?
                .expect("the root ref exists")
                .target)
        })
        .unwrap();
    let upload = store.blobs().upload().unwrap();
    let upload = upload.append(b"p000.txt" as &[u8]).unwrap();
    let blob = write::put_blob(&store, repo, upload).unwrap();
    let mut changes = BTreeMap::new();
    changes.insert(
        RepoPath::new("p000.txt").unwrap(),
        Some(FileState::Regular {
            content: blob,
            executable: false,
        }),
    );
    let cs = Changeset::new(vec![base], identity(), None, b"one\n".to_vec(), changes)
        .expect("changeset fits the format");
    write::create_changeset(&store, REPO_NAME, &cs.encode(), &later()).unwrap();

    // The create's write path trims below each kind's version: both
    // old rows are gone, whatever the fill steps would do later.
    let left = store
        .read(|c| {
            Ok((
                c.get_derived("commit_graph", 0, base)?.is_some(),
                c.get_derived("path_history", 0, base)?.is_some(),
            ))
        })
        .unwrap();
    assert_eq!(
        left,
        (false, false),
        "an old-version row of each kind survives one create: the write \
         path's trim is missing"
    );
}
