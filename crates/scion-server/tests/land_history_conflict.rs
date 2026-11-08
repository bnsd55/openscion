//! A land on an old base checks its conflicts through the head's path
//! history (`check_history`), because the index does not reach that far
//! back. These tests pin the seam's rule: a file put (or deleted) at a
//! directory above a land's path after the base is a conflict, and the
//! base's own touch of the path is not.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use scion_core::{Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::test_support::auth::Caller;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::land::{LandRequest, LandState, Lands};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::write;
use scion_server::Backend;

/// A changeset that puts `path` on `base`. A parentless `base` makes the
/// repo's root, stored through the raw insert (the write path refuses a
/// changeset without a parent).
fn put(store: &Backend, base: Option<ChangesetId>, path: &str) -> ChangesetId {
    let repo = store.read(|c| c.repo_id("r")).unwrap();
    let upload = store.blobs().upload().unwrap();
    let upload = upload.append(path.as_bytes()).unwrap();
    let file = FileState::Regular {
        content: write::put_blob(store, repo, upload).unwrap(),
        executable: false,
    };
    let cs = Changeset::new(
        base.into_iter().collect(),
        identity(),
        None,
        path.as_bytes().to_vec(),
        BTreeMap::from([(path_of(path), Some(file))]),
    )
    .expect("changeset fits the format");
    match base {
        None => store
            .write(|c| c.insert_changeset(repo, &cs))
            .unwrap(),
        Some(_) => write::create_changeset(store, "r", &cs.encode(), &later()).unwrap(),
    }
}

/// A changeset that deletes `path` on `base`.
fn del(store: &Backend, base: ChangesetId, path: &str) -> ChangesetId {
    let cs = Changeset::new(
        vec![base],
        identity(),
        None,
        path.as_bytes().to_vec(),
        BTreeMap::from([(path_of(path), None)]),
    )
    .expect("changeset fits the format");
    write::create_changeset(store, "r", &cs.encode(), &later()).unwrap()
}

fn identity() -> IdentityTime {
    IdentityTime::new(b"Ada".to_vec(), b"ada@example.com".to_vec(), 1_700_000_200, 0)
        .expect("identity fits the format")
}

fn path_of(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

fn later() -> Deadline {
    Deadline::new(Instant::now() + Duration::from_secs(600))
}

/// Lands `cs` on `main`, and retries with the same key while the ref moves.
fn land(store: &Backend, lands: &Lands, cs: ChangesetId) -> scion_server::test_support::land::LandOutcome {
    let request = LandRequest {
        repo: "r".into(),
        changeset: cs,
        target_ref: "main".into(),
        request_key: cs.to_string(),
        deadline: Deadline::new(Instant::now() + Duration::from_secs(600)),
    };
    for tries in 0..100 {
        match lands.land(store, &Caller("t".into()), &request) {
            Err(_) => {}
            outcome => return outcome.unwrap(),
        }
        assert!(tries < 99, "the land aborted {tries} times and never ended");
    }
    unreachable!("the land loop exits by returning");
}

fn open_store() -> Backend {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    std::mem::forget(dir);
    store.write(|c| c.create_repo("r")).unwrap();
    store
}

fn set_main(store: &Backend, cs: ChangesetId) {
    store
        .write(|c| {
            let repo = c.repo_id("r")?;
            let from = c.get_ref(repo, "main")?.map(|r| r.version);
            assert!(c.cas_ref(repo, "main", from, cs)?);
            Ok(())
        })
        .unwrap();
}

/// A chain of `n` filler changesets, each moving `main` forward.
fn fillers(store: &Backend, from: ChangesetId, n: usize) -> ChangesetId {
    let mut head = from;
    for i in 0..n {
        head = put(store, Some(head), &format!("f{i}.txt"));
        set_main(store, head);
    }
    head
}

/// The land path's conflict check for old bases: after the base, a file
/// was put at the directory above the land's path (`a/b` became a file)
/// and was deleted again, so the path itself is untouched since the base
/// and nothing blocks the apply. Only the history's `above` stamp sees
/// the touch; the land must be refused as a conflict. The mutant that
/// fires only at `above == base_gen` never sees it and lets the land in.
#[test]
fn a_file_at_a_directory_above_the_base_conflicts_an_old_base() {
    let store = open_store();
    let lands = Lands::default();
    // The base: main at gen 1 with a filler file.
    let base = put(&store, None, "z.txt");
    set_main(&store, base);
    // The fillers push the base past the index's reach.
    let head = fillers(&store, base, 21);
    // The file a/b is put, then deleted. Both stamps sit above the base;
    // a/b/c itself has no history at all.
    let file = put(&store, Some(head), "a/b");
    set_main(&store, file);
    let gone = del(&store, file, "a/b");
    set_main(&store, gone);
    // A land on the base that creates a/b/c: the apply is clean (no
    // a/b at the head), but the file sat above the path after the base.
    let land_cs = put(&store, Some(base), "a/b/c");
    let out = land(&store, &lands, land_cs);
    assert_eq!(out.state, LandState::Conflict, "{}", out.detail);
    assert_eq!(out.conflict_paths, [path_of("a/b/c")]);
}

/// The base's own touch of the land's path is not a conflict: the base
/// created a/b/c and nothing changed it after. A refusal there would
/// refuse every land that edits a file its base created.
#[test]
fn the_base_touching_the_path_does_not_conflict_an_old_base() {
    let store = open_store();
    let lands = Lands::default();
    // The base creates a/b/c.
    let base = put(&store, None, "a/b/c");
    set_main(&store, base);
    // The fillers push the base past the index's reach.
    let head = fillers(&store, base, 21);
    // Another agent lands a change on a different path.
    let other = put(&store, Some(head), "w.txt");
    set_main(&store, other);
    // A land on the base that edits a/b/c, the path its own base created.
    let land_cs = put(&store, Some(base), "a/b/c");
    let out = land(&store, &lands, land_cs);
    assert_eq!(out.state, LandState::Landed, "{}", out.detail);
}
