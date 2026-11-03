//! The cost of one changed name in a directory of many: a tree object
//! holds a whole directory, so one change rewrites the directory whole,
//! and the format freeze needs the measured shape of that cost. This
//! file measures it on the real land and read paths and prints one
//! grep-able row per run (`target=dir_width`); it never runs with the
//! suites - each run names its width in `SCION_DIR_WIDTH`.
//!
//! Run it through the build slot, for example:
//! `SCION_TEST_POSTGRES_URL=... SCION_DIR_WIDTH=10000 \
//!  cargo test -p open-scion-server --release --test dir_width -- --ignored --nocapture`
//! and read the `target=dir_width` line. The wide points run on CI,
//! through this branch's `dir-width` workflow dispatch.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use scion_core::{BlobId, Changeset, Entry, FileState, RepoPath};
use scion_server::Backend;
use scion_server::test_support::auth::Caller;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::land::{LandRequest, Lands};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::{private_tempdir, write};

const REPO: &str = "width";

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
    Deadline::new(Instant::now() + Duration::from_secs(3600))
}

/// The directory the measurement writes: every name sits in one
/// directory, so a change rewrites exactly it.
fn name(i: usize) -> String {
    format!("d/f{i:07}")
}

/// A store whose `main` holds `names` files in one directory. The files
/// share one blob, so the setup's writes do not shape the measurement:
/// the tree's entries, not their content, are what the cost is about.
fn store_with_wide_dir(names: usize) -> (tempfile::TempDir, Backend, BlobId) {
    let dir = private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let shared = store.blobs().put(b"shared\n").unwrap();
    let changed = store.blobs().put(b"changed\n").unwrap();
    let mut seed = BTreeMap::new();
    for i in 0..names {
        seed.insert(
            RepoPath::new(name(i).as_str()).unwrap(),
            Some(FileState::Regular {
                content: shared,
                executable: false,
            }),
        );
    }
    let root = Changeset::new(Vec::new(), identity(), None, b"root\n".to_vec(), seed)
        .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let (root, _) =
                scion_server::test_support::tree::store_changeset(c, repo, &root, &mut |_| Ok(0))?;
            assert!(c.cas_ref(repo, "main", None, root)?);
            c.add_repo_blob(repo, shared)?;
            c.add_repo_blob(repo, changed)?;
            Ok(())
        })
        .unwrap();
    (dir, store, changed)
}

/// A changeset on `base` that rewrites the directory's last name,
/// through the write path.
fn changeset_on(
    store: &Backend,
    base: scion_core::ChangesetId,
    changed: BlobId,
    names: usize,
) -> scion_core::ChangesetId {
    let mut one_name = BTreeMap::new();
    one_name.insert(
        RepoPath::new(name(names - 1).as_str()).unwrap(),
        Some(FileState::Regular {
            content: changed,
            executable: false,
        }),
    );
    let cs = Changeset::new(
        vec![base],
        identity(),
        None,
        b"one name\n".to_vec(),
        one_name,
    )
    .expect("changeset fits the format");
    write::create_changeset(store, REPO, &cs.encode(), &later()).unwrap()
}

/// One measurement row for a directory of `names` names.
#[ignore = "a measurement: run it with SCION_DIR_WIDTH set, never with the suites"]
#[test]
fn one_changed_name_in_a_wide_directory() {
    let names: usize = std::env::var("SCION_DIR_WIDTH")
        .unwrap_or_else(|_| "1000".to_string())
        .parse()
        .expect("SCION_DIR_WIDTH names a count");
    let (_dir, store, changed) = store_with_wide_dir(names);
    let repo = store.read(|c| c.repo_id(REPO)).unwrap();
    let base = store
        .read(|c| {
            Ok(c.get_ref(repo, "main")?
                .expect("the root ref exists")
                .target)
        })
        .unwrap();
    let cs = changeset_on(&store, base, changed, names);

    let lands = Lands::default();
    let request = LandRequest {
        repo: REPO.into(),
        changeset: cs,
        target_ref: "main".into(),
        request_key: cs.to_string(),
        deadline: later(),
    };
    let started = Instant::now();
    let outcome = lands
        .land(&store, &Caller("width".into()), &request)
        .expect("the land lands");
    let land_ms = started.elapsed().as_secs_f64() * 1000.0;
    let root = outcome
        .result
        .expect("a landed outcome carries its changeset");

    // The bytes the land wrote: the changed directory's new body, plus
    // its parent's (the parent's entry for it changed with it). The
    // root here holds only `d`, so the parent is the root.
    let root_tree =
        scion_server::test_support::read::root_tree(&store, REPO, root, &later()).unwrap();
    let (dir_tree, dir_bytes, root_bytes) = store
        .read(|c| {
            let Some(Entry::Dir(dir)) = scion_server::test_support::tree::lookup(
                c,
                root_tree,
                &RepoPath::new("d").unwrap(),
            )?
            else {
                panic!("the directory survived the land");
            };
            Ok((dir, c.tree_size(dir)?, c.tree_size(root_tree)?))
        })
        .unwrap();

    // The read-back: a ListDir's reply is the directory's stored body,
    // so its bytes are the body's, and its work is the read and the
    // encode; a checkout's directory read is the decode and the walk of
    // every name.
    let list_start = Instant::now();
    let (list_bytes, entries) = store
        .read(|c| {
            let tree = c.get_tree(dir_tree)?;
            Ok((c.tree_size(dir_tree)?, tree.entries().len()))
        })
        .unwrap();
    let list_ms = list_start.elapsed().as_secs_f64() * 1000.0;

    eprintln!(
        "target=dir_width names={names} land_ms={land_ms:.1} land_tree_bytes={dir_bytes} \
         parent_bytes={root_bytes} list_bytes={list_bytes} list_ms={list_ms:.1} entries={entries}"
    );
}

/// The supported width's cost per change, pinned: a tree holds a whole
/// directory, so the pin is the line, not a cliff. The land's tree
/// write stays at the measured fifty-four bytes a name plus slack, and
/// the land's time stays far under the minutes a slow runner needs for
/// the same work. A change that starts costing superlinear bytes, or a
/// land that crawls at the width the docs support, fails here before
/// the format freezes.
#[test]
fn a_change_to_a_supported_width_directory_stays_on_the_measured_line() {
    const SUPPORTED_WIDTH: usize = 100_000;
    const BYTES_PER_NAME: u64 = 54;
    let (_dir, store, changed) = store_with_wide_dir(SUPPORTED_WIDTH);
    let repo = store.read(|c| c.repo_id(REPO)).unwrap();
    let base = store
        .read(|c| {
            Ok(c.get_ref(repo, "main")?
                .expect("the root ref exists")
                .target)
        })
        .unwrap();
    let cs = changeset_on(&store, base, changed, SUPPORTED_WIDTH);

    let lands = Lands::default();
    let request = LandRequest {
        repo: REPO.into(),
        changeset: cs,
        target_ref: "main".into(),
        request_key: cs.to_string(),
        deadline: later(),
    };
    let started = Instant::now();
    let outcome = lands
        .land(&store, &Caller("width".into()), &request)
        .expect("the land lands");
    let land_ms = started.elapsed().as_secs_f64() * 1000.0;
    let root = outcome
        .result
        .expect("a landed outcome carries its changeset");

    let root_tree =
        scion_server::test_support::read::root_tree(&store, REPO, root, &later()).unwrap();
    let dir_bytes = store
        .read(|c| {
            let Some(Entry::Dir(dir)) = scion_server::test_support::tree::lookup(
                c,
                root_tree,
                &RepoPath::new("d").unwrap(),
            )?
            else {
                panic!("the directory survived the land");
            };
            c.tree_size(dir)
        })
        .unwrap();

    let bound = BYTES_PER_NAME * SUPPORTED_WIDTH as u64 + 64 * 1024;
    assert!(
        (dir_bytes as u64) <= bound,
        "one change to a {SUPPORTED_WIDTH}-name directory wrote {dir_bytes} tree bytes; \
         the measured line is {BYTES_PER_NAME} a name plus slack, and a change that costs \
         more means the write shape left it"
    );
    assert!(
        land_ms < 30_000.0,
        "the land of one changed name in a {SUPPORTED_WIDTH}-name directory took \
         {land_ms:.0} ms; the supported width's land stays under the generous bound"
    );
}
