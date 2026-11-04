//! One land costs a fixed number of statements, whatever the changeset
//! touches: a land that puts files in 100 paths costs at most a small
//! constant more than one that puts 10. The count covers everything the
//! land's server-side calls run on the store's Postgres clients — the
//! write connection and the readers alike (every client funnels its
//! statements through one seam). The per-land cost is the floor of the
//! land ceiling: a land under the ref's slot lock cannot beat its own
//! round trips, so this guard pins the count the way the tree batch
//! guard pins the derivation's. The count is a process-wide seam, so
//! this guard owns its binary: no other test speaks SQL in its process.

use scion_core::{Changeset, FileState, RepoPath};
use scion_server::Error;
use scion_server::test_support::auth::Caller;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::land::{LandRequest, LandState, Lands};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, test_support::write};
use std::collections::BTreeMap;

const REPO: &str = "bound";

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

/// A store with repo `bound`, a root changeset on `main` that holds no
/// files, and the blobs of `paths` uploaded.
fn store_with_root(paths: &[String]) -> (tempfile::TempDir, Backend) {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    for path in paths {
        store.blobs().put(path.as_bytes()).unwrap();
    }
    let root = Changeset::new(
        Vec::new(),
        identity(),
        None,
        b"root\n".to_vec(),
        paths
            .iter()
            .map(|path| {
                (
                    RepoPath::new(path.as_str()).unwrap(),
                    Some(FileState::Regular {
                        content: scion_core::BlobId::of(path.as_bytes()),
                        executable: false,
                    }),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    )
    .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let (root, _) =
                scion_server::test_support::tree::store_changeset(c, repo, &root, &mut |_| Ok(0))?;
            assert!(c.cas_ref(repo, "main", None, root)?);
            for path in paths {
                c.add_repo_blob(repo, scion_core::BlobId::of(path.as_bytes()))?;
            }
            Ok(())
        })
        .unwrap();
    (dir, store)
}

/// A changeset on `base` that puts `paths`, through the write path.
fn changeset_on(
    store: &Backend,
    base: scion_core::ChangesetId,
    paths: &[String],
) -> scion_core::ChangesetId {
    for path in paths {
        store.blobs().put(path.as_bytes()).unwrap();
    }
    let repo = store.read(|c| c.repo_id(REPO)).unwrap();
    let mut changes = BTreeMap::new();
    for path in paths {
        let upload = store.blobs().upload().unwrap();
        let upload = upload.append(path.as_bytes()).unwrap();
        let blob = write::put_blob(store, repo, upload).unwrap();
        changes.insert(
            RepoPath::new(path.as_str()).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        );
    }
    let cs = Changeset::new(vec![base], identity(), None, b"land\n".to_vec(), changes)
        .expect("changeset fits the format");
    write::create_changeset(store, REPO, &cs.encode(), &later()).unwrap()
}

/// Lands `cs` on `main` once: the store's ref does not move under the
/// test, so a retry would mean the land itself is broken.
fn land_once(
    store: &Backend,
    lands: &Lands,
    cs: scion_core::ChangesetId,
) -> scion_server::test_support::land::LandOutcome {
    let request = LandRequest {
        repo: REPO.into(),
        changeset: cs,
        target_ref: "main".into(),
        request_key: cs.to_string(),
        deadline: later(),
    };
    match lands.land(store, &Caller("t".into()), &request) {
        Ok(outcome) => outcome,
        Err(Error::Aborted(detail)) => panic!("the land aborted once: {detail}"),
        Err(error) => panic!("the land failed: {error}"),
    }
}

fn land_statement_count(paths: usize) -> u64 {
    let paths: Vec<String> = (0..paths).map(|i| format!("p{i:03}.txt")).collect();
    let (_dir, store) = store_with_root(&paths.iter().take(1).cloned().collect::<Vec<_>>());
    let lands = Lands::default();
    let base = store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?
                .expect("the root ref exists")
                .target)
        })
        .unwrap();
    let cs = changeset_on(&store, base, &paths);
    Backend::test_reset_statement_count();
    let outcome = land_once(&store, &lands, cs);
    assert_eq!(outcome.state, LandState::Landed, "{}", outcome.detail);
    let count = Backend::test_statement_count();
    if std::env::var("SCION_BOUND_SHAPES").is_ok() {
        eprintln!("shapes: {:?}", Backend::test_statement_log());
    }
    eprintln!("a land over {} paths cost {count} statements", paths.len());
    count
}

#[test]
fn a_land_costs_a_constant_more_over_a_hundred_paths_and_stays_under_its_bound() {
    // The seam is process-wide, so the runs share one test: parallel
    // windows would count each other's statements.
    // Today's count, measured on this suite's first run, with slack for
    // one plan attempt. Each fold of the round-trip card lowers this
    // number; a change that raises it must say why. The write path's
    // derived reads ride one preloaded statement and its old-row
    // deletes ride the commit's flush: 40 statements became 35. The
    // record answers from its own row - the lookup's and the insert's
    // columns - so it runs no second read: 35 became 34.
    const BOUND: u64 = 44;
    let count = land_statement_count(3);
    assert!(
        count <= BOUND,
        "a land cost {count} statements, over the bound {BOUND}:\n{:#?}",
        Backend::test_statement_log()
    );
    let small = land_statement_count(10);
    let large = land_statement_count(100);
    let growth = large.saturating_sub(small);
    eprintln!("the growth from 10 to 100 paths is {growth} statements");
    assert!(
        growth <= 8,
        "a land over 100 paths cost {large} statements, over 10 cost {small}: \
         the difference {growth} grew with the paths"
    );
}
