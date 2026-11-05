//! Two calls that land one request key: the land happens once, and both
//! calls hear the same outcome. The record holds no outcome read - the
//! land's own next step reads the row in its first transaction, where a
//! twin that already settled answers - so no call can act on a stale
//! answer and move the ref twice.

use scion_core::{Changeset, FileState, RepoPath};
use scion_server::test_support::auth::Caller;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::land::{LandRequest, LandState, Lands};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, test_support::tree};
use std::collections::BTreeMap;

const REPO: &str = "twin";

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

/// A store with repo `twin`, a root changeset on `main` that holds no
/// files, and the blobs of `paths` uploaded.
fn store_with_root(paths: &[&str]) -> (tempfile::TempDir, Backend) {
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
                    RepoPath::new(*path).unwrap(),
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
            let (root, _) = tree::store_changeset(c, repo, &root, &mut |_| Ok(0), None)?;
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
    paths: &[&str],
) -> scion_core::ChangesetId {
    for path in paths {
        store.blobs().put(path.as_bytes()).unwrap();
    }
    let repo = store.read(|c| c.repo_id(REPO)).unwrap();
    let mut changes = BTreeMap::new();
    for path in paths {
        let upload = store.blobs().upload().unwrap();
        let upload = upload.append(path.as_bytes()).unwrap();
        let blob = scion_server::test_support::write::put_blob(store, repo, upload).unwrap();
        changes.insert(
            RepoPath::new(*path).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        );
    }
    let cs = Changeset::new(vec![base], identity(), None, b"land\n".to_vec(), changes)
        .expect("changeset fits the format");
    scion_server::test_support::write::create_changeset(store, REPO, &cs.encode(), &later())
        .unwrap()
}

fn request(cs: scion_core::ChangesetId, key: &str) -> LandRequest {
    LandRequest {
        repo: REPO.into(),
        changeset: cs,
        target_ref: "main".into(),
        request_key: key.into(),
        deadline: later(),
    }
}

fn main_ref(store: &Backend) -> (scion_core::ChangesetId, i64) {
    store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            let reference = c.get_ref(repo, "main")?.expect("the root ref exists");
            Ok((reference.target, reference.version))
        })
        .unwrap()
}

/// Two calls with one request key that land at the same time: the land
/// happens once, and both calls answer the same outcome. The record
/// holds no outcome read, so neither call can act on a stale answer:
/// the first caller into the ref's slot settles, and the other's finish
/// reads the settled row and returns its twin's result.
#[test]
fn twin_calls_land_once_and_answer_one_outcome() {
    let (dir, store) = store_with_root(&["seed.txt"]);
    let base = store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?
                .expect("the root ref exists")
                .target)
        })
        .unwrap();
    let cs = changeset_on(&store, base, &["a.txt"]);
    let store = std::sync::Arc::new(store);
    let lands = std::sync::Arc::new(Lands::default());
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let mut threads = Vec::new();
    for _ in 0..2 {
        let (store, lands, barrier) = (
            std::sync::Arc::clone(&store),
            std::sync::Arc::clone(&lands),
            std::sync::Arc::clone(&barrier),
        );
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            lands.land(&store, &Caller("t".into()), &request(cs, "twin"))
        }));
    }
    let mut results = Vec::new();
    for thread in threads {
        results.push(thread.join().unwrap().unwrap());
    }
    // Both calls report the same landed outcome, and the ref moved once.
    let schema = Backend::test_schema(dir.path());
    let mut client = postgres::Client::connect(
        &std::env::var("SCION_TEST_POSTGRES_URL").unwrap(),
        postgres::NoTls,
    )
    .unwrap();
    let rows = client
        .query(
            &format!("SELECT land_id, caller, request_key, state FROM {schema}.lands"),
            &[],
        )
        .unwrap();
    let rows: Vec<String> = rows
        .iter()
        .map(|row| {
            format!(
                "{} {} {} {}",
                row.get::<_, i64>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
                row.get::<_, String>(3)
            )
        })
        .collect();
    assert!(
        results.iter().all(|out| out.state == LandState::Landed),
        "the twin calls must both land: {results:?} rows: {rows:?}"
    );
    let first = &results[0];
    assert!(
        results.iter().all(|out| out.result == first.result),
        "the twin calls must agree on the landed changeset: {results:?}"
    );
    let (_, version) = main_ref(&store);
    assert_eq!(version, 2, "the ref moved exactly once");
}

/// A call whose twin settled while it was recorded: the resumed call
/// returns the twin's outcome and does not move the ref again.
#[test]
fn a_call_resumed_after_its_twin_settled_returns_the_twin_s_outcome() {
    let (_dir, store) = store_with_root(&["seed.txt"]);
    let base = store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?
                .expect("the root ref exists")
                .target)
        })
        .unwrap();
    let cs = changeset_on(&store, base, &["a.txt"]);
    let req = request(cs, "twin");

    // The first call records and stops there, as a crash would leave it.
    let (_, recorded) = store
        .write(|c| c.record_land(&Caller("t".into()), &req, None))
        .unwrap();
    let land_id = recorded.land_id;

    // The twin records the same key: the same land, one row.
    let (_, twin_recorded) = store
        .write(|c| c.record_land(&Caller("t".into()), &req, None))
        .unwrap();
    let twin_land_id = twin_recorded.land_id;
    assert_eq!(twin_land_id, land_id, "the twin resumes one land");

    // The twin finishes the land: the ref moves once.
    let twin = Lands::default()
        .land(&store, &Caller("t".into()), &req)
        .unwrap();
    assert_eq!(twin.state, LandState::Landed, "{}", twin.detail);
    let (head, version) = main_ref(&store);
    assert_eq!(version, 2, "the twin's land moved the ref exactly once");

    // The first call resumes: it reads the settled row in its finish and
    // returns its twin's outcome, without moving the ref again.
    let resumed = Lands::default()
        .land(&store, &Caller("t".into()), &req)
        .unwrap();
    assert_eq!(resumed.land_id, land_id, "a stable land id");
    assert_eq!(resumed.state, LandState::Landed, "{}", resumed.detail);
    assert_eq!(resumed.result, twin.result, "the twin's outcome");
    let (after, after_version) = main_ref(&store);
    assert_eq!(after, head, "the resumed call moved no ref");
    assert_eq!(after_version, version, "the resumed call moved no ref");
}
