//! The derive-stop order of the land model (#888): the server dies
//! between a land derive's steps. The steps before the stop stay - the
//! first step's rows are committed while the walk sits parked - and
//! the restart's retry with the same request key walks on from them:
//! each changeset derived once, the land lands, and the ref moves
//! once. The park is the file seam behind `test-hooks`: the test arms
//! the between-steps point of the commit-graph walk, waits for the
//! hit, kills the server, and reads the rows - so the order holds on
//! any machine, and a failure replays to the same order.

#![cfg(unix)]

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use common::*;
use scion_core::{Changeset, ChangesetId, FileState, RepoPath};
use scion_proto::v1::{
    CreateChangesetRequest, GetChangesetRequest, LandRequest, LandResponse, LandState,
};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::{deadline::Deadline, private_tempdir, write};

/// The park point the seam names in the derive path: between two steps
/// of one walk, keyed by the kind's name. The park holds no Postgres
/// transaction, no pool reader and no ref slot lock; the point's
/// comment in `derived.rs` says it beside the code.
const POINT: &str = "between-derives";
const GRAPH: &str = "commit_graph";
const HISTORY: &str = "path_history";

/// The chain's depth: more than two steps of the walk's 100, so the
/// walk has a step boundary to park at and a step to lose.
const CHAIN: usize = 150;

/// A repo with `main` at an empty root changeset, through the seam.
/// The shared seed's wide root would put 1,500 paths into every
/// history value the walk derives; this walk must be fast enough that
/// its first step boundary lands inside the wait's 60 s floor.
fn lean_seed(data: &Path) -> ChangesetId {
    let store = open_store(data);
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = c.insert_changeset(
                repo,
                &Changeset::new(vec![], author(), None, b"root\n".to_vec(), BTreeMap::new())
                    .expect("changeset fits the format"),
            )?;
            c.cas_ref(repo, "main", None, root)?;
            Ok(root)
        })
        .unwrap()
}

/// A changeset of one file on `base`, made the way a client makes one:
/// the blob up, the changeset created, the ID the server answered.
async fn changeset_for(scion: &mut Scion, base: ChangesetId, name: &str) -> ChangesetId {
    let raw = format!("{name} file\n{}\n", "x".repeat(64));
    let blob = put_blob(scion, raw.as_bytes()).await.unwrap();
    let changeset = Changeset::new(
        vec![base],
        author(),
        None,
        format!("{name}\n").into_bytes(),
        BTreeMap::from([(
            RepoPath::new(format!("{name}.txt")).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]),
    )
    .expect("changeset fits the format");
    let (bytes, id) = changeset.encode_with_id();
    let reply = scion
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(changeset_id(&reply.changeset_id), id);
    id
}

/// One land call under `key`, to the ref `target`.
async fn land_on(scion: &mut Scion, id: &ChangesetId, target: &str, key: &str) -> LandResponse {
    scion
        .land(LandRequest {
            repo: REPO.into(),
            changeset_id: id.as_bytes().to_vec(),
            target_ref: target.into(),
            request_key: key.into(),
        })
        .await
        .expect("the land answers")
        .into_inner()
}

/// The changeset an ID names, read through the server's stream.
async fn get_changeset(scion: &mut Scion, id: ChangesetId) -> Changeset {
    let request = GetChangesetRequest {
        repo: REPO.into(),
        changeset_id: id.as_bytes().to_vec(),
    };
    let mut stream = scion
        .get_changeset(request)
        .await
        .unwrap_or_else(|s| panic!("GetChangeset {id}: {s:?}"))
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(part) = stream.message().await.unwrap() {
        bytes.extend_from_slice(&part.changeset);
    }
    Changeset::decode_with_id(&bytes).unwrap().0
}

/// A chain of `CHAIN` changesets on `main`, through the seam, before
/// any server holds the store (the one-server fence refuses a second
/// open beside a live server). Each changeset adds one file on the
/// previous one; `main` ends at the last one.
fn seed_chain(data: &Path) {
    let store = open_store(data);
    let repo = store.read(|c| c.repo_id(REPO)).unwrap();
    let deadline = Deadline::new(Instant::now() + Duration::from_secs(600));
    let mut base = store
        .read(|c| Ok(c.get_ref(repo, "main")?.expect("main exists").target))
        .unwrap();
    for i in 0..CHAIN {
        let raw = format!("chain {i}\n{}", "y".repeat(32));
        let upload = store.blobs().upload().unwrap();
        let upload = upload.append(raw.as_bytes()).unwrap();
        let content = write::put_blob(&store, repo, upload).expect("the blob stores");
        let cs = Changeset::new(
            vec![base],
            author(),
            None,
            format!("chain {i}\n").into_bytes(),
            [(
                RepoPath::new(format!("chain/f{i}.txt")).unwrap(),
                Some(FileState::Regular {
                    content,
                    executable: false,
                }),
            )]
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        )
        .expect("changeset fits the format");
        base = write::create_changeset(&store, REPO, &cs.encode(), &deadline)
            .expect("the chain changeset stores");
    }
    // The chain is main's history: main moves to the last changeset,
    // one compare-and-swap, the way the import's move does.
    let version = store
        .read(|c| Ok(c.get_ref(repo, "main")?.expect("main exists").version))
        .unwrap();
    let moved = store
        .write(|tx| tx.cas_ref(repo, "main", Some(version), base))
        .unwrap();
    assert!(moved, "the chain's move takes the ref");
}

/// Removes the walk's kinds' rows: `write::create_changeset` derives
/// each changeset's values at write, and the land's derive is the
/// backfill of a database that has none - the old-database case the
/// derive exists for. The trees stay.
fn wipe_walk_rows(data: &Path) {
    let schema = scion_server::Backend::test_schema(data);
    std::thread::spawn(move || {
        let url = std::env::var("SCION_TEST_POSTGRES_URL")
            .expect("SCION_TEST_POSTGRES_URL must name the test database");
        let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        client
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        client
            .execute(
                "DELETE FROM derived WHERE kind IN ('commit_graph', 'path_history')",
                &[],
            )
            .unwrap();
    })
    .join()
    .unwrap();
}

/// The rows one derived kind holds.
fn derived_rows(data: &Path, kind: &str) -> i64 {
    let kind = kind.to_owned();
    let schema = scion_server::Backend::test_schema(data);
    std::thread::spawn(move || {
        let url = std::env::var("SCION_TEST_POSTGRES_URL")
            .expect("SCION_TEST_POSTGRES_URL must name the test database");
        let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        client
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        client
            .query_one("SELECT count(*) FROM derived WHERE kind = $1", &[&kind])
            .unwrap()
            .get::<_, i64>(0)
    })
    .join()
    .unwrap()
}

/// The version `main` stands at, read the way the import's move reads
/// it: the count of moves the ref took.
fn ref_version(data: &Path) -> i64 {
    let schema = scion_server::Backend::test_schema(data);
    std::thread::spawn(move || {
        let url = std::env::var("SCION_TEST_POSTGRES_URL")
            .expect("SCION_TEST_POSTGRES_URL must name the test database");
        let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        client
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        let repo_id: i64 = client
            .query_one("SELECT repo_id FROM repos WHERE name = $1", &[&REPO])
            .unwrap()
            .get(0);
        client
            .query_one(
                "SELECT version FROM refs WHERE repo_id = $1 AND name = 'main'",
                &[&repo_id],
            )
            .unwrap()
            .get(0)
    })
    .join()
    .unwrap()
}

/// The park makes the derive stop between steps and the retry walk on
/// from the stored rows: the first step's rows are there while the
/// walk is stopped, the second step's are not; after the kill and the
/// restart, the retry finishes the walk and lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kill_between_derive_steps_leaves_a_walk_that_continues() {
    let dir = private_tempdir();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    let root = lean_seed(&data);
    seed_chain(&data);
    wipe_walk_rows(&data);
    // The index budget's lever: one byte, so the plan's reach stops at
    // the first changeset of the chain and the land must derive the
    // head's history to plan against it.
    let envs = [("SCION_TEST_MAX_INDEX_BYTES", "1")];
    let mut server = Server::start_with_env(&data, &log, &envs);
    let (mut scion, mut objects) = connect(&server.url).await.unwrap();
    let head = resolve_ref(&mut objects, "main")
        .await
        .expect("main exists after the seed");
    let root_version = ref_version(&data);

    // A changeset on the root, the chain's bottom: the land of it must
    // derive the head's history before it can plan against it.
    let a = changeset_for(&mut scion, root, "a").await;

    // A parks between the commit-graph walk's steps.
    arm_park(&data, POINT, GRAPH);
    let parked = {
        let (url, a) = (server.url.clone(), a);
        tokio::spawn(async move {
            let (mut scion, _) = connect(&url).await.expect("A connects");
            land_on(&mut scion, &a, "main", "a").await
        })
    };
    wait_park(&data, POINT, GRAPH);

    // The rows prove the order: the first step's 100 commit-graph rows
    // are committed, and the walk has not begun the history's rows.
    let graph = derived_rows(&data, GRAPH);
    let history = derived_rows(&data, HISTORY);
    assert!(graph >= 100, "the first step committed its rows: {graph}");
    assert_eq!(history, 0, "the second walk has not begun");

    // The kill: the server dies with the walk parked between steps.
    server.kill();
    parked.abort();

    // The restart: the same data directory, a new server; the dead
    // server's store lock dies with its session.
    let restart_log = dir.path().join("restart.log");
    let (server, _) = spawn_until_lock_free(&data, &restart_log, || {
        Server::spawn_with_env(&data, &restart_log, &envs)
    });
    let (mut scion, mut objects) = connect(&server.url).await.unwrap();

    // The retry with the same request key walks on from the stored
    // rows and lands.
    let retry = land_on(&mut scion, &a, "main", "a").await;
    assert_eq!(retry.state(), LandState::Landed, "the retry finishes it");

    // Both walks covered the whole chain: the walk went on from the
    // first step's rows, and every changeset's value is stored once.
    let graph = derived_rows(&data, GRAPH);
    let history = derived_rows(&data, HISTORY);
    assert!(graph > 100, "the walk continued past the first step");
    assert_eq!(graph, history, "both walks covered the chain");

    // The ref moved exactly once, by the retry: the version rose by
    // one, to a result that sits on the head the kill left.
    assert_eq!(
        ref_version(&data),
        root_version + 1,
        "the ref moved exactly once"
    );
    let tip = resolve_ref(&mut objects, "main").await.unwrap();
    assert_eq!(tip, changeset_id(&retry.result_changeset_id));
    let a_result = get_changeset(&mut scion, tip).await;
    let [parent] = a_result.parents() else {
        panic!("A's result carries one parent");
    };
    assert_eq!(*parent, head, "A's result sits on the chain's head");
}
