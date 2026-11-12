//! The kill order of the land model (#888): the server dies between a
//! land's plan and its settle. The land's row - committed before the
//! plan - outlives the process and says pending on a database that no
//! server holds; the restart's retry with the same request key
//! finishes that row: the same land ID, one move of the ref. The park
//! is the file seam behind `test-hooks`: the test arms the after-plan
//! point, waits for the hit, kills the server, and reads the row -
//! so the order holds on any machine, and a failure replays to the
//! same order.

#![cfg(unix)]

mod common;

use std::path::Path;

use common::*;
use scion_core::{Changeset, ChangesetId, FileState, RepoPath};
use scion_proto::v1::{
    CreateChangesetRequest, GetChangesetRequest, LandRequest, LandResponse, LandState,
};
use scion_server::test_support::private_tempdir;

/// The park point the seam names in the land path: between a plan and
/// its settle. The park holds the ref slot lock the plan and settle
/// share, and no Postgres transaction and no pool reader; the point's
/// comment in `land.rs` says it beside the code. A kill while parked
/// is exactly the order the issue names.
const POINT: &str = "after-plan";

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
        [(
            RepoPath::new(format!("{name}.txt")).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>(),
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

/// The land's row of one request key: its ID and its state, read on a
/// connection of the test's own (the one-server fence refuses a second
/// open, so the test reads with plain SQL, the way the import's move
/// does). While the server is dead, this connection is the only one.
fn land_row(data: &Path, key: &str) -> (i64, String) {
    let key = key.to_owned();
    let schema = scion_server::Backend::test_schema(data);
    std::thread::spawn(move || {
        let url = std::env::var("SCION_TEST_POSTGRES_URL")
            .expect("SCION_TEST_POSTGRES_URL must name the test database");
        let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        client
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        let row = client
            .query_one(
                "SELECT land_id, state FROM lands WHERE request_key = $1",
                &[&key],
            )
            .expect("the land's row is there");
        (row.get::<_, i64>(0), row.get::<_, String>(1))
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

/// The kill leaves a resumable land: the row says pending on a
/// database no server holds, and the retry finishes that row - the
/// same land ID, one move of the ref.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kill_between_the_plan_and_the_settle_leaves_a_resumable_land() {
    let dir = private_tempdir();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);
    let mut server = Server::start(&data, &log);
    let (mut scion, mut objects) = connect(&server.url).await.unwrap();
    let root = resolve_ref(&mut objects, "main")
        .await
        .expect("main exists after the seed");
    let a = changeset_for(&mut scion, root, "a").await;
    let root_version = ref_version(&data);

    // A parks between its plan and its settle.
    arm_park(&data, POINT, "a");
    let parked = {
        let (url, a) = (server.url.clone(), a);
        tokio::spawn(async move {
            let (mut scion, _) = connect(&url).await.expect("A connects");
            land_on(&mut scion, &a, "main", "a").await
        })
    };
    wait_park(&data, POINT, "a");

    // The kill: the server dies with A parked between the plan and the
    // settle. The plan read nothing durable; the record's row is what
    // the database keeps.
    server.kill();

    // The row proves the order: the land is there and says pending, on
    // a database that no server holds.
    let (land_id, state) = land_row(&data, "a");
    assert_eq!(state, "pending", "the land stays pending after the kill");

    // The parked call's client learns nothing - the reply never came.
    parked.abort();

    // The restart: the same data directory, a new server. The killed
    // server's store lock dies with its session; the spawn waits it
    // out. The startup sweep spares a fresh pending land: the stale
    // bound is a day by default.
    let restart_log = dir.path().join("restart.log");
    let (server, _) =
        spawn_until_lock_free(&data, &restart_log, || Server::spawn(&data, &restart_log));
    let (mut scion, mut objects) = connect(&server.url).await.unwrap();

    // The retry with the same request key finishes the row: the same
    // land ID, and the land lands.
    let retry = land_on(&mut scion, &a, "main", "a").await;
    assert_eq!(retry.state(), LandState::Landed, "the retry finishes it");
    assert_eq!(
        retry.land_id,
        land_id.to_string(),
        "the retry resumes the row the kill left, not a second land"
    );

    // The ref moved exactly once, by the retry: the root's version rose
    // by one, to the retry's result.
    assert_eq!(
        ref_version(&data),
        root_version + 1,
        "the ref moved exactly once"
    );
    let head = resolve_ref(&mut objects, "main").await.unwrap();
    assert_eq!(head, changeset_id(&retry.result_changeset_id));
    let a_result = get_changeset(&mut scion, head).await;
    let [parent] = a_result.parents() else {
        panic!("A's result carries one parent");
    };
    assert_eq!(*parent, root, "A's result sits on the root");
}
