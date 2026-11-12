//! The lost-reply order of the land model (#888): land A's settle
//! commits and A parks before its reply; the test drops A's call, so
//! the reply is lost; the row A committed stays, and the caller's
//! retry with the same request key replays it - one land, one move of
//! the ref, the same outcome both calls would have heard. The park is
//! the file seam behind `test-hooks`: the test arms the point, waits
//! for the hit, drops the call, and releases A - so the order holds on
//! any machine, and a failure replays to the same order.

#![cfg(unix)]

mod common;

use std::path::Path;

use common::*;
use scion_core::{Changeset, ChangesetId, FileState, RepoPath};
use scion_proto::v1::{
    CreateChangesetRequest, GetChangesetRequest, LandRequest, LandResponse, LandState,
};
use scion_server::test_support::private_tempdir;

/// The park point the seam names in the land path: after the settle's
/// write transaction commits, before the reply goes out. The park
/// holds the ref slot lock the plan and settle share, and no Postgres
/// transaction and no pool reader; the point's comment in `land.rs`
/// says it beside the code.
const POINT: &str = "after-settle";

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
/// does). The row is the proof of the order: it exists and says landed
/// while the caller of that land has no reply.
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

/// The park makes the lost reply replay: A's settle commits and parks,
/// the call is dropped, and the retry with the same key hears the same
/// outcome without a second move of the ref.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_reply_replays_when_the_caller_retries_with_the_key() {
    let dir = private_tempdir();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);
    let server = Server::start(&data, &log);
    let (mut scion, mut objects) = connect(&server.url).await.unwrap();
    let root = resolve_ref(&mut objects, "main")
        .await
        .expect("main exists after the seed");
    let a = changeset_for(&mut scion, root, "a").await;
    let root_version = ref_version(&data);

    // A parks after its settle commits, before its reply.
    arm_park(&data, POINT, "a");
    let parked = {
        let (url, a) = (server.url.clone(), a);
        tokio::spawn(async move {
            let (mut scion, _) = connect(&url).await.expect("A connects");
            land_on(&mut scion, &a, "main", "a").await
        })
    };
    wait_park(&data, POINT, "a");

    // The row A committed is there and says landed, while A's caller
    // has no reply: the row is the proof the settle committed first.
    let (land_id, state) = land_row(&data, "a");
    assert_eq!(state, "landed", "the settle committed before the park");

    // The reply is lost: the call is dropped while A sits parked. The
    // connection closes, and the reply the server would send goes to a
    // dead channel.
    parked.abort();

    // A goes on. Its reply is lost; the row it committed stays.
    release_park(&data, POINT, "a");

    // The caller retries with the same request key: the record's
    // by-key lookup answers the settled row, and the retry replays it.
    let retry = land_on(&mut scion, &a, "main", "a").await;
    assert_eq!(retry.state(), LandState::Landed, "the retry replays");
    assert_eq!(
        retry.land_id,
        land_id.to_string(),
        "the retry replays the first land's row, not a second one"
    );

    // The ref moved exactly once, by the first call: the root's version
    // rose by one, to the replayed result.
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
