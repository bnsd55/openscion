//! One forced interleaving of the land model (#888): land A parks
//! between its plan and its settle; `main` moves under it; A's settle
//! must lose the compare-and-swap, and `finish` replans inside the
//! same call, so the first reply is Landed on the new head. The park
//! is the file seam behind `test-hooks`: the test arms the point,
//! waits for the hit, moves the ref, and releases A - so the order
//! holds on any machine, and a failure replays to the same order.

#![cfg(unix)]

mod common;

use std::net::SocketAddr;
use std::path::Path;

use common::*;
use scion_core::{Changeset, ChangesetId, FileState, RepoPath};
use scion_proto::v1::{
    CreateChangesetRequest, GetChangesetRequest, LandRequest, LandResponse, LandState,
};
use scion_server::test_support::private_tempdir;

/// The park point the seam names in the land path: between a plan and
/// its settle. The park holds no Postgres transaction and no pool
/// reader; it holds the parked call's own ref slot lock, which the
/// test works with: the neighbor land runs on another ref, and the
/// mover that forces the compare-and-swap loss bypasses the slot the
/// way an import does.
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

/// The import's move of `main`: one compare-and-swap against
/// `expected`, the statement `MetaTx::cas_ref` runs for the import
/// (`pg.rs`, the `UPDATE refs ... WHERE version = $` branch). Raw SQL
/// because the fence refuses a second open of the store beside the
/// live server (`Backend::open_test` answers "another server holds
/// this store"), so the test cannot call the real function through a
/// backend of its own. The import never takes the land's ref slot, so
/// A's park does not stop it. A change to `cas_ref`'s statement must
/// change this one the same way.
fn import_move_main(data: &Path, expected: i64, target: &ChangesetId) -> bool {
    // The move runs on its own thread: the test is a tokio one, and
    // the sync postgres client must not start a runtime inside it. The
    // thread owns what it reads.
    let schema = scion_server::Backend::test_schema(data);
    let target = target.as_bytes().to_vec();
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
        let changed = client
            .execute(
                "UPDATE refs SET target = $3, version = version + 1
                 WHERE repo_id = $1 AND name = 'main' AND version = $2",
                &[&repo_id, &expected, &target],
            )
            .unwrap();
        changed == 1
    })
    .join()
    .unwrap()
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

/// Reads `scion_land_replans_total` off the server's /metrics. The
/// render omits the counter while it is zero, so a missing line reads
/// as zero. The scrape runs off the runtime: a blocking read must not
/// sit on a worker.
async fn scrape_replans(metrics: SocketAddr) -> u64 {
    tokio::task::spawn_blocking(move || metric_total(metrics, "scion_land_replans_total"))
        .await
        .unwrap()
        .unwrap_or(0)
}

/// The park makes the compare-and-swap loss replay: A plans on the
/// root, parks, the ref moves, A's settle loses the compare-and-swap,
/// and `finish` replans and lands A on the new head in the same call.
/// The replan counter proves the loss: it rises by exactly one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_land_replans_when_the_ref_moves_under_it() {
    let dir = private_tempdir();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);
    // The metrics spawn: the replan counter must be scrapeable off the
    // real process, so the test fails if the loss never happened.
    let server = Server::start_with_metrics(&data, &log);
    let metrics = metrics_addr(&log).expect("the server serves metrics");
    let (mut scion, mut objects) = connect(&server.url).await.unwrap();
    let root = resolve_ref(&mut objects, "main")
        .await
        .expect("main exists after the seed");
    let a = changeset_for(&mut scion, root, "a").await;
    let b = changeset_for(&mut scion, root, "b").await;
    let c = changeset_for(&mut scion, root, "c").await;

    // A parks between its plan and its settle.
    arm_park(&data, POINT, "a");
    let first = {
        let (url, a) = (server.url.clone(), a);
        tokio::spawn(async move {
            let (mut scion, _) = connect(&url).await.expect("A connects");
            land_on(&mut scion, &a, "main", "a").await.state()
        })
    };
    wait_park(&data, POINT, "a");

    // B lands whole on `dev` while A parks: the proof that the park
    // holds no transaction, no pool reader and no lock outside A's own
    // ref - B never queues behind A.
    assert_eq!(
        land_on(&mut scion, &b, "dev", "b").await.state(),
        LandState::Landed
    );

    // `main` moves the way an import moves it: one compare-and-swap
    // against the version A's plan read (the root stands at version 1),
    // on a connection that never takes the land's ref slot. This is the
    // mover A's settle compare-and-swap exists for.
    assert!(
        import_move_main(&data, 1, &c),
        "the import's move takes the ref"
    );

    // The replan counter before the release: the loss must show on it.
    let before = scrape_replans(metrics).await;

    // A goes on. Its compare-and-swap lost the ref, and the finish
    // replans inside the same call: the attempt that lost reads the
    // import's head, and the land lands on it. The reply never shows
    // the loss - the chain below does: A's result sits on the import's
    // head, which only a plan that read the root and a settle that
    // lost the compare-and-swap can produce.
    release_park(&data, POINT, "a");
    assert_eq!(first.await.unwrap(), LandState::Landed);

    // The replan counter rose by exactly one: the loss happened, and
    // only once. Without it, a later change that moves the park point
    // before the plan would let A plan on the import's head, land
    // without a loss, and the chain would still hold.
    let after = scrape_replans(metrics).await;
    assert_eq!(after, before + 1, "the land replanned exactly once");

    // The chain is root, C, A's result: the import's move first, A's
    // land rebased onto it.
    let head = resolve_ref(&mut objects, "main").await.unwrap();
    let a_result = get_changeset(&mut scion, head).await;
    let [parent] = a_result.parents() else {
        panic!("A's result carries one parent");
    };
    assert_eq!(*parent, c, "A landed on the import's head");
    let c_result = get_changeset(&mut scion, *parent).await;
    let [parent] = c_result.parents() else {
        panic!("C carries one parent");
    };
    assert_eq!(*parent, root, "C sits on the root");
}
