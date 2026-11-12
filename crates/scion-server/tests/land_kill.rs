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

use common::*;
use scion_proto::v1::LandState;
use scion_server::test_support::private_tempdir;

/// The park point the seam names in the land path: between a plan and
/// its settle. The park holds the ref slot lock the plan and settle
/// share, and no Postgres transaction and no pool reader; the point's
/// comment in `land.rs` says it beside the code. A kill while parked
/// is exactly the order the issue names.
const POINT: &str = "after-plan";

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
