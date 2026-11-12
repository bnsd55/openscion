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

use common::*;
use scion_proto::v1::LandState;
use scion_server::test_support::private_tempdir;

/// The park point the seam names in the land path: after the settle's
/// write transaction commits, before the reply goes out. The park
/// holds the ref slot lock the plan and settle share, and no Postgres
/// transaction and no pool reader; the point's comment in `land.rs`
/// says it beside the code.
const POINT: &str = "after-settle";

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
    // call is still out - the park holds before the reply - so the
    // drop takes a call whose reply never went out.
    assert!(
        !parked.is_finished(),
        "the park must hold before the reply, or the drop loses nothing"
    );
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
