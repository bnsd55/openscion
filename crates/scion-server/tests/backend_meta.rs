//! The backend's small contract surfaces the first mutant run's suites
//! did not reach: the marker's UUID read of a wrong-length line, the
//! migration verdict, the fence flag, the metrics wiring, the
//! write-yield bound, and the unreadable marker. Each test fails if the
//! surface it names breaks.

use std::os::unix::fs::PermissionsExt as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use scion_server::test_support::meta::Meta as _;
use scion_server::{Backend, Metrics};

/// The archive manifest names the store the marker names, byte for
/// byte: an archive's manifest says where its metadata came from, not a
/// made-up or zero UUID.
#[test]
fn the_archive_manifest_names_the_stores_marker() {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let out = dir.path().join("archive");
    let manifest = scion_server::archive::export(&store, dir.path(), None, true, &out).unwrap();
    let marker = std::fs::read_to_string(dir.path().join("backend")).unwrap();
    let marker_uuid = marker
        .lines()
        .nth(1)
        .expect("the marker names the store UUID");
    assert_eq!(
        manifest["store_uuid"].as_str(),
        Some(marker_uuid),
        "{manifest}"
    );
}

/// A marker whose UUID line is all hex digits but the wrong length (31
/// here) reads as no UUID: the open adopts the directory and writes the
/// marker anew. Parsing off the end of the line would panic the open
/// instead, so this test fails if the length guard lets a wrong-length
/// line through.
#[test]
fn a_marker_with_a_wrong_length_uuid_line_is_adopted_and_rewritten() {
    let dir = scion_server::test_support::private_tempdir();
    let marker = dir.path().join("backend");
    std::fs::write(&marker, format!("postgres\n{}\n", "a".repeat(31))).unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    drop(store);
    let text = std::fs::read_to_string(&marker).unwrap();
    let uuid = text
        .lines()
        .nth(1)
        .expect("the adopted marker names the store");
    assert_eq!(uuid.len(), 32, "{text:?}");
    assert!(uuid.bytes().all(|b| b.is_ascii_hexdigit()), "{text:?}");
}

/// A marker the process cannot read (mode 000) refuses the open with
/// the read's own error: read as absent, a Postgres directory would
/// open as a fresh, empty store, which looks like a server with no data
/// at all. The refusal needs no Postgres, so these tests open against a
/// URL that cannot answer.
#[test]
fn an_unreadable_marker_refuses_the_open() {
    let dir = scion_server::test_support::private_tempdir();
    let marker = dir.path().join("backend");
    std::fs::write(&marker, format!("postgres\n{}\n", "a".repeat(32))).unwrap();
    std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o000)).unwrap();
    let Err(error) = Backend::open(dir.path(), "postgres://127.0.0.1:5/scion", None) else {
        panic!("an unreadable marker refuses the open");
    };
    let scion_server::Error::Io(io) = &error else {
        panic!("the refusal is the marker read's error, not {error:?}");
    };
    assert_eq!(
        io.kind(),
        std::io::ErrorKind::PermissionDenied,
        "the marker's read error, not another kind: {io}"
    );
}

/// An open that moves the schema reports the versions it moved, and a
/// fresh open reports none. The store here starts at version 3 (the
/// first three migrations, run by hand), so its open runs the rest and
/// the verdict names the move.
#[test]
fn an_open_that_migrates_reports_the_versions_it_moved() {
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let dir = scion_server::test_support::private_tempdir();
    let schema = format!("scion_test_migrated_{}", std::process::id());
    let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema};
             SET search_path TO {schema};
             CREATE TABLE schema_migrations (
                 version BIGINT PRIMARY KEY,
                 name    TEXT NOT NULL
             );"
        ))
        .unwrap();
    for (n, file) in [
        "0001_init.sql",
        "0002_view_roots.sql",
        "0003_view_trees.sql",
    ]
    .into_iter()
    .enumerate()
    {
        let sql = std::fs::read_to_string(format!(
            "{}/migrations/postgres/{file}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        client
            .batch_execute(&format!(
                "{sql}\nINSERT INTO schema_migrations (version, name) \
                 VALUES ({}, 'postgres schema')",
                n + 1
            ))
            .unwrap();
    }
    let store = Backend::open_test_shared_in_schema(dir.path(), &schema).unwrap();
    assert_eq!(
        store.migrated(),
        Some((
            3,
            scion_server::test_support::backend::schema_version::POSTGRES
        )),
        "the open names the versions its migrations moved"
    );
    // A fresh open creates the schema and reports no move.
    let fresh_dir = scion_server::test_support::private_tempdir();
    let fresh = Backend::open_test(fresh_dir.path()).unwrap();
    assert_eq!(fresh.migrated(), None, "a fresh open migrated nothing");
}

/// The fence flag is the backend's own flag, not a fresh one each call:
/// the health service holds the Arc the fence trips, and its flip must
/// be the backend's `fenced` answer.
#[test]
fn the_fence_flag_is_the_backends_own_flag() {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let flag = store.fence_flag();
    let again = store.fence_flag();
    assert!(
        Arc::ptr_eq(&flag, &again),
        "the flag is the backend's own, not a fresh one each call"
    );
    assert!(!store.fenced(), "the fence stands until it trips");
    flag.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(store.fenced(), "the flag's flip is the backend's fence");
}

/// The age of the oldest open read transaction grows past zero while a
/// read holds: the gauge an operator watches for a stuck reader. Zero
/// always would hide every long read.
#[test]
fn the_tx_age_grows_while_a_read_transaction_stays_open() {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let (opened_tx, opened_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let store_ref = &store;
    std::thread::scope(|s| {
        s.spawn(move || {
            store_ref
                .read(|_c| {
                    opened_tx.send(()).unwrap();
                    let _ = release_rx.recv_timeout(Duration::from_secs(60));
                    Ok(())
                })
                .unwrap();
        });
        opened_rx.recv_timeout(Duration::from_secs(60)).unwrap();
        let mut age = Duration::ZERO;
        for _ in 0..6_000 {
            age = store.pg_tx_age();
            if age > Duration::ZERO {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(age > Duration::ZERO, "the open read's age grows past zero");
        release_tx.send(()).unwrap();
    });
}

/// A write that waits for the collector's key records the wait in the
/// registry the backend was given: an operator watches that gauge for a
/// busy database, and a registry the backend never wired stays empty.
#[test]
fn a_write_records_its_collector_key_wait_in_the_backend_metrics() {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let metrics = Arc::new(Metrics::default());
    store.set_metrics(Arc::clone(&metrics));
    assert!(
        !metrics.render().contains("scion_pg_collector_key_wait"),
        "nothing waited yet: {}",
        metrics.render()
    );
    // Another client of the test holds the collector's key, so the
    // write below really waits.
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let schema = Backend::test_schema(dir.path());
    let mut holder = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    holder
        .batch_execute(&format!(
            "SET search_path TO {schema};
             BEGIN;
             SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':gc'));"
        ))
        .unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let store_ref = &store;
    std::thread::scope(|s| {
        let writer = s.spawn(move || {
            store_ref.write(|_c| Ok::<(), scion_server::Error>(()))?;
            done_tx.send(()).unwrap();
            Ok::<(), scion_server::Error>(())
        });
        // The writer reaches the key on its own schedule; the release
        // follows a bounded spin, not a sleep that decides.
        for _ in 0..2_000 {
            std::thread::yield_now();
        }
        holder.batch_execute("COMMIT").unwrap();
        let mut spins = 0;
        while !writer.is_finished() {
            spins += 1;
            assert!(spins < 1_000_000, "the writer never finished");
            std::thread::yield_now();
        }
    });
    done_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the write ended");
    let text = metrics.render();
    assert!(
        text.contains("scion_pg_collector_key_wait_seconds_count 1\n"),
        "{text}"
    );
}

/// A wait for writers while a writer waits runs the yield's bound: the
/// derivation step yields to the writer for at most the bound, and the
/// bound is real time, not an instant Ok. A writer holds the write
/// connection the whole wait, so the only exit is the bound.
#[test]
fn a_wait_for_writers_yields_its_bound_while_a_writer_waits() {
    use scion_server::test_support::deadline::Deadline;
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let (holding_tx, holding_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let store_ref = &store;
    std::thread::scope(|s| {
        // The holder takes the write connection and keeps it until the
        // test releases it, after the wait below has returned.
        s.spawn(move || {
            store_ref
                .write(|_c| {
                    holding_tx.send(()).unwrap();
                    let _ = release_rx.recv_timeout(Duration::from_secs(60));
                    Ok(())
                })
                .unwrap();
        });
        holding_rx.recv_timeout(Duration::from_secs(60)).unwrap();
        // A writer queues behind the holder; a short-deadline wait
        // confirms it waits, without a sleep that decides.
        let queued = s.spawn(move || store_ref.write(|_c| Ok(())));
        let mut confirmed = false;
        for _ in 0..60 {
            if store_ref
                .yield_to_writers(Some(&Deadline::new(
                    Instant::now() + Duration::from_millis(200),
                )))
                .is_err()
            {
                confirmed = true;
                break;
            }
        }
        assert!(confirmed, "the writer queued behind the holder");
        // The yield runs its bound: the writer still waits, so nothing
        // else can end the wait early.
        let started = Instant::now();
        store_ref.yield_to_writers(None).unwrap();
        let yielded = started.elapsed();
        assert!(
            yielded >= Duration::from_secs(1),
            "the yield runs its bound while a writer waits: {yielded:?}"
        );
        release_tx.send(()).unwrap();
        queued.join().unwrap().unwrap();
    });
}
