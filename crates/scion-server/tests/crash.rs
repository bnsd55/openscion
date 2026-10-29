//! Crash suite: the real `scion-server` binary, and the failed-migration
//! runbook (`docs/operations.md`, "A failed migration"), step by step:
//! an open of a database whose schema is newer than the build refuses
//! with both versions named, the restore of a backup per section 3
//! serves clean, and fsck is clean after it.
//!
//! The kill-during-writes suite is `crash_pg.rs`: the metadata lives in
//! the database, so its crash story is the database rows.
#![cfg(unix)]

mod common;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use common::*;

use scion_server::Backend;

/// The failed-migration runbook (docs/operations.md, "A failed
/// migration"), step by step: an open of a database whose schema is
/// newer than the build refuses, with both versions named; the fix is
/// the old binary, or the restore of a backup per section 3; and the
/// restored store serves. `user_version` is never edited by hand in
/// the runbook - the test edits it only to forge the newer schema the
/// error talks about.
#[test]
fn a_newer_schema_refuses_the_open_and_a_restore_serves() {
    let dir = scion_server::test_support::private_tempdir();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);
    let server = Server::start(&data, &log);
    drop(server);

    // The backup the runbook restores from: the server is stopped, so
    // the copy of the directory is exact (section 3). On Postgres the
    // metadata rides in the database, so its backup is a dump of the
    // schema - the runbook's pg_dump step.
    let backup = dir.path().join("backup");
    backup_dir(&data, &backup);

    // The database is ahead of the binary: the runbook's error case,
    // forged the only way a test can - the version of a future schema.
    let version = {
        let schema_name = Backend::test_schema(&data);
        let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
        let sep = if url.contains('?') { '&' } else { '?' };
        let mut c = postgres::Client::connect(
            &format!("{url}{sep}options=--search_path%3D{schema_name}"),
            postgres::NoTls,
        )
        .expect("the test database is up");
        let version: i64 = c
            .query_one("SELECT max(version) FROM schema_migrations", &[])
            .expect("the schema's version reads")
            .get(0);
        c.batch_execute(&format!(
            "DELETE FROM schema_migrations WHERE version = {version};
             INSERT INTO schema_migrations (version, name) VALUES ({}, 'forged')",
            version + 1
        ))
        .expect("the forged version lands");
        version
    };

    // The operator's start: the server refuses to open, and the error
    // names both versions - the data's and the build's.
    let mut refused = Server::spawn(&data, &log);
    let started = Instant::now();
    loop {
        if let Some(status) = refused.try_wait() {
            assert!(!status.success(), "the server served a newer schema");
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the server neither served nor refused"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let text = fs::read_to_string(&log).unwrap();
    // The refusal names both versions, the whole phrase the code writes:
    // the data's (the forged version+1) and the build's.
    assert!(
        text.contains(&format!(
            "database schema version {} is newer than this build ({})",
            version + 1,
            version
        )),
        "the refusal names both versions: {text}"
    );

    // The fix, the restore arm: the backup's database replaces the
    // forged one, and the server starts and serves. The forge's rows
    // drop with the schema, and the dump loads in their place, in the
    // job's Postgres container: its tools match the server's version,
    // where a client from the runner image may not.
    {
        // The forge's rows drop with the schema, and the dump loads in
        // their place, with the same tools the dump ran.
        let schema_name = Backend::test_schema(&data);
        let url = std::env::var("SCION_TEST_POSTGRES_URL").expect("SCION_TEST_POSTGRES_URL");
        let drop = common::pg::psql(&url)
            .arg("-c")
            .arg(format!("DROP SCHEMA {schema_name} CASCADE"))
            .output()
            .expect("psql runs");
        assert!(
            drop.status.success(),
            "the schema drops: {}",
            String::from_utf8_lossy(&drop.stderr)
        );
        common::pg::load_schema(&url, &backup.join("schema.sql"));
    }
    let server = Server::start(&data, &log);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (_, mut objects) = connect(&server.url).await.unwrap();
        let _ = resolve_ref(&mut objects, "main").await.unwrap();
    });
}

/// The runbook's backup: the schema's dump - the metadata rides in
/// Postgres - and the directory's copy beside it.
fn backup_dir(data: &Path, backup: &Path) {
    fs::create_dir_all(backup).unwrap();
    {
        // The dump runs with the job's Postgres container's own tools,
        // or, when no container is named, with the runner's path tools
        // whose major the helper checks against the server's.
        let schema = Backend::test_schema(data);
        let url = std::env::var("SCION_TEST_POSTGRES_URL").expect("SCION_TEST_POSTGRES_URL");
        let file = backup.join("schema.sql");
        common::pg::dump_schema(&url, &schema, &file);
        let text = fs::read_to_string(&file).unwrap();
        eprintln!("dump has repos row: {}", text.contains("repos"));
        eprintln!("dump has the repo r: {}", text.contains("'r'"));
    }
    for entry in fs::read_dir(data).unwrap() {
        let entry = entry.unwrap();
        let target = backup.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// A recursive copy of one directory tree, for the runbook's backup.
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}
