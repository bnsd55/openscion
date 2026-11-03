//! The explicit migrations (docs/operations.md, "Upgrades"): the check
//! that reads the state without touching anything, the migrate command
//! that moves the schema on purpose, and the refusal every other store
//! opener gives a database that is behind the build.

mod common;

use common::*;
use std::fs;
use std::io::Write as _;

/// The harness's 0600 token file, made here: the manual spawns read it
/// before any `Server` start would have written it.
fn ensure_token() -> &'static std::path::PathBuf {
    let path = token_file();
    if !path.exists() {
        use std::os::unix::fs::OpenOptionsExt as _;
        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .unwrap()
            .write_all(b"migrations-test\n")
            .unwrap();
    }
    path
}

/// The test database's base URL, with the schema appended as the
/// search path: the shape an operator's URL takes.
fn url_for(url: &str, schema: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}options=--search_path%3D{schema}")
}

/// Seeds a store at schema version 5: the first five migration files,
/// applied by hand, with the ledger the open's runner makes. The
/// database is behind this build by exactly one step, which is the
/// state the next upgrade starts from.
fn seed_behind_store(url: &str, schema: &str) {
    let mut client = postgres::Client::connect(&url_for(url, schema), postgres::NoTls)
        .expect("the test database connects");
    // A rerun of the suite meets the last run's tables: the seed is the
    // version-5 state, so the schema starts empty.
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .unwrap();
    client
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .unwrap();
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    for (n, sql) in [
        include_str!("../migrations/postgres/0001_init.sql"),
        include_str!("../migrations/postgres/0002_view_roots.sql"),
        include_str!("../migrations/postgres/0003_view_trees.sql"),
        include_str!("../migrations/postgres/0004_store_uuid.sql"),
        include_str!("../migrations/postgres/0005_parent_numbering.sql"),
    ]
    .into_iter()
    .enumerate()
    {
        client.batch_execute(sql).expect("the migration applies");
        client
            .execute(
                "INSERT INTO schema_migrations (version, name) VALUES ($1, 'postgres schema')",
                &[&(i64::try_from(n + 1).unwrap())],
            )
            .unwrap();
    }
}

/// A fresh database: the check reports version 0, every step pending,
/// and exits 3 - the code that says "steps to run", not "failed".
#[test]
fn a_check_on_a_fresh_database_prints_the_steps_and_exits_3() {
    let base = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL must name the test database");
    let url = url_for(&base, "mig_check_fresh");
    let dir = scion_server::test_support::private_tempdir();
    let run = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", dir.path().join("data").to_str().unwrap()])
        .args(["--postgres-url", &url])
        .args(["migrate", "--check"])
        .env("NO_COLOR", "1")
        .output()
        .expect("the check runs");
    assert_eq!(run.status.code(), Some(3), "pending is its own exit code");
    let out = String::from_utf8_lossy(&run.stdout);
    assert!(out.contains("schema version 0, this build ships"), "{out}");
    assert!(out.contains("would run: 0001_init"), "{out}");
    assert!(out.contains("would run: 0006_land_reason"), "{out}");
}

/// A database one version behind: the check names the pending step and
/// exits 3; `migrate` applies it; the check afterwards exits 0.
#[test]
fn a_migrate_applies_the_pending_steps_and_the_check_is_clean() {
    let base = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL must name the test database");
    seed_behind_store(&base, "mig_apply");
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    let run = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .args(["--postgres-url", &url_for(&base, "mig_apply")])
        .args(["migrate", "--check"])
        .env("NO_COLOR", "1")
        .output()
        .expect("the check runs");
    assert_eq!(run.status.code(), Some(3), "the store is one step behind");
    let out = String::from_utf8_lossy(&run.stdout);
    assert!(
        out.contains("schema version 5, this build ships 6"),
        "{out}"
    );
    assert!(out.contains("would run: 0006_land_reason"), "{out}");

    let moved = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .args(["--postgres-url", &url_for(&base, "mig_apply")])
        .args(["migrate"])
        .env("NO_COLOR", "1")
        .output()
        .expect("the migrate runs");
    assert!(moved.status.success(), "the migrate runs: {moved:?}");
    assert!(
        String::from_utf8_lossy(&moved.stdout).contains("from version 5 to 6"),
        "{}",
        String::from_utf8_lossy(&moved.stdout)
    );
    let check = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .args(["--postgres-url", &url_for(&base, "mig_apply")])
        .args(["migrate", "--check"])
        .env("NO_COLOR", "1")
        .output()
        .expect("the check runs");
    assert_eq!(check.status.code(), Some(0), "nothing is pending now");
}

/// `serve` without `--migrate` refuses a behind database, with the fix
/// named; `serve --migrate` moves it and serves.
#[test]
fn a_serve_refuses_a_behind_database_until_asked_to_migrate() {
    let base = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL must name the test database");
    let schema = "mig_serve_refusal";
    let url = url_for(&base, schema);
    seed_behind_store(&base, schema);
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    let refused = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .args(["--postgres-url", &url])
        .args(["serve", "--listen", "127.0.0.1:0", "--token-file"])
        .arg(ensure_token())
        .env("NO_COLOR", "1")
        .output()
        .expect("the refused serve runs");
    assert!(!refused.status.success(), "a behind database is refused");
    let why = String::from_utf8_lossy(&refused.stderr);
    assert!(
        why.contains("run `scion-server migrate` first, or pass --migrate to serve"),
        "the refusal names the fix: {why}"
    );

    // Asked for it, the same serve moves the schema and serves.
    let log = dir.path().join("serve.log");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .args(["--postgres-url", &url])
        .args([
            "serve",
            "--listen",
            "127.0.0.1:0",
            "--token-file",
            ensure_token().to_str().unwrap(),
            "--migrate",
        ])
        .env("NO_COLOR", "1")
        .stdout(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log)
                .unwrap(),
        )
        .stderr(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log)
                .unwrap(),
        )
        .spawn()
        .expect("the migrating serve starts");
    let start = std::time::Instant::now();
    loop {
        if bound_addr(&log).is_some() {
            break;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "the migrating serve exited: {status}: {}",
                fs::read_to_string(&log).unwrap_or_default()
            );
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(60),
            "no listen"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let check = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--postgres-url", &url])
        .args(["migrate", "--check"])
        .env("NO_COLOR", "1")
        .output()
        .expect("the check runs");
    assert_eq!(check.status.code(), Some(0), "the serve migrated it");
    child.kill().unwrap();
    child.wait().unwrap();
}

/// A fresh database is still created by `serve`: the refusal is for a
/// database that is behind, not for one that is empty.
#[test]
fn a_serve_still_creates_a_fresh_database() {
    let base = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL must name the test database");
    let url = url_for(&base, "mig_serve_fresh");
    // The operator's one-time step before the first start: the search
    // path's schema exists, and holds no tables yet.
    postgres::Client::connect(&base, postgres::NoTls)
        .unwrap()
        .batch_execute("CREATE SCHEMA IF NOT EXISTS mig_serve_fresh")
        .unwrap();
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    let log = dir.path().join("serve.log");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .args(["--postgres-url", &url])
        .args(["serve", "--listen", "127.0.0.1:0", "--token-file"])
        .arg(ensure_token())
        .env("NO_COLOR", "1")
        .stdout(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log)
                .unwrap(),
        )
        .stderr(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log)
                .unwrap(),
        )
        .spawn()
        .expect("the fresh serve starts");
    let start = std::time::Instant::now();
    while bound_addr(&log).is_none() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "the fresh serve exited: {status}: {}",
                fs::read_to_string(&log).unwrap_or_default()
            );
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(60),
            "no listen"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    child.kill().unwrap();
    child.wait().unwrap();
}

/// A `migrate` beside a live server refuses: the server holds the
/// store's one-server lock for its life, and a schema moved under it
/// could break a build that does not know the new shape. The refusal
/// names the fix, and the version the server already brought up stays
/// as it is.
#[test]
fn a_migrate_beside_a_live_server_refuses_and_moves_nothing() {
    let base = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL must name the test database");
    let schema = "mig_beside_server";
    let url = url_for(&base, schema);
    seed_behind_store(&base, schema);
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    // The server is an unattended start: it migrates the store to this
    // build's version and serves, holding the one-server lock.
    let log = dir.path().join("serve.log");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .args(["--postgres-url", &url])
        .args([
            "serve",
            "--listen",
            "127.0.0.1:0",
            "--token-file",
            ensure_token().to_str().unwrap(),
            "--migrate",
        ])
        .env("NO_COLOR", "1")
        .stdout(fs::OpenOptions::new().create(true).append(true).open(&log).unwrap())
        .stderr(fs::OpenOptions::new().create(true).append(true).open(&log).unwrap())
        .spawn()
        .expect("the migrating serve starts");
    let start = std::time::Instant::now();
    while bound_addr(&log).is_none() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "the migrating serve exited: {status}: {}",
                fs::read_to_string(&log).unwrap_or_default()
            );
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(60), "no listen");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let version = || -> String {
        let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        client
            .query_opt("SELECT max(version)::text FROM schema_migrations", &[])
            .unwrap()
            .map(|row| row.get::<_, String>(0))
            .unwrap_or_default()
    };
    assert_eq!(version(), "6", "the serve migrated the store");
    let refused = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .args(["--postgres-url", &url])
        .args(["migrate"])
        .env("NO_COLOR", "1")
        .output()
        .expect("the refused migrate runs");
    assert!(!refused.status.success(), "a migrate beside a server refuses");
    let why = String::from_utf8_lossy(&refused.stderr);
    assert!(why.contains("another server holds this store"), "{why}");
    assert!(why.contains("stop the server first"), "{why}");
    assert_eq!(version(), "6", "the refusal moved no version");
    child.kill().unwrap();
    child.wait().unwrap();
    // Stopped, the same migrate runs: nothing is pending.
    let clean = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .args(["--postgres-url", &url])
        .args(["migrate"])
        .env("NO_COLOR", "1")
        .output()
        .expect("the migrate after the stop runs");
    assert!(clean.status.success(), "the migrate runs once the server is gone");
}
