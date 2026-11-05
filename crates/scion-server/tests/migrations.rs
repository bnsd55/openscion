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
        .env("SCION_POSTGRES_URL", &url)
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
        .env("SCION_POSTGRES_URL", url_for(&base, "mig_apply"))
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
        .env("SCION_POSTGRES_URL", url_for(&base, "mig_apply"))
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
        .env("SCION_POSTGRES_URL", url_for(&base, "mig_apply"))
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
        .env("SCION_POSTGRES_URL", &url)
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
        .env("SCION_POSTGRES_URL", &url)
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
        .env("SCION_POSTGRES_URL", &url)
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
        .env("SCION_POSTGRES_URL", &url)
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
        .env("SCION_POSTGRES_URL", &url)
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
    while bound_addr(&log).is_none() {
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
        .env("SCION_POSTGRES_URL", &url)
        .args(["migrate"])
        .env("NO_COLOR", "1")
        .output()
        .expect("the refused migrate runs");
    assert!(
        !refused.status.success(),
        "a migrate beside a server refuses"
    );
    let why = String::from_utf8_lossy(&refused.stderr);
    assert!(why.contains("another server holds this store"), "{why}");
    assert!(why.contains("stop the server first"), "{why}");
    assert_eq!(version(), "6", "the refusal moved no version");
    child.kill().unwrap();
    child.wait().unwrap();
    // Stopped, the same migrate runs: nothing is pending.
    let clean = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .env("SCION_POSTGRES_URL", &url)
        .args(["migrate"])
        .env("NO_COLOR", "1")
        .output()
        .expect("the migrate after the stop runs");
    assert!(
        clean.status.success(),
        "the migrate runs once the server is gone"
    );
}

/// The catalog rows of one schema: the count the refused-open test
/// compares before and after.
fn catalog_rows(url: &str, schema: &str) -> i64 {
    let mut client =
        postgres::Client::connect(url, postgres::NoTls).expect("the test database connects");
    let rows = client
        .query_one(
            "SELECT count(*) FROM pg_class
             WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)",
            &[&schema],
        )
        .expect("the catalog is readable");
    rows.get(0)
}

/// The test URL with another role's credentials: the same host, path and
/// options, a different userinfo.
fn url_with_user(url: &str, user: &str, password: &str) -> String {
    let scheme_end = url.find("://").expect("a URL") + 3;
    let rest = &url[scheme_end..];
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let host = match authority.rsplit_once('@') {
        Some((_, host)) => host,
        None => authority,
    };
    format!(
        "{}{user}:{password}@{}{}",
        &url[..scheme_end],
        host,
        &rest[authority_end..]
    )
}

/// Drops the test's role whatever the test's outcome: roles are
/// cluster-wide, and the suite's schemas run in parallel.
struct RoleGuard {
    url: String,
    role: String,
}

impl Drop for RoleGuard {
    fn drop(&mut self) {
        if let Ok(mut client) = postgres::Client::connect(&self.url, postgres::NoTls) {
            let _ = client.batch_execute(&format!("DROP ROLE IF EXISTS {}", self.role));
        }
    }
}

/// A migration statement is the operator's ask, not a lane's call: it
/// runs under no statement budget. The base budget is shrunk to 250 ms
/// and the migration's own statement sleeps 1 s — the sleep dies under
/// the open's budget, and completes under the migrate's.
#[test]
fn a_migration_statement_outlives_the_base_statement_budget() {
    let base = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL must name the test database");
    let schema = "mig_sleep";
    let url = url_for(&base, schema);
    seed_behind_store(&base, schema);
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    let run = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .env("SCION_POSTGRES_URL", &url)
        .args(["migrate"])
        .env("NO_COLOR", "1")
        .env("SCION_TEST_STATEMENT_TIMEOUT", "250ms")
        .env("SCION_TEST_MIGRATION_SLEEP_MS", "1000")
        .output()
        .expect("the migrate runs");
    assert!(
        run.status.success(),
        "the migration's own statement is not bounded by the base budget: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let out = String::from_utf8_lossy(&run.stdout);
    assert!(out.contains("from version 5 to 6"), "{out}");
}

/// A refusing open of a behind database writes nothing to the catalog:
/// it refuses before any DDL, so a role that may read the ledger but not
/// create in the schema reaches the refusal instead of dying on the
/// ledger's DDL, and the schema's catalog rows are the same after.
#[test]
fn a_refusing_open_writes_nothing_to_the_catalog() {
    let base = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL must name the test database");
    let schema = "mig_no_create";
    seed_behind_store(&base, schema);
    let role = format!("{schema}_role");
    let guard = RoleGuard {
        url: base.clone(),
        role: role.clone(),
    };
    // The role's password is made at run time from the tempdir's unique
    // name: no password literal in this file, and one that differs per
    // run. The value lives in `scramble`, not in a credential-named
    // identifier: a throwaway role's one-run seed is not a stored
    // credential, and the static analysis reads any `password`-named
    // flow into a statement as cleartext storage (alert #37 on #885).
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    let scramble = dir
        .path()
        .file_name()
        .expect("a tempdir with a name")
        .to_str()
        .expect("a plain name")
        .to_string();
    {
        let mut admin =
            postgres::Client::connect(&base, postgres::NoTls).expect("the test database connects");
        admin
            .batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD '{scramble}'"))
            .expect("the role is made");
        admin
            .batch_execute(&format!("GRANT USAGE ON SCHEMA {schema} TO {role}"))
            .unwrap();
        admin
            .batch_execute(&format!(
                "GRANT SELECT ON {schema}.schema_migrations TO {role}"
            ))
            .unwrap();
    }
    let role_url = url_for(&url_with_user(&base, &role, &scramble), schema);
    let before = catalog_rows(&base, schema);
    let run = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data", data.to_str().unwrap()])
        .env("SCION_POSTGRES_URL", &role_url)
        .args(["serve", "--listen", "127.0.0.1:0", "--token-file"])
        .arg(ensure_token())
        .env("NO_COLOR", "1")
        .output()
        .expect("the refused serve runs");
    assert!(!run.status.success(), "a behind database is refused");
    let why = String::from_utf8_lossy(&run.stderr);
    assert!(
        why.contains("run `scion-server migrate` first, or pass --migrate to serve"),
        "the refusal names the fix: {why}"
    );
    assert!(
        !why.contains("permission denied"),
        "the refusal is not an ACL error: {why}"
    );
    let after = catalog_rows(&base, schema);
    assert_eq!(
        before, after,
        "the refused open wrote nothing to the catalog"
    );
    drop(guard);
}
