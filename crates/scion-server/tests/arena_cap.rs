//! The serving processes cap their glibc malloc arenas (#1421): a
//! server whose thread count rises stepped its RSS by tens of MB once
//! while its live heap stayed at ~11 MB - a new arena's heap. The cap
//! applies at the very start of main, before the tokio runtime's
//! workers exist, and the operator's `MALLOC_ARENA_MAX` wins over it.
//! The observable is the startup log, so this test drives the real
//! binary and reads what it logged.
//!
//! The mallopt call itself is not observable in-process without
//! external tooling (an `LD_PRELOAD` hook or `mallinfo` deltas a busy
//! process races with), so there is no in-process assertion of it: the
//! log line is the serve-path check.

#![cfg(unix)]

use std::process::Command;
use std::time::{Duration, Instant};

/// The test database, as the other Postgres suites take it.
fn pg_url() -> String {
    std::env::var("SCION_TEST_POSTGRES_URL").expect("SCION_TEST_POSTGRES_URL")
}

/// A server URL on this test's own schema, recorded here and dropped
/// at the test's end.
fn server_url(schema: &str) -> String {
    format!(
        "{}?options=--search_path%3D{schema}%20-c%20application_name%3Darena1421",
        pg_url().trim_end_matches('/')
    )
}

/// The session's `search_path` must hold a schema that exists before a
/// server may open it: make the test's own.
fn create_schema(schema: &str) {
    run_sql(&format!("CREATE SCHEMA {schema}"));
}

fn drop_schema(schema: &str) {
    run_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
}

fn run_sql(sql: &str) {
    postgres::Client::connect(&pg_url(), postgres::NoTls)
        .expect("the test database connects")
        .batch_execute(sql)
        .expect("the statement applies");
}

/// Runs `scion-server serve`, the server's stderr in `log`, `env`
/// added to the environment.
fn spawn_serve(
    data: &std::path::Path,
    log: &std::path::Path,
    url: &str,
    env: &[(&str, &str)],
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    command
        .arg("--data")
        .arg(data)
        .env("SCION_POSTGRES_URL", url)
        .env("SCION_TOKEN", "arena-cap-token")
        .args(["serve", "--listen", "127.0.0.1:0"])
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(log).expect("the log file is made"));
    for (key, value) in env {
        command.env(key, value);
    }
    command
}

/// Waits for the `serving` line, bounded: a server that never serves
/// fails the test instead of hanging it.
fn wait_for_serving(log: &std::path::Path) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if std::fs::read_to_string(log).is_ok_and(|text| text.contains("serving listen=")) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

#[test]
fn serve_logs_the_arena_cap_decision() {
    let base = pg_url();
    let schema = format!("scion_test_1421_arena_{}", std::process::id() % 100_000);
    create_schema(&schema);
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    let url = server_url(&schema);

    // The built-in cap: applied when the environment is silent.
    let log = dir.path().join("serve-default.log");
    let mut child = spawn_serve(&data, &log, &url, &[])
        .spawn()
        .expect("the server spawns");
    assert!(
        wait_for_serving(&log),
        "the server served: {}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    let log_text = std::fs::read_to_string(&log).expect("the log reads");
    #[cfg(target_os = "linux")]
    assert!(
        log_text.contains("malloc arenas capped at 2"),
        "the serve path applies the built-in cap: {log_text}"
    );
    #[cfg(not(target_os = "linux"))]
    assert!(
        !log_text.contains("malloc arenas capped"),
        "no cap applies on this platform: {log_text}"
    );
    let _ = child.kill();
    let _ = child.wait();

    // The operator's value wins: the log names the value, no cap runs.
    let log2 = dir.path().join("serve-env.log");
    let mut child2 = spawn_serve(&data, &log2, &url, &[("MALLOC_ARENA_MAX", "4")])
        .spawn()
        .expect("the second server spawns");
    assert!(
        wait_for_serving(&log2),
        "the server served with the operator's value: {}",
        std::fs::read_to_string(&log2).unwrap_or_default()
    );
    let log_text2 = std::fs::read_to_string(&log2).expect("the second log reads");
    assert!(
        log_text2.contains("malloc arenas left to MALLOC_ARENA_MAX=4"),
        "the operator's MALLOC_ARENA_MAX wins: {log_text2}"
    );
    let _ = child2.kill();
    let _ = child2.wait();

    drop_schema(&schema);
    let _ = base;
}
