//! The `scion-server preflight` command, as the shell runs it: the
//! binary prints one line per check and exits 0, 1 or 2, and the
//! flags' rules are serve's own. The module tests cover the checks;
//! this one covers the wiring.

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::process::{Command, Output};

/// A temp directory the run owns outright: mode 0700, whatever the
/// umask. The data-directory check refuses one group or others can
/// read.
fn private_tempdir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut mode = fs::metadata(dir.path()).unwrap().permissions();
    mode.set_mode(0o700);
    fs::set_permissions(dir.path(), mode).unwrap();
    dir
}

/// A secret file the CLI reads: mode 0600, as the checked read wants.
fn secret(dir: &std::path::Path, name: &str, text: &str) -> std::path::PathBuf {
    let file = dir.join(name);
    fs::write(&file, text).unwrap();
    let mut mode = fs::metadata(&file).unwrap().permissions();
    mode.set_mode(0o600);
    fs::set_permissions(&file, mode).unwrap();
    file
}

/// A store ready for a first boot: the test database, a marker, a
/// schema at this build's version, no server holding the lock.
fn ready_store() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = private_tempdir();
    let url = scion_server::Backend::test_postgres_url(dir.path()).unwrap();
    let store = scion_server::Backend::open_test(dir.path()).unwrap();
    drop(store);
    let url_file = secret(dir.path(), "postgres-url", &url);
    let token_file = secret(dir.path(), "token", &"t".repeat(32));
    (dir, url_file, token_file)
}

/// Runs `scion-server preflight` with `args`, and fails with the
/// child's own words when it does not exit at all.
fn preflight(data: &std::path::Path, extra: &[String]) -> Output {
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data)
        .arg("preflight")
        .args(extra)
        .env("NO_COLOR", "1")
        .output()
        .unwrap();
    assert!(
        out.status.code().is_some(),
        "the child did not exit: {}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// A ready store answers its checks: the lines name what the operator
/// reads, and the run asserts no runner-dependent exit.
#[test]
fn a_ready_store_passes_the_binary_preflight() {
    let (dir, url_file, token_file) = ready_store();
    let url = fs::read_to_string(&url_file).unwrap();
    let out = preflight(
        dir.path(),
        &[
            format!("--postgres-url-file={}", url_file.display()),
            format!("--token-file={}", token_file.display()),
        ],
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("ok data directory"), "{text}");
    assert!(text.contains("ok schema version"), "{text}");
    assert!(text.contains("ok store pairing"), "{text}");
    assert!(!url.is_empty(), "the URL is not for the assert to keep");
}

/// A URL that does not parse fails naming the parse error and where
/// the URL was read from, never the URL's text: the operator can tell
/// a typo in the URL from a database that is down, and the file may
/// hold a password. (#963)
#[test]
fn a_malformed_postgres_url_names_the_parse_error_and_the_source() {
    let dir = private_tempdir();
    let store = scion_server::Backend::open_test(dir.path()).unwrap();
    drop(store);
    let url_file = secret(
        dir.path(),
        "postgres-url",
        "postgres://user:secret-password@dbhost:notaport/scion",
    );
    let token_file = secret(dir.path(), "token", &"t".repeat(32));
    let out = preflight(
        dir.path(),
        &[
            format!("--postgres-url-file={}", url_file.display()),
            format!("--token-file={}", token_file.display()),
        ],
    );
    assert_eq!(out.status.code(), Some(2), "exit {:?}", out.status.code());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("fail database"), "{text}");
    assert!(
        text.contains("cannot parse the database URL"),
        "the parse error is not named: {text}"
    );
    assert!(
        text.contains("postgres-url"),
        "the file the URL was read from is not named: {text}"
    );
    assert!(
        !text.contains("secret-password") && !text.contains("dbhost"),
        "the URL's own text leaked into the answer: {text}"
    );
}

/// A directory that holds a sqlite store fails with exit 2, the
/// open's refusal's words on its line.
#[test]
fn a_sqlite_directory_fails_the_binary_preflight() {
    let (dir, url_file, token_file) = ready_store();
    fs::write(dir.path().join("scion.db"), b"sqlite bytes\n").unwrap();
    let out = preflight(
        dir.path(),
        &[
            format!("--postgres-url-file={}", url_file.display()),
            format!("--token-file={}", token_file.display()),
        ],
    );
    assert_eq!(out.status.code(), Some(2), "exit {:?}", out.status.code());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("fail data directory") && text.contains("SQLite metadata database"),
        "{text}"
    );
}

/// No token source anywhere: serve's own flags check fails, and the
/// exit code is 2.
#[test]
fn a_missing_token_source_fails_the_binary_preflight() {
    let (dir, url_file, _token_file) = ready_store();
    let out = preflight(
        dir.path(),
        &[format!("--postgres-url-file={}", url_file.display())],
    );
    assert_eq!(out.status.code(), Some(2), "exit {:?}", out.status.code());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("fail serve flags") && text.contains("token source"),
        "{text}"
    );
}

/// A non-loopback listen address without TLS fails: serve's own rule.
#[test]
fn an_insecure_listen_fails_the_binary_preflight() {
    let (dir, url_file, token_file) = ready_store();
    let out = preflight(
        dir.path(),
        &[
            format!("--postgres-url-file={}", url_file.display()),
            format!("--token-file={}", token_file.display()),
            "--listen=0.0.0.0:7878".to_owned(),
        ],
    );
    assert_eq!(out.status.code(), Some(2), "exit {:?}", out.status.code());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("fail serve flags") && text.contains("loopback"),
        "{text}"
    );
}

/// A TLS certificate that does not read fails: serve's own loader.
#[test]
fn a_bad_tls_certificate_fails_the_binary_preflight() {
    let (dir, url_file, token_file) = ready_store();
    let cert = dir.path().join("cert.pem");
    fs::write(&cert, b"not a certificate\n").unwrap();
    let out = preflight(
        dir.path(),
        &[
            format!("--postgres-url-file={}", url_file.display()),
            format!("--token-file={}", token_file.display()),
            format!("--tls-cert={}", cert.display()),
            format!("--tls-key={}", cert.display()),
        ],
    );
    assert_eq!(out.status.code(), Some(2), "exit {:?}", out.status.code());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("fail serve flags"), "{text}");
}

/// A ticket key that does not read fails: serve's own loader.
#[test]
fn a_bad_ticket_key_fails_the_binary_preflight() {
    let (dir, url_file, token_file) = ready_store();
    let seed = dir.path().join("ticket");
    fs::write(&seed, b"short\n").unwrap();
    let out = preflight(
        dir.path(),
        &[
            format!("--postgres-url-file={}", url_file.display()),
            format!("--token-file={}", token_file.display()),
            format!("--ticket-key={}", seed.display()),
        ],
    );
    assert_eq!(out.status.code(), Some(2), "exit {:?}", out.status.code());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("fail serve flags"), "{text}");
}

/// An empty `SCION_POSTGRES_URL` counts as no URL: the check asks for
/// one, with the same words the missing variable uses, not the parse
/// error the empty string would trip.
#[test]
fn an_empty_url_variable_asks_for_a_url() {
    let dir = private_tempdir();
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(dir.path())
        .arg("preflight")
        .env("NO_COLOR", "1")
        .env("SCION_POSTGRES_URL", "")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "exit {:?}", out.status.code());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("no Postgres URL"),
        "the empty variable asks for a URL:\n{text}"
    );
}
