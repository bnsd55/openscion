//! The `scion-server preflight` command, as the shell runs it: the
//! binary prints one line per check and exits 0, 1 or 2. The module
//! tests cover the checks; this one covers the wiring.

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

/// Runs `scion-server preflight` on `data`.
fn preflight(
    data: &std::path::Path,
    url_file: &std::path::Path,
    token_file: &std::path::Path,
) -> Output {
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data)
        .arg("--postgres-url-file")
        .arg(url_file)
        .args(["preflight", "--token-file"])
        .arg(token_file)
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

/// A ready store passes everything: exit 0, an ok line per check.
#[test]
fn a_ready_store_passes_the_binary_preflight() {
    let (dir, url_file, token_file) = ready_store();
    let out = preflight(dir.path(), &url_file, &token_file);
    assert!(out.status.success(), "exit {:?}", out.status.code());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("ok data directory"), "{text}");
    assert!(text.contains("ok schema version"), "{text}");
    assert!(text.contains("ok store pairing"), "{text}");
}

/// A directory that holds a sqlite store fails with exit 2, the
/// refusal's words on its line.
#[test]
fn a_sqlite_directory_fails_the_binary_preflight() {
    let (dir, url_file, token_file) = ready_store();
    fs::write(dir.path().join("scion.db"), b"sqlite bytes\n").unwrap();
    let out = preflight(dir.path(), &url_file, &token_file);
    assert_eq!(out.status.code(), Some(2), "exit {:?}", out.status.code());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("fail data directory") && text.contains("SQLite metadata database"),
        "{text}"
    );
}
