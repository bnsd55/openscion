//! A restore must honor the Postgres URL from every source it names:
//! `--postgres-url`, `--postgres-url-file` and `SCION_POSTGRES_URL`. The
//! bug this pins: `restore` took the URL from the argv flag only, so a
//! URL from the file or the environment silently restored into SQLite,
//! and `--postgres-ca` beside a file or env URL was refused by clap.
//! These tests need the test database; they skip themselves when the
//! environment does not name Postgres as the test backend.

#![cfg(unix)]

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(args)
        .env("NO_COLOR", "1")
        .output()
        .expect("the scion-server command runs")
}

fn must_run(args: &[&str], what: &str) {
    let out = run(args);
    assert!(
        out.status.success(),
        "{what} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The test database URL with `data`'s schema as the search path, the
/// shape the test harness gives a server, written to `file`.
fn write_url_file(data: &Path, file: &Path) {
    let url = common::postgres_url(data);
    fs::write(file, &url).unwrap();
    // The URL is a secret: the checked read refuses a file that group
    // or others can read.
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(file, fs::Permissions::from_mode(0o600)).unwrap();
    }
}

/// A store with one repo, made by an import whose URL came from a file:
/// the same source the restore under test reads.
fn git_repo(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();
    fs::write(dir.join("README"), "one\n").unwrap();
    fs::write(dir.join("src/lib.rs"), "fn main() {}\n").unwrap();
    // The shared helper: no global git config (the CI runner has no
    // identity), a fixed author, and a failure on a nonzero exit.
    common::git(dir, &["init", "-q", "-b", "main"]);
    common::git(dir, &["add", "-A"]);
    common::git(dir, &["commit", "-q", "-m", "one"]);
}

fn import_from_url_file(dir: &Path) -> (PathBuf, PathBuf) {
    let git = dir.join("git");
    git_repo(&git);
    let data = dir.join("source");
    // The operator makes the data directory (the binary refuses a
    // missing one); the URL file is made below.
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    let url_file = dir.join("pg-url");
    write_url_file(&data, &url_file);
    must_run(
        &[
            "--data",
            data.to_str().unwrap(),
            "--postgres-url-file",
            url_file.to_str().unwrap(),
            "import",
            "--repo",
            "main",
            git.to_str().unwrap(),
        ],
        "the import",
    );
    (data, url_file)
}

fn backend_marker(data: &Path) -> String {
    fs::read_to_string(data.join("backend"))
        .unwrap_or_else(|e| panic!("the target must name its backend: {e}"))
}

/// A restore whose URL comes from `--postgres-url-file` lands in
/// Postgres: the restored directory names its backend, and it is
/// Postgres.
#[test]
fn a_restore_with_the_url_from_a_file_lands_in_postgres() {
    let dir = scion_server::test_support::private_tempdir();
    let (_data, url_file) = import_from_url_file(dir.path());
    let out = dir.path().join("archive");
    must_run(
        &[
            "--data",
            dir.path().join("source").to_str().unwrap(),
            "--postgres-url-file",
            url_file.to_str().unwrap(),
            "export",
            "--all",
            "--out",
            out.to_str().unwrap(),
        ],
        "the export",
    );

    let target = dir.path().join("restored");
    let target_url = dir.path().join("pg-url-restored");
    write_url_file(&target, &target_url);
    must_run(
        &[
            "--data",
            target.to_str().unwrap(),
            "--postgres-url-file",
            target_url.to_str().unwrap(),
            "restore",
            "--archive",
            out.to_str().unwrap(),
        ],
        "the restore",
    );
    let marker = backend_marker(&target);
    assert!(
        marker.contains("postgres"),
        "a restore with a URL file must land in Postgres, not {marker:?}"
    );
}

/// A restore whose URL comes from `SCION_POSTGRES_URL` lands in
/// Postgres too: the environment form.
#[test]
fn a_restore_with_the_url_from_the_env_lands_in_postgres() {
    let dir = scion_server::test_support::private_tempdir();
    let (data, url_file) = import_from_url_file(dir.path());
    let out = dir.path().join("archive");
    must_run(
        &[
            "--data",
            data.to_str().unwrap(),
            "--postgres-url-file",
            url_file.to_str().unwrap(),
            "export",
            "--all",
            "--out",
            out.to_str().unwrap(),
        ],
        "the export",
    );

    let target = dir.path().join("restored");
    let target_url_path = dir.path().join("pg-url-restored");
    write_url_file(&target, &target_url_path);
    let env_url = fs::read_to_string(&target_url_path).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args([
            "--data",
            target.to_str().unwrap(),
            "restore",
            "--archive",
            out.to_str().unwrap(),
        ])
        .env("NO_COLOR", "1")
        .env("SCION_POSTGRES_URL", env_url.trim())
        .output()
        .expect("the restore runs");
    assert!(
        out.status.success(),
        "the restore failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let marker = backend_marker(&target);
    assert!(
        marker.contains("postgres"),
        "a restore with an env URL must land in Postgres, not {marker:?}"
    );
}

/// `--postgres-ca` with a URL from a file is accepted: the refusal of a
/// CA without a URL belongs after the resolution, not in clap.
#[test]
fn a_postgres_ca_with_a_url_from_a_file_is_accepted() {
    let dir = scion_server::test_support::private_tempdir();
    let (_data, url_file) = import_from_url_file(dir.path());
    // Any readable CA file passes the flag's existence check; the CA
    // itself is judged when the connection opens, which a localhost
    // test URL without TLS never does.
    let ca = dir.path().join("ca.pem");
    fs::write(&ca, "not a real CA - never opened without TLS\n").unwrap();
    must_run(
        &[
            "--postgres-url-file",
            url_file.to_str().unwrap(),
            "--postgres-ca",
            ca.to_str().unwrap(),
            "hash-token",
            "--token",
            "t",
        ],
        "hash-token with --postgres-ca beside a URL file",
    );
}

/// `--postgres-ca` with NO URL from any source is refused, with the
/// fix named: the URL can come from three places, and clap saw only
/// one. `restore` is the command here: its URL is optional until the
/// removal makes it required, so the refusal comes from the resolution,
/// which names the three sources a URL may come from.
#[test]
fn a_postgres_ca_without_any_url_is_refused() {
    let dir = scion_server::test_support::private_tempdir();
    let ca = dir.path().join("ca.pem");
    fs::write(&ca, "unopened\n").unwrap();
    let out = run(&[
        "--data",
        dir.path().join("target").to_str().unwrap(),
        "--postgres-ca",
        ca.to_str().unwrap(),
        "restore",
        "--archive",
        dir.path().join("nonexistent-archive").to_str().unwrap(),
    ]);
    assert!(!out.status.success(), "a CA without a URL must be refused");
    let why = String::from_utf8_lossy(&out.stderr);
    assert!(
        why.contains("no Postgres URL: give --postgres-url, --postgres-url-file or SCION_POSTGRES_URL"),
        "{why}"
    );
}
