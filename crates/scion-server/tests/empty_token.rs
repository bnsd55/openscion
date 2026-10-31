//! An empty token is no secret, so nothing mints its digest: an access
//! file whose line carries blake3 of the empty string is refused at
//! parse, `hash-token` refuses empty and whitespace-only input, and a
//! server whose access file names the empty digest does not start.

use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::process::{Command, Stdio};

use scion_proto::runtime::auth::token_digest;
use scion_server::Access;

/// The digest an operator must never be able to put in an access file.
fn empty_digest() -> String {
    blake3::Hash::from(token_digest("")).to_hex().to_string()
}

#[test]
fn an_access_file_naming_the_empty_digest_is_refused() {
    let text = format!("token\tci\t{}\nread\tci\tmain\n", empty_digest());
    let error = Access::parse(&text).unwrap_err();
    assert!(
        error.contains("an empty token is no secret"),
        "the refusal names the problem: {error}"
    );
}

/// Runs `scion-server hash-token` with `args`, and returns (code, out, err).
fn hash_token(args: &[&str]) -> (bool, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["hash-token"])
        .args(args)
        .env_remove("SCION_TOKEN")
        .stdin(Stdio::null())
        .output()
        .expect("hash-token runs");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

#[test]
fn hash_token_refuses_an_empty_argv_token() {
    let (ok, out, err) = hash_token(&["--token", ""]);
    assert!(!ok, "hash-token printed {out}");
    assert!(
        err.contains("no secret") || err.contains("empty") || err.contains("whitespace"),
        "the refusal names the problem: {err}"
    );
    assert_ne!(out.trim(), empty_digest(), "the empty digest leaked");
}

#[test]
fn hash_token_refuses_a_whitespace_only_argv_token() {
    let (ok, out, err) = hash_token(&["--token", "   \t "]);
    assert!(!ok, "hash-token printed {out}");
    assert!(
        err.contains("whitespace") || err.contains("no secret"),
        "the refusal names the problem: {err}"
    );
}

/// A server whose access file names the empty digest does not start.
#[test]
fn a_server_whose_access_file_names_the_empty_digest_does_not_start() {
    let dir = scion_server::test_support::private_tempdir();
    let access = dir.path().join("access");
    let mut file = std::fs::File::create(&access).unwrap();
    writeln!(file, "token\tci\t{}", empty_digest()).unwrap();
    writeln!(file, "read\tci\tmain").unwrap();
    drop(file);
    std::fs::set_permissions(&access, std::fs::Permissions::from_mode(0o600)).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args([
            "--data",
            dir.path().join("data").to_str().unwrap(),
            "serve",
            "--access",
            access.to_str().unwrap(),
        ])
        .env_remove("SCION_TOKEN")
        .output()
        .expect("the server runs");
    assert!(
        !out.status.success(),
        "the server started with the empty-token digest in its access file"
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("an empty token is no secret"),
        "the refusal names the problem: {err}"
    );
}
