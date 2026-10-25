//! The server's global flags parse before and after the subcommand:
//! `scion-server --data D fsck` and `scion-server fsck --data D` carry
//! the same values, for every subcommand that takes the data directory.

use std::process::{Command, Stdio};

/// Runs `scion-server` with `args`, and returns (code, stderr).
fn run(args: &[&str]) -> (i32, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(args)
        .env_remove("SCION_DATA")
        .env_remove("SCION_POSTGRES_URL")
        .env_remove("SCION_POSTGRES_URL_FILE")
        .stdin(Stdio::null())
        .output()
        .expect("the binary runs");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// Both flag positions name the same directory: the refusal of a
/// missing data directory carries the path the flag gave.
#[test]
fn the_data_flag_parses_before_and_after_the_subcommand() {
    for sub in ["fsck", "gc"] {
        let missing = format!("/tmp/scion-no-such-dir-{sub}");
        let (code_before, err_before) = run(&["--data", &missing, sub]);
        let (code_after, err_after) = run(&[sub, "--data", &missing]);
        assert_eq!(
            code_before, code_after,
            "{sub}: same exit in both positions"
        );
        assert!(
            err_before.contains(&missing) && err_after.contains(&missing),
            "{sub}: both positions parsed the same value: {err_before} | {err_after}"
        );
        assert!(
            !err_after.contains("unexpected argument"),
            "{sub}: the flag after the subcommand must parse: {err_after}"
        );
    }
}

/// The Postgres flags parse after the subcommand too.
#[test]
fn the_postgres_flags_parse_after_the_subcommand() {
    let missing = "/tmp/scion-no-such-dir-pg";
    let (code_before, err_before) = run(&[
        "--data",
        missing,
        "--postgres-url",
        "postgres://localhost/scion_walk_parse",
        "fsck",
    ]);
    let (code_after, err_after) = run(&[
        "fsck",
        "--data",
        missing,
        "--postgres-url",
        "postgres://localhost/scion_walk_parse",
    ]);
    assert_eq!(code_before, code_after);
    assert!(
        err_before.contains(missing) && err_after.contains(missing),
        "both positions parsed the same data dir: {err_before} | {err_after}"
    );
    assert!(!err_after.contains("unexpected argument"));
}

/// A subcommand's own flags do not clash with the globals: `serve`'s
/// `--token` stays where it was, and a global flag beside it parses.
#[test]
fn a_subcommand_flag_beside_a_global_flag_parses() {
    let missing = "/tmp/scion-no-such-dir-serve";
    let (code, err) = run(&[
        "serve",
        "--token",
        "t",
        "--data",
        missing,
        "--listen",
        "127.0.0.1:39190",
    ]);
    assert_eq!(code, 1, "the missing data dir is the refusal: {err}");
    assert!(err.contains(missing), "{err}");
    assert!(!err.contains("unexpected argument"));
}
