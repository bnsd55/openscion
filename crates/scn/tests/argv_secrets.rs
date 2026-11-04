//! The argv forms of the secret flags are gone: a token on the command
//! line is readable by every local account (`ps`), so a call that still
//! passes one fails at once, before any parsing, and the failure never
//! quotes the value. The file form (a checked read) and the environment
//! form remain, and the failure names them.

use std::process::Command;

/// Runs `scn ARGS` with the environment cleared of the token vars, and
/// returns (exit code, stdout, stderr).
fn scn(args: &[&str]) -> (bool, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(args)
        .env_remove("SCION_TOKEN")
        .output()
        .expect("scn runs");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// The refusal of the removed `--token` flag: both spellings fail at
/// once with the replacement named, and neither the value nor any part
/// of the command's answer carries the secret.
#[test]
fn the_argv_form_refuses_and_names_the_file_and_env_forms() {
    for args in [
        vec![
            "--token",
            "argv-secret-7f3d9c2a1b",
            "--server",
            "http://127.0.0.1:1",
        ],
        vec![
            "--token=argv-secret-7f3d9c2a1b",
            "--server",
            "http://127.0.0.1:1",
        ],
    ] {
        let (ok, stdout, stderr) = scn(&args);
        assert!(!ok, "the argv form must be refused: {stdout}{stderr}");
        let both = format!("{stdout}{stderr}");
        assert!(both.contains("the --token flag is gone"), "{both}");
        assert!(both.contains("SCION_TOKEN"), "{both}");
        assert!(both.contains("--token-file"), "{both}");
        assert!(
            !both.contains("argv-secret-7f3d9c2a1b"),
            "the secret was echoed: {both}"
        );
    }
}

/// A flag whose name only starts the same is not the removed one:
/// `--token-file` still works (the file holds a token, so the refusal
/// is the empty-token one, not the removal).
#[test]
fn a_lookalike_flag_is_not_refused() {
    let (ok, _stdout, stderr) = scn(&[
        "--token-file",
        "/tmp/scion-argv-secrets-no-such-file",
        "--server",
        "http://127.0.0.1:1",
        "log",
        "main",
    ]);
    assert!(!ok, "a missing server must fail, but not by the removal");
    assert!(!stderr.contains("the --token flag is gone"), "{stderr}");
    assert!(
        !stderr.contains("unexpected argument"),
        "the parser must not see the line: {stderr}"
    );
}

/// The scan stops at a bare `--`: after it every argument is a value,
/// even one spelled like the removed flag.
#[test]
fn everything_after_a_bare_double_dash_is_a_value() {
    let (ok, _stdout, stderr) = scn(&[
        "--server",
        "http://127.0.0.1:1",
        "--",
        "--token",
        "argv-secret-7f3d9c2a1b",
    ]);
    assert!(!ok, "a missing server must fail, but not by the removal");
    assert!(!stderr.contains("the --token flag is gone"), "{stderr}");
}
