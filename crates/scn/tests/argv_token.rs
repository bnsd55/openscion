//! The command-line token form: `scn` warns that the process list shows
//! a secret passed in argv, and that warning is all it may cost. The
//! warning never repeats the token, stdout never carries it, and no
//! file `scn` writes under its home or its working directory holds it.

use std::fs;
use std::process::Command;
use tempfile::TempDir;

/// The token this test puts on the command line: distinctive enough
/// that a substring search cannot hit an unrelated byte run.
const TOKEN: &str = "argv-secret-7f3d9c2a1b";

#[test]
fn a_command_line_token_is_warned_and_leaks_nowhere_scn_writes() {
    let home = TempDir::new().unwrap();
    let work = TempDir::new().unwrap();
    // No server is at port 1: the call fails, which is fine — the
    // warning prints at startup, and the failure keeps the run short.
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args([
            "--server",
            "http://127.0.0.1:1",
            "--repo",
            "r",
            "--token",
            TOKEN,
            "log",
            "main",
        ])
        .env("HOME", home.path())
        .current_dir(work.path())
        .output()
        .unwrap();

    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        stderr.contains("--token put a secret on the command line"),
        "the argv form warns: {stderr}"
    );
    assert!(
        !stderr.contains(TOKEN),
        "the warning repeats the token it warns about"
    );

    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(!stdout.contains(TOKEN), "stdout carries the token");

    // Every file under the home and the working directory `scn` got:
    // none of them may carry the token.
    for root in [home.path(), work.path()] {
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let bytes = fs::read(&path).unwrap_or_default();
                    assert!(
                        !bytes
                            .windows(TOKEN.len())
                            .any(|window| window == TOKEN.as_bytes()),
                        "{} carries the token",
                        path.display()
                    );
                }
            }
        }
    }
}
