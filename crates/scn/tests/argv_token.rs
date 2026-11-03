//! The command-line token form is refused: a token on the command line
//! is readable by every local account (`ps`), so a call that still
//! passes one fails at once, before any parsing. The refusal never
//! repeats the token, stdout never carries it, and no file `scn`
//! writes under its home or its working directory holds it.

use std::fs;
use std::process::Command;
use tempfile::TempDir;

/// The token this test puts on the command line: distinctive enough
/// that a substring search cannot hit an unrelated byte run.
const TOKEN: &str = "argv-secret-7f3d9c2a1b";

#[test]
fn a_command_line_token_is_refused_and_leaks_nowhere_scn_writes() {
    let home = TempDir::new().unwrap();
    let work = TempDir::new().unwrap();
    for args in [
        vec!["--token", TOKEN, "--server", "http://127.0.0.1:1"],
        vec![
            &format!("--token={TOKEN}"),
            "--server",
            "http://127.0.0.1:1",
        ],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(&args)
            .env("HOME", home.path())
            .current_dir(work.path())
            .output()
            .unwrap();

        assert!(
            !out.status.success(),
            "the argv form is refused, both spellings"
        );
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        assert!(
            stderr.contains("the --token flag is gone"),
            "the refusal names the flag: {stderr}"
        );
        assert!(
            !stderr.contains(TOKEN),
            "the refusal repeats the token it refuses"
        );

        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(!stdout.contains(TOKEN), "stdout carries the token");

        // Every file under the home and the working directory `scn`
        // got: none of them may carry the token.
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
}
