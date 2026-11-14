//! `scn` on a closed stdout: the reader of a pipe that stops early
//! (`scn blame f | head -1`) goes away, and scn's next write fails with
//! `BrokenPipe`. The command ends quietly with status 0 and no panic
//! text on stderr — a closed stdout is a normal end for a CLI, not the
//! "failed printing to stdout" panic.

use std::io::Read as _;
use std::process::{Command, Stdio};

use scion_core::{ChangesetId, FileState};
use scion_server::Backend;

mod common;
use common::{REPO, TOKEN, Tiers, changeset, land};

/// Lands `big.txt` twice: 2000 lines of about 100 printed bytes each,
/// so blame's output passes what a pipe holds (64 KiB) and scn is
/// still writing when the reader leaves. A smaller output would let
/// scn finish into the pipe buffer before the close, and the test
/// would pass without ever reaching the broken pipe.
fn start(store: &Backend) -> (ChangesetId, ChangesetId) {
    let lines = |tag: &str| {
        use std::fmt::Write as _;
        let mut content = String::new();
        for i in 0..2000 {
            writeln!(content, "{tag} line {i:04} {}", "x".repeat(48)).unwrap();
        }
        content
    };
    let put = |content: &str| {
        let mut blobs = store.blobs().batch();
        blobs.put(content.as_bytes()).unwrap()
    };
    let base = land(
        store,
        &changeset(
            None,
            &[(
                "big.txt",
                FileState::Regular {
                    content: put(&lines("old")),
                    executable: false,
                },
            )],
        ),
    );
    let tip = land(
        store,
        &changeset(
            Some(base),
            &[(
                "big.txt",
                FileState::Regular {
                    content: put(&lines("new")),
                    executable: false,
                },
            )],
        ),
    );
    (base, tip)
}

#[test]
fn a_closed_stdout_ends_the_command_quietly() {
    let data = common::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let _tip = start(&store).1;
    let tiers = Tiers::start(data, store.clone());
    let home = common::private_tempdir();

    let mut child = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args([
            "--server",
            &tiers.server,
            "--repo",
            REPO,
            "blame",
            "main:big.txt",
        ])
        .env("SCION_TOKEN", TOKEN)
        .env("HOME", home.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // One byte proves scn is producing. Dropping the reader closes the
    // pipe while scn still has the rest of the 2000 lines to write, so
    // its next write meets the closed pipe.
    let mut stdout = child.stdout.take().unwrap();
    let mut first = [0u8; 1];
    stdout.read_exact(&mut first).unwrap();
    drop(stdout);

    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(0), "status: {}", out.status);
    assert!(
        out.stderr.is_empty(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A real write error is not a closed pipe: the command must report it
/// and fail with a non-zero status, not swallow it as a quiet end. The
/// device here answers every write with "no space left", which is the
/// disk's own answer, so the behavior pinned is the real code's.
#[cfg(target_os = "linux")]
#[test]
fn a_real_write_error_fails_the_command() {
    use std::fs::OpenOptions;
    use std::io::Write as _;
    let data = common::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let _tip = start(&store).1;
    let tiers = Tiers::start(data, store.clone());
    let home = common::private_tempdir();

    // The child's stdout is the device; a clone of the same description
    // stays here, so the assert below can write it again.
    let mut full = OpenOptions::new().write(true).open("/dev/full").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args([
            "--server",
            &tiers.server,
            "--repo",
            REPO,
            "cat",
            "main:big.txt",
        ])
        .env("SCION_TOKEN", TOKEN)
        .env("HOME", home.path())
        .stdout(Stdio::from(full.try_clone().unwrap()))
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "a lost output must not be a quiet success"
    );
    assert!(
        !out.stderr.is_empty(),
        "the error names the write's failure"
    );
    // And the device really answers with the disk's error, so the test
    // pins scn's behavior and not the device's silence.
    assert!(full.write_all(b"x").is_err());
}

/// The same real error, through the line writer: a command that prints
/// whole lines must fail the same way when its stdout is the full
/// device.
#[cfg(target_os = "linux")]
#[test]
fn a_real_write_error_fails_a_line_writing_command() {
    use std::fs::OpenOptions;
    let data = common::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let _tip = start(&store).1;
    let tiers = Tiers::start(data, store.clone());
    let home = common::private_tempdir();

    // The child's stdout is the device; a clone of the same description
    // stays here, so the assert below can write it again.
    let full = OpenOptions::new().write(true).open("/dev/full").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(["--server", &tiers.server, "--repo", REPO, "log", "main"])
        .env("SCION_TOKEN", TOKEN)
        .env("HOME", home.path())
        .stdout(Stdio::from(full.try_clone().unwrap()))
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "a lost output must not be a quiet success"
    );
    assert!(
        !out.stderr.is_empty(),
        "the error names the write's failure"
    );
}

/// `scn cat` on a closed stdout: the streamed chunks go through the
/// raw write path, and the write that meets the closed pipe ends the
/// command quietly, exactly like the line printer's.
#[test]
fn a_cat_through_a_closed_pipe_ends_quietly() {
    let data = common::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let _tip = start(&store).1;
    let tiers = Tiers::start(data, store.clone());
    let home = common::private_tempdir();

    let mut child = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args([
            "--server",
            &tiers.server,
            "--repo",
            REPO,
            "cat",
            "main:big.txt",
        ])
        .env("SCION_TOKEN", TOKEN)
        .env("HOME", home.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // One byte proves scn is producing; the drop closes the pipe while
    // the rest of the 2000 lines is still to write.
    let mut stdout = child.stdout.take().unwrap();
    let mut first = [0u8; 1];
    stdout.read_exact(&mut first).unwrap();
    drop(stdout);
    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(0), "status: {}", out.status);
    assert!(
        out.stderr.is_empty(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A real write error is not a closed pipe, on the diff text path too:
/// the command's buffered writer holds the lines back, and the flush
/// that meets the full device must fail the command, not end it
/// quietly.
#[cfg(target_os = "linux")]
#[test]
fn a_real_write_error_fails_the_diff_text() {
    use std::fs::OpenOptions;
    use std::io::Write as _;
    let data = common::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    // 8000 changed paths: about 88 KiB of diff text, past the writer's
    // buffer, so the write itself is the call that meets the device.
    let put = |content: &str| {
        let mut blobs = store.blobs().batch();
        blobs.put(content.as_bytes()).unwrap()
    };
    let blob_old = put("old");
    let blob_new = put("new");
    let state = |blob| FileState::Regular {
        content: blob,
        executable: false,
    };
    let base = land(
        &store,
        &changeset(
            None,
            &(0..8000)
                .map(|n| (format!("f{n:04}.txt").leak() as &str, state(blob_old)))
                .collect::<Vec<_>>(),
        ),
    );
    let tip = land(
        &store,
        &changeset(
            Some(base),
            &(0..8000)
                .map(|n| (format!("f{n:04}.txt").leak() as &str, state(blob_new)))
                .collect::<Vec<_>>(),
        ),
    );
    let tiers = Tiers::start(data, store.clone());
    let home = common::private_tempdir();

    // The child's stdout is the device; a clone of the same description
    // stays here, so the assert below can write it again.
    let mut full = OpenOptions::new().write(true).open("/dev/full").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args([
            "--server",
            &tiers.server,
            "--repo",
            REPO,
            "diff",
            &base.to_string(),
            &tip.to_string(),
        ])
        .env("SCION_TOKEN", TOKEN)
        .env("HOME", home.path())
        .stdout(Stdio::from(full.try_clone().unwrap()))
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "a lost output must not be a quiet success"
    );
    assert!(
        !out.stderr.is_empty(),
        "the error names the write's failure"
    );
    // And the device really answers with the disk's error, so the test
    // pins scn's behavior and not the device's silence.
    assert!(full.write_all(b"x").is_err());
}

/// A small diff on the full device: the buffered writer holds the two
/// lines back, so the last flush is the write that meets "no space
/// left", and the command must report it instead of ending quietly. A
/// flush stub would swallow the error and exit 0 with the output lost.
#[cfg(target_os = "linux")]
#[test]
fn a_real_write_error_fails_the_diff_flush() {
    use std::fs::OpenOptions;
    use std::io::Write as _;
    let data = common::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let (base, tip) = start(&store);
    let tiers = Tiers::start(data, store.clone());
    let home = common::private_tempdir();

    // The child's stdout is the device; a clone of the same description
    // stays here, so the assert below can write it again.
    let mut full = OpenOptions::new().write(true).open("/dev/full").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args([
            "--server",
            &tiers.server,
            "--repo",
            REPO,
            "diff",
            &base.to_string(),
            &tip.to_string(),
        ])
        .env("SCION_TOKEN", TOKEN)
        .env("HOME", home.path())
        .stdout(Stdio::from(full.try_clone().unwrap()))
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "a lost output must not be a quiet success"
    );
    assert!(
        !out.stderr.is_empty(),
        "the error names the write's failure"
    );
    assert!(full.write_all(b"x").is_err());
}
