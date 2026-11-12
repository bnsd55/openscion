//! The read-only default: a build without the `unstable-writable-mount`
//! feature refuses the write flags before it connects. The refusal names
//! the build, not the syntax and not the server, and it fires whatever
//! the server answers - here, nothing at all, the address is dead.

#![cfg(unix)]

use std::process::{Command, Output};

/// Runs the `scn` binary with `args` and no token: the refusal must
/// fire before any connection is attempted, so no token is needed.
fn scn(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(args)
        .output()
        .expect("scn runs")
}

/// `scn mount --unstable-writable` on a build without the feature:
/// refused with the build's name in the message, exit 2.
#[test]
fn a_writable_mount_on_a_read_only_build_is_refused_before_it_connects() {
    let out = scn(&["--server", "http://127.0.0.1:1", "mount", "--unstable-writable", "/tmp/scn-mut-refuse"]);
    assert_eq!(out.status.code(), Some(2), "the refusal exits 2");
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(
        text.contains("this build has no writable mount"),
        "{text}"
    );
    assert!(
        text.contains("unstable-writable-mount"),
        "{text}"
    );
}

/// `scn commit --from-mount` on a build without the feature: the same
/// refusal, the same exit.
#[test]
fn a_commit_from_a_mount_on_a_read_only_build_is_refused_before_it_connects() {
    let out = scn(&["--server", "http://127.0.0.1:1", "commit", "--message", "m", "--from-mount", "/tmp/scn-mut-refuse"]);
    assert_eq!(out.status.code(), Some(2), "the refusal exits 2");
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(
        text.contains("this build has no writable mount"),
        "{text}"
    );
}

/// The other side of the guard: without the write flags the commands
/// must NOT refuse with the build message - a plain mount prints its
/// command, a plain commit reaches for the server. This kills the
/// guard folded to `true`, which would refuse every command.
#[test]
fn a_mount_without_the_writable_flag_is_not_refused_by_the_build() {
    let out = scn(&["mount", "--no-mount", "/tmp"]);
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(
        !text.contains("this build has no writable mount"),
        "{text}"
    );
}

#[test]
fn a_commit_without_from_mount_is_not_refused_by_the_build() {
    let out = scn(&["--server", "http://127.0.0.1:1", "commit", "--message", "m"]);
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(
        !text.contains("this build has no writable mount"),
        "{text}"
    );
    // It got past the guard and failed on the dead server instead.
    assert!(text.contains("cannot connect") || out.status.code() == Some(2), "{text}");
}
