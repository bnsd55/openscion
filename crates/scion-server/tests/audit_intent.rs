//! The gc's audit intent when the audit log cannot be written: an
//! append that fails for a reason other than the disk's fullness
//! refuses the run before any change - the intent is recorded before
//! anything moves, and a run that cannot record it must not touch the
//! store. The audit file here is a private regular file at the child's
//! own file-size limit, so the append's failure is a real write's
//! answer, not a stub's. The disk-full deferral's other side lives in
//! the binary's own unit tests, where the audit's test hook makes one
//! append fail as an out-of-space answer would.

use std::os::unix::fs::OpenOptionsExt as _;
use std::process::{Command, Output};

fn run_gc_with_audit(dir: &std::path::Path, audit: &str) -> Output {
    let data = dir.join("data");
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(&data)
        .args(["gc", "--audit-file", audit])
        .env("NO_COLOR", "1")
        .env(
            "SCION_POSTGRES_URL",
            scion_server::Backend::test_postgres_url(&data).unwrap(),
        )
        .output()
        .expect("the gc runs")
}

/// An append failure that is not the disk's answer refuses the run
/// before any change. The audit file here is a private regular file
/// already at the child's own file-size limit: the append fails with
/// the limit's error, which is not out-of-space, so the guard must
/// send it down the refuse path, and a guard that defers every failure
/// would run the collector with no intent recorded.
#[test]
fn a_gc_whose_audit_fails_otherwise_refuses_before_any_change() {
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    let audit = dir.path().join("audit.log");
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .mode(0o600)
        .open(&audit)
        .unwrap();
    // A kilobyte ending in a newline: the open's torn-line check reads
    // the last byte and writes nothing, and the first append crosses
    // the limit whichever unit `ulimit -f` counts in.
    file.write_all(&vec![b'a'; 1023]).unwrap();
    file.write_all(b"\n").unwrap();
    drop(file);
    // The child's own file-size limit is one KiB: the append at the
    // end of a 4 KiB file fails with it, and that error is not the
    // disk's.
    let out = Command::new("sh")
        .arg("-c")
        .args([
            // The limit's write sends SIGXFSZ, whose default ends the
            // process before the code can answer; ignored, the write
            // returns its error instead, which is the answer this test
            // pins.
            "ulimit -f 1; trap '' XFSZ; exec \"$0\" --data \"$1\" gc --audit-file \"$2\"",
            env!("CARGO_BIN_EXE_scion-server"),
            data.to_str().unwrap(),
            audit.to_str().unwrap(),
        ])
        .env("NO_COLOR", "1")
        .env(
            "SCION_POSTGRES_URL",
            scion_server::Backend::test_postgres_url(&data).unwrap(),
        )
        .output()
        .expect("the gc runs");
    assert!(
        !out.status.success(),
        "a gc with no intent recorded must not run: {:?}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("cannot record the gc intent"),
        "the refusal names the intent: {stderr}"
    );
    assert!(
        !String::from_utf8_lossy(&out.stdout).contains("gc:"),
        "the report never prints: {:?}",
        String::from_utf8_lossy(&out.stdout)
    );
}
