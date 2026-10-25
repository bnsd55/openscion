//! A cache that checks no ticket serves its store to any caller: the
//! cache refuses to start without a public key, and the daemon's relay
//! mode is an explicit choice of its own constructor.

use std::io::Read as _;
use std::process::{Command, Stdio};

#[test]
fn a_cache_without_a_public_key_does_not_start() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("cache.log");
    let log_file = std::fs::File::create(&log).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion-cache"))
        .args([
            "--listen",
            "127.0.0.1:0",
            "--token",
            "t",
            "--upstream-token",
            "t",
            "--upstream",
            "http://127.0.0.1:39999",
        ])
        .env_remove("SCION_TOKEN")
        .env_remove("SCION_UPSTREAM_TOKEN")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(log_file.try_clone().unwrap())
        .spawn()
        .expect("the binary runs");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let alive = child.try_wait().expect("the wait works").is_none();
    if alive {
        child.kill().unwrap();
        child.wait().unwrap();
        let mut err = String::new();
        drop(log_file);
        std::fs::File::open(&log)
            .unwrap()
            .read_to_string(&mut err)
            .unwrap();
        panic!(
            "a keyless cache started and served; the doc promises it does \
             not: {err}"
        );
    }
    let mut err = String::new();
    drop(log_file);
    std::fs::File::open(&log)
        .unwrap()
        .read_to_string(&mut err)
        .unwrap();
    assert!(
        err.contains("--ticket-public-key"),
        "the refusal names the flag: {err}"
    );
}
