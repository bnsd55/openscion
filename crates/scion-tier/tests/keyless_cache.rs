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
            "--upstream",
            "http://127.0.0.1:39999",
        ])
        .env("SCION_TOKEN", "t")
        .env("SCION_UPSTREAM_TOKEN", "t")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(log_file.try_clone().unwrap())
        .spawn()
        .expect("the binary runs");
    // The wait reads the exit, not 2 s of hope: a cache that refuses at
    // start exits at once, and the deadline only bounds a slow spawn.
    // A cache still alive at the deadline is the failure the test
    // names, with the log it wrote.
    let started = std::time::Instant::now();
    let mut alive = true;
    while started.elapsed() < std::time::Duration::from_secs(10) {
        if child.try_wait().expect("the wait works").is_some() {
            alive = false;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
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
    child.wait().unwrap();
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

/// A key the cache cannot use is refused at start, and the refusal names
/// the cache's own flag: `--ticket-key` is the server's, which the cache
/// does not take.
#[test]
fn a_bad_public_key_is_refused_by_the_cache_s_own_flag() {
    let key = "0".repeat(64);
    for keys in [vec!["not hex"], vec![key.as_str(); 3]] {
        let mut args = vec![
            "--listen",
            "127.0.0.1:0",
            "--upstream",
            "http://127.0.0.1:39999",
        ];
        for key in &keys {
            args.extend(["--ticket-public-key", key]);
        }
        let out = Command::new(env!("CARGO_BIN_EXE_scion-cache"))
            .args(&args)
            .env("SCION_TOKEN", "t")
            .env("SCION_UPSTREAM_TOKEN", "t")
            .stdin(Stdio::null())
            .output()
            .expect("the binary runs");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success(), "{keys:?} was accepted: {err}");
        assert!(
            err.contains("--ticket-public-key") && !err.contains("--ticket-key"),
            "the refusal of {keys:?} names the cache's flag: {err}"
        );
    }
}
