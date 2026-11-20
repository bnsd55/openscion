//! `scn mount`'s stop: a SIGTERM to a serving mount unmounts, removes
//! its overlay, drops its record, and answers 0 ([#1320]). The mount
//! runs with `--no-mount`, so the run needs no kernel NFS: the NFS
//! listener serves on a free loopback port, and the stop path is the
//! signal's.

#![cfg(unix)]

use std::io::{BufRead as _, BufReader, Read as _};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

use common::{REPO, TOKEN, changeset, land};
use scion_core::{ChangesetId, FileState};
use scion_server::Backend;

mod common;

/// Lands `keep.txt` on `store`, and points `main` at the changeset.
fn start(store: &Backend) -> ChangesetId {
    let store = store.clone();
    let store = &store;
    land(
        store,
        &changeset(
            None,
            &[(
                "keep.txt",
                FileState::Regular {
                    content: store.blobs().batch().put(b"same\n").unwrap(),
                    executable: false,
                },
            )],
        ),
    )
}

/// Serves `main` with `scn mount --no-mount` against `url`, with `home`
/// as the home directory. The mount's stdout and stderr stay piped: the
/// caller reads the serving lines from the stdout pipe, and the stop's
/// line and warnings from the rest.
fn serving_mount(tiers: &common::Tiers, home: &std::path::Path) -> Child {
    let view = home.join("view");
    std::fs::create_dir_all(&view).unwrap();
    Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(["--server", &tiers.server, "--repo", REPO])
        .args(["mount"])
        .arg(&view)
        .args(["main", "--no-mount"])
        .env("SCION_TOKEN", TOKEN)
        .env("HOME", home)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("scn mount runs")
}

#[test]
fn a_terminated_mount_unmounts_and_answers_0() {
    let data = common::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let base = start(&store);
    let tiers = common::Tiers::start(data, store);

    let home = common::private_tempdir();
    let mut child = serving_mount(&tiers, home.path());
    // The warnings go to the stderr pipe; a thread collects them, so a
    // full pipe cannot stall the mount while the test waits for the
    // stdout lines.
    let stderr_pipe = child.stderr.take().expect("the mount's stderr is piped");
    let warnings = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = BufReader::new(stderr_pipe).read_to_string(&mut text);
        text
    });
    let stdout = child.stdout.take().expect("the mount's stdout is piped");
    let mut lines = BufReader::new(stdout).lines();

    // The serving lines come first: the single-user warning, then the
    // mount command. The second is the cue that the record is saved and
    // the listener holds - from then on the process is in the serving
    // window, waiting for a signal.
    let first = lines
        .next()
        .expect("the mount prints its warning")
        .expect("the warning line reads");
    assert!(
        first.contains("single-user machines only"),
        "the warning goes out first: {first}"
    );
    let second = lines
        .next()
        .expect("the mount prints the mount command")
        .expect("the mount command line reads");
    assert!(
        second.contains("sudo mount"),
        "the printed command mounts the served export: {second}"
    );

    // The mount serves until a signal: it holds through a grace window
    // - a mount that stopped on its own would be gone inside one - and
    // its record names the dir it serves and the overlay it must remove.
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert!(
        child.try_wait().unwrap().is_none(),
        "the mount serves until a signal"
    );
    let table = home.path().join(".scion").join("mounts.json");
    let record: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&table).unwrap())
        .expect("the mount's record is in the table");
    assert_eq!(record.as_array().map(Vec::len), Some(1), "{record:?}");
    let dir: PathBuf = record[0]["dir"].as_str().expect("the served dir").into();
    let overlay: PathBuf = record[0]["overlay"].as_str().expect("the overlay").into();
    let pid = record[0]["pid"].as_u64().expect("the serving pid");
    assert!(
        overlay.starts_with(std::env::temp_dir()),
        "the overlay sits under the system's temporary directory: {overlay:?}"
    );
    assert!(overlay.is_dir(), "the overlay exists while serving");
    assert_eq!(pid, u64::from(child.id()), "the record names the server");

    // The stop: SIGTERM, the signal a `scn unmount` sends.
    let sent = Command::new("kill")
        .args(["-s", "TERM", &pid.to_string()])
        .status()
        .expect("the signal goes out");
    assert!(sent.success(), "the signal was delivered");

    // The rest of the stdout: the stop's own line ends it.
    let mut rest = String::new();
    for line in lines {
        rest.push_str(&line.expect("the stop's stdout line reads"));
        rest.push('\n');
    }
    let status = child.wait().expect("the mount ends");
    let stderr = warnings.join().unwrap_or_default();
    assert_eq!(
        status.code(),
        Some(0),
        "a stopped mount answers 0: {stderr}"
    );
    assert!(
        rest.contains(&format!("stopped serving {}", dir.display())),
        "the mount names the dir it stopped serving: {rest}"
    );
    assert!(
        !stderr.contains("scn mount:"),
        "a clean stop warns nothing: {stderr}"
    );
    // The overlay is gone, and the table holds no record: the next
    // mount of the dir is not refused, and the next unmount finds
    // nothing to clean.
    assert!(!overlay.exists(), "the overlay is removed at the stop");
    let after = std::fs::read_to_string(&table).unwrap();
    let after: serde_json::Value =
        serde_json::from_str(&after).expect("the table stays valid JSON");
    assert_eq!(
        after.as_array().map(Vec::len),
        Some(0),
        "the record is dropped: {after:?}"
    );
}
