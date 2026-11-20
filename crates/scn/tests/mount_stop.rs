//! `scn mount`'s stop: a SIGTERM to a serving mount unmounts, removes
//! its overlay, drops its record, and answers 0 ([#1320]). The mount
//! runs with `--no-mount`, so the run needs no kernel NFS: the NFS
//! listener serves on a free loopback port, and the stop path is the
//! signal's.

#![cfg(unix)]

use std::io::{BufRead as _, BufReader, Read as _};
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread::JoinHandle;

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

/// What a serving mount exposes: the child, the thread that collects
/// its stderr, the dir it serves, its overlay, its pid, and the table
/// its record sits in. The child is in the serving window: it printed
/// its serving lines and held through the grace window below, so it
/// waits for a signal.
struct Serving {
    child: Child,
    warnings: JoinHandle<String>,
    /// The serving lines the helper already read came off this reader;
    /// the stop's own line ends the rest of it.
    stdout: std::io::Lines<BufReader<std::process::ChildStdout>>,
    dir: PathBuf,
    overlay: PathBuf,
    pid: u64,
    table: PathBuf,
}

/// Serves `main`, and waits out the mount's start: the two serving
/// lines, then a grace window that a self-stopped mount does not
/// survive, then the record the table holds.
fn serving(tiers: &common::Tiers, home: &std::path::Path) -> Serving {
    let mut child = serving_mount(tiers, home);
    // The warnings go to the stderr pipe; a thread collects them, so a
    // full pipe cannot stall the mount while the test waits for the
    // stdout lines.
    let stderr_pipe = child.stderr.take().expect("the mount's stderr is piped");
    let warnings = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = BufReader::new(stderr_pipe).read_to_string(&mut text);
        text
    });
    let stdout_pipe = child.stdout.take().expect("the mount's stdout is piped");
    let mut lines = BufReader::new(stdout_pipe).lines();

    // The serving lines come first: the single-user warning, then the
    // mount command. The second is the cue that the record is saved and
    // the listener holds.
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
    // - a mount that stopped on its own would be gone inside one.
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert!(
        child.try_wait().unwrap().is_none(),
        "the mount serves until a signal"
    );
    let table = home.join(".scion").join("mounts.json");
    let record: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&table).unwrap())
        .expect("the mount's record is in the table");
    assert_eq!(record.as_array().map(Vec::len), Some(1), "{record:?}");
    let dir: PathBuf = record[0]["dir"].as_str().expect("the served dir").into();
    let overlay: PathBuf = record[0]["overlay"].as_str().expect("the overlay").into();
    let pid = record[0]["pid"].as_u64().expect("the serving pid");
    assert_eq!(
        Some(pid),
        Some(u64::from(child.id())),
        "the record names the server"
    );
    Serving {
        child,
        warnings,
        stdout: lines,
        dir,
        overlay,
        pid,
        table,
    }
}

/// SIGTERM, the signal a `scn unmount` sends.
fn stop(pid: u32) {
    let sent = Command::new("kill")
        .args(["-s", "TERM", &pid.to_string()])
        .status()
        .expect("the signal goes out");
    assert!(sent.success(), "the signal was delivered");
}

#[test]
fn a_terminated_mount_unmounts_and_answers_0() {
    let data = common::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    start(&store);
    let tiers = common::Tiers::start(data, store);
    let home = common::private_tempdir();
    let mut serving = serving(&tiers, home.path());

    assert!(
        serving.overlay.starts_with(std::env::temp_dir()),
        "the overlay sits under the system's temporary directory: {:?}",
        serving.overlay
    );
    assert!(serving.overlay.is_dir(), "the overlay exists while serving");

    stop(u32::try_from(serving.pid).expect("the pid is a process id"));
    let status = serving.child.wait().expect("the mount ends");
    let stderr = serving.warnings.join().unwrap_or_default();
    assert_eq!(
        status.code(),
        Some(0),
        "a stopped mount answers 0: {stderr}"
    );
    // The rest of the stdout: the stop's own line ends it, and it names
    // the dir the mount stopped serving.
    let mut rest = String::new();
    for line in serving.stdout {
        rest.push_str(&line.expect("the stop's stdout line reads"));
        rest.push('\n');
    }
    assert!(
        rest.contains(&format!("stopped serving {}", serving.dir.display())),
        "the mount names the dir it stopped serving: {rest}"
    );
    assert!(!stderr.contains("scn mount:"), "a clean stop warns nothing");
    // The overlay is gone, and the table holds no record: the next
    // mount of the dir is not refused, and the next unmount finds
    // nothing to clean.
    assert!(
        !serving.overlay.exists(),
        "the overlay is removed at the stop"
    );
    let after = std::fs::read_to_string(&serving.table).unwrap();
    let after: serde_json::Value =
        serde_json::from_str(&after).expect("the table stays valid JSON");
    assert_eq!(
        after.as_array().map(Vec::len),
        Some(0),
        "the record is dropped: {after:?}"
    );
}

#[test]
fn a_stop_that_cannot_clean_names_its_warnings() {
    let data = common::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    start(&store);
    let tiers = common::Tiers::start(data, store);
    let home = common::private_tempdir();
    let mut serving = serving(&tiers, home.path());

    // A root user can remove a 0500 directory: the failure this test
    // drives cannot exist for it, so the test skips rather than break.
    let probe = home.path().join("root-probe");
    std::fs::create_dir_all(probe.join("child")).unwrap();
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o500))
        .expect("the probe's mode changes");
    if std::fs::remove_dir_all(&probe).is_ok() {
        // Running as root: the overlay's mode cannot stop the removal.
        eprintln!("skipping: running as root, the mode cannot refuse root");
        return;
    }
    // The probe dir goes back to a deletable mode, so the test's home
    // can go away; the removal of an empty 0500 directory succeeds for
    // any user (rmdir needs only the parent's write), which is why the
    // probe holds a child: unlinking it needs the write the mode took
    // away.
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o700))
        .expect("the probe's mode goes back");

    // The overlay becomes undeletable: the mount's stop cannot remove
    // it, and must say so instead of ending quietly.
    let mode = std::fs::metadata(&serving.overlay).unwrap().permissions();
    std::fs::set_permissions(&serving.overlay, std::fs::Permissions::from_mode(0o500))
        .expect("the overlay's mode changes");

    stop(u32::try_from(serving.pid).expect("the pid is a process id"));
    let status = serving.child.wait().expect("the mount ends");
    let stderr = serving.warnings.join().unwrap_or_default();
    // The failed cleanup does not fail the stop: the runbook's step is
    // to look, and the exit still answers 0.
    assert_eq!(
        status.code(),
        Some(0),
        "a stop whose cleanup failed still answers 0: {stderr}"
    );
    assert!(
        stderr.contains("scn mount: cannot remove the overlay"),
        "the stop names the overlay it could not remove: {stderr}"
    );
    assert!(
        serving.overlay.exists(),
        "the overlay the stop could not remove is still there"
    );
    // Put the overlay's mode back, so the test's home can go away.
    std::fs::set_permissions(&serving.overlay, mode).expect("the overlay's mode goes back");
}
