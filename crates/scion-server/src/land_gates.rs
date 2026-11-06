//! The land path's named pause points, behind the test-hooks feature. A
//! point parks only when the test armed it: the test writes an arm file
//! before it sends the land, the land that reaches the armed point writes
//! a pause marker and waits for its resume file, and a land nobody armed
//! passes without a file touched. A test script orders lands exactly where
//! timing alone leaves chance.
//!
//! The marker root comes from `SCION_TEST_GATE_DIR`, read once at the
//! first gate a process reaches; a process that runs with the variable
//! unset never pauses.
//!
//! What a parked land holds, point by point: `record-committed` and
//! `settled` hold nothing — their transactions are closed and no lock is
//! theirs; `planned`, `before-cas` and `finish-step` hold the ref's slot
//! lock, so a rival land to the same ref queues until the resume (a rival
//! that must move the ref runs as an import, which takes no land lock).
//! The land path is synchronous: a parked land holds one of the blocking
//! threads the runtime lends synchronous work, not a runtime worker — a
//! script keeps its parked count at one or two, far under the pool's size.
//!
//! The files: `<dir>/arm/<point>-<key>` arms the point for the land of
//! request key `key`; `<dir>/pause/<point>-<key>` says the land stands
//! there; `<dir>/resume/<point>-<key>` releases it. The arm goes when it
//! is spent (one park per arm), and the pause marker goes when the resume
//! lands — the run's directory is the test's own temp dir, fresh per run.
//!
//! Release builds have no gate: the module and every call compile out, and
//! the environment variable is never read.
#![cfg(any(test, feature = "test-hooks"))]

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// The points a land can stand at, named as the marker files spell them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Gate {
    /// After the record transaction committed, before the ref's slot lock.
    RecordCommitted,
    /// After the plan's read, before the settle.
    Planned,
    /// After the lens read, before the settle's write transaction opens:
    /// the compare-and-swap's version is the plan's, so a ref move during
    /// the park loses the CAS.
    BeforeCas,
    /// After the settle committed, before the reply (the flush sits
    /// between; the gate fires first).
    Settled,
    /// Between the derive steps of the finish's walk.
    FinishStep,
}

impl Gate {
    fn name(self) -> &'static str {
        match self {
            Gate::RecordCommitted => "record-committed",
            Gate::Planned => "planned",
            Gate::BeforeCas => "before-cas",
            Gate::Settled => "settled",
            Gate::FinishStep => "finish-step",
        }
    }
}

/// The marker root, read once: `SCION_TEST_GATE_DIR`. `None` when the
/// variable is unset — every gate then passes without a file touched.
fn root() -> Option<&'static PathBuf> {
    static ROOT: OnceLock<Option<PathBuf>> = OnceLock::new();
    ROOT.get_or_init(|| std::env::var_os("SCION_TEST_GATE_DIR").map(std::path::PathBuf::from))
        .as_ref()
}

/// The land of request key `key` stands at `gate`: an arm file arms the
/// point, the pause marker says the land stands there, the resume file
/// releases it.
///
/// # Panics
///
/// Panics when the resume never comes inside the ceiling — a park nobody
/// lifts fails the holder with the point and the key in the message, the
/// way the store's write park does.
pub(crate) fn hit(gate: Gate, key: &str) {
    let Some(dir) = root() else {
        return;
    };
    let name = format!("{}-{key}", gate.name());
    let arm = dir.join("arm").join(&name);
    if !arm.exists() {
        return;
    }
    let pause = dir.join("pause").join(&name);
    let resume = dir.join("resume").join(&name);
    std::fs::create_dir_all(dir.join("pause")).expect("the gate's pause dir is writable");
    std::fs::write(&pause, b"").expect("the gate's pause marker is writable");
    // The ceiling sits well above any CI pause the suite lifts, and well
    // under the land client's 600 s deadline, so a park nobody lifts fails
    // this holder, not the client.
    let give_up = Instant::now() + Duration::from_secs(180);
    while !resume.exists() {
        assert!(
            Instant::now() < give_up,
            "the land gate {name} was never resumed inside its ceiling"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let _ = std::fs::remove_file(&pause);
    // One park per arm: a land that replans through the point again goes
    // on without parking, and a stale arm never parks a later land.
    let _ = std::fs::remove_file(&arm);
}
