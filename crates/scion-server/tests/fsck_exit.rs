//! The exit codes `scion-server fsck` promises in 1.x, as one table:
//! 0 when it saw no damage, 1 when it did, 2 when it could not run.
//! Every row of the table is a line here, so a release that changes a
//! code fails this test before it ships (docs/stability.md, 1.x, and
//! docs/operations.md, the fsck section).

#![cfg(unix)]

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

/// The `scion-server fsck` exit-code table.
#[test]
fn the_exit_codes_of_fsck() {
    // 0: a clean directory.
    let dir = scion_server::test_support::private_tempdir();
    let store = scion_server::Backend::open_test(dir.path()).unwrap();
    let _blob = store.blobs().put(b"clean\n").unwrap();
    drop(store);
    assert_eq!(
        fsck(dir.path()).status.code(),
        Some(0),
        "a clean store exits 0"
    );

    // 1: damage. A blob file whose bytes no longer hash to its name is
    // the smallest damage there is.
    let objects = dir.path().join("objects/blob");
    let fan = fs::read_dir(&objects)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let file = fs::read_dir(&fan).unwrap().next().unwrap().unwrap().path();
    let mut torn = fs::read(&file).unwrap();
    let last = torn.len() - 1;
    torn[last] ^= 0xff;
    fs::write(&file, torn).unwrap();
    assert_eq!(
        fsck(dir.path()).status.code(),
        Some(1),
        "a damaged store exits 1"
    );

    // 2: cannot run. The metadata store cannot be opened at all, so
    // fsck cannot check anything. The URL names a port nothing listens
    // on, so the run cannot begin, and the exit code is 2.
    let broken = scion_server::test_support::private_tempdir();
    let url = "postgres://postgres:scion@127.0.0.1:1/scion";
    assert_eq!(
        fsck_with(broken.path(), &["--postgres-url".as_ref(), url.as_ref()])
            .status
            .code(),
        Some(2),
        "a metadata store that cannot be reached exits 2"
    );

    // 2: cannot run, again: no Postgres URL at all. fsck cannot check
    // anything when it does not know where the metadata lives, and its
    // exit table says that is 2, not the generic open failure's 1.
    let no_url = scion_server::test_support::private_tempdir();
    assert_eq!(
        fsck_with(no_url.path(), &[]).status.code(),
        Some(2),
        "fsck without a Postgres URL exits 2"
    );
}

/// Runs `scion-server --data <dir> fsck` and returns its status. The
/// binary needs the URL rule every suite uses: the data directory's own
/// schema.
fn fsck(data: &Path) -> Output {
    let url = common::postgres_url(data);
    fsck_with(data, &["--postgres-url".as_ref(), url.as_ref()])
}

fn fsck_with(data: &Path, extra: &[&std::ffi::OsStr]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    cmd.args(["--data"])
        .arg(data)
        .args(extra)
        .arg("fsck")
        // The run under test sees no URL but the one its arguments name:
        // an env form meant for another suite must not save it.
        .env_remove("SCION_POSTGRES_URL")
        .env("NO_COLOR", "1")
        .output()
        .unwrap()
}

// Every test binary compiles this file and uses its own subset of it.
#[path = "common/mod.rs"]
mod common;
