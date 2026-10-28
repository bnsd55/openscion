//! The exit codes `scion-server fsck` promises in 1.x, as one table:
//! 0 when it saw no damage, 1 when it did, 2 when it could not run.
//! Every row of the table is a line here, so a release that changes a
//! code fails this test before it ships (docs/stability.md, 1.x, and
//! docs/operations.md, the fsck section).

#![cfg(unix)]

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use scion_server::Backend;

/// The `scion-server fsck` exit-code table.
#[test]
fn the_exit_codes_of_fsck() {
    // 0: a clean directory.
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
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
    // fsck cannot check anything. The reason names the engine: on
    // SQLite the directory's scion.db is not a database file; on
    // Postgres the URL names a port nothing listens on. Either way the
    // run cannot begin, and the exit code is the same 2.
    let broken = scion_server::test_support::private_tempdir();
    if Backend::test_is_postgres() {
        let url = "postgres://postgres:scion@127.0.0.1:1/scion";
        assert_eq!(
            fsck_with(broken.path(), &["--postgres-url".as_ref(), url.as_ref()])
                .status
                .code(),
            Some(2),
            "a metadata store that cannot be reached exits 2"
        );
    } else {
        fs::write(broken.path().join("scion.db"), b"not a database at all\n").unwrap();
        assert_eq!(
            fsck(broken.path()).status.code(),
            Some(2),
            "a store that cannot open exits 2"
        );
    }
}

/// Runs `scion-server --data <dir> fsck` and returns its status.
fn fsck(data: &Path) -> Output {
    // The binary needs the same URL rule every suite uses: on Postgres,
    // the data directory's own schema.
    let url = if Backend::test_is_postgres() {
        common::postgres_url(data)
    } else {
        String::new()
    };
    let extra = if Backend::test_is_postgres() {
        vec!["--postgres-url".as_ref(), url.as_ref()]
    } else {
        vec![]
    };
    fsck_with(data, &extra)
}

fn fsck_with(data: &Path, extra: &[&std::ffi::OsStr]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    cmd.args(["--data"])
        .arg(data)
        .args(extra)
        .arg("fsck")
        .env("NO_COLOR", "1")
        .output()
        .unwrap()
}

// Every test binary compiles this file and uses its own subset of it.
#[path = "common/mod.rs"]
mod common;
