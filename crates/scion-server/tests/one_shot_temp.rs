//! A one-shot command ends clean: its staging folder under the data
//! directory's `tmp/` goes with it, on the success path and on the
//! error paths, so the next open's sweep has nothing of a live run to
//! call a crash's leftover. The sweep's own story stays as it was: what
//! it finds is a crash's leftover.

use std::process::Command;

/// A data directory of mode 0700, the store's own rule.
fn data_dir(dir: &std::path::Path) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let data = dir.join("data");
    std::fs::create_dir(&data).unwrap();
    std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o700)).unwrap();
    data
}

fn fsck(data: &std::path::Path) -> (bool, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data)
        .arg("fsck")
        .env("NO_COLOR", "1")
        // fsck opens the store: the URL rides the env, quietly, and the
        // schema is the data directory's own.
        .env(
            "SCION_POSTGRES_URL",
            scion_server::Backend::test_postgres_url(data).unwrap(),
        )
        .output()
        .expect("the fsck runs");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// The staging folders the data directory's `tmp/` holds.
fn staging(data: &std::path::Path) -> Vec<String> {
    let tmp = data.join("tmp");
    let mut names = Vec::new();
    for entry in std::fs::read_dir(&tmp).expect("tmp exists after an open") {
        names.push(entry.unwrap().file_name().to_string_lossy().into_owned());
    }
    names
}

#[test]
fn a_one_shot_command_leaves_no_staging_folder_behind() {
    let dir = scion_server::test_support::private_tempdir();
    let data = data_dir(dir.path());

    // A fresh store: fsck opens it (its first open creates the schema),
    // runs, and exits. The exit is clean, so the run's staging folder
    // goes with it: the next open has nothing of this run to sweep.
    let (ok, stderr) = fsck(&data);
    assert!(ok, "fsck of a fresh store passes: {stderr}");
    assert!(
        staging(&data).is_empty(),
        "a one-shot command leaves no staging folder: {:?}",
        staging(&data)
    );

    // A second fsck on the now-built store: the same story, and the
    // first run left nothing for this one to sweep as a crash's.
    let (ok, stderr) = fsck(&data);
    assert!(ok, "fsck of the built store passes: {stderr}");
    assert!(
        staging(&data).is_empty(),
        "a one-shot command leaves no staging folder: {:?}",
        staging(&data)
    );
}
