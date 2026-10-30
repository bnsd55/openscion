//! The measured restore time of `docs/operations.md`, section 3: build a
//! store of a stated size, take the runbook's backup (the metadata
//! snapshot first, the blob files second), then time the restore into a
//! fresh directory, `fsck` included. A measurement, not a gate: plain
//! `cargo test` skips it, and the `restore time` CI job runs it with
//! `--ignored --nocapture` on a fixed runner label, which is the machine
//! class the docs quote.

use std::fs;
use std::path::Path;
use std::time::Instant;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::Backend;
use scion_server::fsck;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};

/// The stated size of the measured store: repos and changesets per repo.
/// Big enough that the per-byte and per-changeset costs of a restore
/// show past the fixed costs (a store of tens of MiB restores in a
/// fraction of a second, which tells an operator nothing).
const REPOS: u8 = 40;
const CHANGESETS_PER_REPO: u16 = 300;
/// The content bytes of each changeset's own file.
const FILE_BYTES: usize = 64 * 1024;
/// A restore of this size past this bound means something changed in the
/// restore path, not in the machine.
const RESTORE_BOUND_SECS: u64 = 600;

#[test]
#[ignore = "a measurement for the restore-time job: run with --ignored --nocapture"]
fn a_restore_of_a_stated_store_takes_a_stated_time() {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();

    // The build: REPOS repos of CHANGESETS_PER_REPO changesets each, one
    // file of FILE_BYTES per changeset, every chain deriving as it grows.
    let mut blob_bytes: u64 = 0;
    let started = Instant::now();
    for repo in 0..REPOS {
        let name = format!("repo-{repo:03}");
        let mut parent = None;
        // The ref moves once per changeset, through the compare-and-set:
        // the first write creates it (no expectation), the rest name the
        // version they saw, as a client's land does.
        let mut ref_version: Option<i64> = None;
        for step in 0..CHANGESETS_PER_REPO {
            // Distinct, incompressible content per blob: an LCG stream
            // seeded by the repo and the step. A repeating pattern would
            // both deduplicate (two blobs of one ID) and compress (a
            // 64 KiB file of one repeated block stores tens of bytes),
            // and the restore would move a fraction of the raw bytes.
            let mut seed = (u64::from(repo) << 32) ^ u64::from(step);
            // The stream: the LCG's middle bits, truncated to a byte on
            // purpose (clippy::cast_possible_truncation is fine here: the
            // top 31 bits ARE the stream).
            #[expect(
                clippy::cast_possible_truncation,
                reason = "the top 31 bits of the LCG state are the stream"
            )]
            let next = |seed: &mut u64| -> u8 {
                *seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (*seed >> 33) as u8
            };
            let bytes = (0..FILE_BYTES)
                .map(|_| next(&mut seed))
                .collect::<Vec<u8>>();
            let blob = store.blobs().put(&bytes).unwrap();
            blob_bytes += u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let cs = chain_changeset(&name, step, blob, parent);
            let id = store
                .write(|c| {
                    let repo_id = c.create_repo(&name)?;
                    c.add_repo_blob(repo_id, blob)?;
                    let id = c.insert_changeset(repo_id, &cs)?;
                    assert!(c.cas_ref(repo_id, "main", ref_version, id)?);
                    Ok(id)
                })
                .unwrap();
            ref_version = Some(ref_version.map_or(1, |v| v + 1));
            parent = Some(id);
        }
    }
    let build = started.elapsed();
    let changesets = u64::from(REPOS) * u64::from(CHANGESETS_PER_REPO);

    // The backup, in the runbook's order: the metadata snapshot first
    // (the runbook's `pg_dump --schema`, one consistent committed state
    // while the store serves), then the blob files.
    let backup = scion_server::test_support::private_tempdir();
    let snapshot = backup.path().join("metadata.sql");
    // The snapshot and the blob copy are the runbook's own lines.
    take_the_guides_backup(dir.path(), backup.path(), &snapshot);

    // The restore, timed end to end: the metadata and the blob files of
    // the backup into a fresh directory, then `fsck`, then an open and a
    // read - the steps of the runbook's restore. The dump loads into the
    // restored directory's own schema before anything reads it.
    let restore_started = Instant::now();
    let restored_dir = scion_server::test_support::private_tempdir();
    let restored_schema = scion_server::Backend::test_schema(restored_dir.path());
    rename_schema(&snapshot, &restored_schema);
    load_dump(&snapshot);
    copy_tree(
        &backup.path().join("objects"),
        &restored_dir.path().join("objects"),
    );
    // The marker travels with the metadata: the restore copies the data
    // directory back, and the open pairs the marker's UUID with the
    // database's.
    std::fs::copy(
        backup.path().join("backend"),
        restored_dir.path().join("backend"),
    )
    .unwrap();
    // Durability inside the timer: an operator's restore is done when a
    // crash cannot lose what was restored, so the copied files and the
    // directories holding them are fsynced before the clock stops.
    fsync_tree(&restored_dir.path().join("objects"));
    fsync_dir(restored_dir.path());
    let restored = Backend::open_test(restored_dir.path()).unwrap();
    let report = fsck::run(&restored, 1, &mut |_| {}).unwrap();
    assert!(
        report.problems.is_empty(),
        "the restored store has problems: {:?}",
        &report.problems[..report.problems.len().min(3)]
    );
    restored.read(|c| c.repo_id("repo-000")).unwrap();
    drop(restored);
    let restore = restore_started.elapsed();

    // The lines the docs quote: the size, the time, and the rate they
    // imply, so an operator of a bigger store can scale the number. The
    // bytes on disk are the restore's real payload, stated beside the
    // raw content bytes: they are the same only while nothing
    // deduplicates.
    let on_disk = bytes_on_disk(&restored_dir.path().join("objects"));
    #[expect(
        clippy::cast_precision_loss,
        reason = "byte and changeset counts stay far below 2^53"
    )]
    let raw_mib = blob_bytes as f64 / (1024.0 * 1024.0);
    #[expect(
        clippy::cast_precision_loss,
        reason = "byte and changeset counts stay far below 2^53"
    )]
    let disk_mib = on_disk as f64 / (1024.0 * 1024.0);
    println!(
        "restore: {:.1} s (build {:.1} s) for {REPOS} repos, {changesets} changesets, \
         {blob_bytes} raw blob bytes ({raw_mib:.0} MiB) in files of about {FILE_BYTES} bytes;          {on_disk} bytes ({disk_mib:.0} MiB) on disk after the restore",
        restore.as_secs_f64(),
        build.as_secs_f64(),
    );
    #[expect(
        clippy::cast_precision_loss,
        reason = "byte and changeset counts stay far below 2^53"
    )]
    let changeset_rate = changesets as f64 / restore.as_secs_f64().max(0.001);
    println!(
        "restore rate: {:.0} MiB/s on disk, {:.0} changesets/s (ubuntu-24.04, one sample, fsync included)",
        disk_mib / restore.as_secs_f64().max(0.001),
        changeset_rate,
    );
    assert!(
        restore.as_secs() < RESTORE_BOUND_SECS,
        "the restore took {restore:?}, past the {RESTORE_BOUND_SECS} s bound: something \
         changed in the restore path, not in the machine"
    );
}

/// The backup the runbook's own lines take: the metadata snapshot
/// (`pg_dump --no-owner`) first, then the blob copy
/// (`cp -R ... objects`), each with the stated store's directory and
/// the backup folder in place of the documented ones. The dump runs in
/// the job's Postgres container, whose tools match the server's
/// version; its file is the `snapshot` this test restores.
fn take_the_guides_backup(store_dir: &Path, backup: &Path, snapshot: &Path) {
    use scion_server::test_support::docs::run_guide_block_with;
    let schema = Backend::test_schema(store_dir);
    let container = std::env::var("SCION_TEST_PG_CONTAINER").expect("SCION_TEST_PG_CONTAINER");
    let db = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL")
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned();
    let file = fs::File::create(snapshot).unwrap();
    let dump = std::process::Command::new("docker")
        .args(["exec", &container, "pg_dump", "-U", "postgres"])
        .args(["--no-owner", &format!("--schema={schema}"), &db])
        .stdout(std::process::Stdio::from(file))
        .status()
        .expect("docker runs");
    assert!(dump.success(), "pg_dump of {schema} failed");
    let _stdout = run_guide_block_with(
        "The durability point",
        "objects",
        &[
            ("/srv/scion", store_dir.to_str().unwrap()),
            ("/backups/scion-$(date +%F)", backup.to_str().unwrap()),
        ],
    );
    // The guide's third copy: the `backend` marker is part of the
    // metadata - it holds the store UUID the metadata holds too, and
    // the restore pairs the two.
    fs::copy(store_dir.join("backend"), backup.join("backend")).unwrap();
}

/// Renames the schema in the dump, so it loads under the name the
/// restored directory's test backend computes. The names are
/// `scion_test_<16 hex digits>`, which no stored data contains.
fn rename_schema(file: &Path, to: &str) {
    let text = fs::read_to_string(file).unwrap();
    // The bare name: the dump line carries the statement's semicolon,
    // and a `from` that kept it would eat every semicolon it replaced
    // - the CREATE SCHEMA would swallow the next statement, and every
    // other spelling of the name would keep the old schema.
    let from = text
        .lines()
        .find_map(|line| {
            let rest = line.trim_start();
            rest.strip_prefix("CREATE SCHEMA ")
                .map(|name| name.trim_end().trim_end_matches(';').to_owned())
        })
        .expect("the dump carries its schema's name");
    std::fs::write(file, text.replace(&from.clone(), to)).unwrap();
}

/// Loads a dump into the test database, as the runbook's restore does:
/// the job's Postgres container, whose tools match the server's
/// version.
fn load_dump(file: &Path) {
    let container = std::env::var("SCION_TEST_PG_CONTAINER").expect("SCION_TEST_PG_CONTAINER");
    let db = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL")
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned();
    let dump = fs::File::open(file).unwrap();
    let output = std::process::Command::new("docker")
        .args(["exec", "-i", &container, "psql", "-U", "postgres"])
        .args(["-v", "ON_ERROR_STOP=1", "-d", &db])
        .stdin(std::process::Stdio::from(dump))
        .output()
        .expect("docker runs");
    assert!(
        output.status.success(),
        "the dump failed to load: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The byte size of every file under `root`: the restore's real payload.
fn bytes_on_disk(root: &Path) -> u64 {
    walk(root)
        .iter()
        .filter(|p| p.is_file())
        .filter_map(|p| fs::metadata(p).ok())
        .map(|m| m.len())
        .sum()
}

/// fsyncs every file under `root`, then the directories on the way down.
fn fsync_tree(root: &Path) {
    let mut dirs = Vec::new();
    for entry in walk(root) {
        if entry.is_dir() {
            dirs.push(entry);
        } else {
            let _ = fs::File::open(&entry).and_then(|f| f.sync_all());
        }
    }
    for dir in dirs.iter().rev() {
        let _ = fs::File::open(dir).and_then(|f| f.sync_all());
    }
    let _ = fs::File::open(root).and_then(|f| f.sync_all());
}

fn fsync_dir(dir: &Path) {
    let _ = fs::File::open(dir).and_then(|f| f.sync_all());
}

/// Every path under `root`, files and directories, depth first.
fn walk(root: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_owned()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path.clone());
            }
            out.push(path);
        }
    }
    out
}

/// The changeset `step` of a chain in `name`: one file backed by `blob`,
/// on `parent`, with a message that names it.
fn chain_changeset(name: &str, step: u16, blob: BlobId, parent: Option<ChangesetId>) -> Changeset {
    Changeset::new(
        parent.into_iter().collect(),
        IdentityTime::new(name.as_bytes().to_vec(), b"bench@x".to_vec(), 0, 0)
            .expect("identity fits the format"),
        None,
        format!("{name} step {step}").into_bytes(),
        [(
            RepoPath::new(format!("f{step:03}").into_bytes()).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format")
}

/// Every file under `from`, copied to the same path under `to`.
fn copy_tree(from: &Path, to: &Path) {
    let mut stack = vec![from.to_owned()];
    while let Some(d) = stack.pop() {
        for entry in fs::read_dir(&d).unwrap() {
            let entry = entry.unwrap().path();
            if entry.is_dir() {
                stack.push(entry);
            } else {
                let target = to.join(entry.strip_prefix(from).unwrap());
                fs::create_dir_all(target.parent().unwrap()).unwrap();
                fs::copy(&entry, &target).unwrap();
            }
        }
    }
}
