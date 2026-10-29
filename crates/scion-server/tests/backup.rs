//! A backup taken while the store serves restores whole: the metadata
//! snapshot first, the blob files second, the marker with the metadata
//! (`docs/operations.md`, section 3). The test is a server test: writes
//! run against the real server through the whole backup, the operation
//! ledger (`common::ledger`) records every acknowledgment at reply
//! time, and the check runs against the restored store's own server —
//! so what reads back is what a client would read.

#![cfg(unix)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, RepoPath};
use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};

use common::ledger::{self, Kind, OpLog};
use common::{REFS, REPO, author};

mod common;

/// A backup taken while the store serves restores whole.
///
/// The metadata snapshot first, the blob files second, the marker with
/// the metadata (`docs/operations.md`, section 3). The writes run in
/// this process against the test backend (the shape the Postgres job
/// runs; the server harness cannot speak a test schema), the snapshot
/// is the runbook's `pg_dump` of the live database, and the restore
/// loads it into the restored directory's own schema. The ledger
/// records every acknowledgment, and the same properties the server
/// test checks over RPC are checked against the restored store.
#[test]
fn a_backup_taken_while_serving_restores_whole() {
    backup_postgres();
}

/// Copies every file of `from`, in its subdirectories, into `to`.
fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    for entry in walkdir(from) {
        let rel = entry.strip_prefix(from).unwrap();
        let to = to.join(rel);
        if entry.is_dir() {
            std::fs::create_dir_all(&to).unwrap();
        } else {
            if let Some(parent) = to.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::copy(entry, &to).unwrap();
        }
    }
}

/// Every path under `from`, with the parent directories a copy needs
/// made as it goes.
fn walkdir(from: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    let mut stack = vec![from.to_path_buf()];
    while let Some(dir) = stack.pop() {
        paths.push(dir.clone());
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                paths.push(path);
            }
        }
    }
    paths
}

/// The backup of a Postgres-backed store: the writes run in this
/// process against the test backend (the shape the Postgres job runs),
/// the snapshot is the runbook's `pg_dump` of the live database, and
/// the restore loads it into the restored directory's own schema. The
/// ledger records every acknowledgment, and the same properties the
/// server test checks over RPC are checked against the restored store.
fn backup_postgres() {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let (repo, parent) = pg_repo(&store);

    // The writes the backup must keep: a chain of changesets on `main`,
    // acknowledged before the snapshot, recorded as the ledger sees the
    // acks.
    let mut oplog = OpLog::default();
    let parent = pg_chain(&store, &mut oplog, repo, parent);

    // The writes that race the backup: a thread writes more changesets
    // and moves `main` while the caller walks `objects/`, and the
    // ledger counts what it has acknowledged after each round.
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicUsize::new(0));
    let writer = {
        let store = store.clone();
        let stop = Arc::clone(&stop);
        let acked = Arc::clone(&acked);
        std::thread::spawn(move || pg_raced(&store, repo, parent, &stop, &acked))
    };

    // The writer's first raced acknowledgment: the snapshot starts only
    // once the writer has acknowledged a whole round, so the
    // pre-snapshot window holds raced writes, not just the chain's.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while acked.load(Ordering::SeqCst) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the raced writer never acknowledged"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    // The metadata snapshot: the runbook's `pg_dump --no-owner` of the
    // live database, while the store keeps serving. The snapshot is one
    // consistent committed state; everything acknowledged before the
    // call started must read back after the restore, and what the
    // writer acknowledges from here on may be in or out.
    let pre_snapshot = acked.load(Ordering::SeqCst);
    let dump = scion_server::test_support::private_tempdir();
    let dump_file = dump.path().join("metadata.sql");
    let schema = test_schema(dir.path());
    dump_schema(&schema, &dump_file);

    // The copy of the blob files runs while the writer keeps writing: a
    // blob the snapshot names stays reachable, and the copy may or may
    // not hold the newer ones. `gc` must not run during a backup (the
    // runbook's warning); no test runs one here.
    let backup = scion_server::test_support::private_tempdir();
    copy_tree(&dir.path().join("objects"), &backup.path().join("objects"));
    stop.store(true, Ordering::SeqCst);
    let raced = writer.join().unwrap();
    let raced_entries = raced.entries().len();

    // The ledger the restore must satisfy: the chain, plus every writer
    // acknowledgment that came before the dump. What came after is
    // checked softer: in or out, never torn.
    let (raced_pre, raced_post) = raced.split_at(pre_snapshot);
    oplog.merge(raced_pre);

    // The restore: the dump loads into the restored directory's own
    // schema, the marker travels with the metadata (the runbook's
    // step: it holds the store UUID the metadata holds too), and the
    // blob files of the backup go beside them. `fsck` runs before the
    // store serves, the runbook's order.
    let restored_dir = scion_server::test_support::private_tempdir();
    let restored_schema = test_schema(restored_dir.path());
    rename_in(&dump_file, &schema, &restored_schema);
    load_schema(&dump_file);
    std::fs::copy(
        dir.path().join("backend"),
        restored_dir.path().join("backend"),
    )
    .unwrap();
    copy_tree(
        &backup.path().join("objects"),
        &restored_dir.path().join("objects"),
    );
    let restored = Backend::open_test(restored_dir.path()).unwrap();
    ledger::fsck_ok(&restored, "the restored store");

    // The same properties the server test checks over RPC, read from
    // the restored store: C1, every pre-snapshot blob's bytes; C2, every
    // pre-snapshot changeset decodes; C3, every ref observation sits on
    // the ref's restored history.
    pg_verify(&restored, &oplog, repo);

    // What the writer acknowledged beside the backup: in or out, never
    // torn.
    pg_soft(&restored, &raced_post, repo);
    println!("the writer acknowledged {raced_entries}, {pre_snapshot} before the dump");
}

/// Repo `r` with one root changeset, and refs `main` and `dev` at it:
/// the harness's `create_repo`, against the test backend.
fn pg_repo(store: &Backend) -> (scion_server::test_support::meta::RepoId, ChangesetId) {
    let root_blob = store.blobs().put(b"backup suite\n").unwrap();
    let root = Changeset::new(
        vec![],
        author(),
        None,
        b"root\n".to_vec(),
        [(
            RepoPath::new("README").unwrap(),
            Some(FileState::Regular {
                content: root_blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let id = c.insert_changeset(repo, &root)?;
            for name in REFS {
                assert!(c.cas_ref(repo, name, None, id)?);
            }
            Ok((repo, id))
        })
        .unwrap()
}

/// One changeset of the chain: one file backed by `blob`, on `parent`,
/// with a message that names it.
fn pg_changeset(parent: ChangesetId, name: &str, blob: BlobId, path: &str) -> Changeset {
    Changeset::new(
        vec![parent],
        author(),
        None,
        name.as_bytes().to_vec(),
        [(
            RepoPath::new(path.as_bytes()).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format")
}

/// The chain of changesets the backup must keep: `n` rounds of put and
/// create on `main`, each acknowledged with its ledger entry, then the
/// ref moved to the tip. Returns the tip.
fn pg_chain(
    store: &Backend,
    oplog: &mut OpLog,
    repo: scion_server::test_support::meta::RepoId,
    mut parent: ChangesetId,
) -> ChangesetId {
    for i in 0..8 {
        let bytes = format!("chain {i}\n").into_bytes();
        let blob = store.blobs().put(&bytes).unwrap();
        oplog.record(
            Kind::Blob {
                id: blob,
                sha256: ledger::sha256(&bytes),
                len: bytes.len(),
                used_in: None,
            },
            format!("chain{i}: put"),
        );
        let changeset = pg_changeset(parent, &format!("chain {i}"), blob, &format!("f{i}"));
        let id = changeset.encode_with_id().1;
        store
            .write(|c| {
                c.add_repo_blob(repo, blob)?;
                c.insert_changeset(repo, &changeset)
            })
            .unwrap();
        oplog.link_blob(blob, id, format!("f{i}").as_bytes());
        oplog.record(Kind::Changeset { id }, format!("chain{i}: create"));
        parent = id;
    }
    let version = store
        .write(|c| Ok(c.get_ref(repo, REFS[0])?.expect("main exists").version))
        .unwrap();
    store
        .write(|c| {
            let moved = c.cas_ref(repo, REFS[0], Some(version), parent)?;
            assert!(moved);
            Ok(())
        })
        .unwrap();
    oplog.record(
        Kind::Ref {
            name: REFS[0],
            at: parent,
        },
        "chain: main moved to the chain's tip",
    );
    parent
}

/// The writes that race the backup: rounds of put, create and a ref
/// move, acknowledged while the caller walks `objects/`, until `stop`
/// is set. The count of entries recorded so far is published in
/// `acked` after each whole round.
fn pg_raced(
    store: &Backend,
    repo: scion_server::test_support::meta::RepoId,
    mut parent: ChangesetId,
    stop: &Arc<AtomicBool>,
    acked: &Arc<AtomicUsize>,
) -> OpLog {
    let mut raced = OpLog::default();
    for i in 0_u64.. {
        if stop.load(Ordering::SeqCst) {
            return raced;
        }
        let round = format!("raced-{i}");
        let bytes = format!("{round}\n").into_bytes();
        let blob = store.blobs().put(&bytes).unwrap();
        raced.record(
            Kind::Blob {
                id: blob,
                sha256: ledger::sha256(&bytes),
                len: bytes.len(),
                used_in: None,
            },
            format!("{round}: put"),
        );
        let changeset = pg_changeset(parent, &round, blob, &format!("{round}/f"));
        let id = changeset.encode_with_id().1;
        store
            .write(|c| {
                c.add_repo_blob(repo, blob)?;
                c.insert_changeset(repo, &changeset)
            })
            .unwrap();
        raced.link_blob(blob, id, format!("{round}/f").as_bytes());
        raced.record(Kind::Changeset { id }, format!("{round}: create"));
        // The ref moves too: the observation the ledger records must be
        // one the store made, or the restore check would catch a lie.
        let version = store
            .write(|c| Ok(c.get_ref(repo, REFS[0])?.expect("main exists").version))
            .unwrap();
        store
            .write(|c| {
                let moved = c.cas_ref(repo, REFS[0], Some(version), id)?;
                assert!(moved);
                Ok(())
            })
            .unwrap();
        raced.record(
            Kind::Ref {
                name: REFS[0],
                at: id,
            },
            format!("{round}: main moved"),
        );
        parent = id;
        acked.store(raced.entries().len(), Ordering::SeqCst);
    }
    unreachable!("the loop returns when stop is set")
}

/// The first-parent history of `main`, oldest first, read from
/// `store`: the restored answer to the ledger's RPC walk.
fn pg_history(store: &Backend, repo: scion_server::test_support::meta::RepoId) -> Vec<ChangesetId> {
    let mut head = store
        .read(|c| Ok(c.get_ref(repo, REFS[0])?.expect("main reads back").target))
        .unwrap();
    let mut history = Vec::new();
    loop {
        let (changeset, id) = store
            .read(|c| c.get_changeset(repo, head).map(|cs| (cs, head)))
            .unwrap();
        let parent = changeset.parents().first().copied();
        history.push(id);
        match parent {
            Some(p) => head = p,
            None => break, // the root: a ref may legitimately sit at it
        }
    }
    history
}

/// The restored answers to the ledger check: C1, every pre-snapshot
/// blob's bytes; C2, every pre-snapshot changeset decodes; C3, every
/// ref observation sits on the ref's restored history.
fn pg_verify(restored: &Backend, oplog: &OpLog, repo: scion_server::test_support::meta::RepoId) {
    for entry in oplog.entries() {
        if let Kind::Blob {
            id,
            sha256: want,
            len,
            ..
        } = &entry.kind
        {
            let served = restored.blobs().get(*id).unwrap_or_else(|e| {
                panic!(
                    "acknowledged blob {id} ({}) reads back as: {e}",
                    entry.where_
                )
            });
            let got = ledger::sha256(&served);
            assert_eq!(got, *want, "blob {id} ({}) served torn bytes", entry.where_);
            assert_eq!(
                served.len(),
                *len,
                "blob {id} ({}) served torn bytes",
                entry.where_
            );
        }
        if let Kind::Changeset { id } = &entry.kind {
            restored
                .read(|c| c.get_changeset(repo, *id).map(|_| ()))
                .unwrap_or_else(|e| {
                    panic!(
                        "acknowledged changeset {id} ({}) reads back as: {e}",
                        entry.where_
                    )
                });
        }
        if let Kind::Ref { name: _, at } = &entry.kind {
            let history = pg_history(restored, repo);
            assert!(
                history.contains(at),
                "acknowledged position {at} ({}) is not on main's history after the restore",
                entry.where_
            );
        }
    }
}

/// What the writer acknowledged beside the backup: in or out, never
/// torn.
fn pg_soft(restored: &Backend, raced_post: &OpLog, repo: scion_server::test_support::meta::RepoId) {
    for entry in raced_post.entries() {
        let served = match &entry.kind {
            Kind::Blob { id, .. } => restored.blobs().get(*id),
            Kind::Changeset { id } => {
                // In or out: an absent changeset is fine, a torn one is
                // not. `get_changeset` decodes, so a wrong decode fails
                // here.
                let _ = restored.read(|c| c.get_changeset(repo, *id).map(|_| ()));
                continue;
            }
            _ => continue,
        };
        if let Ok(served) = served {
            let Kind::Blob {
                id,
                sha256: want,
                len,
                ..
            } = &entry.kind
            else {
                unreachable!("served came from the Blob branch");
            };
            assert_eq!(
                ledger::sha256(&served),
                *want,
                "blob {id} ({}) reads back torn",
                entry.where_
            );
            assert_eq!(
                served.len(),
                *len,
                "blob {id} ({}) reads back torn",
                entry.where_
            );
        }
    }
}

/// The schema name the test backend gives `data`: the same hash the
/// server's `test_schema` computes, so the dump loads where the
/// restored store looks.
fn test_schema(data: &std::path::Path) -> String {
    let path = data.canonicalize().unwrap_or_else(|_| data.to_path_buf());
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&path, &mut hasher);
    format!("scion_test_{:016x}", std::hash::Hasher::finish(&hasher))
}

/// Dumps `schema` with the runbook's `pg_dump --no-owner`, beside the
/// server that keeps serving. The dump runs in the job's Postgres
/// container, the only one on the runner: its tools match the server's
/// version, where a client from the runner image may not.
fn dump_schema(schema: &str, out: &std::path::Path) {
    let container = std::env::var("SCION_TEST_PG_CONTAINER")
        .expect("SCION_TEST_PG_CONTAINER names the job's Postgres container");
    let db = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL names the test database")
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned();
    let file = std::fs::File::create(out).unwrap();
    let status = std::process::Command::new("docker")
        .args(["exec", &container, "pg_dump", "-U", "postgres"])
        .args(["--no-owner", &format!("--schema={schema}"), &db])
        .stdout(std::process::Stdio::from(file))
        .status()
        .expect("docker runs");
    assert!(status.success(), "pg_dump of {schema} failed");
    let dumped = std::fs::read_to_string(out).unwrap();
    assert!(
        dumped.contains(schema),
        "the dump does not carry the schema {schema}"
    );
}

/// Renames the schema in the dump, so it loads under the name the
/// restored directory's test backend computes. The names are
/// `scion_test_<16 hex digits>`, which no stored data contains.
fn rename_in(file: &std::path::Path, from: &str, to: &str) {
    let text = std::fs::read_to_string(file).unwrap();
    std::fs::write(file, text.replace(from, to)).unwrap();
}

/// Loads a dump into the database, as the runbook's restore does.
fn load_schema(file: &std::path::Path) {
    let container = std::env::var("SCION_TEST_PG_CONTAINER")
        .expect("SCION_TEST_PG_CONTAINER names the job's Postgres container");
    let db = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL names the test database")
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned();
    let dump = std::fs::File::open(file).unwrap();
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
