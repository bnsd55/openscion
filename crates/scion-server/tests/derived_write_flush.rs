//! The write path's derived reads and deletes ride the transaction's own
//! memory: one preloaded statement answers the per-value reads, and the
//! old-row deletes wait for the commit and land as one statement. These
//! tests prove the end state, not the round trips: a row the write path
//! deleted is gone after the transaction commits, and a write that
//! disagrees with a stored row still refuses, whether the read that
//! checks it answers from the transaction's memory or the database.

use std::sync::{Mutex, MutexGuard};

use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};
use scion_server::test_support::auth::Caller;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::land::{LandRequest, LandState, Lands};
use scion_server::test_support::write;

use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::tree;

const REPO: &str = "write-flush";

/// The statement log these tests read is one process-wide seam: the
/// tests run in parallel, and a counting window that only this file
/// held would catch another test's statements and count the wrong
/// shapes. Every test in the file holds the lock for its whole body.
static LOG_LOCK: Mutex<()> = Mutex::new(());

fn log_guard() -> MutexGuard<'static, ()> {
    LOG_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn changeset(message: &str, parent: Option<scion_core::ChangesetId>) -> Changeset {
    Changeset::new(
        parent.map(|parent| vec![parent]).unwrap_or_default(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        message.as_bytes().to_vec(),
        std::collections::BTreeMap::from([(
            RepoPath::new("a.txt").unwrap(),
            Some(FileState::Regular {
                content: BlobId::of(message.as_bytes()),
                executable: false,
            }),
        )]),
    )
    .expect("changeset fits the format")
}

#[test]
fn the_old_rows_a_write_path_deletes_are_gone_after_its_commit() {
    let _log = log_guard();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    let parent = changeset("parent\n", None);
    let parent_id = store
        .write(
            |c| Ok(tree::store_changeset(c, repo, &parent, &mut |_| Ok(0))?.0),
            None,
        )
        .unwrap();

    // A row of an older version of the root-tree kind, as a version
    // bump left behind would hold.
    store
        .write(|c| c.put_derived("root_tree", 0, parent_id, b"old-version-value"))
        .unwrap();
    let before = store
        .read(|c| c.get_derived("root_tree", 0, parent_id))
        .unwrap();
    assert_eq!(before.as_deref(), Some(&b"old-version-value"[..]));

    // The write path deletes below the kind's version and defers the
    // delete to its commit. After the transaction commits, the row is
    // gone: the flush ran inside it, so a rollback would have kept it.
    let child = changeset("child\n", Some(parent_id));
    store
        .write(
            |c| tree::store_changeset(c, repo, &child, &mut |_| Ok(0)),
            None,
        )
        .unwrap();
    let after = store
        .read(|c| c.get_derived("root_tree", 0, parent_id))
        .unwrap();
    assert_eq!(
        after, None,
        "the deferred delete must have run at the commit"
    );
}

#[test]
fn a_write_that_disagrees_with_a_stored_row_still_refuses() {
    let _log = log_guard();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    let cs = changeset("root\n", None);
    let id = store
        .write(
            |c| Ok(tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0),
            None,
        )
        .unwrap();

    // The second write of the same row in one transaction holds a value
    // the stored row disagrees with: corruption, whether the check's
    // read answers from the transaction's memory or the database.
    let err = store
        .write(|c| {
            c.put_derived("root_tree", 1, id, b"first")?;
            c.put_derived("root_tree", 1, id, b"second")
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("differs from the stored value"),
        "expected the stored-value conflict, got {err:?}"
    );
}

#[test]
fn the_write_paths_derived_reads_ride_one_preload() {
    let _log = log_guard();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    let parent = changeset("parent\n", None);
    let parent_id = store
        .write(
            |c| Ok(tree::store_changeset(c, repo, &parent, &mut |_| Ok(0))?.0),
            None,
        )
        .unwrap();
    let child = changeset("child\n", Some(parent_id));
    Backend::test_reset_statement_count();
    store
        .write(
            |c| tree::store_changeset(c, repo, &child, &mut |_| Ok(0)),
            None,
        )
        .unwrap();
    let log = Backend::test_statement_log();
    let reads = log
        .iter()
        .filter(|sql| sql.contains("SELECT value FROM derived"))
        .count();
    let deletes = log
        .iter()
        .filter(|sql| sql.starts_with("DELETE FROM derived"))
        .count();
    let preloads = log
        .iter()
        .filter(|sql| sql.starts_with("SELECT kind, version, cs, value FROM derived"))
        .count();
    assert_eq!(
        reads, 0,
        "the build's per-value reads must answer from the preload"
    );
    assert_eq!(
        deletes, 0,
        "the write path's deletes must wait for the commit flush"
    );
    assert_eq!(
        preloads, 1,
        "the build's derived reads ride one preload statement"
    );
}

/// A throwaway git repository: `fast_commits` writes a chain of commits
/// on `main`, whose import seeds the path history the write path's
/// deferral proves against.
struct Git(tempfile::TempDir);

impl Git {
    fn new() -> Self {
        use std::io::Write as _;
        use std::process::{Command, Stdio};
        let dir = scion_server::test_support::private_tempdir();
        let out = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .current_dir(dir.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(out.status.success());
        let mut stream = Vec::new();
        for i in 0..3 {
            let (message, content) = (format!("c{i}\n"), format!("{i}\n"));
            let when = 1_700_000_000 + i;
            writeln!(stream, "commit refs/heads/main").unwrap();
            writeln!(stream, "committer Bob <bob@example.com> {when} +0000").unwrap();
            write!(stream, "data {}\n{message}", message.len()).unwrap();
            writeln!(stream, "M 100644 inline g/f{i}").unwrap();
            writeln!(stream, "data {}\n{content}", content.len()).unwrap();
        }
        let mut child = Command::new("git")
            .args(["fast-import", "--quiet"])
            .current_dir(dir.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&stream).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        Self(dir)
    }

    fn path(&self) -> &std::path::Path {
        self.0.path()
    }
}

/// A changeset on `base` that puts one file, stored through the write
/// path: the blob is stored and made the repo's first, like the
/// server's own `PutBlob` flow.
fn make_changeset(
    store: &Backend,
    repo: scion_server::test_support::meta::RepoId,
    base: scion_core::ChangesetId,
    name: &str,
) -> scion_core::ChangesetId {
    store.blobs().put(name.as_bytes()).unwrap();
    store
        .write(|c| c.add_repo_blob(repo, BlobId::of(name.as_bytes())))
        .unwrap();
    let cs = Changeset::new(
        vec![base],
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_100,
            0,
        )
        .unwrap(),
        None,
        format!("{name}\n").as_bytes().to_vec(),
        std::collections::BTreeMap::from([(
            RepoPath::new(name).unwrap(),
            Some(FileState::Regular {
                content: BlobId::of(name.as_bytes()),
                executable: false,
            }),
        )]),
    )
    .unwrap();
    let deadline = Deadline::new(std::time::Instant::now() + std::time::Duration::from_secs(600));
    write::create_changeset(store, "rebase-hist", &cs.encode(), &deadline).unwrap()
}

/// Lands `cs` on `main` once.
fn land_once(
    lands: &Lands,
    store: &Backend,
    cs: scion_core::ChangesetId,
    key: &str,
) -> scion_server::test_support::land::LandOutcome {
    lands
        .land(
            store,
            &Caller("t".into()),
            &LandRequest {
                repo: "rebase-hist".into(),
                changeset: cs,
                target_ref: "main".into(),
                request_key: key.into(),
                deadline: Deadline::new(
                    std::time::Instant::now() + std::time::Duration::from_secs(600),
                ),
            },
        )
        .unwrap()
}

/// A land whose rebase makes a new changeset writes its history nodes
/// through the commit's flush: the write path buffers them and the
/// commit writes the transaction's nodes with one statement. The rows
/// are there after the commit, and the land answers.
#[test]
fn a_rebasing_lands_history_nodes_ride_the_commit_flush() {
    let _log = LOG_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let git = Git::new();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    scion_server::import_git(
        &store,
        "rebase-hist",
        git.path(),
        &scion_server::ImportOptions {
            rev: "main".to_owned(),
            target_ref: "main".to_owned(),
            verify_sample: 1000,
        },
    )
    .unwrap();
    let repo = store.read(|c| c.repo_id("rebase-hist")).unwrap();
    let tip = store
        .read(|c| Ok(c.get_ref(repo, "main")?.expect("the ref exists").target))
        .unwrap();

    // One land moves `main`; a second changeset still hangs from the
    // imported tip, so the second land's rebase makes a new changeset,
    // whose path history derives inside the land's write transaction.
    let lands = Lands::default();
    let first = make_changeset(&store, repo, tip, "a.txt");
    let outcome = land_once(&lands, &store, first, "first");
    assert_eq!(outcome.state, LandState::Landed, "{}", outcome.detail);
    let second = make_changeset(&store, repo, tip, "b.txt");

    Backend::test_reset_statement_count();
    let outcome = land_once(&lands, &store, second, "second");
    assert_eq!(outcome.state, LandState::Landed, "{}", outcome.detail);
    let log = Backend::test_statement_log();
    let direct = log
        .iter()
        .filter(|sql| sql.contains("INSERT INTO history_nodes") && sql.contains("VALUES"))
        .count();
    let flushed = log
        .iter()
        .filter(|sql| sql.contains("INSERT INTO history_nodes") && sql.contains("unnest"))
        .count();
    assert_eq!(
        direct, 0,
        "the rebase's history nodes must wait for the commit flush"
    );
    assert_eq!(
        flushed, 1,
        "the commit must write the transaction's history nodes with exactly one statement, \
         not {flushed}"
    );
    // The rows are there, not only the statement: the flush wrote the
    // rebase's nodes into the table, more than the import and the
    // first land left.
    let schema = Backend::test_schema(dir.path());
    let mut client = postgres::Client::connect(
        &std::env::var("SCION_TEST_POSTGRES_URL").unwrap(),
        postgres::NoTls,
    )
    .unwrap();
    let count = |client: &mut postgres::Client| -> i64 {
        let rows = client
            .query(&format!("SELECT count(*) FROM {schema}.history_nodes"), &[])
            .unwrap();
        rows[0].get(0)
    };
    let third = make_changeset(&store, repo, tip, "c.txt");
    let before = count(&mut client);
    Backend::test_reset_statement_count();
    let outcome = land_once(&lands, &store, third, "third");
    assert_eq!(outcome.state, LandState::Landed, "{}", outcome.detail);
    let flushed_third = Backend::test_statement_log()
        .iter()
        .filter(|sql| sql.contains("INSERT INTO history_nodes") && sql.contains("unnest"))
        .count();
    let after = count(&mut client);
    assert!(
        after > before,
        "the land's commit flush stored no history nodes: {after} against {before}"
    );
    assert_eq!(
        flushed_third, 1,
        "the third land's commit wrote its history nodes with exactly one statement, \
         not {flushed_third}"
    );

    // The landed head's history answers: the flush ran inside the
    // transaction, so a rollback would have kept the rows.
    let head = store
        .read(|c| Ok(c.get_ref(repo, "main")?.expect("the ref exists").target))
        .unwrap();
    assert_ne!(
        head, second,
        "the land moved the ref to the rebased changeset"
    );
}
