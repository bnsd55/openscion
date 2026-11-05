//! One land costs a fixed number of statements, whatever the changeset
//! touches: a land that puts files in 100 paths costs at most a small
//! constant more than one that puts 10. The count covers everything the
//! land's server-side calls run on the store's Postgres clients — the
//! write connection and the readers alike (every client funnels its
//! statements through one seam). The per-land cost is the floor of the
//! land ceiling: a land under the ref's slot lock cannot beat its own
//! round trips, so this guard pins the count the way the tree batch
//! guard pins the derivation's. The count is a process-wide seam, so
//! this guard owns its binary: no other test speaks SQL in its process.

use scion_core::{Changeset, FileState, RepoPath};
use scion_server::Error;
use scion_server::test_support::auth::Caller;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::land::{LandRequest, LandState, Lands};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, test_support::write};
use std::collections::BTreeMap;

const REPO: &str = "bound";

fn identity() -> scion_core::IdentityTime {
    scion_core::IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

fn later() -> Deadline {
    Deadline::new(std::time::Instant::now() + std::time::Duration::from_secs(600))
}

/// A store with repo `bound`, a root changeset on `main` that holds no
/// files, and the blobs of `paths` uploaded.
fn store_with_root(paths: &[String]) -> (tempfile::TempDir, Backend) {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    for path in paths {
        store.blobs().put(path.as_bytes()).unwrap();
    }
    let root = Changeset::new(
        Vec::new(),
        identity(),
        None,
        b"root\n".to_vec(),
        paths
            .iter()
            .map(|path| {
                (
                    RepoPath::new(path.as_str()).unwrap(),
                    Some(FileState::Regular {
                        content: scion_core::BlobId::of(path.as_bytes()),
                        executable: false,
                    }),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    )
    .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let (root, _) =
                scion_server::test_support::tree::store_changeset(c, repo, &root, &mut |_| Ok(0))?;
            assert!(c.cas_ref(repo, "main", None, root)?);
            for path in paths {
                c.add_repo_blob(repo, scion_core::BlobId::of(path.as_bytes()))?;
            }
            Ok(())
        })
        .unwrap();
    (dir, store)
}

/// A changeset on `base` that puts `paths`, through the write path.
fn changeset_on(
    store: &Backend,
    base: scion_core::ChangesetId,
    paths: &[String],
) -> scion_core::ChangesetId {
    for path in paths {
        store.blobs().put(path.as_bytes()).unwrap();
    }
    let repo = store.read(|c| c.repo_id(REPO)).unwrap();
    let mut changes = BTreeMap::new();
    for path in paths {
        let upload = store.blobs().upload().unwrap();
        let upload = upload.append(path.as_bytes()).unwrap();
        let blob = write::put_blob(store, repo, upload).unwrap();
        changes.insert(
            RepoPath::new(path.as_str()).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        );
    }
    let cs = Changeset::new(vec![base], identity(), None, b"land\n".to_vec(), changes)
        .expect("changeset fits the format");
    write::create_changeset(store, REPO, &cs.encode(), &later()).unwrap()
}

/// Lands `cs` on `main` once: the store's ref does not move under the
/// test, so a retry would mean the land itself is broken.
fn land_once(
    store: &Backend,
    lands: &Lands,
    cs: scion_core::ChangesetId,
) -> scion_server::test_support::land::LandOutcome {
    let request = LandRequest {
        repo: REPO.into(),
        changeset: cs,
        target_ref: "main".into(),
        request_key: cs.to_string(),
        deadline: later(),
    };
    match lands.land(store, &Caller("t".into()), &request) {
        Ok(outcome) => outcome,
        Err(Error::Aborted(detail)) => panic!("the land aborted once: {detail}"),
        Err(error) => panic!("the land failed: {error}"),
    }
}

fn land_statement_count(paths: usize) -> u64 {
    let paths: Vec<String> = (0..paths).map(|i| format!("p{i:03}.txt")).collect();
    let (_dir, store) = store_with_root(&paths.iter().take(1).cloned().collect::<Vec<_>>());
    let lands = Lands::default();
    let base = store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?
                .expect("the root ref exists")
                .target)
        })
        .unwrap();
    let cs = changeset_on(&store, base, &paths);
    Backend::test_reset_statement_count();
    let outcome = land_once(&store, &lands, cs);
    assert_eq!(outcome.state, LandState::Landed, "{}", outcome.detail);
    let count = Backend::test_statement_count();
    if std::env::var("SCION_BOUND_SHAPES").is_ok() {
        eprintln!("shapes: {:?}", Backend::test_statement_log());
    }
    eprintln!("a land over {} paths cost {count} statements", paths.len());
    count
}

/// The seam is process-wide, so the tests take turns: parallel windows
/// would count each other's statements.
static LOG_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn a_land_costs_a_constant_more_over_a_hundred_paths_and_stays_under_its_bound() {
    const BOUND: u64 = 36;
    const EXACT: u64 = 28;
    let _log = LOG_LOCK.lock().unwrap();
    // Today's count, with slack for one more plan attempt: each fold of
    // the round-trip card lowers this number, and a change that raises
    // it must say why. Measured in CI (probe run 41899707438): main's
    // land over 3 paths costs 37; the read-back fold brought 36; that
    // fold opened each transaction in one round trip - BEGIN and its
    // guard SET LOCALs ride one batch, on the writer transactions and
    // the reader's alike; the next fold joined the record's repos read
    // into the key lookup; the next joined the finish's ref read into
    // the land's outcome read: 30. This fold asks the same read whether
    // the head's root tree is stored, and the pre-settle fill runs only
    // when it is missing: the land costs 28.
    //
    // This run has no rival writer, so the land settles in one attempt;
    // the log's one verdict UPDATE proves it. On that run the count is
    // exact, so a statement coming back fails the test here, and the
    // bound keeps guarding only the retried case it has slack for.
    let count = land_statement_count(3);
    {
        let log = Backend::test_statement_log();
        let settles = log
            .iter()
            .filter(|sql| sql.contains("UPDATE lands SET state = $3"))
            .count();
        assert_eq!(settles, 1, "the land settled more than once:\n{log:#?}");
        assert_eq!(
            count, EXACT,
            "a land cost {count} statements, not {EXACT}:\n{log:#?}"
        );
    }
    assert!(
        count <= BOUND,
        "a land cost {count} statements, over the bound {BOUND}:\n{:#?}",
        Backend::test_statement_log()
    );
    // The stored-tree path fills nothing: between the plan's ROLLBACK
    // and the settle's BEGIN sit no statements at all.
    let log = Backend::test_statement_log();
    let plan_end = log
        .iter()
        .rposition(|sql| sql.starts_with("ROLLBACK"))
        .expect("the plan's transaction rolled back");
    let settle_begin = log[plan_end..]
        .iter()
        .position(|sql| sql.starts_with("BEGIN;"))
        .map(|at| plan_end + at)
        .expect("the settle transaction begins");
    assert_eq!(
        plan_end + 1,
        settle_begin,
        "the stored head's tree fills nothing: between the plan and the \
         settle sit statements:\n{:?}",
        &log[plan_end + 1..settle_begin]
    );
    let small = land_statement_count(10);
    let large = land_statement_count(100);
    let growth = large.saturating_sub(small);
    eprintln!("the growth from 10 to 100 paths is {growth} statements");
    assert!(
        growth <= 8,
        "a land over 100 paths cost {large} statements, over 10 cost {small}: \
         the difference {growth} grew with the paths"
    );
}

/// A head whose root tree row is missing (a version bump's leftover, or
/// a row an operator removed) still lands, and its derive runs before
/// the settle transaction, in a transaction of its own: the fill's
/// reads sit between the plan's ROLLBACK and its own BEGIN, its
/// transaction writes the head's derived row, and the settle
/// transaction - the one that moves the ref - writes the land's own
/// derived row alone.
#[test]
fn a_land_whose_head_tree_is_missing_fills_before_the_write_transaction() {
    let _log = LOG_LOCK.lock().unwrap();
    let paths: Vec<String> = (0..3).map(|i| format!("p{i:03}.txt")).collect();
    let (dir, store) = store_with_root(&paths.iter().take(1).cloned().collect::<Vec<_>>());
    let lands = Lands::default();
    let base = store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?
                .expect("the root ref exists")
                .target)
        })
        .unwrap();
    let cs = changeset_on(&store, base, &paths);
    // The head's root tree row goes away: the delete is the test's own
    // connection, on the store's own URL - preflight rules do not bind
    // a test that stages a broken store.
    let url = scion_server::Backend::test_postgres_url(dir.path()).unwrap();
    let mut raw = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    let deleted = raw
        .execute(
            "DELETE FROM derived WHERE kind = 'root_tree' AND cs = $1",
            &[&base.as_bytes().to_vec()],
        )
        .unwrap();
    assert_eq!(deleted, 1, "the head's root tree row was there");
    drop(raw);
    Backend::test_reset_statement_count();
    let outcome = land_once(&store, &lands, cs);
    assert_eq!(outcome.state, LandState::Landed, "{}", outcome.detail);
    let log = Backend::test_statement_log();
    let plan_end = log
        .iter()
        .rposition(|sql| sql.starts_with("ROLLBACK"))
        .expect("the plan's transaction rolled back");
    // The fill commits its own transaction: derived::fill's step write
    // rides `store.commit`, so the first BEGIN after the plan is the
    // fill's, not the settle's. Name the two windows so the next reader
    // does not trade them.
    let fill_begin = log[plan_end..]
        .iter()
        .position(|sql| sql.starts_with("BEGIN;"))
        .map(|at| plan_end + at)
        .expect("the fill's transaction begins");
    let fill_end = log[fill_begin..]
        .iter()
        .position(|sql| sql == "COMMIT")
        .map(|at| fill_begin + at)
        .expect("the fill's transaction commits");
    // The fill's reads sit before its transaction, reads all: an
    // unbounded derive never holds a write connection until the write
    // itself.
    let fill_reads = &log[plan_end + 1..fill_begin];
    assert!(
        fill_reads.iter().all(|sql| sql.starts_with("SELECT")),
        "the fill only reads before its transaction:\n{fill_reads:#?}"
    );
    assert!(
        fill_reads
            .iter()
            .any(|sql| sql.starts_with("SELECT gen FROM changesets")),
        "the fill reads the generation among its reads:\n{fill_reads:#?}"
    );
    let fill_writes = log[fill_begin..fill_end]
        .iter()
        .filter(|sql| sql.starts_with("INSERT INTO derived"))
        .count();
    assert_eq!(
        fill_writes,
        1,
        "the fill's transaction writes the head's derived row:\n{:#?}",
        &log[fill_begin..fill_end]
    );
    // The settle transaction is the one that moves the ref: its window
    // holds the CAS UPDATE. The transaction still READS the parent's
    // tree value - the store_changeset the land always does, and the
    // reason the fill had to run first.
    let cas = log
        .iter()
        .position(|sql| sql.starts_with("UPDATE refs SET target"))
        .expect("the land's CAS moves the ref");
    let settle_begin = log[..cas]
        .iter()
        .rposition(|sql| sql.starts_with("BEGIN;"))
        .expect("the settle transaction begins");
    let settle_end = log[settle_begin..]
        .iter()
        .position(|sql| sql == "COMMIT")
        .map(|at| settle_begin + at)
        .expect("the settle transaction commits");
    let derived_writes = log[settle_begin..settle_end]
        .iter()
        .filter(|sql| sql.starts_with("INSERT INTO derived"))
        .count();
    assert_eq!(
        derived_writes,
        1,
        "the settle transaction writes the land's own derived row alone:\n{:#?}",
        &log[settle_begin..settle_end]
    );
}
