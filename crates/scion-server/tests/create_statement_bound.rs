//! One `CreateChangeset` costs a fixed number of statements, whatever the
//! changeset puts: the count covers everything the call runs on the
//! store's Postgres clients, the write transaction and the pre-reads
//! alike (every client funnels its statements through one seam). The
//! count is the floor of the create ceiling: a caller under the write
//! lane's lock cannot beat its own round trips. The seam is
//! process-wide, so this guard owns its binary: no other test speaks
//! SQL in its process.

use scion_core::{Changeset, FileState, RepoPath};
use scion_server::Backend;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::write;
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
/// files, and the blob of `paths` uploaded.
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
        BTreeMap::new(),
    )
    .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let (root, _) =
                scion_server::test_support::tree::store_changeset(c, repo, &root, &mut |_| Ok(0))?;
            assert!(c.cas_ref(repo, "main", None, root)?);
            Ok(())
        })
        .unwrap();
    (dir, store)
}

/// One one-put `CreateChangeset` through the write path, its statement
/// count measured around the call alone.
fn create_statement_count() -> u64 {
    let (_dir, store) = store_with_root(&["p000.txt".to_owned()]);
    let repo = store.read(|c| c.repo_id(REPO)).unwrap();
    let base = store
        .read(|c| {
            Ok(c.get_ref(repo, "main")?
                .expect("the root ref exists")
                .target)
        })
        .unwrap();
    let upload = store.blobs().upload().unwrap();
    let upload = upload.append(b"p000.txt" as &[u8]).unwrap();
    let blob = write::put_blob(&store, repo, upload).unwrap();
    let mut changes = BTreeMap::new();
    changes.insert(
        RepoPath::new("p000.txt").unwrap(),
        Some(FileState::Regular {
            content: blob,
            executable: false,
        }),
    );
    let cs = Changeset::new(vec![base], identity(), None, b"one\n".to_vec(), changes)
        .expect("changeset fits the format");
    Backend::test_reset_statement_count();
    write::create_changeset(&store, REPO, &cs.encode(), &later()).unwrap();
    let count = Backend::test_statement_count();
    if std::env::var("SCION_BOUND_SHAPES").is_ok() {
        eprintln!("shapes: {:?}", Backend::test_statement_log());
    }
    eprintln!("a one-put create cost {count} statements");
    count
}

/// The seam is process-wide, so the tests take turns: parallel windows
/// would count each other's statements.
static LOG_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn a_one_put_create_costs_a_fixed_number_of_statements() {
    // Measured on main (2d79948): the create path costs 25 - the two
    // pre-reads (the repo read and the parent check in their own read
    // transaction, then the blob re-check's misses query), then the
    // write transaction's 19: the gc key, the repos read repeated, the
    // parent check repeated, the retry check, the derived preload, the
    // trees read, the trees INSERT, the changesets INSERT, the parents
    // INSERT, the membership INSERT, the repo_blobs INSERT, three
    // derived INSERTs (one per kind), the history read, the history
    // INSERT, the sweep's one deferred DELETE, the COMMIT.
    //
    // #930 folds the three derived INSERTs into one unnest INSERT, and
    // joins the in-transaction repos read, the parent check and the
    // retry check into one statement; #917 folds the lane key into the
    // write transaction's open batch. #1004 folds the create's tail
    // twice more: the changeset row, its parents and its membership
    // ride one CTE chain, and the trees, the derived rows and the repo
    // blobs ride a second - each arm keeping its own ON CONFLICT rule.
    // #1031 folds the history INSERT into that second chain as its
    // fourth arm: the settle drains the transaction's deferred history
    // buffer, so the commit's flush has nothing to write.
    // #1031's pre-read then moves the three reads the settle ran on the
    // write connection - the derived preload, the trees on the changed
    // paths, the history node the build reads first - onto a reader
    // before the transaction begins, in its own read transaction: its
    // begin, the derived values WITHOUT the generation (the transaction
    // reads the generation itself - the insert's own read), the trees
    // read, the history read, and its rollback. Every pre-read answer is
    // committed and immutable or write-once, so it cannot be stale, and
    // every miss re-reads in the transaction. The write transaction is 7
    // statements, from main's 9 (measured with this same window against
    // 3a9ea1e): three reads moved off the lane, and the insert's own
    // generation read took one back - the preload's cache answered it
    // before. The whole call is 18.
    // The exact count below is the measured number (18); a statement
    // coming or going fails in both directions. This test owns its
    // binary: no other test speaks SQL in its process.
    const EXACT: u64 = 18;
    // The lane pin: the statements between the write transaction's begin
    // (the one that carries the lane key) and its commit. The lane is
    // what the PR's gain rides: 7, from main's 9. The total above stays
    // 18 if the pre-read rode the write connection; this count is what
    // catches it.
    const LANE: usize = 7;
    // The red proof: the pre-read on `store.write` instead of
    // `store.read` plants its own begin under the lane key, and the
    // window lands on the pre-read's transaction - the pin fails at 5.
    let _log = LOG_LOCK.lock().unwrap();
    let count = create_statement_count();
    assert_eq!(
        count,
        EXACT,
        "a one-put create cost {count} statements, not {EXACT}:\n{:#?}",
        Backend::test_statement_log()
    );
    let log = Backend::test_statement_log();
    let begin = log
        .iter()
        .position(|s| s.starts_with("BEGIN; SET LOCAL lock_timeout"))
        .expect("the write transaction's begin carries the lane key");
    let lane = log[begin..]
        .iter()
        .position(|s| *s == "COMMIT")
        .expect("the write transaction commits")
        + 1;
    assert_eq!(
        lane, LANE,
        "the create's write transaction cost {lane} statements on the lane, not {LANE}:\n{log:#?}"
    );
}
