//! The log pages read in batches, not per entry: without a path, one
//! statement carries first parents and bodies for the whole page; with a
//! path, the chain of path histories rides one statement per window of
//! 1,000 links, and the entries' bodies one statement per batch. The
//! guards count the statements of one page each, so a per-entry read
//! cannot come back by accident. The node reads stay per link on purpose:
//! a lookup reads one node per part of the path, and the nodes a page
//! reads are the ones its walk visits (see `a_path_log_reads_only_...` in
//! the history's own tests). A blame rides the same windows: its versions
//! come from the path histories the walk needs, and the guards count its
//! statements the same way.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::test_support::blame;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::memory::Bytes;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::read;
use scion_server::{Backend, ImportOptions};

const REPO: &str = "r";

/// The counted sections hold this lock: the statement counter is one
/// process-global number, and another test's reads must not land in a
/// page's count.
static COUNT: Mutex<()> = Mutex::new(());

fn later() -> Deadline {
    Deadline::new(Instant::now() + Duration::from_secs(600))
}

/// A repo whose commits alternate two files: 120 commits, the even ones
/// changing `sub/b.txt` and the odd ones `a.txt`. Returns the store and
/// the tip, with `main` at the tip.
fn setup() -> (tempfile::TempDir, Backend) {
    let dir = scion_server::test_support::private_tempdir();
    let git_dir = tempfile::tempdir().unwrap();
    let d = git_dir.path();
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .args(["-c", "gc.auto=0", "-c", "maintenance.auto=false"])
            .args(args)
            .current_dir(d)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .expect("git runs");
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    git(&["init", "-q", "-b", "main"]);
    git(&["config", "user.email", "a@b.c"]);
    git(&["config", "user.name", "a"]);
    std::fs::write(d.join("a.txt"), b"0\n").unwrap();
    std::fs::create_dir_all(d.join("sub")).unwrap();
    std::fs::write(d.join("sub/b.txt"), b"0\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "root"]);
    for i in 1..=120 {
        let p = if i % 2 == 0 { "sub/b.txt" } else { "a.txt" };
        std::fs::write(d.join(p), format!("{i}\n").into_bytes()).unwrap();
        git(&["add", "."]);
        git(&["commit", "-q", "-m", &format!("c{i}")]);
    }
    let store = Backend::open_test(dir.path()).unwrap();
    let opts = ImportOptions {
        rev: "main".into(),
        target_ref: "main".into(),
        verify_sample: 0,
    };
    scion_server::import_git(&store, REPO, d, &opts).unwrap();
    (dir, store)
}

fn tip(store: &Backend) -> ChangesetId {
    store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?.expect("main exists").target)
        })
        .unwrap()
}

fn path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

/// One page of the plain log, counted: 64 entries, newest first, in a
/// handful of statements - the page and its framing, not 64 reads.
#[test]
fn a_plain_page_rides_one_statement() {
    // The whole test, its seed included, holds the lock: the statement
    // counter is one process-global number, and another test's reads must
    // not land in a page's count.
    let _counted = COUNT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = setup();
    let at = tip(&store);

    Backend::test_reset_statement_count();
    let page = read::log_page(&store, REPO, at, None, 64, &later()).unwrap();
    let statements = Backend::test_statement_count();

    assert_eq!(page.entries.len(), 64, "a full page");
    assert_eq!(page.entries[0].0, at, "the tip leads the page");
    assert!(
        page.next.is_some(),
        "the history has 121 changesets, the walk has more to read"
    );
    assert!(
        statements <= 6,
        "a 64-entry page costs {statements} statements: the page must ride \
         one statement, plus the read transaction's framing"
    );
}

/// The page's cursor goes on where the page stopped: the second page
/// holds the changesets the first one did not, and the whole log reads
/// like one walk.
#[test]
fn a_plain_pages_walk_continues_at_its_cursor() {
    // The whole test, its seed included, holds the lock: the statement
    // counter is one process-global number, and another test's reads must
    // not land in a page's count.
    let _counted = COUNT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = setup();
    let at = tip(&store);

    let first = read::log_page(&store, REPO, at, None, 64, &later()).unwrap();
    let next = first.next.expect("the walk has more to read");
    let rest = read::log_page(&store, REPO, next, None, 64, &later()).unwrap();

    assert_eq!(
        first.entries[63].1.parents()[0],
        next,
        "the cursor is the last entry's own first parent"
    );
    assert_eq!(rest.entries.len(), 57, "121 changesets, 64 read");
    assert_eq!(rest.next, None, "the walk reached the root");
    let mut all: Vec<_> = first.entries.iter().map(|(id, _)| *id).collect();
    all.extend(rest.entries.iter().map(|(id, _)| *id));
    assert_eq!(all.len(), 121, "every changeset exactly once");
}

/// A path page of 30 entries costs its node reads, and three statements
/// for everything else: the chain's values, the entries' bodies, and the
/// framing of the read transactions.
#[test]
fn a_path_page_rides_batches_between_its_node_reads() {
    // The whole test, its seed included, holds the lock: the statement
    // counter is one process-global number, and another test's reads must
    // not land in a page's count.
    let _counted = COUNT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = setup();
    let at = tip(&store);

    Backend::test_reset_statement_count();
    let page = read::log_page(&store, REPO, at, Some(&path("sub/b.txt")), 30, &later()).unwrap();
    let statements = Backend::test_statement_count();

    assert_eq!(page.entries.len(), 30, "a full page");
    for (id, cs) in &page.entries {
        assert!(
            read::changes_path(cs, &path("sub/b.txt")),
            "{id} must change the path"
        );
    }
    // 60 node reads, 2 per entry: the root's row and the sub level's row
    // change with every commit (the two files alternate). Everything else
    // is 3 statements - the chain's values, the bodies, and the changeset
    // of the derive check - plus the framing of up to 3 read transactions.
    assert!(
        statements <= 80,
        "a 30-entry path page costs {statements} statements: the values and \
         the bodies must ride in batches"
    );
}

/// The path page's walk continues at its cursor, and the whole path log
/// reads like one walk: every second commit, oldest first, and the last
/// page's cursor is gone - the first commit made the file.
#[test]
fn a_path_pages_walk_continues_at_its_cursor() {
    // The whole test, its seed included, holds the lock: the statement
    // counter is one process-global number, and another test's reads must
    // not land in a page's count.
    let _counted = COUNT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = setup();
    let at = tip(&store);
    let file = path("sub/b.txt");

    let first = read::log_page(&store, REPO, at, Some(&file), 30, &later()).unwrap();
    let next = first.next.expect("61 commits change the file, 30 read");
    let rest = read::log_page(&store, REPO, next, Some(&file), 30, &later()).unwrap();

    assert_eq!(rest.entries.len(), 30, "a full second page");
    let rest_next = rest.next.expect("the root commit made the file");
    let last = read::log_page(&store, REPO, rest_next, Some(&file), 30, &later()).unwrap();
    assert_eq!(last.entries.len(), 1, "the root commit made the file");
    assert_eq!(last.next, None, "the walk reached the file's first change");
    let mut all: Vec<_> = first.entries.iter().map(|(id, _)| *id).collect();
    all.extend(rest.entries.iter().map(|(id, _)| *id));
    assert_eq!(all.len(), 60, "every second commit, each exactly once");
}

/// The statements one read transaction frames: the guard's room for
/// framing comes from this count, not from a guess.
#[test]
fn a_read_transaction_frames_four_statements() {
    // The whole test, its seed included, holds the lock: the statement
    // counter is one process-global number, and another test's reads must
    // not land in a page's count.
    let _counted = COUNT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = setup();

    Backend::test_reset_statement_count();
    let _ = store.read(|c| c.repo_id(REPO)).unwrap();
    let statements = Backend::test_statement_count();
    assert_eq!(
        statements, 5,
        "one repo lookup plus BEGIN, 2 SETs, ROLLBACK"
    );
}

/// A repo whose commits append a line to `file.txt`: 121 commits, the
/// file at the tip holds 121 lines, and every commit changes the path.
/// Seeded through the import, so the trees are the ones a real repo
/// brings.
fn appended() -> (tempfile::TempDir, Backend) {
    let dir = scion_server::test_support::private_tempdir();
    let git_dir = tempfile::tempdir().unwrap();
    let d = git_dir.path();
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .args(["-c", "gc.auto=0", "-c", "maintenance.auto=false"])
            .args(args)
            .current_dir(d)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .expect("git runs");
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    git(&["init", "-q", "-b", "main"]);
    git(&["config", "user.email", "a@b.c"]);
    git(&["config", "user.name", "a"]);
    std::fs::write(d.join("file.txt"), b"l0\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "root"]);
    for i in 1..=120 {
        let mut text = std::fs::read_to_string(d.join("file.txt")).unwrap();
        writeln!(text, "l{i}").unwrap();
        std::fs::write(d.join("file.txt"), text).unwrap();
        git(&["add", "."]);
        git(&["commit", "-q", "-m", &format!("c{i}")]);
    }
    let store = Backend::open_test(dir.path()).unwrap();
    let opts = ImportOptions {
        rev: "main".into(),
        target_ref: "main".into(),
        verify_sample: 0,
    };
    scion_server::import_git(&store, REPO, d, &opts).unwrap();
    (dir, store)
}

/// A blame whose walk explains every line: 100 versions, one statement
/// window of path histories between the node reads. The first call pays
/// the derivations; the counted second call is the walk's own cost, and
/// a per-version read of the path histories cannot hide in it.
#[test]
fn a_blame_rides_windows_between_its_node_reads() {
    // The whole test, its seed included, holds the lock: the statement
    // counter is one process-global number, and another test's reads must
    // not land in a page's count.
    let _counted = COUNT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = appended();
    let at = tip(&store);
    let file = path("file.txt");

    let warm = blame::blame(
        &store,
        REPO,
        at,
        &file,
        &blame::Resume::default(),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap();
    assert!(
        warm.resume.is_some(),
        "121 versions, the version cap stops the walk"
    );

    Backend::test_reset_statement_count();
    let out = blame::blame(
        &store,
        REPO,
        at,
        &file,
        &blame::Resume::default(),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap();
    let statements = Backend::test_statement_count();

    assert_eq!(out.changesets.len(), 100, "the walk ran to its version cap");
    assert!(out.resume.is_some(), "121 versions, 100 explained");
    // One window of path histories covers the whole walk here, and the
    // window's one statement replaces the per-version read the walk used
    // to pay; the walk itself runs in one read transaction, so the
    // framing of the per-version reads is gone. What is left is the per
    // -version changeset, the node reads, and the tree reads of the
    // diffs.
    assert!(
        statements <= 650,
        "a 100-version blame costs {statements} statements: the path \
         histories must ride one window, not one read per version"
    );
}

/// A cold walk fills its missing values mid-transaction, in the fills'
/// own transactions, and its answer is the warm one: what a fill stores
/// is what the held snapshot reads, whatever order the two happen in.
#[test]
fn a_cold_blame_answers_like_a_warm_one() {
    let _counted = COUNT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = appended();
    let at = tip(&store);
    let file = path("file.txt");

    let cold = blame::blame(
        &store,
        REPO,
        at,
        &file,
        &blame::Resume::default(),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap();
    let warm = blame::blame(
        &store,
        REPO,
        at,
        &file,
        &blame::Resume::default(),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap();
    assert_eq!(cold.ranges.len(), warm.ranges.len(), "the same line map");
    assert_eq!(cold.ranges, warm.ranges, "every range names the same lines");
    assert_eq!(
        cold.changesets, warm.changesets,
        "every range names the same changesets"
    );
    assert_eq!(cold.resume, warm.resume, "the same walk-on point");
}

/// A blame call holds one read transaction for its whole walk: it takes
/// one reader for the walk whatever it steps, beside the call's few
/// setup reads, and a deadline that has passed leaves the pool whole -
/// the transaction's snapshot holdback does not outlive the call.
#[test]
fn a_blame_call_holds_one_reader_and_returns_it() {
    // The whole test, its seed included, holds the lock: the reader
    // counts are one process-global number, and another test's reads
    // must not land in the walk's count.
    let _counted = COUNT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (_dir, store) = appended();
    let at = tip(&store);
    let file = path("file.txt");

    // Warm: every value the walk reads is stored, so the counted call
    // fills nothing and its takes are the call's own.
    let warm = blame::blame(
        &store,
        REPO,
        at,
        &file,
        &blame::Resume::default(),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap();
    assert!(warm.resume.is_some(), "121 versions, 100 explained");

    Backend::test_reset_reader_takes();
    let out = blame::blame(
        &store,
        REPO,
        at,
        &file,
        &blame::Resume::default(),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap();
    assert_eq!(out.changesets.len(), 100, "the walk ran to its version cap");
    // One reader for the walk's one transaction, beside the call's few
    // setup reads. A walk that reads per version takes a reader per
    // read: about three hundred here.
    assert_eq!(
        store.test_reader_takes(),
        5,
        "a 100-version blame took {} readers: the walk must hold one \
         transaction, not one per read",
        store.test_reader_takes()
    );

    // A deadline that has passed: the walk refuses at its first check,
    // and the reader its transaction held is back in the pool - the
    // snapshot holdback of a failed call does not outlive the call.
    let err = blame::blame(
        &store,
        REPO,
        at,
        &file,
        &blame::Resume::default(),
        &Bytes::new(1 << 30),
        Some(Instant::now()),
    )
    .unwrap_err();
    assert!(
        matches!(err, scion_server::Error::DeadlineExceeded(_)),
        "a passed deadline is the caller's retryable signal, not {err:?}"
    );
    assert_eq!(
        store.test_idle_readers(),
        2,
        "the pool sits at its full two readers, not {}",
        store.test_idle_readers()
    );
}

/// A walk longer than one window of chain links crosses the window, and
/// the answer loses nothing: every 12th commit of 1,100, down to the
/// root that made the file, each named once.
#[test]
fn a_blame_crosses_its_window_and_names_every_version() {
    // The whole test, its seed included, holds the lock: the statement
    // counter is one process-global number, and another test's reads must
    // not land in a page's count.
    let _counted = COUNT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    // 1,101 commits: the root puts `file.txt`, every 12th commit after
    // that appends a line, the rest change nothing. The window holds
    // 1,000 links, and the walk's versions spread over 1,092 of them.
    let blobs: Vec<BlobId> = (0..=1100)
        .scan(String::new(), |text, i| {
            writeln!(text, "l{i}").unwrap();
            Some(store.blobs().put(text.as_bytes()).unwrap())
        })
        .collect();
    let ids: Vec<ChangesetId> = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut history: Vec<ChangesetId> = Vec::new();
            for (i, &blob) in blobs.iter().enumerate() {
                let mut changes = std::collections::BTreeMap::new();
                if i == 0 || i % 12 == 0 {
                    changes.insert(
                        RepoPath::new("file.txt").unwrap(),
                        Some(FileState::Regular {
                            content: blob,
                            executable: false,
                        }),
                    );
                }
                let cs = Changeset::new(
                    history.last().copied().into_iter().collect(),
                    IdentityTime::new(b"ada".to_vec(), b"a@x".to_vec(), 1_700_000_000, 0).unwrap(),
                    None,
                    format!("c{i}").into_bytes(),
                    changes,
                )
                .unwrap();
                history.push(c.insert_changeset(repo, &cs)?);
            }
            c.cas_ref(repo, "main", None, *history.last().unwrap())?;
            Ok(history)
        })
        .unwrap();

    let out = blame::blame(
        &store,
        REPO,
        *ids.last().unwrap(),
        &path("file.txt"),
        &blame::Resume::default(),
        &Bytes::new(1 << 30),
        None,
    )
    .unwrap();

    let expected: BTreeSet<ChangesetId> = (0..=1100_usize)
        .filter(|i| *i == 0 || i % 12 == 0)
        .map(|i| ids[i])
        .collect();
    let named: BTreeSet<ChangesetId> = out.changesets.iter().map(|(id, _, _)| *id).collect();
    assert_eq!(named, expected, "every version, each once, none missed");
    assert!(out.resume.is_none(), "92 versions, one call explains them");
    // The walk crossed the window: the chain of path histories came in
    // more than one statement, and the walk went on from where each
    // window ended.
    let windows = Backend::test_statement_log()
        .iter()
        .filter(|s| s.contains("SELECT $1::bytea, 0::bigint"))
        .count();
    assert!(
        windows >= 2,
        "the walk's versions spread past one window of links, so the \
         chain of path histories must come in {windows} windows"
    );
}
