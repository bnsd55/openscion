//! The paged diff's rename search runs once per diff, not once per
//! page: the first page that can hold renames runs the bounded search
//! and the read path stores its map; the pages after it answer from
//! the stored map, so every page gives an entry the same rename. The
//! map is keyed by the roots the walk used, is bounded in bytes with a
//! time to live, and a miss always recomputes the same renames.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};

use scion_server::Backend;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::tree;

/// The statement log is one process-wide seam, and these tests count
/// statements: every test holds the lock for its whole body, so a
/// counting window holds only its own statements.
static LOG_LOCK: Mutex<()> = Mutex::new(());

fn log_guard() -> MutexGuard<'static, ()> {
    LOG_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

const REPO: &str = "rename-cache";

fn later() -> Deadline {
    Deadline::new(std::time::Instant::now() + std::time::Duration::from_secs(600))
}

fn identity() -> IdentityTime {
    IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

/// A wide diff's two snapshots: `count` files below `d/`, every file
/// holding `prefix`'s content, the ones in `moved` gone from `d/` and
/// back below `e/` with their `first` content kept (an exact rename),
/// the ones in `dropped` gone.
fn snapshot_files(
    count: usize,
    prefix: &str,
    moved: &[usize],
    dropped: &[usize],
) -> BTreeMap<RepoPath, Option<FileState>> {
    let mut files = BTreeMap::new();
    for i in 0..count {
        if dropped.contains(&i) {
            continue;
        }
        let content = if moved.contains(&i) {
            format!("first-{i}\n")
        } else {
            format!("{prefix}-{i}\n")
        };
        let path = if moved.contains(&i) {
            format!("e/{i:04}.txt")
        } else {
            format!("d/{i:04}.txt")
        };
        files.insert(
            RepoPath::new(path.as_str()).unwrap(),
            Some(FileState::Regular {
                content: BlobId::of(content.as_bytes()),
                executable: false,
            }),
        );
    }
    files
}

fn seeded(
    moved: &[usize],
    dropped: &[usize],
    added: &[&str],
) -> (
    tempfile::TempDir,
    Backend,
    scion_core::ChangesetId,
    scion_core::ChangesetId,
) {
    seeded_sized(3_501, moved, dropped, added)
}

/// The same, over `files` files: the tests that pin the trim hold
/// small maps, so the cap is a few hundred bytes, not half a gigabyte.
fn seeded_sized(
    files: usize,
    moved: &[usize],
    dropped: &[usize],
    added: &[&str],
) -> (
    tempfile::TempDir,
    Backend,
    scion_core::ChangesetId,
    scion_core::ChangesetId,
) {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    // The blobs the files name exist: the rename search reads the
    // files' bytes to score a pair.
    let mut stored = std::collections::HashSet::new();
    for i in 0..files {
        for content in [format!("first-{i}\n"), format!("second-{i}\n")] {
            let id = BlobId::of(content.as_bytes());
            if stored.insert(id) {
                store.blobs().put(content.as_bytes()).unwrap();
            }
        }
    }
    let _ = &stored;
    let added_blob = store
        .blobs()
        .put(b"nothing like any file it could pair with\n")
        .unwrap();
    let mut new_files = snapshot_files(files, "second", moved, dropped);
    for name in added {
        new_files.insert(
            RepoPath::new(*name).unwrap(),
            Some(FileState::Regular {
                content: added_blob,
                executable: false,
            }),
        );
    }
    let old = Changeset::new(
        vec![],
        identity(),
        None,
        b"old\n".to_vec(),
        snapshot_files(3_501, "first", &[], &[]),
    )
    .unwrap();
    let new = Changeset::new(vec![], identity(), None, b"new\n".to_vec(), new_files).unwrap();
    let ids = store
        .write(|c| {
            let old = tree::store_changeset(c, repo, &old, &mut |_| Ok(0))?.0;
            let new = tree::store_changeset(c, repo, &new, &mut |_| Ok(0))?.0;
            Ok((old, new))
        })
        .unwrap();
    (dir, store, ids.0, ids.1)
}

/// One page of the diff between `old` and `new`, starting after
/// `after`.
fn page(
    store: &Backend,
    old: scion_core::ChangesetId,
    new: scion_core::ChangesetId,
    after: Option<&RepoPath>,
) -> scion_server::test_support::read::DiffPage {
    scion_server::test_support::read::diff(store, REPO, old, new, None, after, &later()).unwrap()
}

/// The renames a page answers, as the path and the other half, sorted.
fn renames_of(page: &scion_server::test_support::read::DiffPage) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    for entry in &page.entries {
        if let Some(rename) = &entry.rename {
            out.push((
                entry.path.as_bytes().to_vec(),
                rename.path.as_bytes().to_vec(),
            ));
        }
    }
    out.sort();
    out
}

/// The deleted side of the rename of `i`, as the page answers it: the
/// deleted `d/{i:04}.txt` went to `e/{i:04}.txt`.
fn deleted_half(i: usize) -> (Vec<u8>, Vec<u8>) {
    (
        format!("d/{i:04}.txt").into_bytes(),
        format!("e/{i:04}.txt").into_bytes(),
    )
}

/// The added side of the rename of `i`, as the page answers it.
fn added_half(i: usize) -> (Vec<u8>, Vec<u8>) {
    (
        format!("e/{i:04}.txt").into_bytes(),
        format!("d/{i:04}.txt").into_bytes(),
    )
}

/// A diff wide enough for four pages, with two exact renames whose
/// halves sit on different pages: the page that finds the first pays
/// the whole diff's search and stores the map; the page that holds the
/// second answers from it, so every page gives an entry the same
/// rename, and the diff cost one search.
#[test]
fn a_paged_diff_runs_its_rename_search_once() {
    let _log = log_guard();
    scion_server::test_support::read::test_clear_rename_cache();
    let (_dir, store, old_id, new_id) = seeded(&[1_000, 3_000], &[], &[]);
    // Page 1: no adds or deletes, so no rename search at all.
    let first = page(&store, old_id, new_id, None);
    assert_eq!(first.entries.len(), 1_000);
    assert!(first.entries.iter().all(|entry| entry.rename.is_none()));
    let after_first = first.next_after.clone().unwrap();
    // Page 2 carries the first rename's deleted half: it runs the
    // search over the whole diff and stores its map.
    Backend::test_reset_statement_count();
    let second = page(&store, old_id, new_id, Some(&after_first));
    let searched = Backend::test_statement_count();
    assert_eq!(renames_of(&second), vec![deleted_half(1_000)]);
    // Page 3 holds no add or delete: it asks nothing.
    let after_second = second.next_after.clone().unwrap();
    let third = page(&store, old_id, new_id, Some(&after_second));
    assert!(third.entries.iter().all(|entry| entry.rename.is_none()));
    // The map from page 2 waits for the pages after it.
    let (maps, bytes) = scion_server::test_support::read::test_rename_cache_size();
    assert_eq!(maps, 1);
    assert!(bytes > 0);
    // Page 4 carries the second rename's halves: it answers from the
    // stored map, not its own search.
    let after_third = third.next_after.clone().unwrap();
    Backend::test_reset_statement_count();
    let fourth = page(&store, old_id, new_id, Some(&after_third));
    let answered = Backend::test_statement_count();
    assert!(
        answered < searched,
        "the page that answers from the map skips the search: {answered} against {searched}"
    );
    // The page also carries the second rename's deleted half: the
    // stored map answers it, as it answers the added halves.
    assert_eq!(
        renames_of(&fourth),
        vec![deleted_half(3_000), added_half(1_000), added_half(3_000)]
    );
    // The diff ended on the last page, so its map went with it.
    let (maps, _) = scion_server::test_support::read::test_rename_cache_size();
    assert_eq!(maps, 0);
}

/// A diff never answers from another diff's map: a second diff that
/// shares the old root but has its own new root runs its own search,
/// carries none of the first diff's renames, and then answers its own
/// pages from its own map. The checks can fail: the delete the second
/// diff carries is a path the first diff's map holds a rename for.
#[test]
fn a_diff_never_answers_from_another_diffs_map() {
    let _log = log_guard();
    scion_server::test_support::read::test_clear_rename_cache();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    // One old root; two new roots over it.
    let old = Changeset::new(
        vec![],
        identity(),
        None,
        b"old\n".to_vec(),
        snapshot_files(3_501, "first", &[], &[]),
    )
    .unwrap();
    // The first diff: `d/0001.txt` becomes `e/0001.txt`, so its map
    // holds that pair - a pair whose deleted path sits on the second
    // diff's first page.
    let renames = Changeset::new(
        vec![],
        identity(),
        None,
        b"renames\n".to_vec(),
        snapshot_files(3_501, "second", &[1], &[]),
    )
    .unwrap();
    // The second diff: same old root, its own new root, the same file
    // deleted with no add to pair with, and an unrelated add.
    let mut files = snapshot_files(3_501, "second", &[], &[1]);
    files.insert(
        RepoPath::new("e/zz.txt").unwrap(),
        Some(FileState::Regular {
            content: BlobId::of(b"nothing like any file it could pair with\n"),
            executable: false,
        }),
    );
    let other = Changeset::new(vec![], identity(), None, b"other\n".to_vec(), files).unwrap();
    // The blobs the files name exist: the rename search reads the
    // files' bytes to score a pair.
    for i in 0..3_501 {
        for content in [format!("first-{i}\n"), format!("second-{i}\n")] {
            store.blobs().put(content.as_bytes()).unwrap();
        }
    }
    store
        .blobs()
        .put(b"nothing like any file it could pair with\n")
        .unwrap();
    let (old_id, renames_id, other_id) = store
        .write(|c| {
            let old = tree::store_changeset(c, repo, &old, &mut |_| Ok(0))?.0;
            let renames = tree::store_changeset(c, repo, &renames, &mut |_| Ok(0))?.0;
            let other = tree::store_changeset(c, repo, &other, &mut |_| Ok(0))?.0;
            Ok((old, renames, other))
        })
        .unwrap();
    // The first diff's page with the delete runs its search and
    // answers the rename; the map is stored for its later pages.
    let first = page(&store, old_id, renames_id, None);
    assert_eq!(renames_of(&first), vec![deleted_half(1)]);
    // The second diff's page with the delete asks the cache: its key
    // carries its own roots, so the cache holds no answer for it, and
    // its own search runs. The delete carries no rename, though the
    // first diff's map has one for that path.
    Backend::test_reset_statement_count();
    let own = page(&store, old_id, other_id, None);
    let own_search = Backend::test_statement_count();
    assert!(
        own.entries.iter().all(|entry| entry.rename.is_none()),
        "the second diff's entries carry none of the first diff's renames"
    );
    // The same diff again: it answers from its own map, which the
    // first page stored, and never from the first diff's map. The
    // saved statements are the proof a map answered.
    Backend::test_reset_statement_count();
    let again = page(&store, old_id, other_id, None);
    let own_map = Backend::test_statement_count();
    assert_eq!(renames_of(&again), renames_of(&own));
    assert!(
        own_map < own_search,
        "the page answers from its own stored map: {own_map} against {own_search}"
    );
}

/// A page after the cache let its map go runs its own search, and
/// answers the same renames the stored map answered.
#[test]
fn an_evicted_map_costs_a_search_and_answers_the_same() {
    let _log = log_guard();
    scion_server::test_support::read::test_clear_rename_cache();
    let (_dir, store, old_id, new_id) = seeded(&[1_000, 3_000], &[], &[]);
    // Page 2 holds the first rename's delete: it runs the search and
    // stores the map.
    let first = page(&store, old_id, new_id, None);
    let after_first = first.next_after.clone().unwrap();
    let searched = page(&store, old_id, new_id, Some(&after_first));
    assert_eq!(renames_of(&searched), vec![deleted_half(1_000)]);
    // The cache forgets: the page runs its own search, and answers the
    // same renames. The proof it searched is that the same page with
    // its map present ran fewer statements.
    Backend::test_reset_statement_count();
    let with_map = page(&store, old_id, new_id, Some(&after_first));
    let hit = Backend::test_statement_count();
    assert_eq!(renames_of(&with_map), renames_of(&searched));
    scion_server::test_support::read::test_clear_rename_cache();
    Backend::test_reset_statement_count();
    let again = page(&store, old_id, new_id, Some(&after_first));
    let searched_again = Backend::test_statement_count();
    assert_eq!(renames_of(&again), renames_of(&searched));
    assert!(
        searched_again > hit,
        "the page after the eviction ran its own search: {searched_again} \
         against the map's {hit}"
    );
}

/// The key's path part keeps two scopes' maps apart: the page scoped
/// to `d` stores its own map, and the unscoped diff still finds the
/// rename whose added half sits outside `d`.
#[test]
fn a_scope_and_the_whole_diff_hold_different_maps() {
    let _log = log_guard();
    scion_server::test_support::read::test_clear_rename_cache();
    let (_dir, store, old_id, new_id) = seeded(&[1_000], &[], &[]);
    let scope = RepoPath::new("d").unwrap();
    // The scoped page: the deleted half sits below `d`, its added half
    // above it, so the scoped search finds no pair and stores a map
    // with no renames.
    let scoped = scion_server::test_support::read::diff(
        &store,
        REPO,
        old_id,
        new_id,
        Some(&scope),
        None,
        &later(),
    )
    .unwrap();
    assert!(scoped.entries.iter().all(|entry| entry.rename.is_none()));
    // The unscoped diff: its map is its own, so the rename answers.
    let whole = page(&store, old_id, new_id, None);
    let after = whole.next_after.clone().unwrap();
    let second = page(&store, old_id, new_id, Some(&after));
    assert_eq!(renames_of(&second), vec![deleted_half(1_000)]);
}

/// The cap trims the oldest map when a new one passes it: the newest
/// map survives, the oldest goes, and a page after it runs its own
/// search. The cap rides the first map's measured bytes, so the test
/// pins the trim at any map size.
#[test]
fn a_map_past_the_cap_evicts_the_oldest() {
    let _log = log_guard();
    scion_server::test_support::read::test_clear_rename_cache();
    let (_dir, store, old_id, new_id) = seeded(&[1_000, 3_000], &[], &[]);
    let page_one = page(&store, old_id, new_id, None);
    let after_one = page_one.next_after.clone().unwrap();
    // The page with the first rename's deleted half: its search runs
    // and its map is stored for the pages after it.
    let first = page(&store, old_id, new_id, Some(&after_one));
    assert_eq!(renames_of(&first), vec![deleted_half(1_000)]);
    let (maps, bytes) = scion_server::test_support::read::test_rename_cache_size();
    assert_eq!(maps, 1);
    assert!(bytes > 0);
    // The cap sits just over the stored map: the next map cannot fit
    // beside it, and the stored one is the older.
    let _cap = scion_server::test_support::read::test_set_rename_cache_cap_for_tests(
        usize::try_from(bytes).unwrap() + 8,
    );
    let (_dir, other_store, other_old, other_new) = seeded(&[1_000], &[], &[]);
    let other_one = page(&other_store, other_old, other_new, None);
    let other_after = other_one.next_after.clone().unwrap();
    let other = page(&other_store, other_old, other_new, Some(&other_after));
    assert_eq!(renames_of(&other), vec![deleted_half(1_000)]);
    // The newest map survived; the oldest went.
    let (maps, _) = scion_server::test_support::read::test_rename_cache_size();
    assert_eq!(maps, 1);
    // A page of the evicted diff runs its own search: more statements
    // than the survivor's hit.
    Backend::test_reset_statement_count();
    let _survivor = page(&other_store, other_old, other_new, Some(&other_after));
    let survivor_hit = Backend::test_statement_count();
    Backend::test_reset_statement_count();
    let evicted = page(&store, old_id, new_id, Some(&after_one));
    let evicted_search = Backend::test_statement_count();
    assert_eq!(renames_of(&evicted), renames_of(&first));
    assert!(
        survivor_hit < evicted_search,
        "the survivor's map answers and the evicted diff's does not: \
         {survivor_hit} against {evicted_search}"
    );
}

/// One empty-map diff over the shared old root: every file's content
/// changed and one file added, so page 1 carries the add, the search
/// runs and finds no renames, and the map waits for the pages after -
/// which the test never asks for.
fn empty_map_diff(
    store: &Backend,
    old: scion_core::ChangesetId,
    i: usize,
) -> scion_core::ChangesetId {
    store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let new = Changeset::new(
                vec![old],
                identity(),
                None,
                format!("new-{i}\n").as_bytes(),
                (0..1_100)
                    .map(|j| {
                        (
                            RepoPath::new(format!("d/{j:04}.txt").as_str()).unwrap(),
                            Some(FileState::Regular {
                                content: BlobId::of(format!("second-{j}\n").as_bytes()),
                                executable: false,
                            }),
                        )
                    })
                    .chain(std::iter::once((
                        RepoPath::new(format!("z/{i:04}.txt").as_str()).unwrap(),
                        Some(FileState::Regular {
                            content: BlobId::of(format!("added-{i}\n").as_bytes()),
                            executable: false,
                        }),
                    )))
                    .collect(),
            )
            .unwrap();
            Ok(tree::store_changeset(c, repo, &new, &mut |_| Ok(0))?.0)
        })
        .unwrap()
}

/// Many paged diffs with no renames, each stopped at page 1: every one
/// stores an empty map, which counts no rename bytes. The cap must
/// still hold the entries to its bound - each entry costs its own
/// overhead - and the gauge must read what the cache keeps.
#[test]
fn the_cap_holds_empty_maps_to_its_bound() {
    let _log = log_guard();
    scion_server::test_support::read::test_clear_rename_cache();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    // The blobs the pages' files name.
    for i in 0..1_100 {
        store
            .blobs()
            .put(format!("second-{i}\n").as_bytes())
            .unwrap();
    }
    let old_files: std::collections::BTreeMap<RepoPath, Option<FileState>> = (0..1_100)
        .map(|i| {
            (
                RepoPath::new(format!("d/{i:04}.txt").as_str()).unwrap(),
                Some(FileState::Regular {
                    content: BlobId::of(format!("first-{i}\n").as_bytes()),
                    executable: false,
                }),
            )
        })
        .collect();
    let old = Changeset::new(vec![], identity(), None, b"old\n".to_vec(), old_files).unwrap();
    let old = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(tree::store_changeset(c, repo, &old, &mut |_| Ok(0))?.0)
        })
        .unwrap();
    for i in 0..16 {
        store
            .blobs()
            .put(format!("added-{i}\n").as_bytes())
            .unwrap();
        let new = empty_map_diff(&store, old, i);
        let _page = page(&store, old, new, None);
        let (maps, bytes) = scion_server::test_support::read::test_rename_cache_size();
        let overhead = scion_server::test_support::read::test_rename_cache_entry_overhead();
        assert!(
            maps <= 4,
            "the cap holds the entries to its bound: {maps} entries after diff {i}"
        );
        let cap = scion_server::test_support::read::test_rename_cache_entry_overhead() * 4;
        assert!(
            (bytes as usize) <= cap,
            "the kept bytes sit under the cap: {bytes} against {cap}"
        );
        let gauge = scion_server::test_support::read::test_rename_cache_gauge();
        assert_eq!(gauge, bytes, "the gauge reads the kept total");
    }
    // Four empty-map entries survive; the twelve oldest are gone.
    let (maps, bytes) = scion_server::test_support::read::test_rename_cache_size();
    assert_eq!(maps, 4, "the cap holds four empty-map entries, not all 16");
    let overhead = scion_server::test_support::read::test_rename_cache_entry_overhead();
    assert_eq!(
        bytes as usize,
        maps * overhead,
        "an empty map costs exactly its overhead against the cap"
    );
}

/// A page that answers from an entry moves it to the young end: a trim
/// then drops another entry first. A refresh that left the old stamp
/// in the order would let the trim drop the entry the page just used.
#[test]
fn a_page_that_answers_moves_its_entry_to_the_young_end() {
    let _log = log_guard();
    scion_server::test_support::read::test_clear_rename_cache();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    for i in 0..1_101 {
        store
            .blobs()
            .put(format!("second-{i}\n").as_bytes())
            .unwrap();
    }
    let old_files: std::collections::BTreeMap<RepoPath, Option<FileState>> = (0..1_100)
        .map(|i| {
            (
                RepoPath::new(format!("d/{i:04}.txt").as_str()).unwrap(),
                Some(FileState::Regular {
                    content: BlobId::of(format!("first-{i}\n").as_bytes()),
                    executable: false,
                }),
            )
        })
        .collect();
    let old = Changeset::new(vec![], identity(), None, b"old\n".to_vec(), old_files).unwrap();
    let old = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(tree::store_changeset(c, repo, &old, &mut |_| Ok(0))?.0)
        })
        .unwrap();
    let overhead = scion_server::test_support::read::test_rename_cache_entry_overhead();
    // The cap holds exactly three entries.
    let _cap = scion_server::test_support::read::test_set_rename_cache_cap_for_tests(overhead * 3);
    let mut new_ids = Vec::new();
    for i in 0..3 {
        new_ids.push(empty_map_diff(&store, old, i));
    }
    // Page 1 of the first two diffs: two entries, one older.
    let _p1 = page(&store, old, new_ids[0], None);
    let _p2 = page(&store, old, new_ids[1], None);
    // A page of the first diff again: it answers from its entry, which
    // moves to the young end - past the second diff's entry.
    Backend::test_reset_statement_count();
    let again = page(&store, old, new_ids[0], None);
    let hit = Backend::test_statement_count();
    assert!(hit > 0);
    let _ = again;
    // The third diff's entry cannot fit: the trim drops the second
    // diff's entry, the older one, not the one the page just used.
    let third = empty_map_diff(&store, old, 3);
    let _p3 = page(&store, old, third, None);
    let (maps, _) = scion_server::test_support::read::test_rename_cache_size();
    assert_eq!(maps, 3);
    // The first diff's page still answers from its map: the refresh
    // kept it young. Its cost is one page's statements, not a search.
    Backend::test_reset_statement_count();
    let _again = page(&store, old, new_ids[0], None);
    let still_hit = Backend::test_statement_count();
    // The second diff's entry was the one trimmed: its page runs the
    // search again, more statements than the survivor's hit.
    Backend::test_reset_statement_count();
    let _second_again = page(&store, old, new_ids[1], None);
    let evicted = Backend::test_statement_count();
    assert!(
        still_hit < evicted,
        "the refreshed entry survived the trim and the older one went: \
         {still_hit} against {evicted}"
    );
}

/// A diff whose rename vector passes its byte bound finds no renames
/// and says so. The bound counts the bytes of the paths too, so it
/// trips far under the entry count the same bytes would allow a
/// short-path diff.
#[test]
fn a_diff_past_its_rename_byte_bound_says_renames_may_be_missing() {
    let _log = log_guard();
    scion_server::test_support::read::test_clear_rename_cache();
    let (_dir, store, old, new) = seeded(&[1, 2], &[], &[]);
    // With the server's bound, the exact renames are found.
    let found = page(&store, old, new, None);
    assert!(!found.renames_incomplete);
    assert_eq!(renames_of(&found), vec![deleted_half(1), deleted_half(2)]);
    scion_server::test_support::read::test_clear_rename_cache();
    // With a half-kilobyte bound, the same diff's vector passes it: the
    // reply says renames may be missing, and none are answered.
    {
        let _small = scion_server::test_support::read::test_set_rename_byte_bound(512);
        let page = page(&store, old, new, None);
        assert!(page.renames_incomplete, "the byte bound tripped");
        assert_eq!(renames_of(&page), [] as [(Vec<u8>, Vec<u8>); 0]);
    }
    scion_server::test_support::read::test_clear_rename_cache();
}
