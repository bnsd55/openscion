//! Reads. Each takes a changeset ID, so each answer is immutable.

// Every pub item of this module is re-exported by the test seam
// (`test_support` in lib.rs), and unreachable_pub does not see
// through a re-export, so this marker would fire on all of them
// under any feature set.
#![allow(unreachable_pub)]
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;

use scion_core::{BlobId, Changeset, ChangesetId, Entry, FileState, PathElem, RepoPath, TreeId};

use crate::backend::Backend;
use crate::deadline::Deadline;
use crate::derived;
use crate::error::{Error, Result};
use crate::graph;
use crate::history::{self, PathHistory};
use crate::meta::{DERIVED_BATCH, Meta as _, MetaTx, RepoId};
use crate::similar::{self, Budget, Chunks, MAX_PAIRS, Progress};
use crate::tree;

/// A changeset the repo may see.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or changeset does not exist.
pub fn get_changeset(store: &Backend, repo: &str, id: ChangesetId) -> Result<Changeset> {
    store.read(|c| {
        let repo = c.repo_id(repo)?;
        c.get_changeset(repo, id)
    })
}

/// The root tree of a changeset the repo may see, deriving it if needed,
/// until `deadline`.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or changeset does not exist.
/// [`Error::DeadlineExceeded`] once `deadline` has passed, or
/// [`Error::Cancelled`] once the call has gone.
pub fn root_tree(
    store: &Backend,
    repo: &str,
    cs: ChangesetId,
    deadline: &Deadline,
) -> Result<TreeId> {
    let repo = store.read(|c| c.repo_id(repo))?;
    tree::root_tree(store, repo, cs, deadline)
}

/// The content of a regular file, or the target of a symlink.
///
/// # Errors
///
/// [`Error::NotFound`] if nothing is at `path`. [`Error::InvalidArgument`] if
/// `path` is a directory or a submodule. [`Error::DeadlineExceeded`] once
/// `deadline` has passed, which a cold root tree may need, or
/// [`Error::Cancelled`] once the call has gone. The tests of the trees and
/// the imports read through it, so it rides behind the test-hooks feature.
#[cfg(any(test, feature = "test-hooks"))]
pub fn read_file(
    store: &Backend,
    repo: &str,
    cs: ChangesetId,
    path: &RepoPath,
    deadline: &Deadline,
) -> Result<Vec<u8>> {
    store
        .blobs()
        .get(file_blob(store, repo, cs, path, deadline)?)
}

/// The blob of a regular file, or of a symlink's target.
///
/// # Errors
///
/// As `read_file`, without reading the blob.
pub fn file_blob(
    store: &Backend,
    repo: &str,
    cs: ChangesetId,
    path: &RepoPath,
    deadline: &Deadline,
) -> Result<BlobId> {
    let root = root_tree(store, repo, cs, deadline)?;
    file_blob_at(store, root, path)
}

/// The blob at `path` below the root tree `root`, which a partial reader's
/// view may name instead of the changeset's real root.
///
/// # Errors
///
/// As `read_file`.
pub fn file_blob_at(store: &Backend, root: TreeId, path: &RepoPath) -> Result<BlobId> {
    match store.read(|c| tree::lookup(c, root, path))? {
        Some(Entry::File(file)) => match file.state().blob() {
            Some(blob) => Ok(blob),
            None => Err(Error::InvalidArgument(format!("{path:?} is a submodule"))),
        },
        Some(Entry::Dir(_)) => Err(Error::InvalidArgument(format!("{path:?} is a directory"))),
        None => Err(Error::NotFound(format!("path {path:?}"))),
    }
}

/// One directory level. `None` is the repo root.
///
/// # Errors
///
/// [`Error::NotFound`] if nothing is at `path`. [`Error::InvalidArgument`] if
/// `path` is a file.
/// Most entries that a `Log` reply holds. Each entry reads one changeset
/// while the call holds one read connection.
pub const MAX_SCAN: usize = 1000;

/// Most bytes of path history that one `Log` call with a path reads. A
/// lookup reads a whole directory level on each part of the path, so a
/// file in a large directory costs a lot per entry: a change in a
/// directory of 100,000 names reads its chunk, about 8 KiB, plus the
/// index. A call stops at this bound, after its first entry. Tests use a
/// small bound, so that they reach it.
pub const MAX_LOG_NODE_BYTES: usize = if cfg!(test) { 512 << 10 } else { 64 << 20 };

/// How many entries a `Log` page reads between two checks of its deadline.
/// A check reads the clock, which is a system call where the clocksource
/// has no user-space read (HPET); there a check per entry made a page with
/// a path about 5% slower. A check every 16 entries still stops a page
/// within 16 entries of its deadline, or of its call going.
pub(crate) const DEADLINE_EVERY: usize = 16;

/// One page of first-parent history.
#[derive(Debug)]
pub struct LogPage {
    /// The changesets found, newest first.
    pub entries: Vec<(ChangesetId, Changeset)>,
    /// Where the next page starts: the first changeset this call did not
    /// check. `None` when the history ends.
    pub next: Option<ChangesetId>,
}

/// Up to `limit` changesets, walking first parents from `cs`, newest first.
/// For tests; the server reads one [`log_page`] per call.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or changeset does not exist.
#[cfg(test)]
pub fn log(
    store: &Backend,
    repo: &str,
    cs: ChangesetId,
    limit: usize,
) -> Result<Vec<(ChangesetId, Changeset)>> {
    let mut entries = Vec::new();
    let mut next = Some(cs);
    let deadline = Deadline::new(std::time::Instant::now() + crate::deadline::CALL_LIMIT);
    while let Some(from) = next.filter(|_| entries.len() < limit) {
        let page = log_page(store, repo, from, None, limit - entries.len(), &deadline)?;
        entries.extend(page.entries);
        next = page.next;
    }
    Ok(entries)
}

/// Up to `limit` changesets, and at most [`MAX_SCAN`], walking first
/// parents from `cs`, newest first. Without a `path`, the page is one
/// statement: first parents and their bodies, `limit + 1` links deep, so
/// a full page knows the link to walk on from. With a `path`, only the
/// changesets that change it (`changes_path`), found in the path history
/// of `cs` ([`history`]): the walk reads the chain's values in windows of
/// [`DERIVED_BATCH`] links, one node per part of the path per link it
/// visits, however many changesets lie between two entries, and the
/// entries' bodies one statement per [`DERIVED_BATCH`]. Such a page stops
/// early, after its first entry, once it has read [`MAX_LOG_NODE_BYTES`]
/// of nodes. If `cs` has no path history yet, this derives it first, in
/// steps, until `deadline`.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or changeset does not exist.
/// [`Error::DeadlineExceeded`] if `deadline` passed while the path history
/// was derived, or [`Error::Cancelled`] once the call has gone; the steps
/// before it stay.
pub fn log_page(
    store: &Backend,
    repo: &str,
    cs: ChangesetId,
    path: Option<&RepoPath>,
    limit: usize,
    deadline: &Deadline,
) -> Result<LogPage> {
    let limit = limit.min(MAX_SCAN);
    let Some(path) = path else {
        return store.read(|c| {
            let repo = c.repo_id(repo)?;
            // One statement for the page: first parents and their bodies
            // ride one round trip, `limit + 1` links deep, so a full page
            // still knows the link to walk on from. The deadline refuses
            // a page whose time is up before its one read; a page that
            // comes back whole is whole, whatever the clock says after.
            deadline.check()?;
            // `limit` is at most MAX_SCAN, far from any wrap.
            #[allow(
                clippy::cast_possible_wrap,
                clippy::cast_sign_loss,
                clippy::cast_possible_truncation
            )]
            let hops = usize::try_from(limit as i64 + 1).unwrap_or(limit + 1);
            #[allow(clippy::cast_possible_wrap)]
            let rows = c.first_parent_page(repo, cs, hops as i64)?;
            // The page statement returns at most `hops + 1` rows: a full
            // page is one with more rows than the page's `limit`, and a
            // short one ended the walk - at a root, or refused as the
            // corruption a tampered link is, by the statement itself.
            let more = rows.len() > hops;
            let keep = rows.len().min(limit);
            let mut entries = Vec::with_capacity(keep);
            for (id, _, body) in rows.into_iter().take(keep) {
                entries.push((id, decode_entry(id, &body)?));
            }
            // A page short of its `limit + 1` links ended the walk: the
            // last entry has no parent. A full one has more to read, and
            // the cursor rides the last entry's own first parent.
            let next = if more {
                entries
                    .last()
                    .and_then(|(_, cs)| cs.parents().first().copied())
                    .or(Some(cs))
            } else {
                None
            };
            Ok(LogPage { entries, next })
        });
    };
    let repo = store.read(|c| c.repo_id(repo))?;
    derived::get_by::<PathHistory>(store, repo, cs, deadline)?;
    // Each statement sees its own snapshot, and none needs another's: a
    // changeset, a node and a value of this version never change and are
    // never removed, and a node is stored in the transaction of the value
    // that names it.
    store.read(|c| path_page(c, repo, cs, path, limit, deadline))
}

/// The walk of a path log page, on one read connection: the chain of path
/// histories in windows of [`DERIVED_BATCH`] links, one statement per
/// window; every link, also where no value is stored, so a missing value
/// is the corruption it is, not a gap to skip. It visits only the links
/// whose lookup it needs, and reads a node per part of the path per link
/// it visits, however many changesets lie between two entries; only the
/// values and the entries' bodies ride in batches. An entry deeper than
/// the window holds costs its body at once, to walk on from its parent.
///
/// # Errors
///
/// [`Error::Corrupt`] if a link has no value or a body does not decode,
/// [`Error::NotFound`] if an entry's changeset is not the repo's,
/// [`Error::DeadlineExceeded`] once `deadline` has passed, or
/// [`Error::Cancelled`] once the call has gone.
fn path_page(
    c: &mut dyn MetaTx,
    repo: RepoId,
    cs: ChangesetId,
    path: &RepoPath,
    limit: usize,
    deadline: &Deadline,
) -> Result<LogPage> {
    {
        let mut nodes = history::Nodes::default();
        // The walk follows the first-parent chain of path histories in
        // windows of [`DERIVED_BATCH`] links, one statement per window:
        // every link, also where no value is stored, so a missing value
        // is the corruption it is, not a gap to skip. It visits only the
        // links whose lookup it needs, and reads a node per part of the
        // path, as before; only the values and the entries' bodies ride
        // in batches. An entry deeper than the window holds costs its
        // body at once, to walk on from its parent.
        let mut chain = history::chain_values(c, cs, DERIVED_BATCH + 1)?;
        let mut pos = 0usize;
        let mut found: Vec<ChangesetId> = Vec::new();
        let mut jumped: HashMap<ChangesetId, Changeset> = HashMap::new();
        let next;
        loop {
            if found.len() >= limit {
                // The page is full: the cursor rides the next link, as
                // the parent of the last entry.
                next = chain.get(pos).map(|(id, _)| *id).or(Some(cs));
                break;
            }
            if found.len().is_multiple_of(DEADLINE_EVERY) {
                deadline.check()?;
            }
            if !found.is_empty() && nodes.bytes() >= MAX_LOG_NODE_BYTES {
                next = chain.get(pos).map(|(id, _)| *id).or(Some(cs));
                break;
            }
            let Some((at, value)) = chain.get(pos) else {
                next = None;
                break;
            };
            let Some(root) = value else {
                return Err(Error::Corrupt(format!("path history: no value for {at}")));
            };
            // Each changeset on the first-parent history of `cs` has a
            // path history, so `at` has one.
            let Some(id) = history::newest_change_at(c, &mut nodes, *root, path)? else {
                next = None;
                break;
            };
            found.push(id);
            // The walk goes on at the parent of the entry it found. In
            // the window, that is the link at its side. Past the window,
            // a fresh window starts where the walk goes on: at the entry's
            // own parent, which costs the entry's body at once.
            let hit = chain[pos..].iter().position(|(seen, _)| *seen == id);
            let (from, resume) = if let Some(off) = hit {
                pos += off + 1;
                if pos < chain.len() {
                    continue;
                }
                if chain.len() < DERIVED_BATCH + 1 {
                    // The chain ended: the entry is the root.
                    next = None;
                    break;
                }
                (chain.last().map_or(cs, |(id, _)| *id), 1)
            } else {
                let cs = c.get_changeset(repo, id)?;
                let parent = cs.parents().first().copied();
                jumped.insert(id, cs);
                if let Some(parent) = parent {
                    (parent, 0)
                } else {
                    next = None;
                    break;
                }
            };
            chain = history::chain_values(c, from, DERIVED_BATCH + 1)?;
            pos = resume;
        }
        // The bodies of the entries, in the order the walk found them.
        let mut bodies: HashMap<ChangesetId, Changeset> = HashMap::with_capacity(found.len());
        let mut page: Vec<ChangesetId> = Vec::with_capacity(found.len());
        for id in &found {
            match jumped.remove(id) {
                Some(cs) => {
                    bodies.insert(*id, cs);
                }
                None => page.push(*id),
            }
        }
        for chunk in page.chunks(DERIVED_BATCH) {
            for (id, cs) in chunk.iter().zip(c.get_changeset_many(repo, chunk)?) {
                bodies.insert(*id, cs);
            }
        }
        let entries = found
            .iter()
            .map(|id| {
                let cs = bodies.remove(id).expect("the batch answered every id");
                (*id, cs)
            })
            .collect();
        Ok(LogPage { entries, next })
    }
}

/// Decodes a changeset body from a page row, and checks it against the id
/// the row names, as [`crate::meta::MetaTx::get_changeset`] checks one.
fn decode_entry(id: ChangesetId, body: &[u8]) -> Result<Changeset> {
    let (cs, actual) = Changeset::decode_with_id(body)
        .map_err(|e| Error::Corrupt(format!("changeset {id}: {e}")))?;
    if actual != id {
        return Err(Error::Corrupt(format!(
            "changeset {id} does not match its ID"
        )));
    }
    Ok(cs)
}

/// Whether a changeset changes `path`: the file itself, or anything below it
/// if it is a directory. A changeset lists every file it changes, so this
/// needs no tree. The path history's tests call it, so it rides behind
/// the test-hooks feature.
#[cfg(any(test, feature = "test-hooks"))]
#[must_use]
pub fn changes_path(cs: &Changeset, path: &RepoPath) -> bool {
    let path = path.as_bytes();
    cs.changes().keys().any(|changed| {
        let changed = changed.as_bytes();
        changed.starts_with(path) && matches!(changed.get(path.len()), None | Some(b'/'))
    })
}

/// Whether `ancestor` is `descendant` or one of its ancestors, over all
/// parents ([`graph::is_ancestor`]). Reads O(log n) nodes of the commit
/// graph for each first-parent history it searches, and three rows for each
/// merge newer than `ancestor` that it finds.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or a changeset does not exist.
/// [`Error::DeadlineExceeded`] once `deadline` has passed, or
/// [`Error::Cancelled`] once the call has gone.
pub fn is_ancestor(
    store: &Backend,
    repo: &str,
    ancestor: ChangesetId,
    descendant: ChangesetId,
    deadline: &Deadline,
) -> Result<bool> {
    let repo = store.read(|c| c.repo_id(repo))?;
    graph::is_ancestor(store, repo, ancestor, descendant, deadline)
}

/// The best common ancestors of `a` and `b`, over all parents: every common
/// ancestor that is not an ancestor of another common ancestor
/// ([`graph::merge_bases`]). This is the rule of `git merge-base --all`.
/// Usually there is one. After criss-cross merges there can be several, and
/// all are returned. Highest generation first, then lowest ID. Empty if the
/// histories share no changeset. Reads a few nodes of the commit graph if
/// neither first-parent history has a merge above where the two meet. Else
/// walks the graph, and jumps over each run of first parents that no other
/// part of the walk reaches.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or a changeset does not exist.
/// [`Error::DeadlineExceeded`] once `deadline` has passed, or
/// [`Error::Cancelled`] once the call has gone.
pub fn merge_base(
    store: &Backend,
    repo: &str,
    a: ChangesetId,
    b: ChangesetId,
    deadline: &Deadline,
) -> Result<Vec<ChangesetId>> {
    let repo = store.read(|c| c.repo_id(repo))?;
    graph::merge_bases(store, repo, a, b, deadline)
}

/// Most paths one `Diff` call returns. Each path costs at most one tree read
/// per part on each side.
pub const MAX_DIFF_ENTRIES: usize = 1000;

/// Most work one `Diff` call does to find similar files, in steps 2 and 3
/// of [`similar::renames`]: each chunk of a file read counts once, each row
/// of a chunk table that the search indexes or compares counts once, and
/// each file size read counts [`similar::SIZE_COST`] ([`similar::Budget`]).
/// Reading 64 MiB of text in lines of 64 bytes or more costs 1M. The budget
/// is the same on every call, so every call finds the same renames. Past
/// it, the search reads no more files, and the reply says so
/// ([`DiffPage::renames_incomplete`]). Exact renames (step 1) cost no file
/// reads, so they run for any diff of up to [`MAX_RENAME_ENTRIES`] entries.
pub const MAX_RENAME_CHUNKS: usize = 4 << 20;

/// Most entries the rename pass of one `Diff` call walks below its path.
/// It holds them in memory, at about 150 bytes each (`DiffEntry` is 128 on
/// the stack, plus the bytes of the path), so a diff of
/// [`MAX_RENAME_ENTRIES`] entries costs the call about 150 MB. Past it, the
/// call finds no renames and the reply says so
/// ([`DiffPage::renames_incomplete`]), as git warns when it skips inexact
/// rename detection.
pub const MAX_RENAME_ENTRIES: usize = 1_000_000;

/// A path whose file differs between two snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiffEntry {
    /// The path.
    pub path: RepoPath,
    /// The file at `path` in the old snapshot. `None` when no file is there:
    /// nothing, or a directory.
    pub old: Option<FileState>,
    /// The file at `path` in the new snapshot. `None` as for `old`.
    pub new: Option<FileState>,
    /// Set when the file is half of a rename ([`diff`]).
    pub rename: Option<Rename>,
}

/// The other half of a rename.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rename {
    /// On an added file (`old` is `None`), the deleted path it came from. On
    /// a deleted file (`new` is `None`), the added path it went to.
    pub path: RepoPath,
    /// How similar the two files are, in percent, as git prints it: 100 for
    /// the same content.
    pub similarity: u32,
}

/// One page of a diff.
#[derive(Debug)]
pub struct DiffPage {
    /// The paths that differ, in tree order ([`diff`]).
    pub entries: Vec<DiffEntry>,
    /// Set when the call stopped at [`MAX_DIFF_ENTRIES`]: the last path in
    /// `entries`. Diff again with `after` set to it to continue.
    pub next_after: Option<RepoPath>,
    /// Set when renames may be missing: the inexact search did not run
    /// (more than [`similar::MAX_PAIRS`] pairs were left), or stopped at
    /// [`MAX_RENAME_CHUNKS`]. The exact renames that `entries` hold are
    /// still whole.
    pub renames_incomplete: bool,
}

/// The paths whose file differs between the snapshots of `old` and `new`, in
/// tree order: paths compare part by part, and parts by raw bytes. A path
/// that is a file on one side and a directory on the other gives one entry
/// for the file, then one for each file below the directory. With `path`,
/// only that file or the files below that directory. With `after`, only the
/// paths after it. Returns at most [`MAX_DIFF_ENTRIES`] entries.
///
/// Walks both trees at once and skips a subtree with the same ID on both
/// sides. So it reads only the trees on the paths it returns, and on the
/// way down to `path` and `after`.
///
/// Renames are found as `git diff` finds them by default
/// ([`similar::renames`]), among the added and deleted files of the whole
/// diff below `path`, not of one page. So a page gives each entry the same
/// rename, whichever page holds it and wherever `after` is. Both halves of a
/// rename stay in the diff, so a client that does not know renames sees a
/// delete and an add. If the page has an added or deleted file, the call
/// walks the whole diff below `path`, unless the page is all of it. Exact
/// renames (same content, by blob ID) cost no file reads, so a diff of up
/// to [`MAX_RENAME_ENTRIES`] entries gets them, whatever its size. The
/// inexact search runs only under git's pair limit ([`similar::MAX_PAIRS`])
/// and stops at [`MAX_RENAME_CHUNKS`]. When the walk was past its entry
/// bound, or the inexact search did not run or did not end, the page says
/// so ([`DiffPage::renames_incomplete`]).
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or a changeset does not exist.
pub fn diff(
    store: &Backend,
    repo: &str,
    old: ChangesetId,
    new: ChangesetId,
    path: Option<&RepoPath>,
    after: Option<&RepoPath>,
    deadline: &Deadline,
) -> Result<DiffPage> {
    diff_with(
        store,
        repo,
        old,
        new,
        path,
        after,
        MAX_RENAME_ENTRIES,
        deadline,
    )
}

/// [`diff`], with the entries that the rename pass may walk bounded by
/// `rename_entries`. Tests use a small bound to reach it cheaply.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or a changeset does not exist.
#[allow(clippy::too_many_arguments)] // the page's knobs, one per caller choice
pub fn diff_with(
    store: &Backend,
    repo: &str,
    old: ChangesetId,
    new: ChangesetId,
    path: Option<&RepoPath>,
    after: Option<&RepoPath>,
    rename_entries: usize,
    deadline: &Deadline,
) -> Result<DiffPage> {
    let roots = (
        root_tree(store, repo, old, deadline)?,
        root_tree(store, repo, new, deadline)?,
    );
    let mut entries = diff_walk(store, roots, path, after, MAX_DIFF_ENTRIES)?;
    // The walk finds one entry more than a page, to know whether more follow.
    let next_after = if entries.len() > MAX_DIFF_ENTRIES {
        entries.truncate(MAX_DIFF_ENTRIES);
        entries.last().map(|entry| entry.path.clone())
    } else {
        None
    };
    let mut renames_incomplete = false;
    if entries.iter().any(|e| e.old.is_none() || e.new.is_none()) {
        // The renames are among the adds and deletes of the whole diff, not
        // of one page, so a page gives each entry the same rename. Exact
        // renames cost no file reads, so the walk has no bound but the
        // memory of holding the entries: up to [`MAX_RENAME_ENTRIES`] of
        // them, the bound of one call.
        let all = if after.is_none() && next_after.is_none() {
            entries.clone()
        } else {
            diff_walk(store, roots, path, None, rename_entries)?
        };
        if all.len() > rename_entries {
            tracing::debug!(
                entries = all.len(),
                "a diff with too many entries to find renames"
            );
            renames_incomplete = true;
        } else {
            let (renames, incomplete) = renames(store, &all)?;
            renames_incomplete = incomplete;
            for entry in &mut entries {
                entry.rename = renames.get(&entry.path).cloned();
            }
        }
    }
    Ok(DiffPage {
        entries,
        next_after,
        renames_incomplete,
    })
}

/// The renames among `entries`, a whole diff: each path of a rename, with
/// the other path, and whether renames may be missing (the inexact search
/// did not run, or stopped at its budget).
fn renames(store: &Backend, entries: &[DiffEntry]) -> Result<(HashMap<RepoPath, Rename>, bool)> {
    let (mut added, mut deleted) = (Vec::new(), Vec::new());
    for entry in entries {
        match (entry.old, entry.new) {
            (None, Some(state)) => added.push((&entry.path, state)),
            (Some(state), None) => deleted.push((&entry.path, state)),
            _ => {}
        }
    }
    // git's order: whole paths by raw bytes, not tree order.
    added.sort_unstable_by_key(|&(path, _)| path);
    deleted.sort_unstable_by_key(|&(path, _)| path);
    let mut cost = 0;
    let read = |_: &RepoPath, blob, _| Ok(Some(Chunks::read(store, blob)?));
    let budget = Budget {
        cost: &mut cost,
        cap: MAX_RENAME_CHUNKS,
        progress: false,
    };
    let from = Progress::default();
    let outcome = similar::renames(store, &added, &deleted, read, MAX_PAIRS, from, budget)?;
    if outcome.skipped {
        tracing::debug!(
            added = added.len(),
            deleted = deleted.len(),
            "a diff with too many file pairs for inexact rename detection"
        );
    }
    if cost >= MAX_RENAME_CHUNKS {
        tracing::debug!(
            cost,
            "the search for similar files in a diff stopped at its budget"
        );
    }
    let mut renames = HashMap::new();
    for pair in outcome.pairs {
        let (to, from) = (added[pair.added].0, deleted[pair.deleted].0);
        let similarity = pair.score * 100 / similar::SAME;
        for (path, other) in [(to, from), (from, to)] {
            let other = other.clone();
            renames.insert(
                path.clone(),
                Rename {
                    path: other,
                    similarity,
                },
            );
        }
    }
    Ok((renames, outcome.skipped || outcome.stopped.is_some()))
}

/// The entries of the diff of two root trees, as [`diff`] returns them, up
/// to one more than `limit`.
fn diff_walk(
    store: &Backend,
    (old_root, new_root): (TreeId, TreeId),
    path: Option<&RepoPath>,
    after: Option<&RepoPath>,
    limit: usize,
) -> Result<Vec<DiffEntry>> {
    let under = path.map(tree::parts).unwrap_or_default();
    let after = after.map(tree::parts);
    // The parts of the resume path below `path`, or `None` when every path
    // below `path` comes after it. Paths compare part by part.
    let after = match &after {
        Some(after) if after.starts_with(&under) => Some(&after[under.len()..]),
        Some(after) if after.as_slice() > under.as_slice() => return Ok(Vec::new()),
        _ => None,
    };
    let (old, new) = match path {
        Some(path) => store.read(|c| {
            Ok((
                tree::lookup(c, old_root, path)?,
                tree::lookup(c, new_root, path)?,
            ))
        })?,
        None => (Some(Entry::Dir(old_root)), Some(Entry::Dir(new_root))),
    };
    let mut walk = DiffWalk {
        store,
        path: path.map(|p| p.as_bytes().to_vec()).unwrap_or_default(),
        entries: Vec::new(),
        limit,
    };
    walk.walk(old, new, after)?;
    Ok(walk.entries)
}

/// The state of one [`diff`] walk.
struct DiffWalk<'a> {
    store: &'a Backend,
    /// The path of the entries being compared, as raw bytes.
    path: Vec<u8>,
    /// What the walk found, up to one more than `limit`.
    entries: Vec<DiffEntry>,
    limit: usize,
}

impl DiffWalk<'_> {
    fn full(&self) -> bool {
        self.entries.len() > self.limit
    }

    /// Compares `old` and `new`, the entries at `self.path`. `after` holds
    /// the parts of the resume path below here, or is `None` once every path
    /// here comes after it.
    ///
    /// The recursion is as deep as a path has parts: at most 256, by the
    /// path rules.
    fn walk(
        &mut self,
        old: Option<Entry>,
        new: Option<Entry>,
        after: Option<&[PathElem]>,
    ) -> Result<()> {
        if old == new || self.full() {
            return Ok(());
        }
        let (old_file, new_file) = (file(old), file(new));
        // A file here is the resume path or before it, unless `after` is
        // `None`.
        if old_file != new_file && after.is_none() {
            let path = RepoPath::new(self.path.clone())
                .map_err(|e| Error::Corrupt(format!("a tree path breaks the path rules: {e}")))?;
            self.entries.push(DiffEntry {
                path,
                old: old_file,
                new: new_file,
                rename: None,
            });
        }
        let (old_dir, new_dir) = (self.children(old)?, self.children(new)?);
        // Names before the resume path do not matter.
        let start = match after {
            Some([first, ..]) => Bound::Included(first),
            _ => Bound::Unbounded,
        };
        let names: BTreeSet<&PathElem> = old_dir
            .range::<PathElem, _>((start, Bound::Unbounded))
            .chain(new_dir.range::<PathElem, _>((start, Bound::Unbounded)))
            .map(|(name, _)| name)
            .collect();
        for name in names {
            let name_after = match after {
                Some([first, rest @ ..]) if name == first => Some(rest),
                // Past the resume path, or here is the resume path: every
                // name follows.
                _ => None,
            };
            let len = self.path.len();
            if len > 0 {
                self.path.push(b'/');
            }
            self.path.extend_from_slice(name.as_bytes());
            let (old_entry, new_entry) = (old_dir.get(name).copied(), new_dir.get(name).copied());
            self.walk(old_entry, new_entry, name_after)?;
            self.path.truncate(len);
            if self.full() {
                break;
            }
        }
        Ok(())
    }

    /// The entries of a directory. Empty for anything else. Each read takes
    /// a read connection on its own, so a long walk does not hold one.
    fn children(&self, entry: Option<Entry>) -> Result<BTreeMap<PathElem, Entry>> {
        match entry {
            Some(Entry::Dir(id)) => Ok(self.store.read(|c| c.get_tree(id))?.entries().clone()),
            _ => Ok(BTreeMap::new()),
        }
    }
}

/// The file state of an entry, if it is a file.
fn file(entry: Option<Entry>) -> Option<FileState> {
    match entry {
        Some(Entry::File(file)) => Some(file.state()),
        _ => None,
    }
}
