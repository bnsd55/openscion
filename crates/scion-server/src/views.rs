//! The view of a repo that a partial reader reads: the real trees with the
//! entries that its read rules do not cover removed. A view tree is an
//! ordinary `TreeV1` object, stored in the same tree table and
//! content-addressed by its own ID, so a ticket minted on a view root
//! walks, serves and caches like any other tree ([#121]). A kept directory
//! entry points at the child's view tree when anything below it is hidden,
//! and at the real child tree when the whole subtree is visible; the same
//! real tree under the same context shares one view. A subtree whose view
//! would be empty is hidden: its parent's view omits the entry, the same
//! as a missing one.
//!
//! Every derived (tree, context) pair is cached per (repo, rule-set
//! digest, projection version, real tree, path-context digest) in
//! `view_trees`, as the design of [#121] keyed it. A derivation asks the
//! table at every directory, so a new changeset re-derives only the
//! folders on its changed paths; the rest comes from the rows. The
//! derivation is a pure function of the rule set and the real trees, so
//! two servers that derive the same view store the same bytes and agree
//! on the rows.
//!
//! What the derivation costs: the first read of a rule set walks the real
//! tree, so its latency varies with the repo; later reads of the same
//! changeset find the root's row and skip the walk, and a new changeset
//! walks only the folders its changes moved.
//!
//! Rows die when their principal does: each derivation's write sweeps up
//! to `VIEW_EXPIRE_STEP` rows of older projection versions and of
//! digests that no grant of the repo hashes to any more, as far as this
//! server's access file knows. A row a second server still needs is
//! re-derived, at that server's next read.

use scion_core::{ChangesetId, Entry, PathElem, RepoPath, Tree, TreeId};

use crate::backend::Backend;
use crate::deadline::Deadline;
use crate::error::Result;
use crate::meta::{Meta as _, MetaTx, RepoId};

/// The context of a tree in the projection: the read prefixes that fall
/// below the tree's path, relative to it, in the rule set's canonical form.
type Context = Vec<Vec<u8>>;

/// What a context says about one directory entry.
#[derive(Debug)]
enum Below {
    /// The whole subtree is visible: the real child tree serves as is.
    Whole,
    /// Nothing below the entry is visible: the entry is hidden.
    Nothing,
    /// The visible suffixes below the child.
    Some(Context),
}

/// The context below `name`.
fn below(ctx: &[Vec<u8>], name: &[u8]) -> Below {
    let mut head = name.to_vec();
    head.push(b'/');
    let mut sub = Context::new();
    for prefix in ctx {
        if prefix.as_slice() == name {
            return Below::Whole;
        }
        if let Some(rest) = prefix.strip_prefix(&head[..]) {
            sub.push(rest.to_vec());
        }
    }
    if sub.is_empty() {
        Below::Nothing
    } else {
        Below::Some(sub)
    }
}

/// The subtrees derived so far, by (tree ID, context digest), so shared
/// subtrees are derived once per derivation; the new view trees it built;
/// and the (real tree, context digest, view) rows it found or derived, for
/// the one short write that stores them. Two counters bound the memo's
/// RAM under [`VIEW_MEMO_BYTES`], the share it reserved: `pending` counts
/// the trees and rows a part has not written yet, and past
/// [`VIEW_PART_BYTES`] they leave for the store as a part; the dedupe
/// map's cost is its entry count, and past [`VIEW_MAP_BYTES`] it clears.
#[derive(Default)]
struct Memo {
    derived: std::collections::HashMap<(TreeId, [u8; 32]), Option<TreeId>>,
    trees: Vec<Tree>,
    rows: Vec<(TreeId, [u8; 32], TreeId)>,
    /// The bytes of the pending trees and rows, estimated at every push.
    /// The dedupe map's bytes are its entry count times the entry
    /// estimate, read where the bound is checked - one counter per thing
    /// that resets, so a part never leaves the map's cost behind.
    pending: usize,
    /// Real trees the walk opened, one per directory. A per-tree memo hit
    /// skips its whole subtree, so the count says how much the derivation
    /// really read.
    walked: u64,
}

/// The RAM one view derivation's memo may hold before it writes a part:
/// the pending trees and rows leave for the store, and the dedupe map
/// clears (shared subtrees are looked up in the store's memo, whose rows
/// the parts already wrote - a clear costs re-walks, not wrong results).
/// The derivation reserves these bytes from a per-store pool of
/// [`VIEW_DERIVES_MAX`] reservations, so at most that many derivations
/// hold theirs at once and FLOOR can count the share.
pub(crate) const VIEW_MEMO_BYTES: usize = 64 << 20;

/// The pending trees and rows one part holds before its own short write
/// stores them, between two read transactions of the walk: about 32,000
/// rows, a few seconds in the write lane. Trees
/// go first, so a row only names a tree the store holds; the root's row
/// lands in the last part, so a derivation that fails mid-walk leaves
/// only valid prefix rows.
const VIEW_PART_BYTES: usize = 4 << 20;

/// The dedupe map's share: past it the map clears, and shared subtrees
/// are looked up in the store's memo, whose rows the parts wrote - a
/// clear costs re-walks, not wrong results.
const VIEW_MAP_BYTES: usize = 60 << 20;

#[cfg(any(test, feature = "test-hooks"))]
static VIEW_PART_BYTES_OVERRIDE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// The part bound this run enforces: the test's small bound, or the
/// server's.
fn view_part_bytes() -> usize {
    #[cfg(any(test, feature = "test-hooks"))]
    {
        let small = VIEW_PART_BYTES_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed);
        if small > 0 {
            return small;
        }
    }
    VIEW_PART_BYTES
}

/// How many view derivations one store runs at once. A derivation beyond
/// the cap waits for a reservation, under its call's deadline, with no
/// store lock and no write lane held - it holds nothing the store needs.
pub(crate) const VIEW_DERIVES_MAX: usize = 2;

/// Holds a small part bound for the tests that pin the mid-walk flush:
/// the next derivation writes parts at it, and the bound returns when the
/// guard drops.
#[cfg(any(test, feature = "test-hooks"))]
pub fn test_set_view_part_bound(bytes: usize) -> impl Drop {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            VIEW_PART_BYTES_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }
    VIEW_PART_BYTES_OVERRIDE.store(bytes, std::sync::atomic::Ordering::Relaxed);
    Reset
}

/// The memo's cost of one derived tree: an estimate, as the walk budget's
/// per-walked byte count is - an entry's name bytes, the entry itself, and
/// the table's share, plus the tree's header and ID.
fn tree_bytes(tree: &Tree) -> usize {
    64 + tree
        .entries()
        .keys()
        .map(|name| name.as_bytes().len() + 64)
        .sum::<usize>()
}

/// The memo's cost of one row: two tree IDs, the digest, and the table's
/// share.
const ROW_BYTES: usize = 128;

/// The memo's cost of one dedupe-map entry: the key's tree ID and digest,
/// the answer, and the table's share.
const DERIVED_BYTES: usize = 160;

/// The version of the projection. Bumped when the derivation changes, so
/// views of an old version stop being served: their rows die in the sweep,
/// and a derivation of the new version writes new ones.
pub(crate) const PROJECTION_VERSION: u8 = 1;

/// View rows the sweep of one derivation removes, at most.
pub(crate) const VIEW_EXPIRE_STEP: usize = 256;

/// The view root that `prefixes` derives for `cs`: the ID of the tree that
/// holds exactly what the rule set covers. The first call for a (repo,
/// rule set, changeset) walks the real tree; the later ones find the
/// root's row; a new changeset walks only the folders its changes moved,
/// the rest served from the per-tree rows. The walk runs on a read
/// connection under `deadline` and stores nothing; one short write follows
/// with the new trees, their rows, and a bounded sweep of dead rows, so a
/// slow derivation never holds the write lock. `live` names the rule-set
/// digests that still have principals, for that sweep.
///
/// # Errors
///
/// Store errors, [`crate::error::Error::DeadlineExceeded`] once `deadline`
/// passes, or [`crate::error::Error::Cancelled`] once the call has gone.
#[allow(clippy::too_many_arguments)]
pub(crate) fn view_root(
    store: &Backend,
    repo: RepoId,
    digest: &[u8; 32],
    cs: ChangesetId,
    prefixes: &[RepoPath],
    live: &[[u8; 32]],
    metrics: &crate::metrics::Metrics,
    deadline: &Deadline,
) -> Result<TreeId> {
    let real = crate::tree::root_tree(store, repo, cs, deadline)?;
    let context: Vec<Vec<u8>> = prefixes.iter().map(|p| p.as_bytes().to_vec()).collect();
    let ctx_digest = context_digest(&context);
    // The digest the caller derives under is live: every caller holds it
    // from `Access::read_digest`, and `Access::view_digests` collects the
    // digests of the same access snapshot (read.rs, objects.rs). The sweep
    // would otherwise call the caller's own rows dead.
    debug_assert!(
        live.iter().any(|live| live == digest),
        "the derived digest is not among the live digests"
    );
    // The root's row: one read serves the whole view.
    if let Some(cached) = store.read(|c| {
        c.get_view_tree(
            repo,
            digest,
            u32::from(PROJECTION_VERSION),
            real,
            ctx_digest,
        )
    })? {
        return Ok(cached);
    }
    // The derivation reads real trees only: it holds no write lock, and it
    // gives up when the call that asked for it has gone. Its memo's RAM is
    // reserved from the store's pool first: a derivation beyond the cap
    // waits for a place, under the call's deadline, holding nothing the
    // store needs.
    let _reservation = store
        .view_memo_budget()
        .take(VIEW_MEMO_BYTES, Some(deadline.at()))?;
    let mut memo = Memo::default();
    // A rule set with prefixes covers at least one path of the repo, but it
    // may cover only paths that this changeset's tree does not have: then
    // the view is the empty tree, which is a valid root. The root is never
    // dropped.
    let view = if context.is_empty() {
        None
    } else {
        derive(store, repo, digest, real, context, &mut memo, deadline)?
    };
    let view = if let Some(view) = view {
        view
    } else {
        let empty = Tree::default();
        let id = empty.encode_with_id().1;
        memo.trees.push(empty);
        // The empty root is a derived view of the real root: row it, so a
        // later read of the same changeset skips the walk too.
        memo.rows.push((real, ctx_digest, id));
        id
    };
    // The real trees the walk opened, as an operator metric: a count that
    // stays high on every changeset says the memo is not serving.
    metrics.view_tree_walked(memo.walked);
    // One short write: the new view trees, their rows, and the sweep. All
    // are safe to repeat; a row that another server stored already must
    // agree, or the derivation is not a pure function and that is a bug.
    // What the walk's part flushes already wrote stays: a view row only
    // ever points at a tree an earlier part wrote, so the end state of a
    // cancelled derivation is a prefix of this write's rows, all valid.
    store.write(|c| {
        write_memo_part(c, repo, digest, &mut memo)?;
        c.delete_stale_view_trees(repo, live, u32::from(PROJECTION_VERSION), VIEW_EXPIRE_STEP)?;
        Ok(view)
    })
}

/// Writes the memo's pending trees, then its pending rows - a tree is in
/// the store before any row names it - and clears both. Called at the
/// memo's byte bound mid-derivation and once at the end.
fn write_memo_part(
    c: &mut dyn MetaTx,
    repo: RepoId,
    digest: &[u8; 32],
    memo: &mut Memo,
) -> Result<()> {
    for tree in &memo.trees {
        c.put_tree(tree)?;
    }
    for (tree, ctx, view) in &memo.rows {
        c.put_view_tree(
            repo,
            digest,
            u32::from(PROJECTION_VERSION),
            *tree,
            *ctx,
            *view,
        )?;
    }
    memo.trees.clear();
    memo.rows.clear();
    memo.pending = 0;
    Ok(())
}

/// One directory of the walk: the real tree, the context below it, the
/// entries the tree holds (None until the walk fetched the tree), the
/// next entry to process, the view tree under construction, the name the
/// frame holds in its parent, and the parent's index (None for the
/// root). The stack of frames outlives any one read transaction, so a
/// part write can end it and the next read continues where this stopped.
struct Frame {
    id: TreeId,
    ctx: Context,
    ctx_digest: [u8; 32],
    /// The name the frame holds in its parent; None for the root.
    via: Option<PathElem>,
    entries: Option<Vec<(PathElem, Entry)>>,
    next: usize,
    kept: std::collections::BTreeMap<PathElem, Entry>,
    parent: Option<usize>,
}

/// Walks the real tree `real` under `context`, and answers the ID of the
/// view tree it derives, or None when nothing below is visible. The walk
/// runs in read transactions that end at the memo's part bound; between
/// two of them the pending trees and rows leave in their own short write,
/// which holds no read transaction and waits on the write lane holding
/// nothing the store needs. The trees are content-addressed, so a new
/// snapshot between two read transactions cannot change what the walk
/// already read: every tree ID on the stack names an immutable tree.
///
/// # Errors
///
/// Store errors, [`crate::error::Error::DeadlineExceeded`] once `deadline`
/// passes, or [`crate::error::Error::Cancelled`] once the call has gone.
fn derive(
    store: &Backend,
    repo: RepoId,
    digest: &[u8; 32],
    real: TreeId,
    context: Context,
    memo: &mut Memo,
    deadline: &Deadline,
) -> Result<Option<TreeId>> {
    let ctx_digest = context_digest(&context);
    let mut stack = vec![Frame {
        id: real,
        ctx: context,
        ctx_digest,
        via: None,
        entries: None,
        next: 0,
        kept: std::collections::BTreeMap::new(),
        parent: None,
    }];
    loop {
        let done = store.read(|c| walk(&mut *c, repo, digest, &mut stack, memo, deadline))?;
        if done {
            break;
        }
        // The read transaction is closed: the part write waits on the
        // write lane holding nothing the store needs.
        store.write(|wc| write_memo_part(wc, repo, digest, memo))?;
    }
    Ok(memo.derived.get(&(real, ctx_digest)).copied().flatten())
}

/// One step of the walk, inside one read transaction: processes entries
/// of the top frame until the memo's part bound or the walk's end. The
/// frame stack carries the state across the transactions.
///
/// # Errors
///
/// Store errors, [`crate::error::Error::DeadlineExceeded`] once `deadline`
/// passes, or [`crate::error::Error::Cancelled`] once the call has gone.
fn walk(
    c: &mut dyn MetaTx,
    repo: RepoId,
    digest: &[u8; 32],
    stack: &mut Vec<Frame>,
    memo: &mut Memo,
    deadline: &Deadline,
) -> Result<bool> {
    while !stack.is_empty() {
        // The part bound ends this read transaction: the caller writes
        // the part with no read transaction open, and the next read
        // continues here.
        if memo.pending >= view_part_bytes() {
            return Ok(false);
        }
        deadline.check()?;
        let top = stack.len() - 1;
        if stack[top].entries.is_none() {
            // The frame's tree: fetched once, on the frame's first step.
            memo.walked += 1;
            let tree = c.get_tree(stack[top].id)?;
            stack[top].entries = Some(
                tree.entries()
                    .iter()
                    .map(|(name, entry)| (name.clone(), *entry))
                    .collect(),
            );
        }
        let next = stack[top].next;
        let len = stack[top].entries.as_ref().expect("fetched above").len();
        if next < len {
            let (name, entry) = stack[top].entries.as_ref().expect("fetched above")[next].clone();
            stack[top].next = next + 1;
            match entry {
                // A file is visible when a prefix names it exactly; a
                // prefix can name nothing below a file.
                Entry::File(_) => {
                    if stack[top]
                        .ctx
                        .iter()
                        .any(|prefix| prefix == name.as_bytes())
                    {
                        stack[top].kept.insert(name, entry);
                    }
                }
                Entry::Dir(child) => match below(&stack[top].ctx, name.as_bytes()) {
                    Below::Whole => {
                        stack[top].kept.insert(name, entry);
                    }
                    Below::Nothing => {}
                    // The child's view takes its ID; an empty view hides
                    // the entry, the same as a missing one.
                    Below::Some(sub) => {
                        let sub_digest = context_digest(&sub);
                        let key = (child, sub_digest);
                        if let Some(hit) = memo.derived.get(&key) {
                            if let Some(view) = *hit {
                                stack[top].kept.insert(name, Entry::Dir(view));
                            }
                        } else if let Some(hit) = c.get_view_tree(
                            repo,
                            digest,
                            u32::from(PROJECTION_VERSION),
                            child,
                            sub_digest,
                        )? {
                            // The store's memo: a row derived by an
                            // earlier call - or an earlier part of this
                            // derivation - serves the subtree whole.
                            memo.derived.insert(key, Some(hit));
                            stack[top].kept.insert(name, Entry::Dir(hit));
                        } else {
                            stack.push(Frame {
                                id: child,
                                ctx: sub,
                                ctx_digest: sub_digest,
                                via: Some(name),
                                entries: None,
                                next: 0,
                                kept: std::collections::BTreeMap::new(),
                                parent: Some(top),
                            });
                        }
                    }
                },
            }
        } else {
            // The frame is done: its view tree is a row for the next
            // derivation, and its ID files into the parent's view.
            let frame = stack.pop().expect("the stack is not empty");
            let view = if frame.kept.is_empty() {
                None
            } else {
                let tree = Tree::new(frame.kept).expect("tree fits the format");
                let view_id = tree.encode_with_id().1;
                memo.pending += tree_bytes(&tree);
                memo.trees.push(tree);
                memo.pending += ROW_BYTES;
                memo.rows.push((frame.id, frame.ctx_digest, view_id));
                Some(view_id)
            };
            memo.derived.insert((frame.id, frame.ctx_digest), view);
            // The dedupe map's share: past it the map clears, and shared
            // subtrees are looked up in the store's memo, whose rows the
            // parts wrote - a clear costs re-walks, never wrong results.
            if memo.derived.len() * DERIVED_BYTES >= VIEW_MAP_BYTES {
                memo.derived.clear();
            }
            if let Some(parent) = frame.parent {
                if let (Some(via), Some(view_id)) = (frame.via, view) {
                    stack[parent].kept.insert(via, Entry::Dir(view_id));
                }
            } else {
                return Ok(true);
            }
        }
    }
    Ok(true)
}

/// The digest of a context, so one derivation memoizes shared subtrees by
/// (tree, context).
fn context_digest(ctx: &Context) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(ctx.len() as u64).to_be_bytes());
    for prefix in ctx {
        hasher.update(&(prefix.len() as u64).to_be_bytes());
        hasher.update(prefix);
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::TestCaseError;
    use scion_core::{Changeset, FileEntry, FileState, PathElem};
    use std::path::Path;
    use std::sync::{Mutex, MutexGuard};
    use std::time::{Duration, Instant};

    /// The part bound is one process-wide seam, and the tests that pin
    /// it count the writes it causes: every such test holds the lock for
    /// its whole body, so a bound window holds only its own derivation.
    static PART_LOCK: Mutex<()> = Mutex::new(());

    fn part_guard() -> MutexGuard<'static, ()> {
        PART_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A deadline that no test outlives.
    fn alive() -> Deadline {
        Deadline::new(Instant::now() + Duration::from_secs(600))
    }

    /// A derivation against a repo of the store that holds no rows yet:
    /// every walk does the work. The repo is real, so a part flush's rows
    /// name a key that is there, whatever other tests do to the shared
    /// part bound.
    fn bare(store: &Backend) -> (RepoId, [u8; 32]) {
        let repo = store.write(|c| c.create_repo("bare")).unwrap();
        (repo, [0; 32])
    }

    fn file(store: &Backend, content: &[u8]) -> Entry {
        let blob = store.blobs().put(content).unwrap();
        let len = u64::try_from(content.len()).unwrap();
        Entry::File(FileEntry::new(
            FileState::Regular {
                content: blob,
                executable: false,
            },
            len,
        ))
    }

    fn store() -> (tempfile::TempDir, Backend) {
        let dir = crate::test_support::private_tempdir();
        let store = Backend::open_test(dir.path()).unwrap();
        (dir, store)
    }

    fn name(raw: &[u8]) -> PathElem {
        PathElem::new(raw.to_vec()).unwrap()
    }

    #[test]
    fn a_view_keeps_visible_files_whole_subtrees_and_drops_hidden_ones() {
        let (_dir, store) = store();

        let (repo, digest) = bare(&store);
        // /README, /src/public, /src/secret, /docs/note, /ops/only-hidden
        let src = Tree::new(
            [
                (name(b"public"), file(&store, b"public\n")),
                (name(b"secret"), file(&store, b"secret\n")),
            ]
            .into(),
        )
        .expect("tree fits the format");
        let (src_id, root_id) = store
            .write(|c| {
                let src_id = c.put_tree(&src)?;
                let root = Tree::new(
                    [
                        (name(b"README"), file(&store, b"readme\n")),
                        (name(b"docs"), file(&store, b"note\n")),
                        (name(b"ops"), Entry::Dir(src_id)),
                    ]
                    .into(),
                )
                .expect("tree fits the format");
                let root_id = c.put_tree(&root)?;
                Ok((src_id, root_id))
            })
            .unwrap();
        // The rule covers README, docs/note and ops/public.
        let ctx: Context = vec![b"README".to_vec(), b"docs".to_vec(), b"ops/public".to_vec()];
        let mut memo = Memo::default();
        let view = derive(&store, repo, &digest, root_id, ctx, &mut memo, &alive())
            .unwrap()
            .expect("the root keeps a visible path");
        store
            .write(|c| {
                for tree in &memo.trees {
                    c.put_tree(tree)?;
                }
                Ok(())
            })
            .unwrap();
        let shown = store.read(|c| c.get_tree(view)).unwrap();
        let names: Vec<Vec<u8>> = shown
            .entries()
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        // `ops` is a view: it holds `public` only, and no `secret`.
        assert_eq!(
            names,
            [b"README".to_vec(), b"docs".to_vec(), b"ops".to_vec()]
        );
        let Entry::Dir(ops) = shown.entries().get(&name(b"ops")).unwrap() else {
            panic!("ops is a directory in the view");
        };
        assert_ne!(*ops, src_id, "the hidden entry changed the subtree");
        let ops_tree = store.write(|c| c.get_tree(*ops)).unwrap();
        let ops_names: Vec<Vec<u8>> = ops_tree
            .entries()
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        assert_eq!(ops_names, [b"public".to_vec()]);

        // The same tree with a context that covers nothing is hidden.
        let ctx: Context = vec![b"absent".to_vec()];
        let mut memo = Memo::default();
        let none = derive(&store, repo, &digest, root_id, ctx, &mut memo, &alive()).unwrap();
        assert!(none.is_none());

        // A context that covers the whole tree names the tree itself: the
        // caller uses the real ID, and the projection is not asked.
        // (Below::Whole is decided by the parent; see the unit test below.)
    }

    #[test]
    fn a_context_below_a_name_splits_and_wholes() {
        let ctx: Context = vec![b"README".to_vec(), b"src/net".to_vec(), b"src/api".to_vec()];
        match below(&ctx, b"src") {
            Below::Some(sub) => {
                assert_eq!(sub, [b"net".to_vec(), b"api".to_vec()]);
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(below(&ctx, b"README"), Below::Whole));
        assert!(matches!(below(&ctx, b"docs"), Below::Nothing));
        let whole: Context = vec![b"src".to_vec()];
        assert!(matches!(below(&whole, b"src"), Below::Whole));
    }

    #[test]
    fn one_derivation_serves_shared_subtrees_once() {
        let (_dir, store) = store();

        let (repo, digest) = bare(&store);
        // Two directories with the same content: one visible, one not.
        let inner = Tree::new(
            [
                (name(b"a"), file(&store, b"a\n")),
                (name(b"b"), file(&store, b"b\n")),
            ]
            .into(),
        )
        .expect("tree fits the format");
        let (left, root_id) = store
            .write(|c| {
                let id = c.put_tree(&inner)?;
                let root = Tree::new(
                    [
                        (name(b"left"), Entry::Dir(id)),
                        (name(b"right"), Entry::Dir(id)),
                    ]
                    .into(),
                )
                .expect("tree fits the format");
                let root_id = c.put_tree(&root)?;
                Ok((id, root_id))
            })
            .unwrap();
        // Only `left/a` is visible: the view drops `right` whole and keeps
        // a view of `left` with `a` only. The memo derives the shared tree
        // once per context, and the two contexts differ here.
        let ctx: Context = vec![b"left/a".to_vec()];
        let mut memo = Memo::default();
        let view = derive(&store, repo, &digest, root_id, ctx, &mut memo, &alive())
            .unwrap()
            .unwrap();
        // The same real tree under the other context derives a second,
        // different view: one tree, two views.
        let ctx2: Context = vec![b"right/b".to_vec()];
        let view2 = derive(&store, repo, &digest, root_id, ctx2, &mut memo, &alive())
            .unwrap()
            .unwrap();
        assert_ne!(view, view2, "one real tree, two contexts, two views");
        // The derivation stores nothing: put the derived trees, then the
        // views below are readable.
        store
            .write(|c| {
                for tree in &memo.trees {
                    c.put_tree(tree)?;
                }
                Ok(())
            })
            .unwrap();
        let right_tree = store.read(|c| c.get_tree(view2)).unwrap();
        let right_names: Vec<Vec<u8>> = right_tree
            .entries()
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        assert_eq!(right_names, [b"right".to_vec()]);
        let shown = store.write(|c| c.get_tree(view)).unwrap();
        let names: Vec<Vec<u8>> = shown
            .entries()
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        assert_eq!(names, [b"left".to_vec()]);
        let Entry::Dir(left_view) = shown.entries().get(&name(b"left")).unwrap() else {
            panic!("left is a directory in the view");
        };
        let left_tree = store.read(|c| c.get_tree(*left_view)).unwrap();
        let kept: Vec<Vec<u8>> = left_tree
            .entries()
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        assert_eq!(kept, [b"a".to_vec()]);
        assert_eq!(memo.derived.len(), 4, "root and child, two contexts");
        assert_eq!(memo.trees.len(), 4, "each derived tree waits, unstored");
        let _ = left;
    }

    /// A folder whose whole subtree is visible keeps the real tree ID: the
    /// view of a fully covered directory is the directory itself.
    #[test]
    fn a_fully_visible_folder_keeps_the_real_tree_id() {
        let (_dir, store) = store();

        let (repo, digest) = bare(&store);
        let docs = Tree::new([(name(b"note"), file(&store, b"note\n"))].into())
            .expect("tree fits the format");
        let (docs_id, root_id) = store
            .write(|c| {
                let docs_id = c.put_tree(&docs)?;
                let root = Tree::new([(name(b"docs"), Entry::Dir(docs_id))].into())
                    .expect("tree fits the format");
                Ok((docs_id, c.put_tree(&root)?))
            })
            .unwrap();
        // The rule covers `docs` whole: the view keeps the real `docs` ID.
        let ctx: Context = vec![b"docs".to_vec()];
        let mut memo = Memo::default();
        let view = derive(&store, repo, &digest, root_id, ctx, &mut memo, &alive())
            .unwrap()
            .unwrap();
        let shown = store.read(|c| c.get_tree(view)).unwrap();
        let Entry::Dir(kept) = shown.entries().get(&name(b"docs")).unwrap() else {
            panic!("docs is a directory in the view");
        };
        assert_eq!(*kept, docs_id, "the fully visible folder is unchanged");
    }

    // ---- The model check: random trees and rule sets against a naive
    // ---- projection.

    /// The file paths of a random tree: unique, nested, names from a small
    /// alphabet so names repeat across directories and subtrees share
    /// content. A path that is also a prefix of another path loses its
    /// file role: a real tree holds one entry per name, file or dir.
    fn file_paths() -> impl proptest::strategy::Strategy<Value = Vec<String>> {
        // "-" and "." sort before "/": a rule "a" must not cover "a-b",
        // and the boundary bugs that confuse them show here.
        proptest::collection::vec("[ab.-]{1,4}", 2..24)
            .prop_filter("no dot-only part", |parts| {
                !parts.iter().any(|n| n == "." || n == "..")
            })
            .prop_map(|parts| {
                let mut paths: Vec<String> = Vec::new();
                for (i, name) in parts.iter().enumerate() {
                    let parent = if i == 0 {
                        String::new()
                    } else {
                        paths[(name.len() + i) % paths.len()].clone()
                    };
                    let candidate = if parent.is_empty() {
                        name.clone()
                    } else {
                        format!("{parent}/{name}")
                    };
                    if !paths.contains(&candidate) {
                        paths.push(candidate);
                    }
                }
                // A path with a descendant in the set is a directory, not a
                // file: drop its file role.
                let has_descendant =
                    |p: &String| paths.iter().any(|q| q.starts_with(&format!("{p}/")));
                let keep: Vec<String> = paths
                    .iter()
                    .filter(|p| !has_descendant(p))
                    .cloned()
                    .collect();
                paths = keep;
                paths.sort();
                paths
            })
    }

    /// The content of a file: unique per path, so every blob ID differs.
    fn content_of(path: &str) -> Vec<u8> {
        format!("content of {path}\n").into_bytes()
    }

    fn elem(raw: &[u8]) -> PathElem {
        PathElem::new(raw.to_vec()).unwrap()
    }

    fn file_entry(store: &Backend, content: &[u8]) -> Entry {
        let blob = store.blobs().put(content).unwrap();
        let len = u64::try_from(content.len()).unwrap();
        Entry::File(FileEntry::new(
            FileState::Regular {
                content: blob,
                executable: false,
            },
            len,
        ))
    }

    /// Builds and stores the real trees for the paths: every intermediate
    /// directory exists, files carry their deterministic content. Returns
    /// the non-root trees by path and the stored root's ID.
    fn build_trees(
        store: &Backend,
        paths: &[String],
    ) -> (std::collections::BTreeMap<String, Tree>, TreeId) {
        let mut all: Vec<String> = vec![String::new()];
        for p in paths {
            let parts: Vec<&str> = p.split('/').collect();
            for i in 1..parts.len() {
                let dir = parts[..i].join("/");
                if !all.contains(&dir) {
                    all.push(dir);
                }
            }
        }
        all.sort();
        // Deepest first, so a child tree exists when its parent needs it.
        all.reverse();
        let mut dirs: std::collections::BTreeMap<String, Tree> = std::collections::BTreeMap::new();
        let mut ids: std::collections::BTreeMap<String, TreeId> = std::collections::BTreeMap::new();
        for path in &all {
            let prefix = if path.is_empty() {
                String::new()
            } else {
                format!("{path}/")
            };
            let mut entries = std::collections::BTreeMap::new();
            for (d, id) in &ids {
                if let Some(rest) = d.strip_prefix(prefix.as_str()).filter(|r| !r.contains('/')) {
                    entries.insert(elem(rest.as_bytes()), Entry::Dir(*id));
                }
            }
            for p in paths {
                if let Some(rest) = p.strip_prefix(prefix.as_str()).filter(|r| !r.contains('/')) {
                    entries.insert(elem(rest.as_bytes()), file_entry(store, &content_of(p)));
                }
            }
            let tree = Tree::new(entries).expect("tree fits the format");
            let id = store.write(|c| c.put_tree(&tree)).unwrap();
            ids.insert(path.clone(), id);
            dirs.insert(path.clone(), tree);
        }
        let root = dirs
            .remove("")
            .expect("every path has the root as a prefix");
        let root_id = store.write(|c| c.put_tree(&root)).unwrap();
        (dirs, root_id)
    }

    /// The canonical rule set: a sorted subset of the paths (files and
    /// directories), with any rule another rule covers removed.
    fn canonical_rules(paths: &[String], seeds: &[u16]) -> Vec<String> {
        let mut all: Vec<String> = Vec::new();
        for p in paths {
            let parts: Vec<&str> = p.split('/').collect();
            for i in 1..parts.len() {
                let dir = parts[..i].join("/");
                if !all.contains(&dir) {
                    all.push(dir);
                }
            }
            if !all.contains(p) {
                all.push(p.clone());
            }
        }
        all.sort();
        let mut picked: Vec<String> = Vec::new();
        for s in seeds {
            let pick = &all[usize::from(*s) % all.len()];
            if !picked.contains(pick) {
                picked.push(pick.clone());
            }
        }
        picked.sort();
        let mut kept_rules: Vec<String> = Vec::new();
        for r in &picked {
            let covered = kept_rules
                .iter()
                .any(|k| k != r && r.starts_with(&format!("{k}/")));
            if !covered {
                kept_rules.push(r.clone());
            }
        }
        kept_rules
    }

    /// The naive model: a path is kept iff some rule covers it.
    fn kept(path: &str, rules: &[String]) -> bool {
        rules
            .iter()
            .any(|r| path == r || path.starts_with(&format!("{r}/")))
    }

    /// What the model says about a directory: nothing below it is kept,
    /// some is, or all of it is.
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum DirState {
        Hidden,
        Partial,
        Whole,
    }

    fn dir_state(dir: &str, paths: &[String], rules: &[String]) -> DirState {
        let below: Vec<&String> = paths
            .iter()
            .filter(|p| p.starts_with(&format!("{dir}/")))
            .collect();
        let any = below.iter().any(|p| kept(p, rules));
        let all = below.iter().all(|p| kept(p, rules));
        match (any, all) {
            (false, _) => DirState::Hidden,
            (true, true) => DirState::Whole,
            (true, false) => DirState::Partial,
        }
    }

    /// The model of the view tree at `dir` (the root at the empty path):
    /// the names it shows and, for directories, whether it holds the real
    /// tree (`Whole`) or a derived view (`Partial`). Files are shown iff
    /// kept.
    fn model_view(dir: &str, paths: &[String], rules: &[String]) -> Vec<(String, DirState)> {
        let prefix = if dir.is_empty() {
            String::new()
        } else {
            format!("{dir}/")
        };
        let mut direct: Vec<String> = Vec::new();
        for p in paths {
            if let Some(rest) = p.strip_prefix(prefix.as_str()) {
                let first = rest.split('/').next().unwrap().to_owned();
                if !direct.contains(&first) {
                    direct.push(first);
                }
            }
        }
        // The tree lists its entries by name bytes; the first-seen order of
        // the paths is not that order once a name sorts before "/" ("-" vs
        // "---").
        direct.sort();
        let mut shown = Vec::new();
        for name in direct {
            let child = if dir.is_empty() {
                name.clone()
            } else {
                format!("{prefix}{name}")
            };
            let is_dir = paths.iter().any(|p| {
                p.strip_prefix(prefix.as_str())
                    .is_some_and(|rest| rest.starts_with(&format!("{name}/")))
            });
            if is_dir {
                match dir_state(&child, paths, rules) {
                    DirState::Hidden => {}
                    state => shown.push((name, state)),
                }
            } else if kept(&child, rules) {
                shown.push((name, DirState::Whole));
            }
        }
        shown
    }

    proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(256))]

        /// The view that `project` derives equals the naive model, keeps
        /// real IDs for fully covered directories, and leaks nothing.
        #[test]
        fn the_view_equals_the_model(
            paths in file_paths(),
            rule_seeds in proptest::collection::vec(proptest::num::u16::ANY, 1..8),
        ) {
            prop_assume!(!paths.is_empty());
            let rules = canonical_rules(&paths, &rule_seeds);
            prop_assume!(!rules.is_empty(), "every rule was covered by another");
            let (_dir, store) = store();

            let (repo, digest) = bare(&store);
            let (dirs, root_id) = build_trees(&store, &paths);

            // The rule set, in the canonical form the caller passes.
            let ctx: Context = rules
                .iter()
                .map(|r| RepoPath::new(r.clone()).unwrap().as_bytes().to_vec())
                .collect();
            let mut memo = Memo::default();
            let view = derive(&store, repo, &digest, root_id, ctx, &mut memo, &alive())
                .unwrap();
            // The derivation stores nothing: put the derived trees, then
            // the walk can read the views below.
            store
                .write(|c| {
                    for tree in &memo.trees {
                        c.put_tree(tree)?;
                    }
                    Ok(())
                })
                .unwrap();

            // Walk the view against the model, collecting what it shows.
            let mut w = Walk {
                store: &store,
                dirs: &dirs,
                paths: &paths,
                rules: &rules,
                names: std::collections::BTreeSet::new(),
                blobs: std::collections::BTreeSet::new(),
                tree_ids: Vec::new(),
            };
            walk_model(&mut w, view, "")?;

            // Nothing hidden leaks: a name that is hidden everywhere it
            // exists, the blob of an uncovered file, and the real ID of a
            // directory whose view is not the real tree.
            let every_name: std::collections::BTreeSet<String> = paths
                .iter()
                .flat_map(|p| p.split('/').map(str::to_owned).collect::<Vec<_>>())
                .collect();
            let mut places: Vec<String> = vec![String::new()];
            for p in &paths {
                let parts: Vec<&str> = p.split('/').collect();
                for i in 1..parts.len() {
                    places.push(parts[..i].join("/"));
                }
            }
            places.sort();
            places.dedup();
            for name in every_name {
                let shown_somewhere = places.iter().any(|d| {
                    model_view(d, &paths, &rules)
                        .iter()
                        .any(|(n, _)| n.as_bytes() == name.as_bytes())
                });
                if !shown_somewhere {
                    prop_assert!(
                        !w.names.contains(&name.clone().into_bytes()),
                        "the hidden name {name} shows in a view"
                    );
                }
            }
            for p in &paths {
                if !kept(p, &rules) {
                    let blob = scion_core::BlobId::of(&content_of(p))
                        .as_bytes()
                        .to_vec();
                    prop_assert!(
                        !w.blobs.contains(&blob),
                        "the blob of the hidden file {p} shows in a view"
                    );
                }
            }
            for d in dirs.keys() {
                if dir_state(d, &paths, &rules) != DirState::Whole {
                    let real = tree_id_of(&dirs[d]);
                    prop_assert!(
                        !w.tree_ids.contains(&real),
                        "the real tree of the hidden or partial directory {d} shows in a view"
                    );
                }
            }
        }
    }

    /// The content address of a tree that was built in memory: its
    /// canonical encoding hashes to the ID the store gave it.
    fn tree_id_of(tree: &Tree) -> TreeId {
        tree.encode_with_id().1
    }

    /// What the view walk carries: the store, the real trees by path, the
    /// rule set, and what the view has shown so far.
    struct Walk<'a> {
        store: &'a Backend,
        dirs: &'a std::collections::BTreeMap<String, Tree>,
        paths: &'a [String],
        rules: &'a [String],
        names: std::collections::BTreeSet<Vec<u8>>,
        blobs: std::collections::BTreeSet<Vec<u8>>,
        tree_ids: Vec<TreeId>,
    }

    /// Walks the view tree, comparing it against the model, and collecting
    /// every name, blob ID and tree ID the view shows.
    fn walk_model(w: &mut Walk<'_>, view: Option<TreeId>, dir: &str) -> Result<(), TestCaseError> {
        let paths: &[String] = w.paths;
        let rules: &[String] = w.rules;
        let shown = match view {
            Some(id) => w.store.write(|c| c.get_tree(id)).unwrap(),
            None => Tree::default(),
        };
        // The names the view shows, straight from the projected tree: the
        // leak check below compares these against the model's visibility.
        for name in shown.entries().keys() {
            w.names.insert(name.as_bytes().to_vec());
        }
        let model = model_view(dir, paths, rules);
        let shown_names: Vec<Vec<u8>> = shown
            .entries()
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        let model_names: Vec<Vec<u8>> = model.iter().map(|(n, _)| n.clone().into_bytes()).collect();
        prop_assert_eq!(shown_names, model_names, "the view differs from the model");
        for (name, state) in &model {
            let child_path = if dir.is_empty() {
                name.clone()
            } else {
                format!("{dir}/{name}")
            };
            let entry = shown.entries().get(&elem(name.as_bytes())).unwrap();
            match state {
                DirState::Whole => {
                    if paths.contains(&child_path) {
                        // A file: the real entry.
                        let Entry::File(f) = entry else {
                            panic!("{child_path} is a file in the view");
                        };
                        if let FileState::Regular { content: blob, .. } = f.state() {
                            w.blobs.insert(blob.as_bytes().to_vec());
                        }
                    } else {
                        // A directory whose subtree is fully covered: the
                        // real tree ID.
                        let Entry::Dir(id) = entry else {
                            panic!("{child_path} is a directory in the view");
                        };
                        prop_assert_eq!(
                            *id,
                            tree_id_of(&w.dirs[&child_path]),
                            "a fully covered directory shows the real tree"
                        );
                        w.tree_ids.push(*id);
                    }
                }
                DirState::Partial => {
                    // A derived view: walk it against the model of the
                    // covered part.
                    let Entry::Dir(id) = entry else {
                        panic!("{child_path} is a directory in the view");
                    };
                    w.tree_ids.push(*id);
                    walk_model(w, Some(*id), &child_path)?;
                }
                DirState::Hidden => unreachable!("hidden entries are not shown"),
            }
        }
        Ok(())
    }

    /// A repo with a tree whose two folders each hold a file, and a rule
    /// set that names each file. Returns the store, the repo, the tip and
    /// the rule set's digest and prefixes.
    #[allow(clippy::type_complexity)]
    fn repo_of_two_folders_on(
        dir: tempfile::TempDir,
        store: Backend,
    ) -> (tempfile::TempDir, Backend, RepoId, ChangesetId, [u8; 32]) {
        let (a, b, z) = (
            file(&store, b"a\n"),
            file(&store, b"b\n"),
            file(&store, b"z\n"),
        );
        let regular = |content: Entry| match content {
            Entry::File(file) => file.state(),
            Entry::Dir(_) => unreachable!("a file"),
        };
        let ada = scion_core::IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format");
        let tip = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                c.insert_changeset(
                    repo,
                    &Changeset::new(
                        vec![],
                        ada,
                        None,
                        b"root\n".to_vec(),
                        [
                            (RepoPath::new("left/x").unwrap(), Some(regular(a))),
                            (RepoPath::new("right/y").unwrap(), Some(regular(b))),
                            (RepoPath::new("secret/z").unwrap(), Some(regular(z))),
                        ]
                        .into(),
                    )
                    .expect("changeset fits the format"),
                )
            })
            .unwrap();
        let repo = store.read(|c| c.repo_id("r")).unwrap();
        let prefixes: Vec<RepoPath> = ["left/x", "right/y"]
            .iter()
            .map(|p| RepoPath::new(*p).unwrap())
            .collect();
        let digest = crate::access::rule_set_digest(Some(&prefixes));
        (dir, store, repo, tip, digest)
    }

    /// How many view rows `store` holds, on either backend: the count
    /// goes through the damage module's two arms, so the row-count tests
    /// run on both engines.
    fn rows(store: &Backend) -> usize {
        crate::pg::damage::view_tree_rows(store).unwrap()
    }

    /// A memo row serves a later derivation of the same real tree under
    /// the same context, without a walk, on any changeset that reaches it.
    #[test]
    fn a_stored_row_serves_the_next_derivation_without_a_walk() {
        let (dir, store) = store();
        let (_dir, store, repo, tip, digest) = repo_of_two_folders_on(dir, store);
        let prefixes: Vec<RepoPath> = ["left/x", "right/y"]
            .iter()
            .map(|p| RepoPath::new(*p).unwrap())
            .collect();
        let deadline = alive();
        let live = [digest];
        let metrics = crate::metrics::Metrics::default();
        let first = view_root(
            &store, repo, &digest, tip, &prefixes, &live, &metrics, &deadline,
        )
        .unwrap();
        // The second derivation of the same changeset finds the root row:
        // the walk counter does not move.
        let walked_before = metrics.view_trees_walked();
        let again = view_root(
            &store, repo, &digest, tip, &prefixes, &live, &metrics, &deadline,
        )
        .unwrap();
        assert_eq!(again, first, "the same view");
        assert_eq!(metrics.view_trees_walked() - walked_before, 0, "no walk");
        // A new changeset that changes only a hidden path keeps the view:
        // its root tree is new, so the walk visits the root and stops —
        // every folder below comes from the rows.
        let cs2 = store
            .write(|c| {
                let z2 = store.blobs().put(b"z changed\n").unwrap();
                let state = FileState::Regular {
                    content: z2,
                    executable: false,
                };
                c.insert_changeset(
                    repo,
                    &Changeset::new(
                        vec![tip],
                        scion_core::IdentityTime::new(
                            b"Ada".to_vec(),
                            b"ada@example.com".to_vec(),
                            1_700_000_000,
                            0,
                        )
                        .expect("identity fits the format"),
                        None,
                        b"hidden change\n".to_vec(),
                        [(RepoPath::new("secret/z").unwrap(), Some(state))].into(),
                    )
                    .expect("changeset fits the format"),
                )
            })
            .unwrap();
        let before = rows(&store);
        let third = view_root(
            &store, repo, &digest, cs2, &prefixes, &live, &metrics, &deadline,
        )
        .unwrap();
        assert_eq!(third, first, "the same view, from the rows");
        assert_eq!(rows(&store) - before, 1, "only the new root row");
    }

    /// A new changeset re-derives only the folders on its changed path:
    /// the rest comes from the rows, and their view IDs do not move.
    #[test]
    fn a_changed_folder_rederives_only_its_path() {
        let (dir, store) = store();
        let (_dir, store, repo, tip, digest) = repo_of_two_folders_on(dir, store);
        let prefixes: Vec<RepoPath> = ["left/x", "right/y"]
            .iter()
            .map(|p| RepoPath::new(*p).unwrap())
            .collect();
        let deadline = alive();
        let live = [digest];
        let metrics = crate::metrics::Metrics::default();
        let head = view_root(
            &store, repo, &digest, tip, &prefixes, &live, &metrics, &deadline,
        )
        .unwrap();
        // head+1: the file under `left` changes; `right` keeps its tree.
        let head_plus_1 = store
            .write(|c| {
                let blob = store.blobs().put(b"a changed\n").unwrap();
                let state = FileState::Regular {
                    content: blob,
                    executable: false,
                };
                c.insert_changeset(
                    repo,
                    &Changeset::new(
                        vec![tip],
                        scion_core::IdentityTime::new(
                            b"Ada".to_vec(),
                            b"ada@example.com".to_vec(),
                            1_700_000_000,
                            0,
                        )
                        .expect("identity fits the format"),
                        None,
                        b"left/x\n".to_vec(),
                        [(RepoPath::new("left/x").unwrap(), Some(state))].into(),
                    )
                    .expect("changeset fits the format"),
                )
            })
            .unwrap();
        // The path to the change is the root and `left`: two new rows. The
        // unchanged `right` serves from its row, no walk, no new row.
        let before = rows(&store);
        let next = view_root(
            &store,
            repo,
            &digest,
            head_plus_1,
            &prefixes,
            &live,
            &metrics,
            &deadline,
        )
        .unwrap();
        assert_eq!(
            rows(&store) - before,
            2,
            "only the root and `left` re-derive"
        );
        assert_ne!(next, head, "left/x's entry changed");
        // The unchanged `right` view keeps its ID across the changeset.
        let head_right = store
            .read(|c| c.get_tree(head))
            .unwrap()
            .entries()
            .get(&name(b"right"))
            .copied();
        let next_right = store
            .read(|c| c.get_tree(next))
            .unwrap()
            .entries()
            .get(&name(b"right"))
            .copied();
        assert_eq!(
            head_right, next_right,
            "the unchanged folder keeps its view"
        );
    }

    /// The measurement of the per-tree memo (#184 review): derive the view
    /// of a store's `main` head, then of a head+1 that changes one file,
    /// and report the real trees walked and the wall time of each
    /// derivation. Runs against a store that `scion-server import` built:
    ///
    /// ```sh
    /// SCION_TEST_VIEW_BENCH_DATA=/tmp/store \
    ///   cargo test -p open-scion-server --lib views::tests::a_head_plus_one -- \
    ///   --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "needs a store from `scion-server import`: SCION_TEST_VIEW_BENCH_DATA"]
    fn a_head_plus_one_rewalks_what_the_memo_cannot_serve() {
        let dir = std::env::var("SCION_TEST_VIEW_BENCH_DATA").expect("SCION_TEST_VIEW_BENCH_DATA");
        let repo_name =
            std::env::var("SCION_TEST_VIEW_BENCH_REPO").unwrap_or_else(|_| "main".into());
        let store = Backend::open_test(Path::new(&dir)).unwrap();
        let repo = store.read(|c| c.repo_id(&repo_name)).unwrap();
        let tip = store
            .read(|c| c.get_ref(repo, "main"))
            .unwrap()
            .expect("main")
            .target;
        // A file-granular rule set: every `.rs` file under `tokio/src`,
        // up to the access file's cap of 256 prefixes. This is the shape
        // the design named as the memo's ceiling: many narrow prefixes
        // make most directories partially covered, so every derivation
        // re-opens them unless the per-tree memo serves them.
        let prefixes = rs_prefixes(&store, repo, tip);
        println!("rule set: {} prefixes", prefixes.len());
        let digest = crate::access::rule_set_digest(Some(&prefixes));
        let deadline =
            Deadline::new(std::time::Instant::now() + std::time::Duration::from_secs(600));

        // head: the first derivation walks the tree.
        let metrics = crate::metrics::Metrics::default();
        let live = [digest];
        let (root_1, head_walk, head_time) = time_walk(
            &store, repo, digest, tip, &prefixes, &live, &metrics, &deadline,
        );

        // head+1: one file under tokio/src changes.
        let blob = store.blobs().put(b"one changed file\n").unwrap();
        let lib_rs = RepoPath::new("tokio/src/lib.rs").unwrap();
        let state = FileState::Regular {
            content: blob,
            executable: false,
        };
        let head_plus_1 = store
            .write(|c| {
                c.insert_changeset(
                    repo,
                    &Changeset::new(
                        vec![tip],
                        scion_core::IdentityTime::new(
                            b"Ada".to_vec(),
                            b"ada@example.com".to_vec(),
                            1_700_000_000,
                            0,
                        )
                        .expect("identity fits the format"),
                        None,
                        b"one file\n".to_vec(),
                        [(lib_rs, Some(state))].into(),
                    )
                    .expect("changeset fits the format"),
                )
            })
            .unwrap();

        // The same rule set, the next changeset.
        let (_root_2, plus_walk, plus_time) = time_walk(
            &store,
            repo,
            digest,
            head_plus_1,
            &prefixes,
            &live,
            &metrics,
            &deadline,
        );
        let _ = root_1;
        println!("head:   {head_walk} trees walked in {head_time:?}");
        println!("head+1: {plus_walk} trees walked in {plus_time:?}");
    }

    #[allow(clippy::too_many_arguments)]
    fn time_walk(
        store: &Backend,
        repo: RepoId,
        digest: [u8; 32],
        cs: ChangesetId,
        prefixes: &[RepoPath],
        live: &[[u8; 32]],
        metrics: &crate::metrics::Metrics,
        deadline: &Deadline,
    ) -> (TreeId, u64, std::time::Duration) {
        let start = std::time::Instant::now();
        let before = metrics.view_trees_walked();
        let view = view_root(store, repo, &digest, cs, prefixes, live, metrics, deadline).unwrap();
        (view, metrics.view_trees_walked() - before, start.elapsed())
    }

    /// Every `.rs` file path under `tokio/src`, at most
    /// [`crate::access::MAX_PREFIXES_PER_SET`] of them, as one read rule
    /// set.
    fn rs_prefixes(store: &Backend, repo: RepoId, cs: ChangesetId) -> Vec<RepoPath> {
        let root = crate::tree::root_tree_unbounded(store, repo, cs).unwrap();
        let mut out = Vec::new();
        let mut stack = vec![(root, Vec::new())];
        while let Some((id, path)) = stack.pop() {
            let tree = store.read(|c| c.get_tree(id)).unwrap();
            for (name, entry) in tree.entries() {
                let mut path = path.clone();
                path.extend_from_slice(name.as_bytes());
                match entry {
                    Entry::File(_) => {
                        if path.starts_with(b"tokio/src/")
                            && std::str::from_utf8(&path)
                                .is_ok_and(|p| p.to_ascii_lowercase().ends_with(".rs"))
                            && out.len() < crate::access::MAX_PREFIXES_PER_SET
                        {
                            out.push(RepoPath::new(path.clone()).unwrap());
                        }
                    }
                    Entry::Dir(child) => {
                        let mut below = path.clone();
                        below.push(b'/');
                        stack.push((*child, below));
                    }
                }
            }
        }
        out.sort();
        out
    }

    /// A derivation whose map passes the part bound does not write a
    /// part per push: the parts run at the pending bound while the walk
    /// pushes trees and rows, and a later derivation that answers every
    /// subtree from the store's rows writes no part at all, whatever the
    /// map's size.
    #[test]
    fn a_map_past_the_part_bound_does_not_write_a_part_per_push() {
        let (_dir, store) = store();
        // A real repo: the parts' rows name it, and its key must be there.
        let repo = store.write(|c| c.create_repo("parts")).unwrap();
        let digest = [7; 32];
        // Two hundred directories of two files each, one visible one
        // hidden: every directory is a partial view, so the walk derives
        // two hundred view trees, and the map holds every one of them
        // past the four-kilobyte part bound.
        let root_id = store
            .write(|c| {
                let mut entries = std::collections::BTreeMap::new();
                for i in 0..200 {
                    let d = name(format!("d{i:03}").as_bytes());
                    let inner = Tree::new(
                        [
                            (name(b"see"), file(&store, b"body\n")),
                            (name(b"hide"), file(&store, b"secret\n")),
                        ]
                        .into(),
                    )
                    .expect("tree fits the format");
                    let id = c.put_tree(&inner)?;
                    entries.insert(d, Entry::Dir(id));
                }
                let root = Tree::new(entries).expect("tree fits the format");
                c.put_tree(&root)
            })
            .unwrap();
        let ctx: Context = (0..200)
            .map(|i| format!("d{i:03}/see").into_bytes())
            .collect();
        // The first derivation: the map fills past the part bound and
        // stays there, and the parts still run at the pending bound.
        let before = store.commits();
        let first = {
            let _lock = part_guard();
            let _bound = test_set_view_part_bound(4 << 10);
            derive(
                &store,
                repo,
                &digest,
                root_id,
                ctx.clone(),
                &mut Memo::default(),
                &alive(),
            )
            .unwrap()
            .expect("the root keeps a visible path")
        };
        let parts = store.commits() - before;
        assert!(
            (10..=40).contains(&parts),
            "the parts ran at the pending bound, not one per push: {parts}"
        );
        // The second derivation: every subtree's row is in the store, so
        // the pending never grows - no parts, whatever the map's size.
        let before = store.commits();
        let second = {
            let _lock = part_guard();
            let _bound = test_set_view_part_bound(4 << 10);
            derive(
                &store,
                repo,
                &digest,
                root_id,
                ctx,
                &mut Memo::default(),
                &alive(),
            )
            .unwrap()
            .expect("the root keeps a visible path")
        };
        assert_eq!(first, second, "the parts changed the view");
        let parts = store.commits() - before;
        assert_eq!(
            parts, 0,
            "a map past the part bound wrote a part per push: {parts}"
        );
    }

    /// A cold view whose part write waits on the write lane answers even
    /// when the lane stays held past the read transaction's idle bound:
    /// the walk ends its read transaction before the part write, so the
    /// wait holds no read transaction, and the next read continues the
    /// walk where it stopped.
    #[test]
    fn a_cold_view_answers_while_a_writer_holds_the_lane_past_the_idle_bound() {
        let (dir, store) = store();
        // Ten partial directories: at a one-kilobyte part bound the walk
        // flushes a part mid-derivation.
        let files: Vec<(RepoPath, Option<FileState>)> = (0..10)
            .flat_map(|i| {
                [
                    (format!("d{i:02}/see"), b"body\n".as_slice()),
                    (format!("d{i:02}/hide"), b"secret\n".as_slice()),
                ]
            })
            .map(|(p, content)| {
                (
                    RepoPath::new(p.as_str()).unwrap(),
                    Some(FileState::Regular {
                        content: store.blobs().put(content).unwrap(),
                        executable: false,
                    }),
                )
            })
            .collect();
        let ada = scion_core::IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format");
        let tip = store
            .write(|c| {
                let repo = c.create_repo("lane")?;
                c.insert_changeset(
                    repo,
                    &Changeset::new(
                        vec![],
                        ada,
                        None,
                        b"root\n".to_vec(),
                        files.into_iter().collect(),
                    )
                    .expect("changeset fits the format"),
                )
            })
            .unwrap();
        let repo = store.read(|c| c.repo_id("lane")).unwrap();
        let prefixes: Vec<RepoPath> = (0..10)
            .map(|i| RepoPath::new(format!("d{i:02}/see").as_str()).unwrap())
            .collect();
        let digest = crate::access::rule_set_digest(Some(&prefixes));
        let _log = part_guard();
        let part = test_set_view_part_bound(1 << 10);
        // A writer holds the store's write key, past the read
        // transaction's idle bound.
        let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
        let schema = Backend::test_schema(dir.path());
        let holder = std::thread::spawn(move || {
            let mut c = postgres::Client::connect(&url, postgres::NoTls).unwrap();
            c.batch_execute(&format!("SET search_path TO {schema}"))
                .unwrap();
            c.batch_execute("BEGIN").unwrap();
            c.batch_execute("SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':gc'))")
                .unwrap();
            std::thread::sleep(std::time::Duration::from_secs(35));
            c.batch_execute("COMMIT").unwrap();
        });
        // The key is held before the walk starts: the first part write
        // waits the holder out.
        std::thread::sleep(std::time::Duration::from_secs(2));
        let started = std::time::Instant::now();
        let view = view_root(
            &store,
            repo,
            &digest,
            tip,
            &prefixes,
            &[digest],
            &crate::metrics::Metrics::default(),
            &alive(),
        )
        .unwrap();
        assert!(
            started.elapsed() >= std::time::Duration::from_secs(30),
            "the walk never waited on the lane: {:?}",
            started.elapsed()
        );
        // A later read answers from the root's row the derivation wrote.
        let again = view_root(
            &store,
            repo,
            &digest,
            tip,
            &prefixes,
            &[digest],
            &crate::metrics::Metrics::default(),
            &alive(),
        )
        .unwrap();
        assert_eq!(view, again, "the same view");
        drop(part);
        holder.join().unwrap();
    }

    /// A derivation that waits for one of the cap's places waits under
    /// its call's deadline and ends in time when the deadline passes;
    /// with a place free, the same derivation runs whole.
    #[test]
    fn a_derivation_beyond_the_cap_waits_under_its_deadline() {
        let (dir, store) = store();
        let (_dir, store, repo, tip, digest) = repo_of_two_folders_on(dir, store);
        let prefixes: Vec<RepoPath> = ["left/x", "right/y"]
            .iter()
            .map(|p| RepoPath::new(*p).unwrap())
            .collect();
        let live = [digest];
        let metrics = crate::metrics::Metrics::default();
        // Both places go to the test's hands.
        let first = store
            .view_memo_budget()
            .take(VIEW_MEMO_BYTES, None)
            .unwrap();
        let second = store
            .view_memo_budget()
            .take(VIEW_MEMO_BYTES, None)
            .unwrap();
        // A call whose deadline passes while it waits ends in time, with
        // the deadline's error.
        let started = std::time::Instant::now();
        let short = Deadline::new(started + Duration::from_millis(100));
        let answer = view_root(
            &store, repo, &digest, tip, &prefixes, &live, &metrics, &short,
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the waiter ended late: {:?}",
            started.elapsed()
        );
        assert!(
            matches!(answer, Err(crate::error::Error::DeadlineExceeded(_))),
            "the waiter passed its deadline: {answer:?}"
        );
        // A place frees: the same derivation runs whole.
        drop(first);
        let answer = view_root(
            &store,
            repo,
            &digest,
            tip,
            &prefixes,
            &live,
            &metrics,
            &alive(),
        );
        assert!(answer.is_ok(), "the waiter ran once a place freed");
        drop(second);
    }

    /// A part writes its pending trees before its pending rows, so a row
    /// it stores names a tree the store holds; the root's row lands only
    /// in the last part, so a derivation that fails mid-walk leaves no
    /// root row - only valid prefix rows a later read serves.
    #[test]
    fn a_part_writes_its_trees_before_its_rows_and_leaves_no_root_row() {
        let (_dir, store) = store();
        // A real repo: the part's row names it, and its key must be there.
        let repo = store.write(|c| c.create_repo("parts")).unwrap();
        let digest = [9; 32];
        let real =
            Tree::new([(name(b"f"), file(&store, b"x\n"))].into()).expect("tree fits the format");
        let view =
            Tree::new([(name(b"f"), file(&store, b"x\n"))].into()).expect("tree fits the format");
        let (real_id, view_id) = store
            .write(|c| Ok((c.put_tree(&real)?, c.put_tree(&view)?)))
            .unwrap();
        // A memo the walk could have been carrying when it failed: its
        // rows' trees are only in the memo, never yet in the store.
        let deeper =
            Tree::new([(name(b"g"), file(&store, b"y\n"))].into()).expect("tree fits the format");
        let mut memo = Memo::default();
        memo.trees.push(deeper);
        memo.trees.push(view);
        memo.rows
            .push((real_id, context_digest(&Context::new()), view_id));
        store
            .write(|c| write_memo_part(c, repo, &digest, &mut memo))
            .unwrap();
        assert!(
            memo.trees.is_empty() && memo.rows.is_empty(),
            "the part cleared"
        );
        // The part's row is readable, and its view tree is in the store.
        let row = store
            .read(|c| {
                c.get_view_tree(
                    repo,
                    &digest,
                    u32::from(PROJECTION_VERSION),
                    real_id,
                    context_digest(&Context::new()),
                )
            })
            .unwrap();
        assert_eq!(row, Some(view_id), "the part's row is readable");
    }
}
