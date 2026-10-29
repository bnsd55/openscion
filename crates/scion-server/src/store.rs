//! Durable state: metadata in Postgres, a folder of zstd files for blobs.
//!
//! ```text
//! <data>/backend                   names the backend and its store UUID
//! <data>/open.lock                 held while a process opens <data>
//! <data>/objects/blob/ab/cd..      one zstd file per blob, named by ID
//! <data>/tmp/                      staging for atomic blob writes and uploads,
//!                                  one folder per process, held with a lock
//! ```
//!
//! Every directory that a store makes, `<data>` and the ones above it
//! included, gets mode 0700 whatever the umask
//! ([`create_private_dir_all`]): the data is this user's alone, and
//! [`refuse_bad_dir`] refuses a directory the group can write, as a
//! plain mkdir under Ubuntu's umask 002 makes it.
//!
//! The locks are `flock`s on open files. A forked child shares its parent's
//! file descriptions, and with them the locks, until it execs, so a child
//! spawned while a blob store opens can hold its lock for that window.
//! This server never forks, so a server cannot hit the window. A library
//! user that forks while it opens a data directory can: its tests that
//! assert a released lock must keep child spawns out of that window, as
//! `spawn_guard` does in this crate's tests.

// Every pub item of this module is re-exported by the test seam
// (`test_support` in lib.rs), and unreachable_pub does not see
// through a re-export, so this marker would fire on all of them
// under any feature set.
#![allow(unreachable_pub)]
use std::collections::BTreeSet;
use std::fs;
use std::io::{self, ErrorKind, Read, Seek, Write};
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::SystemTime;

use rustix::fs::FlockOperation;
use rustix::io::Errno;
use scion_core::{BlobHasher, BlobId};
use scion_proto::runtime::auth::create_private_dir_all;

use crate::error::{Error, Result};

#[cfg(any(test, feature = "test-hooks"))]
#[path = "test_support/store.rs"]
pub(crate) mod test_hooks;

#[cfg(any(test, feature = "test-hooks"))]
use test_hooks::{DurabilityLog, DurabilityStep, WritePark};

/// Largest object accepted or read back, uncompressed: 100 MiB, the hard
/// per-file limit on GitHub, so any file from a GitHub repository fits.
pub const MAX_OBJECT_BYTES: usize = 100 << 20;

/// Largest zstd window that a [`BlobReader`] accepts, as a power of two:
/// 2 MiB, the most that [`Blobs::put`] writes at its level. A frame that
/// needs a larger window is refused as corrupt.
const READER_WINDOW_LOG: u32 = 21;

/// Most memory that a [`BlobReader`] holds between parts: its zstd window
/// and the decoder's buffers and tables, which take less than 1 MiB.
pub const READER_BYTES: usize = (1 << READER_WINDOW_LOG) + (1 << 20);

/// Read-only connections of one backend's Postgres pool, so at most this
/// many reads run at once. They are more than the graph queries that can
/// run at once, so those never take every one.
pub(crate) const READERS: usize = 8;

/// The cap on derive walks in flight, across all kinds and repos of one
/// backend. Each walk holds a writer's turn, so walks beyond the cap only
/// repeat work and starve writes ([#142]).
pub(crate) const DERIVE_WALKS_CAP: usize = 8;

/// What is stored under a blob ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobStatus {
    /// No file.
    Missing,
    /// The file unpacks to content with this ID.
    Valid,
    /// A file exists but is not this blob: torn, truncated or overwritten.
    Corrupt,
}

/// Opens (or makes) `dir`'s file `name`, for a `flock`. `open.lock`, the
/// one open at a time, and the staging folders' `.lock` files ride on it.
fn lock_file(dir: &Path, name: &str) -> io::Result<fs::File> {
    fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join(name))
}

/// Locks `file` with `op`. Returns false if `op` does not wait and another
/// holder's lock conflicts. Locks on two opens of a file conflict, even in
/// one process.
fn flock(file: &fs::File, op: FlockOperation) -> io::Result<bool> {
    match rustix::fs::flock(file, op) {
        Ok(()) => Ok(true),
        Err(Errno::WOULDBLOCK) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Refuses a data directory that a symlink or a group/others-writable
/// mode would make unsafe, before anything opens inside it. The refusal
/// is by kind, not by the link's mode: on macOS lchmod can give a
/// symlink any mode, so a mode check alone does not catch one, and a
/// link's target can change behind the checks.
pub(crate) fn refuse_bad_dir(dir: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(dir)?;
    if meta.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} is a symlink (the data directory must be a real directory: a symlink's target and mode can change behind the checks)",
                dir.display()
            ),
        )
        .into());
    }
    // Readable is reachable: the daemon refuses a socket directory open
    // to other users, and the store's data directory answers the same
    // rule - group or others get no bits at all.
    {
        use std::os::unix::fs::MetadataExt as _;
        if meta.mode() & 0o077 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "{} can be read or written by group or others (fix: chmod 700 {})",
                    dir.display(),
                    dir.display()
                ),
            )
            .into());
        }
    }
    // The mode check covers the directory itself; the ancestors' check is
    // the swappable parent: a group- or others-writable directory above
    // the data lets another user replace the data directory whole.
    let me = rustix::process::geteuid().as_raw();
    scion_proto::runtime::auth::check_ancestors(dir, me)?;
    Ok(())
}

/// The held fan-out directory flock of a blob: the handle's close
/// releases the lock, so a drop is enough and a crash holds nothing.
pub(crate) struct DirFlock {
    _file: fs::File,
}

/// Immutable file contents, one zstd-compressed file per [`BlobId`].
pub struct Blobs {
    root: PathBuf,
    /// This process's folder in `tmp/`, held with a lock, so an open of
    /// another process can tell that this one lives.
    mine: PathBuf,
    /// The lock file of `mine`, held for as long as this `Blobs` lives.
    /// Never read: its life is the point.
    _mine_lock: fs::File,
    /// One lock per stripe of blob IDs: the write of one blob runs alone
    /// in this process, so two puts of the same bytes never race their
    /// renames onto one path ([#185]).
    write_locks: Vec<Mutex<()>>,
    /// The test park of the write path, armed by the environment.
    #[cfg(any(test, feature = "test-hooks"))]
    park: WritePark,
    group_flush: GroupFlush,
    /// The operator metrics, set once by the server before it serves:
    /// every full flush records how long it took. Empty in import, fsck
    /// and gc, which export none.
    metrics: std::sync::OnceLock<std::sync::Arc<crate::metrics::Metrics>>,
    /// Calls to [`Blobs::verify`] so far.
    #[cfg(any(test, feature = "test-hooks"))]
    verifies: std::sync::atomic::AtomicU64,
    /// The test hook that fails a file write of this backend with the
    /// disk's answer: `n` lets `n` writes through and fails the one
    /// after, once; [`UNARMED`] lets every write through.
    fail_writes: std::sync::atomic::AtomicUsize,
    /// The errno the write hook raises: 28 (ENOSPC) unless set otherwise,
    /// so [`Blobs::fail_writes_after`] keeps its meaning.
    fail_writes_errno: std::sync::atomic::AtomicU8,
    /// The test hook that fails a file read of this backend with the
    /// disk's answer, the same shape as [`Blobs::fail_writes_after`].
    fail_reads: std::sync::atomic::AtomicUsize,
    /// The test hook that fails an fsync of this backend with EIO: the
    /// file fsync before the rename, and the directory fsync after it.
    fail_fsyncs: std::sync::atomic::AtomicUsize,
    /// How many fsyncs passed through [`Blobs::fsync_gate`], pass or
    /// fail: a test reads it to see that a kept file was fsynced.
    fsync_count: std::sync::atomic::AtomicU64,
    /// The durability steps this `Blobs` ran, in order, for the order
    /// test: a test-hooks build records them, a production build stores
    /// nothing.
    #[cfg(any(test, feature = "test-hooks"))]
    log: DurabilityLog,
}

/// Which entry the order test records for one gated fsync. Production
/// code names the step and pays nothing for it; only a test-hooks build
/// records anything, so a release build constructs the payloads and
/// never reads them.
#[cfg_attr(not(any(test, feature = "test-hooks")), allow(dead_code))]
enum SyncStep<'a> {
    /// The temp file of a blob, before its rename.
    TempFile(BlobId),
    /// A stored blob file, on the keep path.
    StoredFile(BlobId),
    /// A blob directory.
    Dir(&'a Path),
    /// An fsync the order test does not name (the blob root of an upload).
    Unnamed,
}

/// How many stripes the per-blob write locks have: 256 on the first byte
/// of the blob's ID. Puts of different blobs wait on each other only when
/// their IDs share a stripe, and one ID is always the same stripe.
const WRITE_STRIPES: usize = 256;

impl Blobs {
    /// This process's staging folder under the data directory's `tmp/`:
    /// a one-shot command removes it when it ends, whatever way it ends,
    /// so its folder is not left for the next open to sweep.
    #[must_use]
    pub fn temp_folder(&self) -> &Path {
        &self.mine
    }

    pub(crate) fn open(dir: &Path) -> Result<Self> {
        // Folders of `tmp/` made in this process, so each `Blobs` gets its own.
        static FOLDERS: AtomicU64 = AtomicU64::new(0);
        // Opens take turns: the open.lock is held for the folder creation
        // and the sweep, so no other process creates a staging folder or
        // sweeps while this one does. A folder whose `.lock` is not yet
        // flocked would look dead to a concurrent sweep; the open lock
        // closes that window.
        let opening = lock_file(dir, "open.lock")?;
        flock(&opening, FlockOperation::LockExclusive)?;
        let objects = dir.join("objects");
        let root = objects.join("blob");
        let tmp = dir.join("tmp");
        create_private_dir_all(&root)?;
        create_private_dir_all(&tmp)?;
        // This process stages its temp files in its own folder of `tmp/`, and
        // holds the folder's lock file for as long as it lives. An open that
        // finds no holder for a folder removes it, so a crashed import leaves
        // nothing once any process opens the directory again (issue #101).
        // Opens hold `open.lock` exclusively, so no other process creates a
        // folder or sweeps while this one does.
        let (mine, mine_lock) = loop {
            let name = format!(
                "{:x}-{:x}",
                process::id(),
                FOLDERS.fetch_add(1, Ordering::Relaxed)
            );
            let mine = tmp.join(name);
            if let Err(e) = fs::DirBuilder::new().mode(0o700).create(&mine) {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    continue;
                }
                return Err(e.into());
            }
            let lock = fs::File::create(mine.join(".lock"))?;
            if !flock(&lock, FlockOperation::NonBlockingLockExclusive)? {
                // Another process holds a folder of the same name. The
                // process id and the counter of this one are new, so that
                // cannot be this process's own folder.
                continue;
            }
            break (mine, lock);
        };
        // A blob file is only as durable as the directories above it. A batch
        // flushes the fan-out directories it writes to; these are made once.
        for path in [&root, &objects, &tmp, dir] {
            fsync_dir(path)?;
        }
        // The sweep runs under the open lock: no other open creates a
        // folder or sweeps while this one does.
        Self::sweep_tmp_dir(&tmp, &mine)?;
        drop(opening);
        Ok(Self {
            // The park files live under the data dir, not under
            // `objects/blob`: the test writes them next to the data root.
            #[cfg(any(test, feature = "test-hooks"))]
            park: WritePark::from_env(dir),
            root,
            mine,
            _mine_lock: mine_lock,
            write_locks: (0..WRITE_STRIPES).map(|_| Mutex::new(())).collect(),
            group_flush: GroupFlush::default(),
            metrics: std::sync::OnceLock::new(),
            #[cfg(any(test, feature = "test-hooks"))]
            verifies: std::sync::atomic::AtomicU64::default(),
            fail_writes: std::sync::atomic::AtomicUsize::new(UNARMED),
            fail_writes_errno: std::sync::atomic::AtomicU8::new(28),
            fail_reads: std::sync::atomic::AtomicUsize::new(UNARMED),
            fail_fsyncs: std::sync::atomic::AtomicUsize::new(UNARMED),
            fsync_count: std::sync::atomic::AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            log: DurabilityLog::default(),
        })
    }

    /// Gives the blob folder the server's metrics registry, once: every
    /// full flush then records how long it took.
    pub(crate) fn set_metrics(&self, metrics: std::sync::Arc<crate::metrics::Metrics>) {
        let _ = self.metrics.set(metrics);
    }

    fn write_lock(&self, id: BlobId) -> MutexGuard<'_, ()> {
        // One stripe per value of the first ID byte: the index needs no
        // modulo, and two writes race only when their IDs share it.
        let stripe = usize::from(id.as_bytes()[0]);
        self.write_locks[stripe]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Holds the fan-out directory of `id` against the file work of
    /// another process: the rename of a put, the touch of a keep, and
    /// the re-stat and unlink of the collector's sweep all run under it,
    /// so a touch or rename that lands first changes the mtime the sweep
    /// re-stats, and an unlink that lands first makes the put's keep see
    /// a missing file and write anew. The metadata transaction orders the
    /// rows; the file work itself needs this lock. Always taken inside
    /// the stripe of the blob: the lock order is stripe, then dir flock,
    /// everywhere. The kernel releases it when the handle drops, so a
    /// crashed process holds nothing.
    pub(crate) fn dir_flock(&self, id: BlobId) -> io::Result<DirFlock> {
        let (dir, _) = self.location(id);
        // A first put reaches here before anything created the fan-out
        // directory; create it, the way the write below would.
        let file = match fs::File::open(&dir) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                create_private_dir_all(&dir)?;
                fs::File::open(&dir)?
            }
            Err(e) => return Err(e),
        };
        flock(&file, FlockOperation::LockExclusive)?;
        Ok(DirFlock { _file: file })
    }

    /// Removes what crashes left in `tmp/`: the folders of processes that
    /// no longer hold them, with the blob writes that were never renamed.
    /// No blob name points at any of them. The folders of live processes
    /// are held, and this process's own folder is held too. Runs under the
    /// open lock, from [`Blobs::open`], so no other open creates a folder
    /// or sweeps while this one does.
    fn sweep_tmp_dir(tmp: &Path, mine: &Path) -> io::Result<()> {
        let mut removed = 0;
        for entry in fs::read_dir(tmp)? {
            let path = entry?.path();
            if path.is_dir() {
                if path == mine {
                    continue;
                }
                // A lock file that cannot be opened was never flocked: its
                // process died before it made one.
                let held = match fs::File::open(path.join(".lock")) {
                    Ok(lock) => !flock(&lock, FlockOperation::NonBlockingLockExclusive)?,
                    Err(_) => false,
                };
                if !held {
                    fs::remove_dir_all(&path)?;
                    removed += 1;
                }
            }
        }
        if removed > 0 {
            tracing::info!(removed, "removed temp folders that a crash left behind");
        }
        Ok(())
    }

    /// Waits for a full flush that starts after this call: the blob root
    /// directory, then the drive cache. That covers everything that reached
    /// the drive before it, the database too, which lives on the same drive.
    pub(crate) fn full_flush(&self) -> io::Result<()> {
        let started = std::time::Instant::now();
        let result = self
            .group_flush
            .run(|| fs::File::open(&self.root)?.sync_all());
        #[cfg(any(test, feature = "test-hooks"))]
        if result.is_ok() {
            self.log.record(DurabilityStep::FullFlush);
        }
        self.record_sync(started);
        result
    }

    /// Stores one file's content durably. Safe to repeat.
    ///
    /// # Errors
    ///
    /// Fails if the content is larger than [`MAX_OBJECT_BYTES`] or the write fails.
    pub fn put(&self, raw: &[u8]) -> Result<BlobId> {
        let mut batch = self.batch();
        let id = batch.put(raw)?;
        batch.sync()?;
        Ok(id)
    }

    /// Starts an upload: content that arrives in parts.
    ///
    /// # Errors
    ///
    /// Fails if the spool file cannot be made.
    pub fn upload(&self) -> Result<Upload> {
        Ok(Upload {
            spool: tempfile::tempfile_in(&self.mine)?,
            hasher: BlobHasher::default(),
            len: 0,
        })
    }

    /// Stores a finished upload. The content streams from the spool file
    /// through the compressor, so it is never all in memory. An intact stored
    /// copy is kept only if `keep_stored`; else the file is written again.
    /// Safe to repeat.
    ///
    /// The blob is durable after the next [`Store::flush`]: each file and
    /// directory that changed is flushed with a plain `fsync`, so only the
    /// full flush of the drive cache is left, which that flush shares.
    ///
    /// # Errors
    ///
    /// Fails if a read or write fails.
    pub fn put_upload(&self, upload: Upload, keep_stored: bool) -> Result<BlobId> {
        let Upload {
            mut spool,
            hasher,
            len,
        } = upload;
        let id = hasher.finalize();
        spool.rewind()?;
        let read_bytes = zstd::zstd_safe::CCtx::in_size();
        let raw = io::BufReader::with_capacity(read_bytes, spool);
        let mut batch = self.batch();
        if keep_stored {
            batch.write(id, len, raw)?;
        } else {
            // The upload may replace an intact stored copy that other repos
            // name. The full flush runs on every call, so the time of the
            // reply does not show whether a copy exists.
            batch.replace_stored(id, len, raw)?;
        }
        if batch.flush_dirs()? {
            // A new fan-out directory is an entry in the root. The full flush
            // of `sync` would cover it.
            self.gated_fsync(
                "the blob root",
                "fsync blob root",
                &SyncStep::Unnamed,
                || fsync_dir(&self.root),
            )?;
        }
        Ok(id)
    }

    /// Starts a batch of writes that become durable together.
    pub fn batch(&self) -> BlobBatch<'_> {
        BlobBatch {
            blobs: self,
            dirs: BTreeSet::new(),
        }
    }

    /// Reads a file's content and checks it against its ID.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if absent, [`Error::Corrupt`] if the file is not
    /// this blob, [`Error::Io`] if it cannot be read.
    pub fn get(&self, id: BlobId) -> Result<Vec<u8>> {
        let file = self.open_file(id)?;
        let corrupt = |why: String| Error::Corrupt(format!("blob {id}: {why}"));
        // The whole file is read before unpacking, so a read failure stays an
        // I/O error and only bad bytes count as corruption.
        let limit = zstd::zstd_safe::compress_bound(MAX_OBJECT_BYTES);
        let mut packed = Vec::new();
        file.take(limit as u64 + 1).read_to_end(&mut packed)?;
        if packed.len() > limit {
            return Err(corrupt(format!("file is larger than {limit} bytes")));
        }
        let raw = zstd::bulk::decompress(&packed, MAX_OBJECT_BYTES)
            .map_err(|e| corrupt(format!("cannot unpack: {e}")))?;
        if BlobId::of(&raw) != id {
            return Err(corrupt("content does not match the ID".into()));
        }
        Ok(raw)
    }

    /// A blob's uncompressed size, from the zstd frame header, without
    /// reading the content. Every blob is written with its size in the header.
    /// An empty file gives 0, because [`Blobs::get`] reads it as empty.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if absent, [`Error::Corrupt`] if the header is bad,
    /// has no size, or gives a size above [`MAX_OBJECT_BYTES`], [`Error::Io`]
    /// if it cannot be read.
    pub fn size(&self, id: BlobId) -> Result<usize> {
        frame_size(id, &mut self.open_file(id)?)
    }

    /// Opens a blob to read in parts with [`BlobReader::read_part`]. Between
    /// parts the reader holds at most [`READER_BYTES`].
    ///
    /// # Errors
    ///
    /// As [`Blobs::size`]. An empty file that is not the empty blob is
    /// [`Error::Corrupt`].
    pub fn reader(&self, id: BlobId) -> Result<BlobReader> {
        let mut file = self.open_file(id)?;
        let size = frame_size(id, &mut file)?;
        let mut reader = BlobReader {
            id,
            left: size,
            unpacked: None,
            hasher: BlobHasher::default(),
        };
        // As in `verify`: an empty file holds no zstd frame.
        if file.metadata()?.len() == 0 {
            reader.end()?;
            return Ok(reader);
        }
        file.rewind()?;
        let watched = Watched {
            inner: file,
            failed: false,
        };
        let mut unpacked = zstd::stream::read::Decoder::new(watched)?;
        unpacked.window_log_max(READER_WINDOW_LOG)?;
        reader.unpacked = Some(unpacked);
        Ok(reader)
    }

    /// The folder of blob files, `objects/blob`.
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Opens a blob's file: one zstd frame, which nothing here unpacks or
    /// checks.
    pub(crate) fn open_file(&self, id: BlobId) -> Result<fs::File> {
        let (_, path) = self.location(id);
        self.read_gate(&path, "read blob file")?;
        fs::File::open(&path).map_err(|e| match e.kind() {
            ErrorKind::NotFound => Error::NotFound(format!("blob {id}")),
            _ => e.into(),
        })
    }

    /// Whether a blob is stored intact. Streams the file through the decoder
    /// into the hasher, so it holds the zstd window and small buffers, not
    /// the whole blob. It gives the same answer as [`Blobs::get`].
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the file exists but cannot be read.
    pub fn verify(&self, id: BlobId) -> Result<BlobStatus> {
        #[cfg(any(test, feature = "test-hooks"))]
        self.verifies
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let file = match self.open_file(id) {
            Ok(file) => file,
            Err(Error::NotFound(_)) => return Ok(BlobStatus::Missing),
            Err(e) => return Err(e),
        };
        let corrupt = |why: String| {
            // No repo on this line: a blob may be shared by repos. The call
            // whose read failed logs the repo and the caller.
            tracing::warn!(blob = %id, %why, "stored blob is corrupt");
            Ok(BlobStatus::Corrupt)
        };
        let len = file.metadata()?.len();
        let limit = zstd::zstd_safe::compress_bound(MAX_OBJECT_BYTES);
        if len > limit as u64 {
            return corrupt(format!("file is larger than {limit} bytes"));
        }
        // `get` unpacks an empty file to empty content, though it holds no
        // zstd frame. The streaming decoder would call it torn.
        if len == 0 && id == BlobId::of(b"") {
            return Ok(BlobStatus::Valid);
        }
        let mut file = Watched {
            inner: file,
            failed: false,
        };
        // One byte over the limit is enough to know the blob is too large.
        let mut unpacked =
            zstd::stream::read::Decoder::new(&mut file)?.take(MAX_OBJECT_BYTES as u64 + 1);
        let hashed = BlobId::of_reader(&mut unpacked);
        let oversized = unpacked.limit() == 0;
        drop(unpacked);
        match hashed {
            // A read error of the file is not bad bytes.
            Err(e) if file.failed => Err(e.into()),
            Err(e) => corrupt(format!("cannot unpack: {e}")),
            Ok(_) if oversized => corrupt(format!("unpacks to more than {MAX_OBJECT_BYTES} bytes")),
            Ok(got) if got != id => corrupt("content does not match the ID".into()),
            Ok(_) => Ok(BlobStatus::Valid),
        }
    }

    /// The gate the fsyncs of this process pass: the same countdown as
    /// [`Blobs::write_gate`], with EIO as the disk's answer. A failed
    /// fsync leaves the data only in the page cache: the caller must not
    /// answer as if it reached the drive.
    fn fsync_gate(&self, name: &str, operation: &str) -> Result<()> {
        self.fsync_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        loop {
            let left = self.fail_fsyncs.load(std::sync::atomic::Ordering::SeqCst);
            let pass = match left {
                UNARMED => true,
                0 => {
                    if self
                        .fail_fsyncs
                        .compare_exchange(
                            0,
                            UNARMED,
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    false
                }
                left => {
                    if self
                        .fail_fsyncs
                        .compare_exchange(
                            left,
                            left - 1,
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    true
                }
            };
            if pass {
                return Ok(());
            }
            let error = std::io::Error::from_raw_os_error(5); // EIO
            tracing::error!(
                file = name,
                operation,
                error = %error,
                "an fsync failed: the drive did not take the data"
            );
            return Err(error.into());
        }
    }

    /// Records how long one fsync of the blob path took.
    fn record_sync(&self, started: std::time::Instant) {
        if let Some(metrics) = self.metrics.get() {
            metrics.blob_sync(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        }
    }

    /// Runs the real fsync of `f` through the gate, records `step` for
    /// the order test, and records how long the fsync took: the gate,
    /// the fsync, the record, and the timer in one place. One observation
    /// of `scion_blob_sync_seconds` per fsync of the blob path (a temp
    /// file, a stored file, a blob directory, or the drive cache).
    fn gated_fsync(
        &self,
        name: &str,
        operation: &str,
        step: &SyncStep<'_>,
        f: impl FnOnce() -> io::Result<()>,
    ) -> Result<()> {
        self.fsync_gate(name, operation)?;
        let started = std::time::Instant::now();
        let out = f();
        if out.is_ok() {
            #[cfg(any(test, feature = "test-hooks"))]
            match step {
                SyncStep::TempFile(id) => self.log.record(DurabilityStep::FileFsync(*id)),
                SyncStep::StoredFile(id) => {
                    self.log.record(DurabilityStep::StoredFileFsync(*id));
                }
                SyncStep::Dir(path) => self
                    .log
                    .record(DurabilityStep::DirFsync(path.to_path_buf())),
                SyncStep::Unnamed => {}
            }
            // A release build names the step and pays nothing for it.
            #[cfg(not(any(test, feature = "test-hooks")))]
            let _ = step;
        }
        self.record_sync(started);
        out.map_err(Error::Io)
    }

    /// The gate the file reads of this process pass: the same countdown
    /// as [`Blobs::write_gate`], with EIO as the disk's answer.
    fn read_gate(&self, path: &Path, operation: &str) -> Result<()> {
        loop {
            let left = self.fail_reads.load(std::sync::atomic::Ordering::SeqCst);
            let pass = match left {
                UNARMED => true,
                0 => {
                    if self
                        .fail_reads
                        .compare_exchange(
                            0,
                            UNARMED,
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    false
                }
                left => {
                    if self
                        .fail_reads
                        .compare_exchange(
                            left,
                            left - 1,
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    true
                }
            };
            if pass {
                return Ok(());
            }
            let error = std::io::Error::from_raw_os_error(5); // EIO
            tracing::error!(
                path = %path.display(),
                operation,
                error = %error,
                "a blob read failed: the disk refused it"
            );
            return Err(error.into());
        }
    }

    /// The gate the file writes of this process pass: it lets the next
    /// `n` writes through, fails the one after with the disk's answer,
    /// and lets the rest through. One error line per failure, with the
    /// file and the operation.
    fn write_gate(&self, path: &Path, operation: &str) -> Result<()> {
        loop {
            let left = self.fail_writes.load(std::sync::atomic::Ordering::SeqCst);
            let pass = match left {
                // The hook is not armed.
                UNARMED => true,
                // This is the write the hook fails; the hook is spent.
                0 => {
                    if self
                        .fail_writes
                        .compare_exchange(
                            0,
                            UNARMED,
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    false
                }
                // A write the hook lets through: count it down, without
                // the two-writers race where a plain subtract wraps past
                // zero and re-arms the hook silently.
                left => {
                    if self
                        .fail_writes
                        .compare_exchange(
                            left,
                            left - 1,
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    true
                }
            };
            if pass {
                return Ok(());
            }
            // 28 is ENOSPC; the hook may name another disk answer instead
            // (5 is EIO, 30 is EROFS: the same on Linux and macOS).
            let errno = self
                .fail_writes_errno
                .load(std::sync::atomic::Ordering::SeqCst);
            let error = std::io::Error::from_raw_os_error(i32::from(errno));
            tracing::error!(
                path = %path.display(),
                operation,
                error = %error,
                "a blob write failed: the disk refused it"
            );
            return Err(error.into());
        }
    }

    /// The fan-out directory and the file path of a blob.
    pub fn location(&self, id: BlobId) -> (PathBuf, PathBuf) {
        let hex = id.to_string();
        let dir = self.root.join(&hex[..2]);
        let path = dir.join(&hex[2..]);
        (dir, path)
    }

    /// Whether the blob's file is on disk. A cheap stat, no read: the
    /// importer's skip checks it, so a blob whose row survives but whose
    /// file was deleted under the server still refuses the import.
    pub(crate) fn file_exists(&self, id: BlobId) -> bool {
        self.location(id).1.exists()
    }

    #[cfg(test)]
    pub(crate) fn full_flushes(&self) -> u64 {
        self.group_flush.lock().finished
    }
}

/// The content size in a blob file's zstd frame header. An empty file holds
/// empty content, as in [`Blobs::get`].
fn frame_size(id: BlobId, file: &mut fs::File) -> Result<usize> {
    /// Longest zstd frame header (`ZSTD_FRAMEHEADERSIZE_MAX`).
    const FRAME_HEADER_MAX: u64 = 18;
    let mut header = Vec::new();
    file.take(FRAME_HEADER_MAX).read_to_end(&mut header)?;
    if header.is_empty() {
        return Ok(0);
    }
    let corrupt = |why: &str| Error::Corrupt(format!("blob {id}: {why}"));
    match zstd::zstd_safe::get_frame_content_size(&header) {
        Ok(Some(size)) => usize::try_from(size)
            .ok()
            .filter(|&size| size <= MAX_OBJECT_BYTES)
            .ok_or_else(|| corrupt("its header gives a size above the object limit")),
        Ok(None) => Err(corrupt("its header has no content size")),
        Err(_) => Err(corrupt("bad zstd frame header")),
    }
}

/// The streaming zstd decoder over a blob file.
type Unpacked = zstd::stream::read::Decoder<'static, io::BufReader<Watched<fs::File>>>;

/// A blob's content, read in parts and checked against its ID. Made by
/// [`Blobs::reader`]. It holds the zstd window, not the whole blob.
pub struct BlobReader {
    id: BlobId,
    /// Bytes of the content not read yet, from the frame header.
    left: usize,
    /// `None` once the content has ended, or for an empty file.
    unpacked: Option<Unpacked>,
    hasher: BlobHasher,
}

impl BlobReader {
    /// The next part of the content, of at most `max` bytes (`max > 0`), or
    /// `None` after the end. The part that reaches the end comes only after
    /// the whole content matched the ID, so a caller never gets all of a blob
    /// that is not intact. At the end the reader frees its decoder.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if the content does not unpack, is not the size in
    /// its header, or does not match the ID. [`Error::Io`] if the file cannot
    /// be read.
    pub fn read_part(&mut self, max: usize) -> Result<Option<Vec<u8>>> {
        let Some(unpacked) = self.unpacked.as_mut() else {
            return Ok(None);
        };
        let mut part = vec![0; self.left.min(max)];
        if let Err(e) = unpacked.read_exact(&mut part) {
            return Err(unpack_error(self.id, unpacked, e));
        }
        self.hasher.update(&part);
        self.left -= part.len();
        if self.left == 0 {
            self.end()?;
        }
        Ok((!part.is_empty()).then_some(part))
    }

    /// Whether the whole content has been read and checked.
    #[must_use]
    pub fn ended(&self) -> bool {
        self.unpacked.is_none()
    }

    /// Checks that nothing follows the content and that the content matches
    /// the ID, then frees the decoder.
    fn end(&mut self) -> Result<()> {
        let id = self.id;
        let corrupt = |why: &str| Error::Corrupt(format!("blob {id}: {why}"));
        if let Some(mut unpacked) = self.unpacked.take() {
            match unpacked.read(&mut [0]) {
                Ok(0) => {}
                Ok(_) => return Err(corrupt("it unpacks to more than its header says")),
                Err(e) => return Err(unpack_error(id, &unpacked, e)),
            }
        }
        if self.hasher.finalize() != id {
            return Err(corrupt("content does not match the ID"));
        }
        Ok(())
    }
}

/// A read error of the file is I/O; anything else is bad bytes.
fn unpack_error(id: BlobId, unpacked: &Unpacked, e: io::Error) -> Error {
    if unpacked.get_ref().get_ref().failed {
        e.into()
    } else {
        Error::Corrupt(format!("blob {id}: cannot unpack: {e}"))
    }
}

/// A reader that notes whether it failed, so [`Blobs::verify`] can tell a
/// read error of the file from bad bytes in it.
struct Watched<R> {
    inner: R,
    failed: bool,
}

impl<R: Read> Read for Watched<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf);
        // An interrupted read is retried, so it is not a failure.
        self.failed |= matches!(&read, Err(e) if e.kind() != ErrorKind::Interrupted);
        read
    }
}

/// A blob that arrives in parts. Each part goes to a spool file in `tmp/` and
/// into the hasher, so an upload holds no more than one part in memory. The
/// spool file holds raw bytes: the zstd frame header needs the size before
/// the first compressed byte, and the size is known only at the end. The
/// spool file has no name, so a dropped upload, or a crash, leaves nothing
/// behind. Store it with [`Blobs::put_upload`].
pub struct Upload {
    /// The raw content so far.
    spool: fs::File,
    hasher: BlobHasher,
    len: usize,
}

impl Upload {
    /// Adds the next part of the content. On an error the upload is dropped.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] if the content gets larger than
    /// [`MAX_OBJECT_BYTES`], [`Error::Io`] if the write fails.
    pub fn append(mut self, part: &[u8]) -> Result<Self> {
        if part.len() > MAX_OBJECT_BYTES - self.len {
            return Err(too_large());
        }
        self.spool.write_all(part)?;
        self.hasher.update(part);
        self.len += part.len();
        Ok(self)
    }

    /// The ID of the content so far.
    #[must_use]
    pub fn id(&self) -> BlobId {
        self.hasher.finalize()
    }
}

fn too_large() -> Error {
    Error::InvalidArgument(format!("blob is larger than {MAX_OBJECT_BYTES} bytes"))
}

/// The bytes free for this user on the filesystem that holds `dir`, as
/// the operator's free-space gauge reads them. `f_bavail`, not `f_bfree`:
/// the blocks a non-root process can really get.
///
/// # Errors
///
/// The `statvfs` of `dir` failed (a directory that is gone).
pub fn free_bytes(dir: &Path) -> std::io::Result<u64> {
    let stats = rustix::fs::statvfs(dir)?;
    Ok(stats.f_bavail.saturating_mul(stats.f_frsize))
}

/// Blobs written but not yet durable. Call [`BlobBatch::sync`] before
/// committing any metadata that names them.
///
/// Each blob is flushed to the drive when written (a plain `fsync`, cheap);
/// `sync` then flushes the directories that changed and waits for one full
/// flush of the drive cache for the whole batch. On macOS a full flush
/// (`F_FULLFSYNC`) costs about 3 ms, so one per batch, not one per blob, is
/// what makes a large import fast. Batches that sync at the same time share
/// one full flush, so many small uploads are fast too.
#[must_use = "blobs in a batch are not durable until `sync`"]
pub struct BlobBatch<'a> {
    blobs: &'a Blobs,
    dirs: BTreeSet<PathBuf>,
}

impl BlobBatch<'_> {
    /// Writes one blob, or keeps the stored file if it is intact. A file that
    /// is not intact is replaced. Safe to repeat.
    ///
    /// # Errors
    ///
    /// Fails if the content is larger than [`MAX_OBJECT_BYTES`] or a read or
    /// write fails.
    pub fn put(&mut self, raw: &[u8]) -> Result<BlobId> {
        if raw.len() > MAX_OBJECT_BYTES {
            return Err(too_large());
        }
        let id = BlobId::of(raw);
        self.write(id, raw.len(), raw)?;
        Ok(id)
    }

    /// Writes blob `id`, whose `len` bytes `raw` gives, unless the stored file
    /// is intact. A file that is not intact is replaced.
    fn write(&mut self, id: BlobId, len: usize, raw: impl Read) -> Result<()> {
        // One blob, one writer: the lock spans the check of the stored
        // file, the touch of a kept one, and the rename that replaces it
        // (see `Blobs::write_lock`). The dir flock under it orders this
        // file work against the collector's sweep in another process.
        let _write = self.blobs.write_lock(id);
        let _dir = self.blobs.dir_flock(id)?;
        match self.keep(id)? {
            BlobStatus::Valid => {
                // A put that keeps the stored copy writes nothing, so the
                // file's mtime would stay at its first upload. Touch it:
                // the grace period of `gc` counts from the last upload,
                // not the first, so a client that uploaded again does not
                // lose the blob before it names it.
                self.touch(id)?;
                // The stored copy may not be durable: another writer's
                // batch may not have synced yet, and a failed directory
                // fsync somewhere else may have left it only in the page
                // cache. This caller makes it durable instead of trusting
                // the file's presence: fsync the file and the directory,
                // every time, so a retry after a failed fsync ends with
                // the blob on the drive and no name is ever removed.
                let (dir, path) = self.blobs.location(id);
                let file = fs::File::open(&path)?;
                self.blobs.gated_fsync(
                    "the stored blob file",
                    "fsync stored blob",
                    &SyncStep::StoredFile(id),
                    || fsync(&file),
                )?;
                let name = dir.display().to_string();
                self.blobs.gated_fsync(
                    &name,
                    "fsync blob directory",
                    &SyncStep::Dir(dir.as_path()),
                    || fsync_dir(&dir),
                )?;
                return Ok(());
            }
            // The rename below replaces it in one step. No repo on this
            // line: a blob may be shared by repos.
            BlobStatus::Corrupt => tracing::warn!(blob = %id, "rewriting a corrupt blob file"),
            BlobStatus::Missing => {}
        }
        // The test seam of [#268]: the writer has passed keep and holds
        // its stripe, and parks here while the park file exists, so the
        // rename lands after another process's rename of the same path.
        #[cfg(any(test, feature = "test-hooks"))]
        self.blobs.park.park(&id);
        self.replace(id, len, raw)
    }

    /// Refreshes the mtime of a stored blob file, as one `utimensat`.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the file cannot be opened or its time not set.
    fn touch(&self, id: BlobId) -> Result<()> {
        let (_, path) = self.blobs.location(id);
        self.blobs.write_gate(&path, "touch blob file")?;
        let file = fs::File::options().write(true).open(&path)?;
        file.set_modified(SystemTime::now())?;
        Ok(())
    }

    /// Writes blob `id`, whose `len` bytes `raw` gives, whether or not the
    /// stored file is intact. Then the time it takes does not show whether
    /// the blob was stored.
    fn replace(&mut self, id: BlobId, len: usize, raw: impl Read) -> Result<()> {
        let (dir, path) = self.blobs.location(id);
        // Write a temp file and flush it before the rename, so the name never
        // points at data that is not on the drive. The frame header holds the
        // size, for `Blobs::size`. Compression fails if `raw` does not give
        // exactly `len` bytes.
        self.blobs.write_gate(&path, "write blob file")?;
        let mut tmp = tempfile::NamedTempFile::new_in(&self.blobs.mine)?;
        zstd::stream::compress_sized_into(raw, &mut tmp, 3, len as u64)?;
        #[cfg(any(test, feature = "test-hooks"))]
        self.blobs.log.record(DurabilityStep::TmpWrite(id));
        self.blobs.gated_fsync(
            "the blob's temp file",
            "fsync blob file",
            &SyncStep::TempFile(id),
            || fsync(tmp.as_file()),
        )?;
        create_private_dir_all(&dir)?;
        #[cfg(any(test, feature = "test-hooks"))]
        test_hooks::persist_logged(&self.blobs.log, id, tmp, &path)?;
        #[cfg(not(any(test, feature = "test-hooks")))]
        tmp.persist(&path).map_err(|e| {
            tracing::warn!(blob = %id, path = %path.display(), error = %e.error,
                "the rename of the blob's temp file failed");
            e.error
        })?;
        self.dirs.insert(dir);
        Ok(())
    }

    /// Writes blob `id`, whose `len` bytes `raw` gives, over whatever is
    /// stored: a full flush runs on the temp file before the rename, every
    /// time, so the time it takes does not show whether a copy exists. For a
    /// put that may replace an intact stored copy that other repos name: the
    /// temp file is durable first, so a power loss never leaves the stored
    /// blob with data that the drive lost. A put of a blob that is missing or
    /// corrupt replaces no intact file, and its batch `sync` covers the
    /// rename.
    fn replace_stored(&mut self, id: BlobId, len: usize, raw: impl Read) -> Result<()> {
        // The rename below lands on the same path as any other write of
        // this blob, so it waits for the same stripe, and under it the
        // dir flock orders the rename against the collector's sweep.
        let _write = self.blobs.write_lock(id);
        let _dir = self.blobs.dir_flock(id)?;
        self.blobs.full_flush()?;
        // The same test seam as `write`: a put of a blob the repo does
        // not have yet takes this path, and the race test parks it here.
        #[cfg(any(test, feature = "test-hooks"))]
        self.blobs.park.park(&id);
        self.replace(id, len, raw)
    }

    /// Checks a stored blob and, if it is intact, keeps it: `sync` makes it
    /// durable too. Use it before committing metadata that names a blob this
    /// batch did not write.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the file exists but cannot be read.
    pub fn keep(&mut self, id: BlobId) -> Result<BlobStatus> {
        let status = self.blobs.verify(id)?;
        if status == BlobStatus::Valid {
            // Another batch may have written it and not synced yet, so this
            // batch flushes its directory too.
            // SHORTCUT: so keeping a stored blob costs a full flush, shared
            // with concurrent batches. Upgrade: track the blobs written but
            // not yet synced, and flush every blob directory and the drive at
            // open; a stored blob outside that set is then durable (#35).
            self.dirs.insert(self.blobs.location(id).0);
        }
        Ok(status)
    }

    /// Makes every blob written or kept in this batch durable.
    ///
    /// # Errors
    ///
    /// Fails if a flush fails.
    pub fn sync(self) -> Result<()> {
        let blobs = self.blobs;
        if self.flush_dirs()? {
            blobs.full_flush()?;
        }
        #[cfg(any(test, feature = "test-hooks"))]
        blobs.log.record(DurabilityStep::Answer);
        Ok(())
    }

    /// Like [`BlobBatch::sync`], but leaves the full flush of the drive cache
    /// to the next [`Store::flush`]. Returns whether the batch wrote or kept
    /// any blob.
    // SHORTCUT: the power-loss suite's cut drops unsynced file data, but
    // LazyFS persists a rename at rename time, so the directory fsync here
    // is never exercised by it; the block-level replay that covers it runs
    // in CI (tests/dm_log_writes.rs). The next upgrade: the same replay
    // over a filesystem whose directory fsync does not force a journal
    // commit, when a bug report says a directory entry was lost on one.
    fn flush_dirs(self) -> Result<bool> {
        for dir in &self.dirs {
            let name = dir.display().to_string();
            self.blobs.gated_fsync(
                &name,
                "fsync blob directory",
                &SyncStep::Dir(dir.as_path()),
                || fsync_dir(dir),
            )?;
        }
        Ok(!self.dirs.is_empty())
    }
}

/// The test hook is not armed: every file write goes.
const UNARMED: usize = usize::MAX;

/// Group commit: concurrent callers share one full flush of the drive cache.
///
/// A full flush is slow, and one covers every write that reached the drive
/// before it started. So a caller first makes its writes reach the drive (a
/// plain `fsync`), then waits for a flush that starts after that; one waiting
/// caller runs it for all the others.
///
/// ```text
/// A: fsync │ runs flush 1 ────────┤ Ok
/// B:      fsync │ waits ──────────┤ runs flush 2 ────────┤ Ok
/// C:          fsync │ waits ──────┴──────────────────────┤ Ok
/// ```
///
/// B and C arrive while flush 1 runs. It may have started before their writes,
/// so they need flush 2, and B runs it for both.
#[derive(Default)]
struct GroupFlush {
    state: Mutex<FlushState>,
    finished: Condvar,
}

/// Flushes are numbered from 1. One runs while `started > finished`.
#[derive(Default)]
struct FlushState {
    started: u64,
    finished: u64,
    /// The latest flush that failed, and its error.
    failed: Option<(u64, Arc<io::Error>)>,
    /// Callers of [`GroupFlush::run`] in the wait, for tests.
    #[cfg(test)]
    waiting: usize,
}

impl GroupFlush {
    /// Waits for a run of `flush` that starts after this call, and returns its
    /// result. Only one caller's `flush` runs (as in `OnceLock::get_or_init`),
    /// so every caller must pass the same operation, and it must not panic or
    /// later callers wait forever. A caller may also see the error of a later
    /// failed run, which errs on the safe side.
    fn run(&self, flush: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let mut state = self.lock();
        // A running flush may have started before this caller's writes.
        let target = state.started + 1;
        #[cfg(test)]
        {
            state.waiting += 1;
        }
        state = self
            .finished
            .wait_while(state, |s| s.started > s.finished && s.finished < target)
            .unwrap_or_else(PoisonError::into_inner);
        if state.finished < target {
            // No flush is running and `target` is the next one: lead it.
            state.started = target;
            drop(state);
            let result = flush();
            state = self.lock();
            state.finished = target;
            if let Err(e) = result {
                state.failed = Some((target, Arc::new(e)));
            }
            self.finished.notify_all();
        }
        match &state.failed {
            Some((failed, e)) if *failed >= target => Err(io::Error::new(e.kind(), Arc::clone(e))),
            _ => Ok(()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, FlushState> {
        // Nothing panics while the lock is held.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Whether [`fsync`] can leave data in the drive cache: true on Apple
/// platforms. Elsewhere it flushes the cache too.
pub(crate) const FSYNC_LEAVES_DRIVE_CACHE: bool = cfg!(target_vendor = "apple");

/// A plain `fsync`: data reaches the drive, not necessarily past its cache.
/// (`File::sync_all` is a full cache flush on macOS, about 20x slower.)
fn fsync(file: &fs::File) -> io::Result<()> {
    rustix::fs::fsync(file).map_err(Into::into)
}

pub(crate) fn fsync_dir(path: &Path) -> io::Result<()> {
    fsync(&fs::File::open(path)?)
}

/// Keeps child processes out of the window where a test drops a store and
/// asserts its lock released. A forked child shares the flock of every open
/// file description until it execs, so a child spawned while another test
/// drops a store can hold the dropped store's lock past that assertion.
/// Every test that spawns a child takes the read side across the spawn, and
/// a test that asserts a lock is released takes the write side across its
/// drop and reopen: a pending write side also stops new read sides, so the
/// spawners finish before the release window opens.
#[cfg(test)]
pub(crate) mod spawn_guard {
    use super::{flock, lock_file};
    use rustix::fs::FlockOperation;
    use rustix::io::Errno;
    use std::path::Path;
    use std::sync::RwLock;
    use std::time::{Duration, Instant};

    static SPAWNS: RwLock<()> = RwLock::new(());

    /// Holds the read side while `f` spawns its child: any number of tests
    /// may spawn at once.
    pub fn across_spawn<T>(f: impl FnOnce() -> T) -> T {
        let _held = SPAWNS.read().expect("the spawn guard is not poisoned");
        f()
    }

    /// Waits until the open lock of `dir` is free. Call it on the
    /// write side (inside [`Self::across_release`]), after dropping the
    /// stores and before the reopen that must see the lock released.
    ///
    /// The poll takes the real condition — the lock is free — instead of
    /// a timing guess: it opens `<dir>/open.lock` and takes a
    /// non-blocking exclusive flock every 1 ms, up to 2 s, then fails
    /// with the holder message, so a real leak still fails. The delay it
    /// absorbs is the operating system's: macOS tears a reaped child's
    /// inherited `flock` down one tick after `waitpid`, and about 7% of
    /// immediate reopens after a Rust spawn and its wait fail with
    /// `EWOULDBLOCK` there, sub-ms; Linux shows 0 of 2,000 rounds (a
    /// standalone probe). The server never spawns a child, so an open
    /// itself needs none of this.
    pub fn wait_for_lock_release(dir: &Path) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let file = lock_file(dir, "open.lock")
                .unwrap_or_else(|e| panic!("cannot open the lock of {dir:?}: {e}"));
            let free = flock(&file, FlockOperation::NonBlockingLockExclusive).unwrap_or(false);
            drop(file);
            if free {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the server lock of {} was never released within 2 s",
                dir.display()
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Primes gix's one-time `git` installation lookup under the read
    /// side. The first `gix::open` in a process forks `git` outside our
    /// control (gix-path), and caches the result afterwards. Every test
    /// that reaches `gix` calls this first, so that fork waits out any
    /// release window like our own spawns do.
    pub fn prime_gix_lookup() {
        across_spawn(|| {
            let _ = gix::path::env::installation_config();
        });
    }

    /// Holds the write side while `f` drops a store and checks its lock is
    /// released: no child is in its fork-to-exec window, and no new spawn
    /// starts, until `f` is done.
    pub fn across_release<T>(f: impl FnOnce() -> T) -> T {
        let _held = SPAWNS.write().expect("the spawn guard is not poisoned");
        f()
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::old_store;
    use crate::meta::MetaTx as _;
    use std::cell::{Cell, RefCell};
    use std::panic;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Barrier, mpsc};
    use std::thread;

    use super::*;

    /// A symlinked data directory is refused by kind, not by the link's
    /// mode: on macOS lchmod can give a symlink any mode, so a mode check
    /// alone does not catch one, and a link's target can change behind
    /// the checks.
    #[test]
    fn a_symlinked_data_directory_is_refused() {
        let real = crate::test_support::private_tempdir();
        let holder = crate::test_support::private_tempdir();
        let link = holder.path().join("data");
        std::os::unix::fs::symlink(real.path(), &link).unwrap();
        let err = match Backend::open(&link, NO_SERVER_URL, None) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("a symlinked data directory is accepted"),
        };
        assert!(err.contains("is a symlink"), "{err}");
    }

    /// A write of one blob waits for the stripe of a write that is held:
    /// while the stripe is held by hand, the blob's file stays absent,
    /// and a write released, the file is there ([#185]). Without the
    /// striped lock the write renames at once, and the absence assert
    /// fails. The replacing upload takes the same lock, so the second
    /// test repeats the shape on the `replace_stored` path, which the
    /// gRPC race of an upload with a copy stored reaches.
    #[test]
    fn a_write_of_one_blob_waits_for_the_stripe() {
        held_stripe_waits(|store, id, content| {
            let mut batch = store.blobs().batch();
            batch.put(content).unwrap();
            let _ = id;
        });
    }

    #[test]
    fn a_write_of_one_blob_waits_for_the_stripe() {
        held_stripe_waits(|blobs, id, content| {
            let mut batch = blobs.batch();
            batch.put(content).unwrap();
            let _ = id;
        });
    }

    #[test]
    fn a_replacing_upload_of_one_blob_waits_for_the_stripe() {
        held_stripe_waits(|blobs, id, content| {
            let mut batch = blobs.batch();
            batch.replace_stored(id, content.len(), content).unwrap();
            batch.flush_dirs().unwrap();
        });
    }

    /// Holds the stripe of one blob by hand, runs `write` on a thread,
    /// and says the blob stays absent while the stripe is held and is
    /// whole once the stripe is free. The lease drops the stripe
    /// however the test ends, so a panicking assert cannot wedge the
    /// writer ([#142]).
    fn held_stripe_waits(write: impl Fn(&Blobs, BlobId, &'static [u8]) + Send + Sync) {
        let dir = crate::test_support::private_tempdir();
        let blobs = Blobs::open(dir.path()).unwrap();
        let content = b"the same bytes for both writes";
        let id = BlobId::of(content);
        // The stripe is held by hand, and drops however the test ends: a
        // panicking assert frees the writer at the scope's join ([#142]).
        let stripe = Mutex::new(blobs.write_lock(id));
        thread::scope(|scope| {
            let writer = scope.spawn(|| write(&blobs, id, content));
            // A ceiling only bounds the wait of the good code: the write
            // of the broken code renames at once, and the absence assert
            // fails.
            let until = std::time::Instant::now() + Duration::from_millis(200);
            let mut landed = false;
            while std::time::Instant::now() < until {
                if blobs.verify(id).unwrap() == BlobStatus::Valid {
                    landed = true;
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
            assert!(
                !landed,
                "the write renamed while another write held the stripe"
            );
            drop(stripe);
            writer.join().unwrap();
        });
        assert_eq!(
            blobs.verify(id).unwrap(),
            BlobStatus::Valid,
            "the blob of the held stripe"
        );
    }

    #[test]
    fn an_open_refuses_a_data_directory_that_group_or_others_can_write() {
        use std::os::unix::fs::PermissionsExt as _;
        let writable = |path: &Path| {
            let mut mode = fs::metadata(path).unwrap().permissions();
            mode.set_mode(0o777);
            fs::set_permissions(path, mode).unwrap();
        };
        let dir = crate::test_support::private_tempdir();
        let path = dir.path().join("data");
        fs::create_dir(&path).unwrap();
        writable(&path);
        // The directory checks run before the database is contacted, so
        // a URL that names no live database never matters here.
        let Err(err) = Backend::open(&path, NO_URL, None) else {
            panic!("the open of a writable data directory must fail");
        };
        assert!(
            err.to_string()
                .contains("can be read or written by group or others (fix: chmod 700"),
            "{err}"
        );
        // The same through a parent that another user can change: a
        // group-writable directory without the sticky bit above the data.
        let parent = dir.path().join("shared");
        fs::create_dir(&parent).unwrap();
        writable(&parent);
        let path = parent.join("data");
        fs::create_dir(&path).unwrap();
        // The data directory itself is private here: the check under
        // test is the swappable parent, not the directory's own mode.
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        let Err(err) = Backend::open(&path, NO_URL, None) else {
            panic!("the open under a swappable parent must fail");
        };
        assert!(
            err.to_string().contains("could swap")
                || err.to_string().contains("not private")
                || err.to_string().contains("owned by another user"),
            "{err}"
        );
    }

    #[test]
    fn a_blob_size_comes_from_its_frame_header() {
        let dir = crate::test_support::private_tempdir();
        let blobs = Blobs::open(dir.path()).unwrap();
        for raw in [b"".to_vec(), b"hello".to_vec(), vec![7; 100_000]] {
            let id = blobs.put(&raw).unwrap();
            assert_eq!(blobs.size(id).unwrap(), raw.len());
        }

        // A frame with no size in its header is refused, not guessed.
        let id = BlobId::of(b"streamed");
        let (fan_out, file) = blobs.location(id);
        fs::create_dir_all(fan_out).unwrap();
        fs::write(file, zstd::stream::encode_all(&b"streamed"[..], 3).unwrap()).unwrap();
        assert!(matches!(blobs.size(id), Err(Error::Corrupt(_))));
        let absent = BlobId::of(b"absent");
        assert!(matches!(blobs.size(absent), Err(Error::NotFound(_))));
    }

    /// A forked child shares the flock of every open file description
    /// until it execs, so a `git` spawn beside a dropped store can keep
    /// the dropped store's lock for the fork-to-exec window. Without
    /// [`spawn_guard`] this test fails within a few hundred cycles; with
    /// it, the spawns wait out every release window.
    ///
    /// The read side covers a spawn until the child has exited
    /// (`status`), so no child of this binary holds any descriptor once
    /// the read side is released, whatever the platform does at `exec`.
    /// Spawns outside the guard, like gix's one-time `git` lookup, are
    /// primed under the read side before any test opens a store (see
    /// [`spawn_guard::prime_gix_lookup`]).
    #[test]
    fn a_dropped_store_lock_is_shared_by_a_forked_child_until_it_execs() {
        use std::process::Command;
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let spawner = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    super::spawn_guard::across_spawn(|| {
                        // Any child shares the lock between its fork and
                        // its exec; `/bin/true` keeps this test fast, and
                        // the unguarded repro failed with `git` too, with
                        // and without `current_dir`.
                        let _ = Command::new("/bin/true").status();
                    });
                }
            })
        };
        for _ in 0..100 {
            let dir = crate::test_support::private_tempdir();
            let first = Store::open(dir.path()).unwrap();
            let clone = first.clone();
            super::spawn_guard::across_release(|| {
                drop(first);
                drop(clone);
                // The window between the drops and this open is where a
                // forked child still holds the lock, one tick past the
                // reaper on macOS. The poll waits for the real
                // condition: the lock is free.
                super::spawn_guard::wait_for_lock_release(dir.path());
                Store::open(dir.path()).unwrap();
            });
        }
        stop.store(true, Ordering::Relaxed);
        spawner.join().unwrap();
    }

    #[test]
    fn a_data_directory_opens_in_one_server_store_at_a_time() {
        // The last part of this test drops every store and asserts the
        // lock released, so no child may spawn beside it.
        super::spawn_guard::across_release(|| {
            let dir = crate::test_support::private_tempdir();
            let first = Store::open(dir.path()).unwrap();
            let clone = first.clone();
            assert_in_use(Store::open(dir.path()), dir.path(), "another server");
            // Shared stores open beside it, and a server beside them.
            let shared = [0, 1].map(|_| Store::open_shared(dir.path()).unwrap());
            drop(first);
            assert!(
                Store::open(dir.path()).is_err(),
                "a live clone keeps the lock"
            );
            drop(clone);
            super::spawn_guard::wait_for_lock_release(dir.path());
            Store::open(dir.path()).unwrap();
            drop(shared);
        });
    }

    #[test]
    fn an_open_removes_temp_files_before_another_open_ends() {
        // The drops and reopens of this test must see a lock released, so
        // no child may be in its fork-to-exec window (see `spawn_guard`).
        super::spawn_guard::across_release(|| {
            // What a crashed import left: its staging folder with many temp
            // files, so removing it takes a while.
            const LEFT: usize = 5_000;
            let dir = crate::test_support::private_tempdir();
            let tmp = dir.path().join("tmp");
            let dead = tmp.join("dead");
            fs::create_dir_all(&dead).unwrap();
            fs::write(dead.join(".lock"), b"").unwrap();
            for i in 0..LEFT {
                fs::write(dead.join(format!("left{i}")), b"").unwrap();
            }
            // The sweep removes the dead folder's files in the order that the
            // folder lists them.
            let listed_first = fs::read_dir(&dead).unwrap().next().unwrap().unwrap().path();
            let path = dir.path().to_owned();
            let first = thread::spawn(move || Store::open(&path));
            while listed_first.exists() && !first.is_finished() {
                thread::yield_now();
            }
            // The first open is removing the dead folder. A second open waits
            // for it, so the folder that it writes next is safe from that sweep:
            // the dead folder is gone, and the two live ones stay.
            let second = Store::open_shared(dir.path()).unwrap();
            assert_eq!(fs::read_dir(&tmp).unwrap().count(), 2);
            drop((first.join().unwrap().unwrap(), second));
        });
    }

    #[test]
    fn opens_take_turns() {
        // The drops and reopens of this test must see a lock released, so
        // no child may be in its fork-to-exec window (see `spawn_guard`).
        super::spawn_guard::across_release(|| {
            let dir = crate::test_support::private_tempdir();
            // Another store is opening the directory.
            let opening = lock_file(dir.path(), "open.lock").unwrap();
            assert!(flock(&opening, FlockOperation::NonBlockingLockExclusive).unwrap());
            let path = dir.path().to_owned();
            let open = thread::spawn(move || Store::open_shared(&path).map(drop));
            // The open must not finish while the lock is held: a negative
            // cannot be polled for once, so it is asserted across a
            // window, every 20 ms. A slow start of the thread only
            // strengthens the check, and a broken wait fails inside it.
            let give_up = std::time::Instant::now() + Duration::from_secs(2);
            while std::time::Instant::now() < give_up {
                assert!(!open.is_finished(), "an open waits for the one in progress");
                thread::sleep(Duration::from_millis(20));
            }
            drop(opening);
            open.join().unwrap().unwrap();
        });
    }

    #[test]
    fn a_put_records_one_observation_per_fsync_of_the_blob_path() {
        let dir = crate::test_support::private_tempdir();
        let metrics = std::sync::Arc::new(crate::metrics::Metrics::default());
        let store = Store::open(dir.path()).unwrap();
        store.set_metrics(Arc::clone(&metrics));
        assert!(
            !metrics.render().contains("scion_blob_sync_seconds"),
            "nothing synced yet: {}",
            metrics.render()
        );
        store.blobs().put(b"recorded").unwrap();
        let text = metrics.render();
        // One put fsyncs the temp file and the fan-out directory in
        // `sync`, and `sync` then runs one full flush of the drive cache
        // on every platform: three observations.
        let want = 3;
        assert!(
            text.contains(&format!("scion_blob_sync_seconds_count {want}\n")),
            "{text}"
        );
        assert!(
            text.contains(&format!(
                "scion_blob_sync_seconds_bucket{{le=\"+Inf\"}} {want}\n"
            )),
            "{text}"
        );
    }

    #[test]
    fn free_bytes_of_a_tempdir_is_positive() {
        let dir = crate::test_support::private_tempdir();
        let free = free_bytes(dir.path()).unwrap();
        assert!(free > 0, "{free}");
    }

    #[test]
    fn concurrent_puts_share_full_flushes() {
        const WRITERS: u64 = 32;
        let dir = crate::test_support::private_tempdir();
        let blobs = Blobs::open(dir.path()).unwrap();
        // Odd writers put a blob that is already stored.
        blobs.put(b"stored").unwrap();
        let content = |i: u64| {
            if i.is_multiple_of(2) {
                format!("blob {i}")
            } else {
                "stored".to_owned()
            }
        };
        let before = blobs.full_flushes();
        // Run callers so far, so the arrival count below is relative.
        let runs_before = blobs
            .group_flush
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .waiting as u64;
        let running = Barrier::new(2);
        let ids: Vec<BlobId> = thread::scope(|s| {
            // A first flush that ends only when every writer waits in the
            // group, so they all share the next one, whatever the speed of
            // the machine or its drive.
            let slow = s.spawn(|| {
                blobs.group_flush.run(|| {
                    running.wait();
                    loop {
                        let state = blobs
                            .group_flush
                            .state
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner);
                        // The slow caller counts itself, so all have arrived
                        // at the runs before the test plus one more.
                        if state.waiting as u64 >= runs_before + 1 + WRITERS {
                            break;
                        }
                        drop(state);
                        thread::yield_now();
                    }
                    Ok(())
                })
            });
            running.wait();
            let writers: Vec<_> = (0..WRITERS)
                .map(|i| {
                    let (blobs, content) = (&blobs, &content);
                    s.spawn(move || blobs.put(content(i).as_bytes()))
                })
                .collect();
            slow.join().unwrap().unwrap();
            writers
                .into_iter()
                .map(|w| w.join().unwrap().unwrap())
                .collect()
        });
        // Not counting the slow flush: each writer waits in the group once,
        // while it runs, so one flush serves them all.
        let flushes = blobs.full_flushes() - before - 1;
        assert_eq!(flushes, 1, "{WRITERS} writers ran {flushes} full flushes");
        for id in ids {
            assert_eq!(blobs.verify(id).unwrap(), BlobStatus::Valid);
        }
    }

    /// What [`Blobs::get`] says about a blob, as a status.
    fn status_by_get(blobs: &Blobs, id: BlobId) -> Result<BlobStatus> {
        match blobs.get(id) {
            Ok(_) => Ok(BlobStatus::Valid),
            Err(Error::NotFound(_)) => Ok(BlobStatus::Missing),
            Err(Error::Corrupt(_)) => Ok(BlobStatus::Corrupt),
            Err(e) => Err(e),
        }
    }

    /// What a [`BlobReader`] says about a blob, read in parts of 3 bytes.
    /// Checks that it gives all of the content only for an intact blob.
    fn status_by_reader(blobs: &Blobs, id: BlobId) -> Result<BlobStatus> {
        let mut content = Vec::new();
        let mut read = || -> Result<()> {
            let mut reader = blobs.reader(id)?;
            while let Some(part) = reader.read_part(3)? {
                assert!((1..=3).contains(&part.len()), "a part of 1 to 3 bytes");
                content.extend(part);
            }
            assert!(reader.ended());
            Ok(())
        };
        let status = match read() {
            Ok(()) => BlobStatus::Valid,
            Err(Error::NotFound(_)) => BlobStatus::Missing,
            Err(Error::Corrupt(_)) => BlobStatus::Corrupt,
            Err(e) => return Err(e),
        };
        let whole = BlobId::of(&content) == id;
        assert_eq!(whole, status == BlobStatus::Valid, "{content:?}");
        Ok(status)
    }

    #[test]
    fn a_reader_gives_what_put_wrote_in_parts() {
        let dir = crate::test_support::private_tempdir();
        let blobs = Blobs::open(dir.path()).unwrap();
        // Larger than the window, so the frame uses the whole window.
        let raw: Vec<u8> = (0..5_u32 << 20)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
            .collect();
        let id = blobs.put(&raw).unwrap();
        let mut reader = blobs.reader(id).unwrap();
        let mut read = Vec::new();
        while let Some(part) = reader.read_part(1 << 20).unwrap() {
            assert!(part.len() <= 1 << 20);
            read.extend(part);
        }
        assert!(read == raw, "read {} bytes of {}", read.len(), raw.len());
    }

    #[test]
    fn verify_gives_the_same_answer_as_get() {
        let dir = crate::test_support::private_tempdir();
        let blobs = Blobs::open(dir.path()).unwrap();
        let packed = zstd::bulk::compress(b"content", 3).unwrap();
        let mut flipped = packed.clone();
        *flipped.last_mut().unwrap() ^= 1;
        // One byte more than a blob may hold, built with zstd directly, as
        // `put` refuses it.
        let too_large = || io::repeat(0).take(MAX_OBJECT_BYTES as u64 + 1);
        let content = BlobId::of(b"content");
        let truncated = packed[..packed.len() - 1].to_vec();
        let cases = [
            ("valid", content, Some(packed), BlobStatus::Valid),
            ("missing", content, None, BlobStatus::Missing),
            (
                "a flipped byte",
                content,
                Some(flipped),
                BlobStatus::Corrupt,
            ),
            ("truncated", content, Some(truncated), BlobStatus::Corrupt),
            (
                "another blob's file",
                content,
                Some(zstd::bulk::compress(b"another blob", 3).unwrap()),
                BlobStatus::Corrupt,
            ),
            (
                "empty, not the empty blob",
                content,
                Some(vec![]),
                BlobStatus::Corrupt,
            ),
            (
                "too large",
                BlobId::of_reader(too_large()).unwrap(),
                Some(zstd::stream::encode_all(too_large(), 1).unwrap()),
                BlobStatus::Corrupt,
            ),
            (
                "empty, empty blob",
                BlobId::of(b""),
                Some(vec![]),
                BlobStatus::Valid,
            ),
        ];
        for (case, id, file, want) in cases {
            let (dir, path) = blobs.location(id);
            fs::create_dir_all(dir).unwrap();
            match &file {
                Some(bytes) => fs::write(&path, bytes).unwrap(),
                None => fs::remove_file(&path).unwrap(),
            }
            assert_eq!(blobs.verify(id).unwrap(), want, "{case}");
            assert_eq!(status_by_get(&blobs, id).unwrap(), want, "{case}");
            assert_eq!(status_by_reader(&blobs, id).unwrap(), want, "{case}");
        }
    }

    #[test]
    fn verify_reports_a_read_error_as_io_not_as_corrupt() {
        let dir = crate::test_support::private_tempdir();
        let blobs = Blobs::open(dir.path()).unwrap();
        // A directory opens like a file, but every read of it fails.
        let id = BlobId::of(b"content");
        fs::create_dir_all(blobs.location(id).1).unwrap();
        assert!(matches!(blobs.verify(id), Err(Error::Io(_))));
        assert!(matches!(status_by_get(&blobs, id), Err(Error::Io(_))));
        assert!(matches!(status_by_reader(&blobs, id), Err(Error::Io(_))));
    }

    /// Entries in the temp directory.
    fn staged(blobs: &Blobs) -> usize {
        fs::read_dir(&blobs.mine)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_name() != ".lock")
            .count()
    }

    /// Blob files in the blob folder.
    fn stored(blobs: &Blobs) -> usize {
        fs::read_dir(&blobs.root)
            .unwrap()
            .map(|dir| fs::read_dir(dir.unwrap().path()).unwrap().count())
            .sum()
    }

    /// An upload of `parts`, in order.
    fn fill<'a>(blobs: &Blobs, parts: impl IntoIterator<Item = &'a [u8]>) -> Result<Upload> {
        parts.into_iter().try_fold(blobs.upload()?, Upload::append)
    }

    #[test]
    fn an_upload_stores_the_blob_of_all_its_parts() {
        let dir = crate::test_support::private_tempdir();
        let blobs = Blobs::open(dir.path()).unwrap();
        let cases: [&[&[u8]]; 4] = [&[], &[b""], &[b"a", b"", b"bc"], &[b"abc"]];
        for parts in cases {
            let content = parts.concat();
            let id = blobs
                .put_upload(fill(&blobs, parts.iter().copied()).unwrap(), true)
                .unwrap();
            assert_eq!(id, BlobId::of(&content), "{parts:?}");
            assert_eq!(blobs.get(id).unwrap(), content, "{parts:?}");
            assert_eq!(blobs.size(id).unwrap(), content.len(), "{parts:?}");
        }
        // `b"abc"` in one part or three is one blob.
        assert_eq!(stored(&blobs), 2);
        assert_eq!(staged(&blobs), 0);
    }

    #[test]
    fn an_upload_keeps_its_parts_on_disk_not_in_memory() {
        const PART: usize = 1 << 20;
        let dir = crate::test_support::private_tempdir();
        let blobs = Blobs::open(dir.path()).unwrap();
        // A period (251) that does not divide the part size, so parts stored
        // in the wrong order give other bytes.
        let content: Vec<u8> = (0..251_u8).cycle().take(3 * PART + 7).collect();
        let mut upload = blobs.upload().unwrap();
        let mut sent = 0;
        for part in content.chunks(PART) {
            upload = upload.append(part).unwrap();
            sent += part.len();
            assert_eq!(upload.spool.metadata().unwrap().len(), sent as u64);
        }
        let id = blobs.put_upload(upload, true).unwrap();
        assert_eq!(id, BlobId::of(&content));
        assert!(blobs.get(id).unwrap() == content, "content differs");
        assert_eq!(blobs.size(id).unwrap(), content.len());
    }

    #[test]
    fn one_byte_over_the_limit_fails_an_upload() {
        let dir = crate::test_support::private_tempdir();
        let blobs = Blobs::open(dir.path()).unwrap();
        // As if all but one byte of the limit had arrived.
        let mut upload = blobs.upload().unwrap();
        upload.len = MAX_OBJECT_BYTES - 1;
        let at_limit = upload.append(b"x").unwrap();
        match at_limit.append(b"y").err() {
            Some(Error::InvalidArgument(why)) => assert!(why.contains("larger than"), "{why}"),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
        assert_eq!(staged(&blobs), 0);
    }

    #[test]
    fn a_crash_before_the_rename_leaves_no_blob_and_no_file() {
        let dir = crate::test_support::private_tempdir();
        let store = Store::open(dir.path()).unwrap();
        let content = b"never stored";
        let id = BlobId::of(content);
        // A crash during an upload: no destructor runs.
        std::mem::forget(store.blobs().upload().unwrap().append(content).unwrap());
        // A crash after the compressed file is written, before its rename,
        // in the staging folder of the process that crashed.
        let mut torn = tempfile::NamedTempFile::new_in(&store.blobs().mine).unwrap();
        torn.write_all(&zstd::bulk::compress(content, 3).unwrap())
            .unwrap();
        torn.keep().unwrap();
        assert_eq!(staged(store.blobs()), 1, "the spool file has no name");
        drop(store);

        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.blobs().verify(id).unwrap(), BlobStatus::Missing);
        // The folder of the dead process is gone, and this one is empty.
        assert_eq!(staged(store.blobs()), 0, "open removes what a crash left");
    }

    #[test]
    fn concurrent_uploads_of_one_blob_leave_one_file() {
        const UPLOADERS: usize = 8;
        let dir = crate::test_support::private_tempdir();
        let blobs = Blobs::open(dir.path()).unwrap();
        let content: Vec<u8> = (0..251_u8).cycle().take(3 << 20).collect();
        let ready = Barrier::new(UPLOADERS);
        let ids: Vec<BlobId> = thread::scope(|s| {
            let uploaders: Vec<_> = (0..UPLOADERS)
                .map(|_| {
                    s.spawn(|| {
                        let upload = fill(&blobs, content.chunks(1 << 20))?;
                        // All store at once, so their writes overlap. None
                        // keeps a stored copy, so each writes the file.
                        ready.wait();
                        blobs.put_upload(upload, false)
                    })
                })
                .collect();
            uploaders
                .into_iter()
                .map(|u| u.join().unwrap().unwrap())
                .collect()
        });
        let id = BlobId::of(&content);
        assert!(ids.iter().all(|got| *got == id), "{ids:?}");
        assert_eq!(blobs.verify(id).unwrap(), BlobStatus::Valid);
        assert_eq!(stored(&blobs), 1);
        assert_eq!(staged(&blobs), 0);
    }

    #[test]
    fn a_put_of_a_stored_blob_still_waits_for_a_full_flush() {
        // Another put may have renamed the file and not synced it yet.
        let dir = crate::test_support::private_tempdir();
        let blobs = Blobs::open(dir.path()).unwrap();
        blobs.put(b"stored").unwrap();
        let before = blobs.full_flushes();
        blobs.put(b"stored").unwrap();
        assert_eq!(blobs.full_flushes(), before + 1);
    }

    #[test]
    fn a_failed_full_flush_fails_every_caller_that_shares_it() {
        const CALLERS: usize = 8;
        let group = GroupFlush::default();
        let runs = AtomicUsize::new(0);
        let ready = AtomicUsize::new(0);
        let started = AtomicBool::new(false);
        let arrive = Barrier::new(CALLERS);
        let results: Vec<io::Result<()>> = thread::scope(|s| {
            let callers: Vec<_> = (0..CALLERS)
                .map(|_| {
                    s.spawn(|| {
                        arrive.wait();
                        // Every caller announces itself before the call,
                        // so the flush that owns the group can hold it
                        // open until every caller has had the chance to
                        // share it. A fixed sleep here rested the sharing
                        // on luck: under load, a caller that arrives late
                        // runs its own flush, and the count goes wrong.
                        ready.fetch_add(1, Ordering::SeqCst);
                        group.run(|| {
                            runs.fetch_add(1, Ordering::Relaxed);
                            if started.swap(true, Ordering::SeqCst) {
                                // A straggler that missed the group runs
                                // its own flush; the count assert below
                                // still holds.
                                return Err(io::Error::other("the drive is gone"));
                            }
                            // Generous, so a loaded machine only slows
                            // the test; the bound keeps a lost scheduler
                            // slice from hanging it.
                            let give_up = std::time::Instant::now() + Duration::from_secs(30);
                            while ready.load(Ordering::SeqCst) < CALLERS
                                && std::time::Instant::now() < give_up
                            {
                                thread::sleep(Duration::from_millis(5));
                            }
                            Err(io::Error::other("the drive is gone"))
                        })
                    })
                })
                .collect();
            callers.into_iter().map(|c| c.join().unwrap()).collect()
        });
        let runs = runs.into_inner();
        assert!(runs < CALLERS, "{CALLERS} callers ran {runs} flushes");
        for result in results {
            let err = result.expect_err("every caller sees the failure");
            assert_eq!(err.to_string(), "the drive is gone");
        }
    }

    thread_local! {
        /// What the WAL hook of a test runs.
        static ON_COMMIT: RefCell<Option<Box<dyn FnMut()>>> = const { RefCell::new(None) };
    }
}
