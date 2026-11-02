//! The metadata backend of a server: a Postgres database behind the
//! crate's internal metadata trait.
//!
//! A backend is opaque: the caller opens it (a shared Postgres database
//! from a URL) and reads or writes through it, without seeing the
//! database handle. The transaction type is `BackendTx`, and the queries
//! of a call go through the metadata trait.

use scion_core::ChangesetId;
use scion_proto::runtime::metrics::Metrics;
use std::sync::Arc;
use std::time::Duration;

use crate::error::Result;
use crate::meta::{Meta, MetaTx};
use crate::pg::{Pg, PgTx};
use std::fmt::Write as _;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use crate::store::Blobs;

#[cfg(any(test, feature = "test-hooks"))]
#[path = "test_support/backend.rs"]
pub(crate) mod test_hooks;

/// The metadata storage of a server: a shared Postgres database. The
/// backend is opaque — the database is private, so a caller reads and
/// writes through the trait methods, never the engine.
#[derive(Clone)]
pub struct Backend {
    pg: Pg,
}

/// Clients that a Postgres backend pools for its reads: as many as a
/// data directory holds, so the graph-walk bound of the server keeps its
/// meaning on both backends.
const PG_READERS: usize = crate::store::READERS;

const _: () = assert!(PG_READERS == crate::store::READERS);

/// The file that names the backend of a data directory. Absent, the
/// directory was made by a build that wrote no marker; its files say
/// what it holds.
const MARKER: &str = "backend";

/// The store UUID the data directory's marker held when an open began.
#[derive(Clone, Copy, Default)]
pub(crate) struct Marker {
    pub(crate) uuid: Option<[u8; 16]>,
}

impl Marker {
    /// Reads the marker of `data`, if any.
    pub(crate) fn read(data: &Path) -> Self {
        Self {
            uuid: marker_uuid(data),
        }
    }
}

/// Refuses to open Postgres on a directory whose metadata a SQLite
/// store already holds: its rows live in that database, and a second,
/// empty store here would look like a server with no data at all. This
/// build keeps metadata only in Postgres, so such a directory stays
/// with the build that made it.
fn refuse_sqlite_dir(data: &Path) -> Result<()> {
    let db = data.join("scion.db");
    if db.metadata().is_ok_and(|m| m.len() > 0) {
        return Err(crate::error::Error::InvalidArgument(
            "this data directory holds a SQLite metadata database (scion.db), \
             which this build does not read: this build keeps its metadata \
             only in Postgres, so this directory stays with the build that \
             made it"
                .into(),
        ));
    }
    Ok(())
}

/// The marker's first line, if the file exists: `postgres` for this
/// build, and the second line, when present, is the store UUID. Read as
/// bytes, so a damaged marker is refused, not mistaken for absent.
fn marker_first_line(data: &Path) -> Result<Option<String>> {
    let bytes = match std::fs::read(data.join(MARKER)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(crate::error::Error::Io(e)),
    };
    let line = bytes.split(|&b| b == b'\n').next().unwrap_or_default();
    // For the comparison only: the file this build wrote is ASCII, and a
    // byte that is not UTF-8 becomes a replacement character, which the
    // comparison refuses.
    let text = String::from_utf8_lossy(line);
    Ok(Some(text.trim().to_owned()))
}

/// Refuses a marker whose first line is neither `postgres` nor absent.
/// This build writes either nothing (no marker, the directories of older
/// builds) or `postgres`; an empty or garbled marker is damage, and a
/// marker read as absent would open a Postgres directory as a fresh,
/// empty store, which looks like a server with no data at all.
fn refuse_damaged_marker(data: &Path) -> Result<()> {
    match marker_first_line(data)? {
        Some(first) if first != "postgres" => {
            let path = data.join(MARKER);
            Err(crate::error::Error::FailedPrecondition(format!(
                "the backend marker {} is damaged: its first line is \
                 {:?}, but this build wrote either no marker or `postgres`. \
                 Check whether the directory was restored over, \
                 hand-edited, or written by another build",
                path.display(),
                first
            )))
        }
        _ => Ok(()),
    }
}

/// The store UUID a marker carries, if it names one: 32 hex digits on
/// its second line.
/// The store UUID the data directory's marker holds, for the archive's
/// manifest: it names where the metadata came from.
pub(crate) fn store_uuid_of(data: &Path) -> Option<[u8; 16]> {
    marker_uuid(data)
}

fn marker_uuid(data: &Path) -> Option<[u8; 16]> {
    let text = std::fs::read_to_string(data.join(MARKER)).ok()?;
    let line = text.lines().nth(1)?.trim();
    if line.len() != 32 || !line.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&line[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

/// Puts the database's UUID in the marker, and refuses when the marker
/// names another store: a database restored on the directory of another
/// store would serve metadata whose blobs are not on this disk. A
/// refusal writes nothing: the UUID was compared in the database open,
/// before any blob of this directory was named.
fn marker_agrees(data: &Path, uuid: [u8; 16]) -> Result<()> {
    let marker = data.join(MARKER);
    match marker_uuid(data) {
        Some(held) if held != uuid => {
            return Err(crate::error::Error::FailedPrecondition(format!(
                "this data directory belongs to the store whose UUID is \
                 {}, but the database names the store {}: one was \
                 restored over the other; give the directory of the \
                 database's own store, or a directory the database has \
                 never served",
                hex16(&held),
                hex16(&uuid)
            )));
        }
        Some(_) => {}
        None => {
            // A fresh pair, a marker that predates the UUID, or a first
            // open cut before it wrote the marker: the database's
            // identity is the one this directory takes, and
            // `write_marker` writes it durable at once.
            write_marker(&marker, uuid)?;
        }
    }
    Ok(())
}

/// Writes the marker with `uuid` on its second line, durable: a marker
/// half-written would name no store and refuse the next open.
fn write_marker(marker: &Path, uuid: [u8; 16]) -> Result<()> {
    let tmp = tempfile::NamedTempFile::new_in(marker.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(crate::error::Error::Io)?;
    (|| -> std::io::Result<()> {
        use std::io::Write as _;
        tmp.as_file()
            .write_all(format!("postgres\n{}\n", hex16(&uuid)).as_bytes())?;
        tmp.as_file().sync_all()?;
        tmp.persist(marker).map_err(|e| e.error)?;
        if let Some(dir) = marker.parent() {
            marker_dir_fsync(dir)?;
        }
        Ok(())
    })()
    .map_err(crate::error::Error::Io)
}

/// The directory fsync of the marker write, durable at once: the persist
/// above is only as durable as the directory entry it made, so its
/// failure must refuse the open, the way every other fsync's does.
#[cfg(not(any(test, feature = "test-hooks")))]
fn marker_dir_fsync(dir: &Path) -> std::io::Result<()> {
    crate::store::fsync_dir(dir)
}

/// The directory the armed hook fails the next marker write's fsync of.
/// A path, not a bare flag: the suite's tests run on threads in one
/// process, and a bare flag would fail whichever open reaches the
/// marker first, not the armed one.
#[cfg(any(test, feature = "test-hooks"))]
static MARKER_DIR_FSYNC_FAIL: std::sync::Mutex<Option<std::path::PathBuf>> =
    std::sync::Mutex::new(None);

#[cfg(any(test, feature = "test-hooks"))]
fn marker_dir_fsync(dir: &Path) -> std::io::Result<()> {
    let mut armed = MARKER_DIR_FSYNC_FAIL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if armed.as_deref() == Some(dir) {
        *armed = None;
        // EIO, the disk's answer: the drive did not take the entry.
        return Err(std::io::Error::from_raw_os_error(5));
    }
    drop(armed);
    crate::store::fsync_dir(dir)
}

/// Armed by [`crate::backend::test_hooks::fail_marker_dir_fsync`]: the
/// next marker write whose data directory is the armed path fails its
/// fsync with EIO, once. The marker writes before any store exists, so
/// the store's fsync gate cannot carry this hook.
#[cfg(any(test, feature = "test-hooks"))]
fn arm_marker_dir_fsync_failure(dir: &Path) {
    *MARKER_DIR_FSYNC_FAIL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(dir.to_path_buf());
}

/// 16 bytes as 32 lowercase hex digits.
fn hex16(bytes: &[u8; 16]) -> String {
    let mut hex = String::with_capacity(32);
    for byte in bytes {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

impl Backend {
    /// Opens the backend of a server: the data directory's database at
    /// `url`, the one server of its schema. `ca` verifies the
    /// certificate of the Postgres server; it is required for a host
    /// that is not local.
    ///
    /// # Errors
    ///
    /// What the database returns.
    pub fn open(data: &Path, url: &str, ca: Option<&Path>) -> Result<Self> {
        Self::open_shared_with(url, ca, data, Pg::open_exclusive)
    }

    /// Opens the backend of a process that does not land, such as an
    /// import: the database sorts its writers out itself.
    ///
    /// # Errors
    ///
    /// What the database returns.
    pub fn open_shared(data: &Path, url: &str, ca: Option<&Path>) -> Result<Self> {
        Self::open_shared_with(url, ca, data, Pg::open)
    }

    fn open_shared_with(
        url: &str,
        ca: Option<&Path>,
        data: &Path,
        open: impl FnOnce(&str, Option<&Path>, usize, &Path, Marker) -> Result<Pg>,
    ) -> Result<Self> {
        // A fresh open makes the data directory it names, private (the
        // data is this user's alone): the first open on a new host
        // writes its marker into a directory that exists. A directory
        // that already exists keeps the checks below, and its mode is
        // not touched (the DirBuilder's mode applies to created
        // components only).
        scion_proto::runtime::auth::create_private_dir_all(data)
            .map_err(crate::error::Error::Io)?;
        // The directory's own checks run before anything reads it: a
        // symlink or a shared mode is refused by kind, before the marker
        // read could follow a link.
        crate::store::refuse_bad_dir(data)?;
        refuse_damaged_marker(data)?;
        let marker = Marker::read(data);
        refuse_sqlite_dir(data)?;
        let pg = open(url, ca, PG_READERS, data, marker)?;
        marker_agrees(data, pg.store_uuid())?;
        Ok(Self { pg })
    }

    /// The backend's database, for the crate's tests.
    #[cfg(test)]
    pub(crate) fn into_pg(self) -> Pg {
        self.pg
    }

    /// The schema versions this open moved, when the open migrated: the
    /// process that opened records the move in its audit log. A fresh
    /// open creates the schema and reports none.
    #[must_use]
    pub fn migrated(&self) -> Option<(u32, u32)> {
        self.pg.migrated()
    }

    /// Whether this backend lost its database to another process: every
    /// read and write fails from then on.
    #[must_use]
    pub fn fenced(&self) -> bool {
        self.pg.fenced()
    }

    /// The flag of [`Backend::fenced`], for the health service of a
    /// serving process: it flips to `NOT_SERVING` when the fence trips.
    /// The server sets it on the health service before it serves; it is
    /// wiring, not a test seam, so it is public and documented instead
    /// of doc(hidden).
    #[must_use]
    pub fn fence_flag(&self) -> Arc<AtomicBool> {
        self.pg.fence_flag()
    }

    /// The blob folder. Blob files stay on local disk.
    #[must_use]
    pub fn blobs(&self) -> &Blobs {
        self.pg.blobs()
    }

    /// This process's staging folder under the data directory's `tmp/`:
    /// a one-shot command removes it when it ends, whatever way it ends,
    /// so its folder is not left for the next open to sweep.
    #[must_use]
    pub fn temp_folder(&self) -> &Path {
        self.pg.blobs().temp_folder()
    }

    /// Gives the backend the server's metrics registry, once before the
    /// server serves: the reader pool's metrics, the blob full flushes,
    /// the derive-walk gauges and the collector-key wait. Without it,
    /// import, fsck and gc record nothing.
    pub fn set_metrics(&self, metrics: Arc<Metrics>) {
        self.pg.set_metrics(metrics);
    }

    /// The age of the oldest open Postgres read transaction, for the
    /// tx-age gauge.
    #[must_use]
    pub fn pg_tx_age(&self) -> Duration {
        self.pg.tx_age()
    }

    /// Runs `f` so every statement sees one snapshot, as an export's
    /// sections claim: [`Backend::read`] already runs in one REPEATABLE
    /// READ transaction.
    ///
    /// # Errors
    ///
    /// What `f` returns, or a database error from the transaction's
    /// begin.
    pub(crate) fn read_snapshot<T>(
        &self,
        f: impl FnOnce(&mut BackendTx<'_>) -> Result<T>,
    ) -> Result<T> {
        self.pg.read(f)
    }

    /// The derive walks of this backend: which are in flight, and the cap
    /// on how many may be.
    pub(crate) fn derive_flights(&self) -> &crate::derived::DeriveFlights {
        self.pg.derive_flights()
    }

    /// How many writers wait for the write connection.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn writers_waiting(&self) -> usize {
        self.pg.writers_waiting()
    }

    /// Waits while writers wait for the write connection, until the
    /// deadline: a derivation step yields to a writer that waited.
    ///
    /// # Errors
    ///
    /// [`Error::DeadlineExceeded`] once the deadline has passed.
    pub(crate) fn wait_for_writers(
        &self,
        deadline: Option<&crate::deadline::Deadline>,
    ) -> Result<()> {
        self.pg.wait_for_writers(deadline)
    }

    /// Where a ref points.
    ///
    /// # Errors
    ///
    /// [`crate::error::Error::NotFound`] if the repo or ref does not
    /// exist.
    pub fn resolve_ref(&self, repo: &str, name: &str) -> Result<ChangesetId> {
        use crate::meta::Meta as _;
        self.read(|c| {
            let repo = c.repo_id(repo)?;
            c.get_ref(repo, name)?
                .map(|r| r.target)
                .ok_or_else(|| crate::error::Error::NotFound(format!("ref {name:?}")))
        })
    }
}

impl Meta for Backend {
    type Tx<'a>
        = PgTx<'a>
    where
        Self: 'a;

    fn read<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        self.pg.read(f)
    }

    fn read_long<T>(
        &self,
        idle: std::time::Duration,
        f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>,
    ) -> Result<T> {
        self.pg.read_bounded(
            f,
            crate::pg::READ_STATEMENT_TIMEOUT,
            idle,
            crate::pg::READER_WAIT,
        )
    }

    fn read_counted<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> (Result<T>, u64) {
        self.pg.read_counted(f)
    }

    fn wait_durable(&self, seen: u64) -> Result<()> {
        self.pg.wait_durable(seen)
    }

    fn read_unflushed<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        self.pg.read_unflushed(f)
    }

    fn commit<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        self.pg.commit(f)
    }

    fn write<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        self.pg.write(f)
    }

    fn flush(&self) -> Result<()> {
        self.pg.flush()
    }
}

/// The transaction of a [`Backend`]: the queries of one call, as the
/// metadata trait sees them.
#[allow(unreachable_pub)]
pub type BackendTx<'a> = PgTx<'a>;
