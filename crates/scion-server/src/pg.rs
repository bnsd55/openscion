//! The Postgres metadata backend of [`Meta`](crate::meta::Meta)
//! ([#47](https://github.com/bnsd55/openscion/issues/47)).
//!
//! One shared database takes the place of one data directory: many
//! processes can hold clients, and Postgres sorts the writers out, so
//! there is no directory lock, one write connection, or reader pool of a
//! fixed set of pages. A small pool of clients stands in for the read
//! connections, and one client for writes, in the shape of
//! [`Store`](crate::store::Store).
//!
//! The rules that a backend must keep hold here too:
//!
//! - `synchronous_commit = on` on every client, so a commit is durable
//!   when the statement returns. `flush` is a no-op, `read` need not wait,
//!   and `read_counted` returns no count, because there is nothing to wait
//!   for.
//! - Reads and writes run at READ COMMITTED. Rows that content addresses
//!   never change once written, and refs and lands move only through
//!   compare-and-swap predicates (`version = $n`, `state = 'pending'`), so
//!   a lost swap is visible to the caller.
//! - Land IDs come from an identity sequence, so no ID is handed out
//!   twice, and the newest land holds the largest one.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use gix::ObjectId;
use postgres::types::ToSql;
use postgres::{Client, Config, NoTls};

/// Shorthand for the owned query parameter.
type P = Param;
use scion_core::{BlobId, Changeset, ChangesetId, RepoPath, Tree, TreeId};
use scion_proto::runtime::metrics::Metrics;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::auth::Caller;
use crate::backend::Marker;
use crate::error::{Error, Result, Storage};
use crate::history::NodeId;
use crate::import::SHA1;
use crate::land::{
    EXPIRE_PER_LAND, LAND_RETENTION_SECS, LandOutcome, LandRejectReason, LandRequest, LandState,
    decode_paths, encode_paths,
};
use crate::meta::{FsckDerivedRow, Meta, MetaTx, RefState, RepoChangeset, RepoId, StaleLand};
use crate::store::Blobs;

#[cfg(any(test, feature = "test-hooks"))]
#[path = "test_support/pg.rs"]
pub(crate) mod test_hooks;

/// The damage tests make: one function per kind, each doing its kind's
/// damage. A child module of `pg` (same as `test_hooks`) so the tests
/// can reach this file's error mapping.
#[cfg(test)]
#[path = "test_support/damage.rs"]
pub(crate) mod damage;

/// The schema version this build makes and reads.
pub(crate) const PG_SCHEMA_VERSION: u32 = 6;

/// How long a call may wait for a Postgres reader before the server
/// answers [`Error::Unavailable`]. Well below the 600 s call deadline:
/// a wait this long means the pool is stuck in long reads, and a retry
/// after the pool drains is kinder than blocking to the deadline.
pub(crate) const READER_WAIT: Duration = Duration::from_secs(30);

/// The bound on one statement of a read transaction, below the 600 s
/// call deadline (`scn`'s), so one stuck query cannot hold a reader to
/// the end of a stream.
pub(crate) const READ_STATEMENT_TIMEOUT: Duration = Duration::from_secs(300);

/// How long a read transaction may sit idle between statements, so an
/// abandoned walk (its call gone, its reply unread) gives the reader
/// back to the pool.
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// The advisory lock of the one server process of a database.
const SERVER_LOCK: i64 = 0x5343_494F_4E53_5256;

/// The readers this process has taken since the last reset, for a test
/// that counts one walk's transactions by the readers it held.
#[cfg(any(test, feature = "test-hooks"))]
static TEST_READER_TAKES: AtomicU64 = AtomicU64::new(0);

// The tests bound a walk's changeset-read cost by this counter, and the
// full-stop gate keeps another test's statements out of its delta.
#[cfg(any(test, feature = "test-hooks"))]
thread_local! {
    pub(crate) static CHANGESET_READS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}
/// The advisory lock of one migration run of a database.
const SCHEMA_LOCK: i64 = 0x5343_494F_4E53_4348;

/// Schema migrations in order. Migration `n`, from 1, moves the database
/// from version `n - 1` to version `n`. A released migration never changes.
const PG_MIGRATIONS: [&str; PG_SCHEMA_VERSION as usize] = [
    include_str!("../migrations/postgres/0001_init.sql"),
    include_str!("../migrations/postgres/0002_view_roots.sql"),
    include_str!("../migrations/postgres/0003_view_trees.sql"),
    include_str!("../migrations/postgres/0004_store_uuid.sql"),
    include_str!("../migrations/postgres/0005_parent_numbering.sql"),
    include_str!("../migrations/postgres/0006_land_reason.sql"),
];

/// The name of every migration, in the order [`PG_MIGRATIONS`] runs
/// them: what `migrate --check` prints as the pending steps. One test
/// keeps the two tables the same length.
pub(crate) const PG_MIGRATION_NAMES: [&str; PG_SCHEMA_VERSION as usize] = [
    "0001_init",
    "0002_view_roots",
    "0003_view_trees",
    "0004_store_uuid",
    "0005_parent_numbering",
    "0006_land_reason",
];

/// The fence of a process that lost the one-server lock. The worker of
/// the exclusive client trips it when a setup that once held the lock no
/// longer does: another process may serve the database now, while this
/// one still answers reads from its reader pool and its local blobs.
/// A tripped fence fails every read and write of the backend, the health
/// service flips to `NOT_SERVING`, and the server process exits.
struct Fence {
    tripped: Arc<AtomicBool>,
}

impl Fence {
    fn new() -> Self {
        Self {
            tripped: Arc::new(AtomicBool::new(false)),
        }
    }

    fn tripped(&self) -> bool {
        self.tripped.load(Ordering::SeqCst)
    }

    /// Trips the fence once: the log line is the operator's answer to
    /// "why did every call start failing".
    fn trip(&self) {
        if !self.tripped.swap(true, Ordering::SeqCst) {
            tracing::error!(
                "this server process lost the database to another server \
                 process; it serves nothing from here on, and the health \
                 service answers NOT_SERVING until the process exits"
            );
        }
    }
}

/// Handle to one Postgres database. Cheap to clone.
#[derive(Clone)]

pub(crate) struct Pg {
    inner: Arc<PgInner>,
}

/// How many slot holders may take the write client ahead of other
/// waiting writers before the lane must let one through: a stream of
/// settles must not starve the record commits that feed it.
const WRITER_LANE_AHEAD: usize = 4;

/// The write lane's state: who holds the write client, who waits, and
/// how the lane orders them. A land that holds its ref's lock (the
/// settle's commit and the sweep's reject; the finish's reads ride the
/// read pool now) enters ahead of other waiting writers, because its
/// wait costs the whole queue of its ref; the bound keeps the other
/// writers moving. Without a slot holder waiting, the lane is today's
/// plain queue.
#[derive(Default)]
struct WriterGate {
    /// The write client is out with a transaction.
    taken: bool,
    /// Takers not yet released: waiting or granted. The derivation
    /// step's wait watches this one.
    waiting: usize,
    /// Slot holders that wait, not granted yet.
    holders_waiting: usize,
    /// Other writers that wait, not granted yet.
    plain_waiting: usize,
    /// Slot holders granted since the last other writer was.
    holders_ahead: usize,
}

/// Whether a taker of this class may take the client now: a slot
/// holder goes while the other writers have not earned the bound's
/// mercy, and an other writer goes when no slot holder waits or after
/// the bound's run of slot holders.
fn lane_admits(gate: &WriterGate, slot_holder: bool) -> bool {
    if slot_holder {
        gate.plain_waiting == 0 || gate.holders_ahead < WRITER_LANE_AHEAD
    } else {
        gate.holders_waiting == 0 || gate.holders_ahead >= WRITER_LANE_AHEAD
    }
}

struct PgInner {
    /// The write client, for every commit and [`Pg::read_unflushed`]. The
    /// gate below decides who holds it when; the invariants this one
    /// client keeps are listed on [`WriterGate`], where the queue lives.
    writer: Mutex<PgClient>,
    /// The write lane's gate: who may take the write client next, and
    /// when it is out. The one write client keeps these invariants, in
    /// words, so the next change to this lock reads them here:
    ///
    /// - Write transactions are serial. One client means two write
    ///   transactions of this process never run their statements side by
    ///   side, so they cannot deadlock on row locks and cannot read each
    ///   other's uncommitted state. The gate grants one taker at a time.
    /// - Every write transaction takes the collector's advisory key
    ///   (`pg_advisory_xact_lock(hashtext(current_schema() || ':gc'))`)
    ///   and holds it to the transaction's end, with the lock-timeout
    ///   layering that rides it: the 60 s guard set before the key's
    ///   statement, the 5 s writer clock set with it for the rest of the
    ///   transaction. The collector's re-read pins the blobs a writer
    ///   just put.
    /// - A changeset's generation is computed inside the transaction
    ///   that inserts it (the parents' generations read there, the
    ///   child's written as their max + 1), so no other write can slip
    ///   a sibling in between.
    /// - The land's settle runs its row writes and the ref's CAS in one
    ///   transaction, and the verdict path re-reads the ref in the same
    ///   transaction as the land row's update: a ref that moved aborts
    ///   the verdict and the land replans.
    /// - The land row's update is guarded by `state = 'pending'`: a twin
    ///   call that settles first is stopped there.
    /// - The fence lives in this client's session: the process knows it
    ///   still owns the store because this session holds the one-server
    ///   lock, and every commit checks the fence before it runs.
    /// - A transaction that held changeset or derived rows without the
    ///   flush rolls back and refuses; the write order is changesets,
    ///   parents, membership, derived, history, git imports.
    /// - The waiting count lets a derivation step wait while writers
    ///   wait, so a steady stream of writers delays a cold read up to
    ///   its deadline, never a land.
    writer_gate: Mutex<WriterGate>,
    writer_woken: Condvar,
    /// Set when this process lost the one-server lock: every read and
    /// write of this backend fails from then on, and the server that
    /// holds the backend exits (the [`Fence`]).
    fence: Arc<Fence>,
    /// The UUID of this database, written at the first open and carried
    /// by the marker file of the data directory.
    store_uuid: [u8; 16],
    /// The schema versions this open moved, when it migrated. Version 0's
    /// first open creates the schema, and reports none.
    migrated: Option<(u32, u32)>,
    /// Clients for [`Pg::read`], at most [`READERS`](crate::store::READERS)
    /// at once.
    readers: Readers,
    /// The blob folder: blob files stay on local disk in every backend.
    blobs: Blobs,
    /// The walks in flight of this backend's derivations.
    derive: crate::derived::DeriveFlights,
    /// The RAM the view derivations of this backend reserve their memos
    /// from: [`crate::views::VIEW_DERIVES_MAX`] reservations of
    /// [`crate::views::VIEW_MEMO_BYTES`], so at most that many derivations
    /// hold a memo at once and FLOOR counts the share.
    view_memo: crate::memory::Bytes,
    /// The import's test hooks and busy-retry count.
    #[cfg(any(test, feature = "test-hooks"))]
    hooks: crate::store::test_hooks::ImportHooks,
    /// The commit seams the tests watch: the commits made, the write
    /// transactions run, and how many of the next commits fail as a busy
    /// database.
    #[cfg(any(test, feature = "test-hooks"))]
    committed: std::sync::atomic::AtomicU64,
    /// How many deadline-less waits for writers ended - by the yield's
    /// bound, or because the writers were done. The tests of the bound
    /// read it: a retry of a refused write cannot bump it, so the turn
    /// the bound grants is told apart from any writer's churn.
    yield_turns: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "test-hooks"))]
    txes: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "test-hooks"))]
    fail_commits: std::sync::atomic::AtomicU64,
    /// The operator metrics, set once by the server before it serves: a
    /// commit's wait for the collector's advisory key records how long
    /// it waited. Empty in import, fsck and gc, which export none.
    metrics: std::sync::OnceLock<std::sync::Arc<crate::metrics::Metrics>>,
}

/// Clients for [`Pg::read`], so at most [`READERS`](crate::store::READERS)
/// reads run at once. A caller takes one out and returns it when it is
/// done.
struct Readers {
    idle: Mutex<Vec<PgClient>>,
    returned: Condvar,
    /// The start time of every live read, so the gauges can say how old
    /// the oldest open read transaction is.
    open_since: Mutex<Vec<Instant>>,
    /// How many clients the pool holds, for the busy gauge.
    size: usize,
    /// The registry the pool records into. Set once, by the server,
    /// through [`Backend::set_metrics`](crate::backend::Backend::set_metrics);
    /// a second set is a no-op. [`None`] until then: import, fsck, gc,
    /// and tests without a registry record nothing.
    metrics: OnceLock<Arc<Metrics>>,
}

impl Readers {
    /// [`Readers::new`], with libpq `options` on every client. The
    /// schema is the search path each reader sets up on its own worker:
    /// without it a reader reads the database's default, which holds no
    /// tables of this store.
    fn new_with(
        size: usize,
        url: &str,
        ca: Option<&Path>,
        options: &str,
        schema: Option<&str>,
    ) -> Result<Self> {
        // The open is sequential, so every client sees the same schema.
        let idle = (0..size)
            .map(|_| {
                let client = connect_with(url, ca, options, schema)?;
                let on: String = client.query("SHOW synchronous_commit", &[])?[0].get(0);
                if on != "on" {
                    return Err(Error::FailedPrecondition(format!(
                        "the database runs synchronous_commit={on}; this \
                         backend needs synchronous_commit=on"
                    )));
                }
                Ok(client)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            idle: Mutex::new(idle),
            returned: Condvar::new(),
            open_since: Mutex::new(Vec::new()),
            size,
            metrics: OnceLock::new(),
        })
    }

    /// Takes a reader, or waits at most `wait` for one. A full pool means
    /// the readers are stuck in long reads: the caller hears
    /// [`Error::Unavailable`] and may retry, instead of waiting forever
    /// behind a transaction that never ends.
    fn take(&self, wait: Duration) -> Result<ReaderGuard<'_>> {
        let started = Instant::now();
        // The idle lock guards the pool list only: it is dropped before
        // the tail, whose report_busy takes it again. Holding it across
        // the tail deadlocks take against itself on the first read.
        let client = {
            let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
            loop {
                if let Some(client) = idle.pop() {
                    break client;
                }
                let left = wait.checked_sub(started.elapsed());
                let Some(left) = left else {
                    return Err(Error::Unavailable(format!(
                        "no Postgres reader free after {wait:?}; the readers are stuck in long reads"
                    )));
                };
                let (guard, _woke) = self
                    .returned
                    .wait_timeout(idle, left)
                    .unwrap_or_else(PoisonError::into_inner);
                idle = guard;
                // A timed-out wait falls through to the top of the loop, so a
                // reader given back in this window is taken before refusing.
            }
        };
        let started_at = Instant::now();
        self.open_since
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(started_at);
        #[cfg(any(test, feature = "test-hooks"))]
        TEST_READER_TAKES.fetch_add(1, Ordering::Relaxed);
        self.report_busy();
        if let Some(metrics) = self.metrics() {
            metrics.pg_pool_wait(started.elapsed());
        }
        Ok(ReaderGuard {
            readers: self,
            client: Some(client),
            started: started_at,
        })
    }

    /// The start time of the oldest live read, for the tx-age gauge.
    fn oldest_open(&self) -> Option<Instant> {
        self.open_since
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .min()
    }

    /// A read gave its reader back: forget its start time.
    fn closed(&self, started: Instant) {
        let mut open = self
            .open_since
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(at) = open.iter().position(|known| *known == started) {
            open.swap_remove(at);
        }
        drop(open);
        self.report_busy();
    }

    /// How many readers are out of the pool, for the busy gauge.
    fn report_busy(&self) {
        let idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
        let busy = self.size.saturating_sub(idle.len());
        drop(idle);
        if let Some(metrics) = self.metrics() {
            metrics.pg_readers_busy(u64::try_from(busy).unwrap_or(u64::MAX));
        }
    }

    /// The metrics to record into, if the server gave the backend its
    /// registry.
    fn metrics(&self) -> Option<Arc<Metrics>> {
        self.metrics.get().cloned()
    }

    /// Gives the pool the server's metrics registry. Set once: a second
    /// set is a no-op, and says so in the debug log.
    fn set_metrics(&self, metrics: Arc<Metrics>) {
        if self.metrics.set(metrics).is_err() {
            tracing::debug!("the metrics registry was already set");
        }
        self.report_busy();
    }
}

/// A reader that is out of the pool. Returns it when it drops.
struct ReaderGuard<'a> {
    readers: &'a Readers,
    client: Option<PgClient>,
    started: Instant,
}

impl ReaderGuard<'_> {
    fn client(&mut self) -> &mut PgClient {
        self.client.as_mut().expect("a taken reader has a client")
    }
}

impl Drop for ReaderGuard<'_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            self.readers.closed(self.started);
            self.readers
                .idle
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(client);
            // The gauge reads the pool after the client is back in it: a
            // report before the push would leave one reader counted busy
            // until the next take.
            self.readers.report_busy();
            self.readers.returned.notify_one();
        }
    }
}

/// A value that a query binds, owned, so it can cross to the worker
/// thread. The parameters of this schema are integers, text, and bytes.
#[derive(Debug, Clone)]
enum Param {
    I64(i64),
    Text(String),
    Bytes(Vec<u8>),
    OptBytes(Option<Vec<u8>>),
    /// A list of byte strings, for a `= ANY($n)` over `bytea[]`.
    ByteaArray(Vec<Vec<u8>>),
    /// A list of integers, for the bulk seed's and the batch flush's
    /// `unnest` over `int8[]`.
    I64Array(Vec<i64>),
    /// A list of strings, for the bulk seed's and the batch flush's
    /// `unnest` over `text[]`.
    TextArray(Vec<String>),
}

impl Param {
    /// An integer parameter.
    fn i64(v: i64) -> Param {
        Param::I64(v)
    }

    /// A text parameter.
    fn text(v: impl Into<String>) -> Param {
        Param::Text(v.into())
    }

    /// A byte parameter.
    fn bytes(v: &[u8]) -> Param {
        Param::Bytes(v.to_vec())
    }

    /// An optional byte parameter.
    fn opt_bytes(v: Option<Vec<u8>>) -> Param {
        Param::OptBytes(v)
    }

    /// A list of byte strings, as one `bytea[]` parameter.
    fn bytea_array(v: Vec<Vec<u8>>) -> Param {
        Param::ByteaArray(v)
    }

    /// A list of integers, as one `int8[]` parameter.
    fn i64_array(v: Vec<i64>) -> Param {
        Param::I64Array(v)
    }

    /// A list of strings, as one `text[]` parameter.
    fn text_array(v: Vec<String>) -> Param {
        Param::TextArray(v)
    }
}

impl ToSql for Param {
    fn to_sql(
        &self,
        t: &postgres::types::Type,
        out: &mut postgres::types::private::BytesMut,
    ) -> std::result::Result<postgres::types::IsNull, Box<dyn std::error::Error + Send + Sync>>
    {
        match self {
            Self::I64(v) => v.to_sql(t, out),
            Self::Text(v) => v.to_sql(t, out),
            Self::Bytes(v) => v.to_sql(t, out),
            Self::OptBytes(v) => v.to_sql(t, out),
            Self::ByteaArray(v) => v.to_sql(t, out),
            Self::I64Array(v) => v.to_sql(t, out),
            Self::TextArray(v) => v.to_sql(t, out),
        }
    }

    fn accepts(_: &postgres::types::Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        t: &postgres::types::Type,
        out: &mut postgres::types::private::BytesMut,
    ) -> std::result::Result<postgres::types::IsNull, Box<dyn std::error::Error + Send + Sync>>
    {
        match self {
            Self::I64(v) => v.to_sql(t, out),
            Self::Text(v) => v.to_sql(t, out),
            Self::Bytes(v) => v.to_sql(t, out),
            Self::OptBytes(v) => v.to_sql(t, out),
            Self::ByteaArray(v) => v.to_sql(t, out),
            Self::I64Array(v) => v.to_sql(t, out),
            Self::TextArray(v) => v.to_sql(t, out),
        }
    }
}

/// The reply of one statement.
enum Reply {
    Rows(Vec<postgres::Row>),
    Count(u64),
}

/// What a statement returns.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    /// All rows of a SELECT.
    Rows,
    /// The affected-row count of an INSERT, UPDATE or DELETE.
    Count,
    /// A batch of statements, as one transaction, such as a migration.
    Batch,
    /// No statement: the job's closure runs the client itself.
    Raw,
}

/// One statement to run on the worker thread.
struct Job {
    sql: String,
    params: Vec<Param>,
    kind: Kind,
    reply: std::sync::mpsc::Sender<Result<Reply>>,
    /// A setup closure for [`Kind::Raw`]: it runs the client on the
    /// worker thread, where the sync client belongs.
    raw: Option<RawFn>,
}

type RawFn = Box<dyn FnOnce(&mut Client) -> Result<()> + Send>;

/// A client of the database, driven by one worker thread. The sync
/// `postgres` client runs its own tokio runtime, which must not start
/// inside the server's, so a statement is sent to the worker and the
/// caller blocks for its reply. The client itself never crosses threads.
struct PgClient {
    jobs: std::sync::mpsc::Sender<Job>,
}

/// The worker loop's next step: run another job, or stop the thread.
enum Flow {
    Go,
    Stop,
}

/// The panic's message, for the log: a panic payload is `&str`, `String`,
/// or anything else.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_owned()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "a panic with no message".to_owned()
    }
}

/// The reply a caller gets when the worker thread is gone. The log names
/// why: the worker logs its own death, panic or shutdown.
pub(crate) const WORKER_STOPPED: &str = "the database worker stopped: its log line names why";

/// Serves one job on the worker thread: the loop body of
/// [`PgClient::spawn`], lifted out so its panics are caught and logged
/// instead of ending the thread in silence. `continue` is
/// [`Flow::Go`], `break` is [`Flow::Stop`].
#[allow(clippy::too_many_arguments)]
fn run_job(
    job: Job,
    client: &mut Client,
    setup: &mut Setup,
    conn: &Conn,
    tx: &mut TxState,
    ever_held: &mut bool,
    fence: Option<&std::sync::Arc<Fence>>,
) -> Flow {
    // A transaction the reconnect dropped fails every
    // statement until its caller ends it. Its COMMIT
    // never runs: on the fresh session it would answer
    // Ok about a transaction that is gone. A BEGIN may
    // pass: the reconnect ended the old tx, so a BEGIN
    // starts a fresh one on the fresh session, and a new
    // BEGIN comes only from a new holder of the client,
    // which the lost transaction cannot reach.
    if tx.lost() && !job.sql.starts_with("ROLLBACK") && !job.sql.starts_with("BEGIN") {
        // The caller may be gone, which is fine: the reply
        // fails, and the job's outcome is the caller's
        // business. The worker goes on to the next job.
        let _ = job.reply.send(Err(Error::Unavailable(TX_LOST.into())));
        return Flow::Go;
    }
    // A client the server closed, as after a restart of
    // the database, connects again: the caller sees one
    // error at most. A reconnect inside a transaction
    // loses the transaction.
    let lost_tx = tx.in_progress();
    let mut reconnected = false;
    if client.is_closed() {
        match reconnect(client, conn) {
            Ok(fresh) => {
                *setup = fresh;
                if lost_tx {
                    *tx = TxState::Lost;
                }
            }
            Err(error) => {
                if job.reply.send(Err(error)).is_err() {
                    return Flow::Stop;
                }
                return Flow::Go;
            }
        }
        reconnected = true;
        // The lock state of the fresh session is checked
        // here, before any lost-tx reply: a lock lost
        // across a wedge of dead transactions still fences
        // the process.
        if !setup.ok() {
            if *ever_held && matches!(setup.outcome, SetupOutcome::LockRefused) {
                trip_fence(fence);
            }
            // A database that cannot be reached is no
            // proof of a takeover: the caller retries.
            let _ = job.reply.send(Err(setup_error(setup, *ever_held)));
            return Flow::Go;
        }
    }
    // The setup failed on a client that still lives: a
    // network break left the old session of this very
    // connection holding the lock, and the session dies
    // some while after the connection did. One retry a
    // job, so the recovery needs no process restart.
    if !setup.ok() {
        *setup = Setup::new(conn.schema.as_deref(), conn.exclusive, client);
    }
    if !setup.ok() {
        if *ever_held && matches!(setup.outcome, SetupOutcome::LockRefused) {
            trip_fence(fence);
        }
        let _ = job.reply.send(Err(setup_error(setup, *ever_held)));
        return Flow::Go;
    }
    *ever_held = true;
    // A transaction the reconnect dropped refuses this job
    // too: it was asked while the old connection held the
    // transaction, and the new one holds none (its COMMIT
    // would answer Ok about a transaction that is gone).
    // A new BEGIN passes: the reconnect ended the old tx,
    // so the BEGIN opens a fresh one on the fresh session.
    // As above: a gone caller is fine, the worker goes on.
    if reconnected && lost_tx && !job.sql.starts_with("ROLLBACK") && !job.sql.starts_with("BEGIN") {
        let _ = job.reply.send(Err(Error::Unavailable(TX_LOST.into())));
        return Flow::Go;
    }
    if job.kind == Kind::Raw {
        let mut job = job;
        let f = job.raw.take().expect("a raw job carries its closure");
        let _ = job.reply.send(f(client).map(|(): ()| Reply::Count(0)));
        return Flow::Go;
    }
    let out = run_with_retry(client, setup, conn, &job, tx, *ever_held, fence);
    if setup.ok() {
        *ever_held = true;
    }
    *tx = tx.after(&job.sql, out.is_ok());
    if job.reply.send(out).is_err() {
        return Flow::Stop;
    }
    Flow::Go
}

/// Opens a transaction with `open`'s statements, in one round trip. A
/// batch whose later command fails after its `BEGIN` ran leaves the
/// session inside a transaction Postgres has marked aborted: the next
/// holder of the client would meet "current transaction is aborted" on
/// every statement until a rollback. So a failed open rolls back before
/// the error returns; the rollback's own failure (a lost connection)
/// is fine - the reconnect's setup handles that session.
fn open_transaction(c: &PgClient, open: &str) -> Result<()> {
    if let Err(error) = c.batch(open) {
        let _ = c.execute("ROLLBACK", &[]);
        return Err(error);
    }
    Ok(())
}

impl PgClient {
    /// Runs one closure with the client on the worker thread. The sync
    /// client must never be touched from a tokio thread; the test
    /// schema's preparation calls it. No production path needs the
    /// client whole, so the method is test-only.
    #[cfg(any(test, feature = "test-hooks"))]
    fn raw(&self, f: RawFn) -> Result<()> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.jobs
            .send(Job {
                sql: String::new(),
                params: Vec::new(),
                kind: Kind::Raw,
                reply,
                raw: Some(f),
            })
            .map_err(|_| Error::Corrupt(WORKER_STOPPED.into()))?;
        match rx.recv() {
            Ok(Ok(Reply::Count(_))) => Ok(()),
            Ok(Ok(_)) => unreachable!("a raw job replies with a count"),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::Corrupt(WORKER_STOPPED.into())),
        }
    }

    /// Puts `client` on its own worker thread.
    ///
    /// # Errors
    ///
    /// The OS refused the worker thread (a thread limit, `EAGAIN`): the
    /// open answers it instead of crashing.
    fn spawn(cfg: ClientConfig) -> Result<PgClient> {
        let (jobs, rx) = std::sync::mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("postgres-client".into())
            .spawn(move || {
                let ClientConfig {
                    mut client,
                    conn,
                    fence,
                } = cfg;
                // The schema of the connection, named again after every
                // reconnect; the one-server lock, taken again after every
                // reconnect. A lock the database does not give back fails
                // every statement that follows: another server process may
                // hold it, and this one must not land beside it.
                let mut setup = Setup::new(conn.schema.as_deref(), conn.exclusive, &mut client);
                // BEGIN opens one; COMMIT and ROLLBACK end it. A
                // transaction a reconnect lost fails every statement until
                // its rollback: on the new connection, autocommit would
                // answer COMMIT with Ok.
                let mut tx = TxState::Idle;
                // Whether a setup of this worker ever held the lock: the
                // fence trips on a lock that was held and is gone, never
                // on a first setup that never got one. The flag lives for
                // the worker's life, so a wedge of dead transactions
                // cannot hide a takeover from the fence.
                let mut ever_held = setup.ok();
                for job in rx {
                    // A panic in this body must not end the thread in
                    // silence: the caller's only answer would be "the
                    // worker stopped", with nothing in the log. The body
                    // runs under the catch, its panic lands in the log,
                    // and the thread still stops: a panic leaves the
                    // transaction and the setup state undefined.
                    let reply = job.reply.clone();
                    let done = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run_job(
                            job,
                            &mut client,
                            &mut setup,
                            &conn,
                            &mut tx,
                            &mut ever_held,
                            fence.as_ref(),
                        )
                    }));
                    match done {
                        Ok(Flow::Go) => {}
                        Ok(Flow::Stop) => break,
                        Err(payload) => {
                            tracing::error!(
                                panic = panic_text(payload.as_ref()),
                                "the postgres worker thread panicked and stopped; \
                                 every statement of this client fails until the \
                                 process restarts"
                            );
                            let _ = reply.send(Err(Error::Corrupt(WORKER_STOPPED.into())));
                            break;
                        }
                    }
                }
            })
            .map_err(|error| {
                Error::Unavailable(format!(
                    "the OS refused the postgres worker thread: {error}"
                ))
            })?;
        Ok(Self { jobs })
    }

    /// Runs one statement and waits for its reply.
    fn ask(&self, sql: &str, params: Vec<Param>, kind: Kind) -> Result<Reply> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.jobs
            .send(Job {
                sql: sql.to_owned(),
                params,
                kind,
                reply,
                raw: None,
            })
            .map_err(|_| Error::Internal(WORKER_STOPPED.into()))?;
        rx.recv()
            .map_err(|_| Error::Internal(WORKER_STOPPED.into()))?
    }

    /// The rows of one query.
    fn query(&self, sql: &str, params: &[Param]) -> Result<Vec<postgres::Row>> {
        match self.ask(sql, params.to_vec(), Kind::Rows)? {
            Reply::Rows(rows) => Ok(rows),
            Reply::Count(_) => unreachable!("a query returns rows"),
        }
    }

    /// The first row of a query, or [`None`] when it has none.
    fn query_opt(&self, sql: &str, params: &[Param]) -> Result<Option<postgres::Row>> {
        Ok(self.query(sql, params)?.into_iter().next())
    }

    /// The affected-row count of one statement.
    fn execute(&self, sql: &str, params: &[Param]) -> Result<u64> {
        match self.ask(sql, params.to_vec(), Kind::Count)? {
            Reply::Count(n) => Ok(n),
            Reply::Rows(_) => unreachable!("an execute returns a count"),
        }
    }

    /// A batch of statements, as one transaction.
    fn batch(&self, sql: &str) -> Result<()> {
        match self.ask(sql, Vec::new(), Kind::Batch)? {
            Reply::Count(_) => Ok(()),
            Reply::Rows(_) => unreachable!("a batch returns a count"),
        }
    }
}

/// The libpq options of every connection of this backend. The commit of
/// a transaction reaches the drive before it returns, and no statement
/// runs longer than the graph-walk bound. The search path is not set
/// here: it is a statement on the live connection, which the server
/// cannot lose between connections.
fn base_options() -> String {
    // A lock a writer waits for is a busy database: 5 s, the lock
    // bound. A statement itself may walk the graph for minutes, so only
    // the lock is under the short clock.
    format!(
        "-csynchronous_commit=on -cstatement_timeout=600000 -clock_timeout={LOCK_TIMEOUT_FOR_WRITERS}"
    )
}

/// The lock clock every writer runs under: a lock a writer waits for is a
/// busy database, answered Busy so the client retries. The one exception
/// is the collector's key, whose wait has its own bound (the commit's
/// `LOCK_TIMEOUT_FOR_KEY`); everything after it is back on this clock.
/// The value is bare: the base options take it as `-clock_timeout=<v>`,
/// and the SQL `SET LOCAL` in `Pg::commit` quotes it.
const LOCK_TIMEOUT_FOR_WRITERS: &str = "5s";

/// How long the collector's key wait may run, in `Pg::commit` below: the
/// queue drains as fast as the writers commit.
const LOCK_TIMEOUT_FOR_KEY: &str = "60s";

/// The one decision an open and a preflight both need, made here so
/// the two cannot drift.
pub(crate) enum Pairing {
    /// The marker and the row name one store.
    Agree,
    /// The database names a store the directory has no marker of, and
    /// the schema holds no repos: a first open cut before its marker.
    /// The open adopts the database's store and writes the marker.
    AdoptCut,
    /// Neither side names a store: the first open mints one.
    Fresh,
    /// A refusal, in the open's own words.
    Refuse(Error),
}

/// The marker's pairing with the database's store row. `has_repos`:
/// the schema holds at least one repo row, so a store ever served it -
/// the one meaning the flag carries at every call.
pub(crate) fn pairing_decision(
    marker: Option<[u8; 16]>,
    held: Option<[u8; 16]>,
    has_repos: bool,
) -> Pairing {
    match (marker, held) {
        (Some(marker_uuid), Some(held_uuid)) if marker_uuid == held_uuid => Pairing::Agree,
        (Some(marker_uuid), Some(held_uuid)) => {
            Pairing::Refuse(Error::FailedPrecondition(format!(
                "this data directory belongs to the store whose \
                 UUID is {}, but the database names the store {}: \
                 one was restored over the other; give the \
                 directory of the database's own store, or a \
                 directory the database has never served",
                hex16(&marker_uuid),
                hex16(&held_uuid)
            )))
        }
        (None, Some(held_uuid)) if has_repos => {
            Pairing::Refuse(Error::FailedPrecondition(format!(
                "this database names the store {}, but the data \
                 directory carries no marker of it: give the \
                 directory the database was served with, or a \
                 directory it has never served",
                hex16(&held_uuid)
            )))
        }
        (None, Some(_)) => Pairing::AdoptCut,
        (Some(marker_uuid), None) if !has_repos => {
            Pairing::Refuse(Error::FailedPrecondition(format!(
                "this data directory belongs to the store whose UUID \
                 is {}, but this database holds no store: check \
                 SCION_POSTGRES_URL and its search_path",
                hex16(&marker_uuid)
            )))
        }
        (Some(_) | None, None) => Pairing::Fresh,
    }
}

/// The identity of this database, minted at the first open. The marker
/// file of the data directory carries the same UUID, and an open that
/// finds the two different refuses: a database restored on a directory
/// of another store would name blobs of another disk. The comparison and
/// the mint run before anything else is written, so a refusal leaves the
/// schema untouched.
fn store_identity(writer: &mut PgClient, marker: Marker) -> Result<[u8; 16]> {
    let first = writer.query("SELECT uuid FROM store_uuid WHERE one = 1", &[])?;
    if let Some(row) = first.first() {
        // The database already names its store, and the comparison runs
        // before anything is written: no marker, or the marker of
        // another store, both refuse here. A database restored on the
        // directory of another store would serve metadata whose blobs
        // are not on this disk.
        let uuid = uuid_of(row.get::<_, Vec<u8>>("uuid"))?;
        // The repos probe only decides the no-marker case: a schema
        // that holds repos was served, one that holds none was not.
        let has_repos = !writer.query("SELECT 1 FROM repos LIMIT 1", &[])?.is_empty();
        match pairing_decision(marker.uuid, Some(uuid), has_repos) {
            Pairing::Agree => {}
            Pairing::AdoptCut => {
                // A first open cut between its store's UUID row and
                // its marker: the row commits early in the open, the
                // marker is written at its end, and a kill in that
                // window left a store that could never open again. A
                // schema with no repos row served nothing, so no
                // blob of another disk can be named: the directory
                // takes the database's store as its own, and this
                // open writes the marker the cut fell before.
                tracing::warn!(
                    "the data directory carries no marker of store {}; \
                     the schema holds no repos, so this open takes the \
                     store (a first open cut before its marker)",
                    hex16(&uuid)
                );
            }
            Pairing::Fresh => unreachable!("the database names its store"),
            Pairing::Refuse(error) => return Err(error),
        }
        Ok(uuid)
    } else {
        // A marker that names a store is the identity to adopt: a
        // dump taken before the UUID existed restores next to its
        // own directory, and they agree. But only a schema that
        // holds the store it names: a store that ever served wrote
        // its repo rows, so a schema with no repos row was never
        // served by the directory's store, and adopting would give
        // the marker's identity to an empty schema - a wrong URL in
        // SCION_POSTGRES_URL, a typo in its search_path, or one never
        // served - and every caller would read "repo not found"
        // with no error at the start. An empty schema refuses while
        // the marker names a store, and mints when it names none.
        let has_repos = !writer.query("SELECT 1 FROM repos LIMIT 1", &[])?.is_empty();
        let fresh = match pairing_decision(marker.uuid, None, has_repos) {
            Pairing::Agree | Pairing::AdoptCut => unreachable!("the database holds no store"),
            Pairing::Fresh => marker.uuid.unwrap_or_else(uuid_new),
            Pairing::Refuse(error) => return Err(error),
        };
        writer.query(
            "INSERT INTO store_uuid (one, uuid) VALUES (1, $1) \
             ON CONFLICT (one) DO NOTHING",
            &[P::bytes(&fresh)],
        )?;
        // A concurrent first open may have minted its own: read the
        // row again, and whatever it holds is the identity.
        Ok(uuid_of(
            writer.query("SELECT uuid FROM store_uuid WHERE one = 1", &[])?[0]
                .get::<_, Vec<u8>>("uuid"),
        )?)
    }
}

impl Pg {
    /// Opens the database at `url`, and the blob folder under `data`, as
    /// its server: the one `Pg` that may land. `readers` clients serve
    /// reads beside it. With `ca`, a certificate the server must present
    /// is checked against this CA (PEM); a CA is required for a host that
    /// is not on this machine.
    ///
    /// The open does not move the schema: a database that is behind this
    /// build is refused with the fix named (`scion-server migrate`, or
    /// `serve --migrate`). A fresh database is created, as before.
    ///
    /// # Errors
    ///
    /// A URL that does not parse, a database that does not connect, a
    /// schema of a newer build, a database that is behind this build, or
    /// a migration that fails.
    pub(crate) fn open(
        url: &str,
        ca: Option<&Path>,
        readers: usize,
        data: &Path,
        marker: Marker,
    ) -> Result<Self> {
        Self::open_in(url, ca, readers, data, None, false, marker, None, false)
    }

    /// [`Pg::open`], as the one server process of the database: a second
    /// `serve` or `derive` on the same URL is refused. Import runs beside a
    /// server, so it opens without the lock. This is the migrating shape:
    /// `serve --migrate` runs it, and the schema moves here on purpose.
    ///
    /// # Errors
    ///
    /// As [`Pg::open`], and another process holds the one-server lock.
    pub(crate) fn open_exclusive_migrating(
        url: &str,
        ca: Option<&Path>,
        readers: usize,
        data: &Path,
        marker: Marker,
    ) -> Result<Self> {
        // The schema the URL rides is the connection's own business: the
        // setup asks the database `current_schema()` and locks that, so
        // parallel servers of distinct schemas do not fence each other,
        // and a plain URL fences the whole default schema.
        Self::open_in(url, ca, readers, data, None, true, marker, None, true)
    }

    /// [`Pg::open_exclusive_migrating`], without the right to move the
    /// schema: `serve` and `derive` refuse a database that is behind
    /// this build, with the fix named.
    ///
    /// # Errors
    ///
    /// As [`Pg::open_exclusive_migrating`], and another process holds
    /// the one-server lock.
    pub(crate) fn open_exclusive(
        url: &str,
        ca: Option<&Path>,
        readers: usize,
        data: &Path,
        marker: Marker,
    ) -> Result<Self> {
        Self::open_in(url, ca, readers, data, None, true, marker, None, false)
    }

    /// [`Pg::open`], in the search path of `schema` when it is set.
    #[allow(clippy::unused_self, clippy::too_many_arguments)]
    fn open_in(
        url: &str,
        ca: Option<&Path>,
        readers: usize,
        data: &Path,
        schema: Option<&str>,
        exclusive: bool,
        marker: Marker,
        // The derive walks to run with, or the default cap when `None`.
        // The test constructor of the walk caps passes its own.
        flights: Option<crate::derived::DeriveFlights>,
        // Whether this open may move the schema: true for `serve
        // --migrate`, `migrate`, and the test openers, false for
        // everything else.
        allow_migrate: bool,
    ) -> Result<Self> {
        let options = base_options();
        let fence = Arc::new(Fence::new());
        // The one-server lock of the schema the connection lands in: the
        // setup asks the database `current_schema()` and locks that, so a
        // plain URL and one that rides `--search_path=public` take the
        // same lock, and schemas ride separately. A reader takes none.
        let mut writer = if exclusive {
            connect_exclusive(url, ca, &options, schema, Arc::clone(&fence))?
        } else {
            connect_with(url, ca, &options, schema)?
        };
        let migrated = migrate(&mut writer, allow_migrate)?;
        // The durability of a commit is a promise of this backend: check
        // that the server kept its side of it.
        let on: String = writer.query("SHOW synchronous_commit", &[])?[0].get(0);
        if on != "on" {
            return Err(Error::FailedPrecondition(format!(
                "the database runs synchronous_commit={on}; this backend \
                 promises that a returned commit is durable, so it needs \
                 synchronous_commit=on"
            )));
        }
        // The identity of this database, minted at the first open, and
        // the marker's agreement checked before anything is written: see
        // [`store_identity`].
        let store_uuid = store_identity(&mut writer, marker)?;
        let blobs = Blobs::open(data)?;
        let inner = Arc::new(PgInner {
            writer: Mutex::new(writer),
            fence,
            store_uuid,
            migrated,
            readers: Readers::new_with(readers, url, ca, &options, schema)?,
            blobs,
            writer_gate: Mutex::new(WriterGate::default()),
            writer_woken: Condvar::new(),
            derive: flights.unwrap_or_else(|| {
                crate::derived::DeriveFlights::new(crate::store::DERIVE_WALKS_CAP)
            }),
            view_memo: crate::memory::Bytes::named(
                crate::views::VIEW_MEMO_BYTES * crate::views::VIEW_DERIVES_MAX,
                "view memo",
            ),
            #[cfg(any(test, feature = "test-hooks"))]
            hooks: crate::store::test_hooks::ImportHooks::default(),
            #[cfg(any(test, feature = "test-hooks"))]
            committed: std::sync::atomic::AtomicU64::new(0),
            yield_turns: std::sync::atomic::AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            txes: std::sync::atomic::AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            fail_commits: std::sync::atomic::AtomicU64::new(0),
            metrics: std::sync::OnceLock::new(),
        });
        // The probe of a server that lost its lock: once a minute the
        // session checks itself, through the job queue like every
        // statement, so the lock and the fence are checked on the way.
        // It rides a weak handle: the probe dies with the backend, and
        // holds nothing open.
        if exclusive {
            let probe = Arc::downgrade(&inner);
            std::thread::Builder::new()
                .name("pg-probe".into())
                .spawn(move || {
                    loop {
                        std::thread::sleep(std::time::Duration::from_secs(60));
                        let Some(inner) = probe.upgrade() else {
                            return;
                        };
                        // A dead connection or a fenced store both fail
                        // here and are seen on the next real call; the
                        // probe exists to trip the fence on time. The
                        // lock is taken the way lock_writer takes it: a
                        // poisoned lock must not kill the probe, whose
                        // death would end the fence's clock in silence.
                        let probed = inner
                            .writer
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .query_opt("SELECT 1", &[]);
                        if let Err(error) = probed {
                            tracing::warn!(
                                error = %error,
                                "the write client's keepalive probe failed"
                            );
                        }
                    }
                })
                .map_err(crate::error::Error::Io)?;
        }
        Ok(Self { inner })
    }

    /// The UUID of this database, which the marker file of the data
    /// directory also carries.
    pub(crate) fn store_uuid(&self) -> [u8; 16] {
        self.inner.store_uuid
    }

    /// The schema versions this open moved, when it migrated: the
    /// process that opened records the move in its audit log.
    pub(crate) fn migrated(&self) -> Option<(u32, u32)> {
        self.inner.migrated
    }

    /// Fails when the fence tripped: the process lost the database to
    /// another one, and serves nothing from here on.
    fn check_fenced(&self) -> Result<()> {
        if self.inner.fence.tripped() {
            return Err(Error::FailedPrecondition(
                "this server process lost the database to another server \
                 process; it serves nothing from here on"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Whether this process lost the database to another one: every read
    pub(crate) fn fenced(&self) -> bool {
        self.inner.fence.tripped()
    }

    /// The fence flag, for the health service of a serving process.
    pub(crate) fn fence_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.inner.fence.tripped)
    }

    /// The blob folder.
    #[must_use]
    pub(crate) fn blobs(&self) -> &Blobs {
        &self.inner.blobs
    }

    /// Gives the pool the server's metrics registry. Called once by the
    /// server before it serves; import, fsck, and gc pass none, and the
    /// pool then records nothing.
    pub(crate) fn set_metrics(&self, metrics: Arc<Metrics>) {
        let _ = self.inner.metrics.set(Arc::clone(&metrics));
        self.inner.blobs.set_metrics(Arc::clone(&metrics));
        self.inner.derive.set_metrics(Arc::clone(&metrics));
        self.inner.readers.set_metrics(metrics);
    }

    /// The age of the oldest open read transaction, for the
    /// `scion_pg_tx_age_seconds` gauge. Zero when no read is open.
    #[must_use]
    pub(crate) fn tx_age(&self) -> Duration {
        self.inner
            .readers
            .oldest_open()
            .map(|started| started.elapsed())
            .unwrap_or_default()
    }

    /// A read on the write client, so it sees the caller's own commits.
    /// The land's finish reads now ride the read pool (`read_latest`), so
    /// the lane serves only the writes that hold their ref's slot.
    pub(crate) fn read_unflushed<T>(
        &self,
        f: impl FnOnce(&mut PgTx<'_>) -> Result<T>,
    ) -> Result<T> {
        self.check_fenced()?;
        let (mut writer, _waiting) = self.lock_writer(false);
        let mut pending = Vec::new();
        let mut derived = DerivedPending::default();
        let mut preload = std::collections::HashMap::new();
        let mut history = HistoryPending::default();
        let mut pg = PgTx {
            c: &mut writer,
            gen_cache: std::collections::HashMap::new(),
            inserted_gens: std::collections::HashMap::new(),
            cs_batching: false,
            cs_pending: Some(&mut pending),
            derived_pending: Some(&mut derived),
            derived_preload: Some(&mut preload),
            deferred_deletes: Vec::new(),
            defer_derived: false,
            history_pending: Some(&mut history),
            ended: true,
        };
        f(&mut pg)
    }

    /// A read on one client of the pool, in one READ COMMITTED READ ONLY
    /// transaction with the pooled read's bounds: every statement sees
    /// the newest committed state, and none of them queues behind the
    /// write client. The land's outcome check and plan ride this: their
    /// writes happen later, in their own transaction, and the land row's
    /// pending guard is what stops a twin call, not a shared read.
    ///
    /// READ COMMITTED is the per-statement newest state the land's
    /// outcome check and plan ran under on the writer's autocommit.
    /// READ ONLY turns a write that slips onto this path into an error
    /// (25006): a write on a pooled reader would land outside the
    /// writer's lock and the collector's key, and must not be possible
    /// to add silently. The statement and idle bounds keep an abandoned
    /// read from holding a reader; the transaction rolls back at the
    /// end, like every pooled read.
    ///
    /// # Errors
    ///
    /// What `f` returns, a database error from begin, or
    /// [`Error::Unavailable`] when no reader was free in time.
    pub(crate) fn read_latest<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
        self.check_fenced()?;
        let mut reader = self.inner.readers.take(READER_WAIT)?;
        let out = {
            let mut tx = PgTx::begin_read(
                reader.client(),
                ReadIsolation::ReadCommitted,
                READ_STATEMENT_TIMEOUT,
                READ_IDLE_TIMEOUT,
            )?;
            let out = f(&mut tx);
            // A read transaction: nothing to keep. The rollback's own error
            // must not hide the error of `f`; the drop rolls back anyway.
            let _ = tx.rollback();
            out
        };
        drop(reader);
        out
    }

    /// A strong read on one client of the pool. The read runs in one
    /// transaction, so all its statements see one snapshot; on this backend
    /// every commit is durable when it returns, so what `f` saw is durable.
    /// The transaction is bounded: a statement past
    /// [`READ_STATEMENT_TIMEOUT`] or an idle past [`READ_IDLE_TIMEOUT`]
    /// fails, so an abandoned read cannot hold a reader forever.
    ///
    /// # Errors
    ///
    /// What `f` returns, a database error from begin, or
    /// [`Error::Unavailable`] when no reader was free in time.
    pub(crate) fn read<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
        self.read_bounded(f, READ_STATEMENT_TIMEOUT, READ_IDLE_TIMEOUT, READER_WAIT)
    }

    /// [`Pg::read`], with the bounds in the open: the per-statement
    /// timeout, the idle-in-transaction timeout, and how long a caller
    /// may wait for a reader. The tests use short bounds; the server
    /// uses [`Pg::read`].
    pub(crate) fn read_bounded<T>(
        &self,
        f: impl FnOnce(&mut PgTx<'_>) -> Result<T>,
        statement: Duration,
        idle: Duration,
        wait: Duration,
    ) -> Result<T> {
        self.check_fenced()?;
        let mut reader = self.inner.readers.take(wait)?;
        let out = {
            let mut tx = PgTx::begin_read(
                reader.client(),
                ReadIsolation::RepeatableRead,
                statement,
                idle,
            )?;
            let out = f(&mut tx);
            // A read transaction: nothing to keep. The rollback's own error
            // must not hide the error of `f`; the drop rolls back anyway.
            let _ = tx.rollback();
            out
        };
        drop(reader);
        out
    }

    /// Runs `f` in one write transaction. It commits when `f` returns `Ok`
    /// and rolls back otherwise. A commit is durable when it returns, so
    /// this is also `write`.
    ///
    /// # Errors
    ///
    /// What `f` returns, or a database error from begin or commit.
    pub(crate) fn commit<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
        self.commit_in_lane(f, false)
    }

    /// [`Pg::commit`], as a caller that holds its ref's slot: the
    /// transaction enters the write lane ahead of other waiting
    /// writers, within the lane's bound. What the slot holder writes is
    /// what its ref's next land needs, and its wait costs the whole
    /// queue of its ref, so it stops queueing behind writers it does
    /// not depend on.
    pub(crate) fn commit_for_slot_holder<T>(
        &self,
        f: impl FnOnce(&mut PgTx<'_>) -> Result<T>,
    ) -> Result<T> {
        self.commit_in_lane(f, true)
    }

    fn commit_in_lane<T>(
        &self,
        f: impl FnOnce(&mut PgTx<'_>) -> Result<T>,
        slot_holder: bool,
    ) -> Result<T> {
        self.check_fenced()?;
        let (mut writer, _waiting) = self.lock_writer(slot_holder);
        // A write transaction runs at READ COMMITTED, the Postgres
        // default: every change of the caller checks the newest state of
        // the row it touches (the CAS predicates).
        let mut pending = Vec::new();
        let mut derived = DerivedPending::default();
        let mut preload = std::collections::HashMap::new();
        let mut history = HistoryPending::default();
        let mut tx = PgTx::begin(
            &mut writer,
            &mut pending,
            &mut derived,
            &mut preload,
            &mut history,
            Some(LOCK_TIMEOUT_FOR_KEY),
        )?;
        // Every writer and the collector's batches of one schema order on
        // one advisory key, held to the end of the transaction: the
        // store's write transactions serialize on it.
        //
        // SHORTCUT: one key per schema orders every writer of the schema
        // against one collector, which is right for one server per schema
        // (#178): a test database hosts one store per schema, so tests of
        // different stores do not serialize against each other. Many
        // servers (#47) need a finer design: per-repo keys, or the
        // collector's batches reading a snapshot instead of holding a
        // lock. One key, not one per blob: a transaction that puts
        // thousands of blobs would hold thousands of entries in Postgres's
        // shared lock table and run it out (E53200,
        // `max_locks_per_transaction`). A changeset that names a blob the
        // collector is about to remove
        // commits first, and the collector's re-read pins the blob; a
        // batch of the collector waits for every writer that has begun.
        // The wait ignores the writer's 5 s lock clock: the queue drains
        // as fast as the writers commit, and a writer that gave up would
        // turn healthy contention into Busy for every parallel caller.
        // The 60 s covers only this key: right after the statement, the
        // transaction is back on the writer's clock, so the row locks it
        // meets later still answer Busy at 5 s (SET LOCAL lasts to the
        // end of the transaction).
        // The 60 s guard is in force before the statement that takes
        // the key: the transaction's open batch carries it (BEGIN and
        // the SET LOCAL in one round trip). `lock_timeout` is read when
        // the lock wait begins (the timer starts in the lock's sleep),
        // so the guard cannot ride the same statement as the key: it
        // stays its own statement, before the key's - inside the open.
        //
        // The restore is a data dependency, not a target-list neighbor:
        // it reads FROM the subquery that takes the key, so it cannot
        // evaluate until the key returned, and the wait always ran under
        // the 60 s. OFFSET 0 keeps the planner from flattening the
        // subquery into one target list (tests/pg_guard_timeouts.rs
        // checks the plan's shape and times the wait).
        let key_started = std::time::Instant::now();
        tx.c.query_opt(
            &format!(
                "SELECT set_config('lock_timeout', '{LOCK_TIMEOUT_FOR_WRITERS}', true) \
                 FROM (SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':gc')) \
                       OFFSET 0) AS held"
            ),
            &[],
        )?;
        if let Some(metrics) = self.inner.metrics.get() {
            metrics.collector_key_wait(
                u64::try_from(key_started.elapsed().as_micros()).unwrap_or(u64::MAX),
            );
        }
        match f(&mut tx) {
            Ok(value) => {
                let unflushed = tx.cs_pending.as_deref().is_some_and(|p| !p.is_empty())
                    || tx.derived_pending.as_deref().is_some_and(|p| !p.is_empty());
                #[cfg(any(test, feature = "test-hooks"))]
                if self.inner.fail_commits.load(Ordering::Relaxed) > 0 {
                    self.inner.fail_commits.fetch_sub(1, Ordering::Relaxed);
                    // Counted as ended, as a real failed commit is: the
                    // transaction rolls back below, nothing was written,
                    // exactly as a busy commit ends.
                    self.inner.committed.fetch_add(1, Ordering::Release);
                    let _ = tx.rollback();
                    let fail =
                        std::io::Error::other("the commit was made to fail by the test hook");
                    return Err(Error::Storage(Storage::busy(fail)));
                }
                if unflushed {
                    // A caller that turned the importer's batching on and
                    // ended the transaction without the flush would
                    // commit changesets with no parents and no repo
                    // membership: rows that name changesets no read can
                    // reach. The transaction rolls back and refuses, so
                    // the bug surfaces at the caller that forgot the
                    // flush, not as missing rows later.
                    tracing::error!(
                        pending = tx.cs_pending.as_ref().map_or(0, |p| p.len())
                            + tx.derived_pending.as_ref().map_or(0, |p| p.len()),
                        "the transaction holds changeset rows it never flushed; \
                         the transaction rolls back"
                    );
                    let _ = tx.rollback();
                    return Err(Error::FailedPrecondition(
                        "the transaction holds changeset rows it never flushed".to_owned(),
                    ));
                }
                #[cfg(any(test, feature = "test-hooks"))]
                self.inner.committed.fetch_add(1, Ordering::Release);
                // A write path that deferred its per-row flushes hands
                // them to the commit, inside the transaction, so a
                // rollback takes them with it and the end state is the
                // one the per-call writes would have reached.
                if tx.defer_derived {
                    tx.flush_deferred()?;
                }
                let committed = tx.commit();
                #[cfg(any(test, feature = "test-hooks"))]
                self.inner.txes.fetch_add(1, Ordering::Relaxed);
                committed?;
                Ok(value)
            }
            Err(error) => {
                // The rollback's own error must not hide the error of `f`;
                // the drop rolls back anyway.
                let _ = tx.rollback();
                Err(error)
            }
        }
    }

    /// [`Pg::commit`]: on this backend, durable on return.
    ///
    /// # Errors
    ///
    /// As [`Pg::commit`].
    pub(crate) fn write<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
        self.commit(f)
    }

    /// Commits made so far: the seam the tests of the batching watch.
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub(crate) fn commits(&self) -> u64 {
        self.inner.committed.load(Ordering::Acquire)
    }

    /// Write transactions run so far: the batching tests' counter.
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub(crate) fn txes(&self) -> u64 {
        self.inner.txes.load(Ordering::Relaxed)
    }

    /// Sets how many of the next commits fail as a busy database: the
    /// transaction runs and rolls back, so the retry re-runs it whole.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn fail_next_commits(&self, n: u32) {
        self.inner
            .fail_commits
            .store(u64::from(n), Ordering::Relaxed);
    }

    /// Takes the write client, counted as a writer that waits: the
    /// derivation walk waits while writers wait, so a steady stream of
    /// writers delays a cold read up to its deadline, never a land
    /// ([#142]). The count falls when the client is back, so a walk step
    /// that ends hands the client to a writer, not to its own next step.
    /// Takes the write client for one transaction, through the lane:
    /// `slot_holder` marks a caller that holds its ref's slot (the
    /// settle's commit and the sweep's reject), which enters ahead of
    /// other waiting writers, within the lane's bound. The invariants
    /// the one client keeps are listed on [`PgInner`'s gate
    /// field](Self::writer_gate).
    fn lock_writer(&self, slot_holder: bool) -> (MutexGuard<'_, PgClient>, PgWriteWaiter<'_>) {
        let mut gate = self
            .inner
            .writer_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        gate.waiting += 1;
        if slot_holder {
            gate.holders_waiting += 1;
        } else {
            gate.plain_waiting += 1;
        }
        while gate.taken || !lane_admits(&gate, slot_holder) {
            gate = self
                .inner
                .writer_woken
                .wait(gate)
                .unwrap_or_else(PoisonError::into_inner);
        }
        gate.taken = true;
        if slot_holder {
            gate.holders_waiting -= 1;
            gate.holders_ahead += 1;
        } else {
            gate.plain_waiting -= 1;
            gate.holders_ahead = 0;
        }
        let writer = self
            .inner
            .writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        (
            writer,
            PgWriteWaiter(&self.inner.writer_gate, &self.inner.writer_woken),
        )
    }

    /// The walks in flight of this backend's derivations.
    pub(crate) fn derive_flights(&self) -> &crate::derived::DeriveFlights {
        &self.inner.derive
    }

    /// The pool the view derivations reserve their memos from: the cap is
    /// [`crate::views::VIEW_DERIVES_MAX`] reservations of
    /// [`crate::views::VIEW_MEMO_BYTES`], and a derivation beyond it waits
    /// for a place under its call's deadline.
    pub(crate) fn view_memo_budget(&self) -> &crate::memory::Bytes {
        &self.inner.view_memo
    }

    /// How many writers wait for the write client right now.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn writers_waiting(&self) -> usize {
        self.inner
            .writer_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .waiting
    }

    /// How many deadline-less waits for writers ended.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn yield_turns(&self) -> u64 {
        self.inner.yield_turns.load(Ordering::Relaxed)
    }

    /// The lane's queue, for the tests: the slot holders and the other
    /// writers that wait, not granted yet.
    #[cfg(test)]
    #[allow(dead_code)] // the lane tests read it through Backend
    #[must_use]
    pub(crate) fn writer_lane_waiting(&self) -> (usize, usize) {
        let gate = self
            .inner
            .writer_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        (gate.holders_waiting, gate.plain_waiting)
    }

    /// Waits while writers wait for the write client, until the deadline.
    ///
    /// Without a deadline the yield to writers is bounded: after
    /// [`WRITE_YIELD_BOUND`] in total, across every wake-up, the caller
    /// takes its turn, and the writers wait behind at most one of its
    /// transactions. The bound is for the whole wait, not per wake-up: a
    /// writer stream that keeps the count above zero only between two
    /// wake-ups would otherwise reset the wait forever.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DeadlineExceeded`] once the deadline has passed.
    pub(crate) fn wait_for_writers(
        &self,
        deadline: Option<&crate::deadline::Deadline>,
    ) -> Result<()> {
        let mut gate = self
            .inner
            .writer_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(deadline) = deadline else {
            let yielded_since = std::time::Instant::now();
            while gate.waiting > 0 {
                let bound_left = WRITE_YIELD_BOUND.saturating_sub(yielded_since.elapsed());
                if bound_left.is_zero() {
                    // The yield's bound passed with writers still waiting:
                    // this caller takes its turn. The writers it passes
                    // wait behind at most one of its transactions.
                    self.inner.yield_turns.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                let (guard, timed_out) = self
                    .inner
                    .writer_woken
                    .wait_timeout_while(gate, bound_left, |gate| gate.waiting > 0)
                    .unwrap_or_else(PoisonError::into_inner);
                gate = guard;
                if timed_out.timed_out() {
                    self.inner.yield_turns.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
            }
            self.inner.yield_turns.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        };
        while gate.waiting > 0 {
            let budget = deadline.remaining();
            let (guard, timed_out) = self
                .inner
                .writer_woken
                .wait_timeout_while(gate, budget, |gate| gate.waiting > 0)
                .unwrap_or_else(PoisonError::into_inner);
            gate = guard;
            if timed_out.timed_out() {
                return Err(Error::DeadlineExceeded(
                    "the call ran past its deadline".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

/// How long a call without a deadline yields to waiting writers before
/// it takes its turn: the derivation walk's yield keeps #142's promise
/// (writers first) for this long per step, then the step proceeds, and
/// the writers it passed wait behind at most one step's transaction.
const WRITE_YIELD_BOUND: Duration = Duration::from_secs(2);

/// The count of a taken write client, while it is taken: it falls, and
/// wakes the waiters, when the client is back.
// SHORTCUT: one condvar, notify_all, so a release wakes every waiting
// writer and a commit costs O(waiters) wake-ups. Per-class condvars if
// waiting writers often pass ~50.
struct PgWriteWaiter<'a>(&'a Mutex<WriterGate>, &'a Condvar);

impl Drop for PgWriteWaiter<'_> {
    fn drop(&mut self) {
        let mut gate = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        gate.taken = false;
        gate.waiting -= 1;
        self.1.notify_all();
    }
}

/// The derived rows a batching transaction holds for the flush.
type DerivedPending = Vec<(String, i64, ChangesetId, Vec<u8>)>;

/// The derived values of the chains a batching transaction preloaded, by
/// (kind, version, id).
type DerivedPreload = std::collections::HashMap<(String, i64, Vec<u8>), Vec<u8>>;

/// The history nodes a batching transaction holds for the flush, by
/// id, with their bytes. The nodes are content-addressed, so a put of
/// a node the transaction already holds is the same row. The count
/// rides the map, so every view of the buffer counts the same, and
/// the flush can run mid-transaction when the count passes its bound.
#[derive(Default)]
struct HistoryPending {
    rows: std::collections::HashMap<Vec<u8>, Vec<u8>>,
    bytes: usize,
}

/// The isolation level of one read transaction. Repeatable read pins
/// one snapshot for the whole read; read committed takes the newest
/// state of each statement. The choices are named, not passed as
/// strings, so a call site says what its read sees.
#[derive(Clone, Copy)]
enum ReadIsolation {
    RepeatableRead,
    ReadCommitted,
}

impl ReadIsolation {
    fn as_sql(self) -> &'static str {
        match self {
            Self::RepeatableRead => "REPEATABLE READ",
            Self::ReadCommitted => "READ COMMITTED",
        }
    }
}

/// One client of the database, inside one transaction. The queries are its
/// [`MetaTx`] methods. The transaction is begun and ended with statements
/// of the client, so a `PgTx` is a plain client handle: it stays covariant
/// in its lifetime, which the backend enum's transaction needs.
///
/// `pub` with the unreachable-pub allow: [`Meta::Tx`] names it, and the
/// trait re-exports through `test_support::meta`.
#[allow(unreachable_pub)]
pub struct PgTx<'a> {
    c: &'a mut PgClient,
    /// The generations this transaction has already read. A changeset's
    /// generation is fixed at insert and never updated, so one read per
    /// transaction is one round trip saved for every later read of the
    /// same changeset - the derived walks and the land ask for the same
    /// few ids again and again.
    gen_cache: std::collections::HashMap<(i64, Vec<u8>), i64>,
    /// The generations this transaction has inserted, by id: the reads of
    /// the same transaction (the derived writes of the commit it just
    /// stored) must see them before anything else does. The generations
    /// are fixed at insert and never updated.
    inserted_gens: std::collections::HashMap<Vec<u8>, i64>,
    /// Whether the importer's batching is on: `insert_changeset` then
    /// writes the changesets row only, and the parents and the repo
    /// membership wait in `cs_pending` for the flush at the transaction's
    /// end, which writes them one statement per 1,000 rows. The
    /// changesets row cannot wait: the derived rows of the same commit
    /// name it by a foreign key that checks at the statement.
    cs_batching: bool,
    /// The changesets the transaction inserted while batching: their
    /// parents and repo membership rows wait for the flush at the
    /// transaction's end, which writes them one statement per 1,000
    /// rows. A crash loses whole commits either way: the rows land in
    /// the caller's transaction, before its commit. The list is shared
    /// with the transaction's reborrows (the backend's transaction
    /// wrapper runs the caller's closure on one), so the commit path of
    /// the original transaction sees what they inserted and refuses to
    /// commit without the flush. `None` on a read transaction, which
    /// inserts nothing.
    cs_pending: Option<&'a mut Vec<(ChangesetId, Vec<ChangesetId>)>>,
    /// The derived rows the transaction inserted while batching: the
    /// flush writes them one statement per 1,000 rows, after the
    /// changesets they name. Shared with the reborrows, like the
    /// changesets' pending list.
    derived_pending: Option<&'a mut DerivedPending>,
    /// The derived values of the chains the importer preloaded, by
    /// (kind, version, id): the batch's reads of ancestors outside the
    /// transaction cost one chain read for the whole chain, not one per
    /// commit. Shared with the reborrows.
    derived_preload: Option<&'a mut DerivedPreload>,
    /// The `delete_derived_below` calls a deferring write path made: the
    /// commit deletes with one statement, honoring the summed limits per
    /// kind and version. Only `defer_derived_deletes` fills it.
    deferred_deletes: Vec<(String, i64, i64)>,
    /// Whether the write path's per-row flushes wait for the commit:
    /// `delete_derived_below` collects its calls, and history node
    /// writes collect in the buffer the commit writes with one
    /// statement. Off for the importer's batching (its flush owns the
    /// shapes) and for the sweeps, which count the rows they removed.
    defer_derived: bool,
    /// The history nodes the transaction inserted while batching: the
    /// flush writes them after the derived rows. Shared with the
    /// reborrows; the bytes ride the map.
    history_pending: Option<&'a mut HistoryPending>,
    /// The transaction was ended by `commit` or `rollback`; a drop then
    /// leaves the client alone.
    ended: bool,
}

/// The bytes of history nodes one transaction's buffer may hold before
/// the flush writes them mid-transaction. A transaction whose writes
/// pack many commits - the archive restore packs up to 64 MiB of
/// changeset bodies, tens of thousands of commits with a few nodes
/// each - would otherwise hold the whole history's nodes in RAM until
/// the commit, hundreds of MB, and copy them once more into the
/// flush's chunk arrays. At the bound the flush runs inside the
/// transaction: the rows land in the table, this transaction's reads
/// of them read the table, and the commit writes only what came after.
const HISTORY_PENDING_MAX_BYTES: usize = 16 << 20;

#[cfg(any(test, feature = "test-hooks"))]
static HISTORY_PENDING_MAX_BYTES_OVERRIDE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// The bound, as the tests' override holds it when one does.
fn history_pending_max_bytes() -> usize {
    #[cfg(any(test, feature = "test-hooks"))]
    {
        let small = HISTORY_PENDING_MAX_BYTES_OVERRIDE.load(Ordering::Relaxed);
        if small != 0 {
            return small;
        }
    }
    HISTORY_PENDING_MAX_BYTES
}

/// The most bytes the buffer held at once. The restore test reads it:
/// a restore that packs many commits per transaction must not hold the
/// whole history's nodes.
#[cfg(any(test, feature = "test-hooks"))]
static HISTORY_PENDING_PEAK: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Holds a small buffer bound for the test that pins the restore's
/// flush, and resets the peak. The guard puts the server's bound back.
#[cfg(any(test, feature = "test-hooks"))]
pub fn test_set_history_pending_bound_for_tests(bytes: usize) -> impl Drop {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            HISTORY_PENDING_MAX_BYTES_OVERRIDE.store(0, Ordering::Relaxed);
        }
    }
    HISTORY_PENDING_MAX_BYTES_OVERRIDE.store(bytes, Ordering::Relaxed);
    HISTORY_PENDING_PEAK.store(0, Ordering::Relaxed);
    Reset
}

/// The most bytes the buffer held since the last reset.
#[cfg(any(test, feature = "test-hooks"))]
#[must_use]
pub fn test_history_pending_peak_bytes() -> usize {
    HISTORY_PENDING_PEAK.load(Ordering::Relaxed)
}

/// The length of the prefix of one kind's rows that the transaction's
/// deletes take: each call, in call order, adds what its step leaves of
/// the rows its version still sees, after the calls before it took
/// theirs from the same low end. Returns one (kind, total) per kind
/// with calls, in first-call order, for the flush's one delete
/// statement. `belows` is one count per call, in call order: how many
/// rows of the kind were still below the call's version.
fn delete_prefixes(calls: &[(String, i64)], belows: &[i64]) -> (Vec<String>, Vec<i64>) {
    let mut kinds: Vec<String> = Vec::new();
    let mut totals: Vec<i64> = Vec::new();
    for ((kind, step), below) in calls.iter().zip(belows) {
        let i = if let Some(i) = kinds.iter().position(|k| k == kind) {
            i
        } else {
            kinds.push(kind.clone());
            totals.push(0);
            totals.len() - 1
        };
        totals[i] += (*step).min((*below - totals[i]).max(0));
    }
    (kinds, totals)
}

impl<'a> PgTx<'a> {
    /// The pending-lands quota counts new pending rows. A retry of an
    /// existing key resumes its land and adds no row, so a client can
    /// always finish what it started. The count and the insert need no
    /// lock of their own: every write transaction of the schema holds
    /// the schema's one advisory key (see `commit`) until it commits, so
    /// two lands that record at the same time, even from two processes,
    /// run one after the other, and the second counts the first one's
    /// row. The one writer serializes them.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceExhausted`] when the principal is over its cap.
    fn check_pending_lands_quota(&mut self, caller: &Caller, limit: Option<usize>) -> Result<()> {
        let Some(limit) = limit else {
            return Ok(());
        };
        let pending: i64 = self
            .c
            .query_opt(
                "SELECT count(*) FROM lands WHERE caller = $1 AND state = 'pending'",
                &[P::text(&caller.0)],
            )?
            .and_then(|row| row.get(0))
            .unwrap_or(0);
        if pending >= i64::try_from(limit).unwrap_or(i64::MAX) {
            return Err(Error::ResourceExhausted(format!(
                "principal {:?} has its {limit} lands pending \
                 (--max-principal-pending-lands {limit}); wait for one to finish",
                caller.0
            )));
        }
        Ok(())
    }

    /// Writes the per-row flushes a deferring write path collected: the
    /// history nodes the build wrote, one statement per 1,000 rows, and
    /// the derived old-row deletes, one query for the counts and one
    /// statement for the whole transaction. The deletes of one kind, in
    /// call order, take a prefix of the kind's rows: each call takes up
    /// to its step from the rows still there below its version, and the
    /// calls before it took theirs from the same low end. The prefix's
    /// length folds over the calls in order, so the end state is what
    /// the per-call deletes would have reached, in order, for any mix
    /// of kinds and versions - the per-call deletes pick the same low
    /// end, by version and changeset. Runs inside the transaction at
    /// its commit, so a rollback takes the writes with it.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn flush_deferred(&mut self) -> Result<()> {
        self.flush_history_rows()?;
        if self.deferred_deletes.is_empty() {
            return Ok(());
        }
        let kinds: Vec<String> = self
            .deferred_deletes
            .iter()
            .map(|(k, _, _)| k.clone())
            .collect();
        let versions: Vec<i64> = self.deferred_deletes.iter().map(|(_, v, _)| *v).collect();
        let steps: Vec<i64> = self.deferred_deletes.iter().map(|(_, _, s)| *s).collect();
        // One version per kind, the case every caller is in today: the
        // one statement ranks each row once against its kind's single
        // call and cuts at the step, as it has since the deferral. Only
        // a kind that repeats in the batch needs the fold, and no caller
        // reaches that today (#855); the lands' settle statement bound
        // counts on this shape staying one statement.
        let repeats = {
            let mut seen = std::collections::HashSet::new();
            kinds.iter().any(|k| !seen.insert(k))
        };
        if !repeats {
            let deleted = self.c.execute(
                "WITH k AS (
                     SELECT kind, version, SUM(step) AS step
                     FROM unnest($1::text[], $2::int8[], $3::int8[]) AS t(kind, version, step)
                     GROUP BY kind, version),
                 v AS (
                     SELECT d.ctid AS ctid, k.step AS step,
                            row_number() OVER (PARTITION BY d.kind
                                               ORDER BY d.version) AS rn
                     FROM derived d JOIN k ON d.kind = k.kind
                                                            AND d.version < k.version)
                 DELETE FROM derived WHERE ctid IN (SELECT ctid FROM v WHERE rn <= step)",
                &[
                    P::text_array(kinds),
                    P::i64_array(versions),
                    P::i64_array(steps),
                ],
            )?;
            if deleted > 0 {
                tracing::debug!(deleted, "flushed the transaction's derived deletes");
            }
            return Ok(());
        }
        // A kind repeated: its calls, in call order, take a prefix of
        // the kind's rows - each call takes up to its step from the rows
        // still there below its version, from the same low end the
        // per-call deletes pick. How many rows each call still had below
        // its version, one row per call in call order:
        let belows = self.c.query(
            "SELECT c.kind, count(d.cs)
             FROM unnest($1::text[], $2::int8[]) WITH ORDINALITY AS c(kind, version, i)
             LEFT JOIN derived d ON d.kind = c.kind AND d.version < c.version
             GROUP BY c.i, c.kind ORDER BY c.i",
            &[P::text_array(kinds.clone()), P::i64_array(versions)],
        )?;
        let belows: Vec<i64> = belows
            .iter()
            .map(|row| {
                row.try_get::<_, i64>(1).map_err(|error| {
                    Error::Corrupt(format!("the flush's count is no number: {error}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let calls: Vec<(String, i64)> = kinds.iter().cloned().zip(steps).collect();
        let (kinds, totals) = delete_prefixes(&calls, &belows);
        let deleted = self.c.execute(
            "WITH rank AS (
                 SELECT d.ctid, t.total,
                        row_number() OVER (PARTITION BY d.kind
                                           ORDER BY d.version, d.cs) AS rn
                 FROM derived d
                 JOIN unnest($1::text[], $2::int8[]) AS t(kind, total)
                                                      ON d.kind = t.kind)
             DELETE FROM derived WHERE ctid IN (SELECT ctid FROM rank WHERE rn <= total)",
            &[P::text_array(kinds), P::i64_array(totals)],
        )?;
        if deleted > 0 {
            tracing::debug!(deleted, "flushed the transaction's derived deletes");
        }
        Ok(())
    }

    /// Writes the history nodes the transaction inserted while batching,
    /// after the derived rows and before the `git_import` rows: one
    /// statement per 1,000 rows. The nodes are content-addressed, so
    /// their order does not matter; the ids are sorted for a
    /// deterministic statement.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn flush_history_rows(&mut self) -> Result<()> {
        const CHUNK: usize = 1_000;
        let Some(history) = self.history_pending.as_deref_mut() else {
            return Ok(());
        };
        let mut node_rows: Vec<(Vec<u8>, Vec<u8>)> = history.rows.drain().collect();
        history.bytes = 0;
        node_rows.sort();
        for chunk in node_rows.chunks(CHUNK) {
            let ids = chunk.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
            let bodies = chunk
                .iter()
                .map(|(_, body)| body.clone())
                .collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO history_nodes (id, body)
                 SELECT * FROM unnest($1::bytea[], $2::bytea[])
                 ON CONFLICT (id) DO NOTHING",
                &[P::bytea_array(ids), P::bytea_array(bodies)],
            )?;
            #[cfg(test)]
            {
                let bytes: usize = chunk.iter().map(|(_, body)| body.len()).sum();
                crate::history::note_flushed_node_bytes(bytes);
            }
        }
        Ok(())
    }

    /// Begins one transaction on the client, sharing `pending` with the
    /// caller, which flushes it and refuses to commit while it holds rows.
    /// `lock_timeout` rides the BEGIN when the transaction's first lock
    /// wait needs a guard of its own (the collector's key): one round
    /// trip, and the guard is in force before the key's statement, which
    /// `lock_timeout`'s read-at-wait rule requires.
    fn begin(
        c: &'a mut PgClient,
        pending: &'a mut Vec<(ChangesetId, Vec<ChangesetId>)>,
        derived: &'a mut DerivedPending,
        preload: &'a mut DerivedPreload,
        history: &'a mut HistoryPending,
        lock_timeout: Option<&str>,
    ) -> Result<PgTx<'a>> {
        open_transaction(
            c,
            &match lock_timeout {
                Some(v) => format!("BEGIN; SET LOCAL lock_timeout = '{v}';"),
                None => "BEGIN;".to_owned(),
            },
        )?;
        Ok(Self {
            c,
            gen_cache: std::collections::HashMap::new(),
            inserted_gens: std::collections::HashMap::new(),
            cs_batching: false,
            cs_pending: Some(pending),
            derived_pending: Some(derived),
            derived_preload: Some(preload),
            deferred_deletes: Vec::new(),
            defer_derived: false,
            history_pending: Some(history),
            ended: false,
        })
    }

    /// Begins one read-only transaction at the level the caller names:
    /// every statement of a repeatable-read read sees one snapshot; a
    /// read-committed read sees the newest state of each statement. The
    /// transaction is bounded server side:
    /// one statement may take `statement`, and the transaction may sit
    /// idle between statements at most `idle` (the timeouts are
    /// `SET LOCAL`, so they end with the transaction, and a stream that
    /// stopped reading cannot hold the reader forever).
    //
    // SHORTCUT: every read but `read_latest` runs at repeatable read,
    // one snapshot per read, even though reads that touch only immutable
    // rows would be safe at read committed with a shorter snapshot hold.
    // Nobody has measured the gain, and the split needs an audit of
    // every walk that re-reads a ref. Measure first, then split.
    fn begin_read(
        c: &'a mut PgClient,
        isolation: ReadIsolation,
        statement: Duration,
        idle: Duration,
    ) -> Result<PgTx<'a>> {
        // One round trip for the open: the three statements are
        // parameterless strings (the durations are interpolated), and the
        // client's prepared statements take no second command (E42601) -
        // the batch is the simple protocol, which takes many. A batch
        // whose SET LOCAL fails after its BEGIN ran leaves the session
        // inside an aborted transaction: `open_transaction` rolls that
        // back before it returns the error, so the pooled client is
        // clean for its next holder.
        open_transaction(
            c,
            &format!(
                "BEGIN TRANSACTION ISOLATION LEVEL {} READ ONLY; \
                 SET LOCAL statement_timeout = {}; \
                 SET LOCAL idle_in_transaction_session_timeout = {};",
                isolation.as_sql(),
                statement.as_millis(),
                idle.as_millis()
            ),
        )?;
        Ok(Self {
            c,
            gen_cache: std::collections::HashMap::new(),
            inserted_gens: std::collections::HashMap::new(),
            cs_batching: false,
            cs_pending: None,
            derived_pending: None,
            derived_preload: None,
            deferred_deletes: Vec::new(),
            defer_derived: false,
            history_pending: None,
            ended: false,
        })
    }

    /// Reads back the land another writer recorded for this request key
    /// first: its land wins, and its input must match this one's.
    /// The repo's id and the land a request key recorded, in one read:
    /// the record's first statement names the repo and the key together.
    /// A repo with no such land answers its id and no land; a repo that
    /// does not exist answers [`Error::NotFound`].
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] when no repository has that name;
    /// [`Error::InvalidArgument`] when the key named a different land;
    /// [`Error::Corrupt`] when the row's bytes do not decode.
    fn repo_and_land_by_key(
        &mut self,
        caller: &Caller,
        req: &LandRequest,
    ) -> Result<(RepoId, Option<LandOutcome>)> {
        let row = self
            .c
            .query_opt(
                "SELECT r.repo_id, l.land_id, l.input_cs, l.target_ref, l.state,
                        l.detail, l.result_cs, l.conflict_paths, l.reject_reason
                 FROM repos r
                 LEFT JOIN lands l
                   ON l.repo_id = r.repo_id AND l.caller = $2 AND l.request_key = $3
                 WHERE r.name = $1",
                &[
                    P::text(&req.repo),
                    P::text(&caller.0),
                    P::text(&req.request_key),
                ],
            )?
            .map(|row| {
                (
                    row.get::<_, i64>(0),
                    row.get::<_, Option<i64>>(1),
                    row.get::<_, Option<Vec<u8>>>(2),
                    row.get::<_, Option<String>>(3),
                    row.get::<_, Option<String>>(4),
                    row.get::<_, Option<String>>(5),
                    row.get::<_, Option<Option<Vec<u8>>>>(6),
                    row.get::<_, Option<Vec<u8>>>(7),
                    row.get::<_, Option<String>>(8),
                )
            })
            .ok_or_else(|| Error::NotFound(format!("repo {:?}", req.repo)))?;
        let (repo_id, land_id, input, target, state, detail, result_cs, conflict_paths, reason) =
            row;
        let repo = RepoId(repo_id);
        let Some(land_id) = land_id else {
            return Ok((repo, None));
        };
        // A LEFT JOIN's land columns are all set or all null together: the
        // row exists only when the join found the land.
        let input = input.ok_or_else(|| {
            Error::Corrupt(format!(
                "land {land_id} of repo {} lost its columns",
                repo.0
            ))
        })?;
        let target = target.ok_or_else(|| {
            Error::Corrupt(format!(
                "land {land_id} of repo {} lost its columns",
                repo.0
            ))
        })?;
        let state = state.ok_or_else(|| {
            Error::Corrupt(format!(
                "land {land_id} of repo {} lost its columns",
                repo.0
            ))
        })?;
        let detail = detail.unwrap_or_default();
        let reason = reason.unwrap_or_default();
        let conflict_paths = conflict_paths.unwrap_or_default();
        if input != req.changeset.as_bytes() || target != req.target_ref {
            return Err(Error::InvalidArgument(format!(
                "request key {:?} was already used for a different land",
                req.request_key
            )));
        }
        // The join wraps the column's own null in the row's presence: one
        // flatten answers the column, which the early return above already
        // proved present.
        let result = result_cs
            .flatten()
            .map(|bytes| ChangesetId::try_from(bytes.as_slice()))
            .transpose()
            .map_err(|e| Error::Corrupt(format!("land {land_id} result: {e}")))?;
        Ok((
            repo,
            Some(LandOutcome {
                moved: false,
                land_id,
                state: LandState::parse(&state)?,
                result,
                previous: None,
                detail,
                conflict_paths: decode_paths(&conflict_paths)
                    .ok_or_else(|| Error::Corrupt(format!("land {land_id} conflict paths")))?,
                reject_reason: LandRejectReason::parse(&reason)?,
            }),
        ))
    }

    /// Reads the land a request key recorded back, for the ON CONFLICT
    /// race: a twin recorded the same key in the same instant, so this
    /// insert wrote nothing and the twin's row is the answer.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] when the row vanished or its bytes do not
    /// decode.
    fn read_back_land(
        &mut self,
        repo: RepoId,
        caller: &Caller,
        req: &LandRequest,
    ) -> Result<LandOutcome> {
        self.land_by_key(repo, caller, req)?.ok_or_else(|| {
            Error::Corrupt(format!(
                "land {:?} of {:?} was recorded by another process and \
                 then vanished",
                req.request_key, caller.0
            ))
        })
    }

    /// The land a request key recorded, as its row answers it: the
    /// record's first read and the read-back share this one shape.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] when the key named a different land;
    /// [`Error::Corrupt`] when the row's bytes do not decode.
    fn land_by_key(
        &mut self,
        repo: RepoId,
        caller: &Caller,
        req: &LandRequest,
    ) -> Result<Option<LandOutcome>> {
        let row = self
            .c
            .query_opt(
                "SELECT land_id, input_cs, target_ref, state, detail, result_cs,
                        conflict_paths, reject_reason
                 FROM lands
                 WHERE repo_id = $1 AND caller = $2 AND request_key = $3",
                &[
                    P::i64(repo.0),
                    P::text(&caller.0),
                    P::text(&req.request_key),
                ],
            )?
            .map(|row| {
                (
                    row.get::<_, i64>(0),
                    row.get::<_, Vec<u8>>(1),
                    row.get::<_, String>(2),
                    row.get::<_, String>(3),
                    row.get::<_, String>(4),
                    row.get::<_, Option<Vec<u8>>>(5),
                    row.get::<_, Vec<u8>>(6),
                    row.get::<_, String>(7),
                )
            });
        let Some((land_id, input, target, state, detail, result_cs, conflict_paths, reason)) = row
        else {
            return Ok(None);
        };
        if input != req.changeset.as_bytes() || target != req.target_ref {
            return Err(Error::InvalidArgument(format!(
                "request key {:?} was already used for a different land",
                req.request_key
            )));
        }
        let result = result_cs
            .map(|bytes| ChangesetId::try_from(bytes.as_slice()))
            .transpose()
            .map_err(|e| Error::Corrupt(format!("land {land_id} result: {e}")))?;
        Ok(Some(LandOutcome {
            moved: false,
            land_id,
            state: LandState::parse(&state)?,
            result,
            previous: None,
            detail,
            conflict_paths: decode_paths(&conflict_paths)
                .ok_or_else(|| Error::Corrupt(format!("land {land_id} conflict paths")))?,
            reject_reason: LandRejectReason::parse(&reason)?,
        }))
    }

    /// Records a land this record starts: the state the ref's existence
    /// gives it, the quota, the insert, the expiry. A row this insert
    /// wrote answers from its own columns; a twin that recorded the same
    /// key in the same instant answers from its read-back.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn record_new_land(
        &mut self,
        repo: RepoId,
        caller: &Caller,
        req: &LandRequest,
        pending_limit: Option<usize>,
    ) -> Result<LandOutcome> {
        // A land to a ref that does not exist is rejected at once.
        let detail = format!("ref {:?} does not exist", req.target_ref);
        let (state, detail, reason) = if self.get_ref(repo, &req.target_ref)?.is_some() {
            (
                LandState::Pending,
                String::new(),
                LandRejectReason::Unspecified,
            )
        } else {
            (LandState::Rejected, detail, LandRejectReason::MissingRef)
        };
        self.check_pending_lands_quota(caller, pending_limit)?;
        let inserted = self
            .c
            .query_opt(
                "INSERT INTO lands
                     (repo_id, caller, request_key, target_ref, input_cs, state, detail,
                      reject_reason, finished_at, created_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                         CASE WHEN $6 = 'pending' THEN NULL ELSE now_epoch() END,
                         now_epoch())
                     ON CONFLICT DO NOTHING
                     RETURNING land_id",
                &[
                    P::i64(repo.0),
                    P::text(&caller.0),
                    P::text(&req.request_key),
                    P::text(&req.target_ref),
                    P::bytes(&id_bytes(req.changeset.as_bytes())),
                    P::text(state.as_str()),
                    P::text(&detail),
                    P::text(reason.as_str()),
                ],
            )?
            .map(|row| row.get::<_, i64>(0));
        let recorded = match inserted {
            Some(land_id) => {
                // A row this record wrote: nothing is settled yet.
                LandOutcome {
                    moved: false,
                    land_id,
                    state,
                    result: None,
                    previous: None,
                    detail,
                    conflict_paths: Vec::new(),
                    reject_reason: reason,
                }
            }
            None => {
                // A twin recorded the same key in the same instant: read
                // its row back.
                self.read_back_land(repo, caller, req)?
            }
        };
        self.expire_lands()?;
        Ok(recorded)
    }

    /// Commits the transaction. The client runs again afterwards.
    fn commit(&mut self) -> Result<()> {
        self.ended = true;
        self.c.execute("COMMIT", &[])?;
        Ok(())
    }

    /// Rolls the transaction back. The client runs again afterwards.
    fn rollback(&mut self) -> Result<()> {
        if self.ended {
            return Ok(());
        }
        self.ended = true;
        self.c.execute("ROLLBACK", &[])?;
        Ok(())
    }
}

impl Drop for PgTx<'_> {
    fn drop(&mut self) {
        // A transaction that its caller did not end rolls back.
        if !self.ended {
            let _ = self.c.execute("ROLLBACK", &[]);
        }
    }
}

/// Connects one client of `url`. TLS follows the rule of the gRPC client: a
/// host on this machine may go without TLS; a host off it must present a
/// certificate that `ca` signed.
/// The session setup a worker applies to its client: the schema it runs
/// in, and, for the one server process of a database, the advisory lock
/// that refuses a second one.
enum SetupOutcome {
    /// The schema was set, and the lock, if one was asked for, was given.
    Applied,
    /// The database answered and refused the one-server lock: another
    /// session holds it.
    LockRefused,
    /// The URL's `search_path` names no existing schema: the database
    /// answered, only the schema is missing. A refusal, not an
    /// unreachable: no retry can fix the URL.
    NoSchema,
    /// The database could not be reached, or refused the schema
    /// statements: nothing is known about the lock.
    Unreachable,
}

struct Setup {
    schema: Option<String>,
    /// Whether this session takes the one-server lock of the schema it
    /// lands in (an exclusive writer; None-equivalent for a reader, an
    /// import).
    exclusive: bool,
    outcome: SetupOutcome,
}

impl Setup {
    /// Applies the setup to a fresh client. A failed setup leaves the
    /// statements failed until a reconnect applies it again.
    fn new(schema: Option<&str>, exclusive: bool, client: &mut Client) -> Self {
        let mut setup = Self {
            schema: schema.map(str::to_owned),
            exclusive,
            outcome: SetupOutcome::Unreachable,
        };
        setup.outcome = setup.apply(client);
        setup
    }

    /// Whether the setup holds: it ran to the end, and a one-server
    /// lock, if one was asked for, is held.
    fn ok(&self) -> bool {
        matches!(self.outcome, SetupOutcome::Applied)
    }
}

impl Setup {
    fn apply(&mut self, client: &mut Client) -> SetupOutcome {
        if let Some(schema) = &self.schema {
            // Every statement that follows resolves its unqualified names
            // inside the schema, and the cleanup of stale test schemas
            // sees the connection in `pg_stat_activity` under its name.
            // The SHARED session lock on the schema's name is the
            // cleanup's real check: a live connection holds it, so its
            // schema never yields the cleanup's exclusive try, whatever
            // a snapshot saw.
            if client
                .batch_execute(&format!(
                    "SET search_path TO {schema}; SET application_name TO '{schema}';
                     SELECT pg_advisory_lock_shared(hashtext('{schema}'))"
                ))
                .is_err()
            {
                return SetupOutcome::Unreachable;
            }
        }
        if self.exclusive {
            // What the connection actually writes into, whatever the URL
            // spelled: a plain URL and one that rides
            // `--search_path=public` land in the same schema and must
            // fence each other, and schemas ride separately.
            let Ok(row) = client.query_one("SELECT current_schema()::text", &[]) else {
                return SetupOutcome::Unreachable;
            };
            // A search_path whose members all miss answers NULL: the
            // connection would create nothing, and no lock names it.
            // The database answered, so this is a URL mistake, not an
            // unreachable database: refuse with the fix.
            let Some(current) = row.get::<_, Option<String>>(0) else {
                return SetupOutcome::NoSchema;
            };
            if !server_lock_raw(client, schema_lock_key(&current)) {
                // The database answered: another session holds the lock.
                return SetupOutcome::LockRefused;
            }
        }
        SetupOutcome::Applied
    }
}

/// A fresh store UUID: 16 bytes of system randomness.
fn uuid_new() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("system randomness");
    bytes
}

/// The 16 bytes of a UUID row, checked: a row of another length is a
/// database that is not this backend's.
pub(crate) fn uuid_of(raw: Vec<u8>) -> Result<[u8; 16]> {
    let len = raw.len();
    raw.try_into().map_err(|_| {
        Error::Corrupt(format!(
            "the store_uuid row holds {len} bytes, not 16: this \
             database is not this backend's store"
        ))
    })
}

/// The statement's parameters as the trait objects `postgres` binds.
fn as_refs(params: &[Param]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
}

/// Trips the process fence of the exclusive client, when it has one:
/// a lock that once was held is lost, so another process may serve the
/// database now.
fn trip_fence(fence: Option<&Arc<Fence>>) {
    if let Some(fence) = fence {
        fence.trip();
    }
}

/// Runs one statement, and once more on a fresh session when the
/// connection died under it: the first statement after a loss goes
/// through the reconnect and the lock check, and answers what the fresh
/// session holds, not the death of the old one. Only a job outside a
/// transaction retries: a transaction was dropped with the connection,
/// and its caller must see that.
fn run_with_retry(
    client: &mut Client,
    setup: &mut Setup,
    conn: &Conn,
    job: &Job,
    tx: &mut TxState,
    ever_held: bool,
    fence: Option<&Arc<Fence>>,
) -> Result<Reply> {
    let out = run_statement(client, job);
    if out.is_ok() || !client.is_closed() {
        return out;
    }
    let Ok(fresh) = reconnect(client, conn) else {
        // The connection stays dead; the original error stands, and the
        // next job reconnects.
        return out;
    };
    *setup = fresh;
    if !setup.ok() {
        if ever_held && matches!(setup.outcome, SetupOutcome::LockRefused) {
            trip_fence(fence);
        }
        return Err(setup_error(setup, ever_held));
    }
    // The fresh session holds no transaction: one that was in progress
    // was dropped with the old session, and nothing of it runs here.
    if tx.in_progress() {
        *tx = TxState::Lost;
        return Err(Error::Unavailable(TX_LOST.into()));
    }
    run_statement(client, job)
}

/// The statements this process has run against Postgres since the last
/// reset. A test hook: the land round-trip guard reads the delta around
/// one land and fails the run when it grows past the budget. The count
/// lives on the worker thread's side, so the read crosses no lock; the
/// guard's binary runs one measuring test, so a process-wide total is
/// one land's count.
#[cfg(any(test, feature = "test-hooks"))]
static TEST_STATEMENTS: AtomicU64 = AtomicU64::new(0);

/// The SQL of the statements run since the last reset, for a test that
/// wants the kinds and not just the count.
#[cfg(any(test, feature = "test-hooks"))]
static TEST_STATEMENT_LOG: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// The SQL of the statements run since the last reset.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_statement_log() -> Vec<String> {
    TEST_STATEMENT_LOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// The statement count of this process since the last reset.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_statement_count() -> u64 {
    TEST_STATEMENTS.load(Ordering::Relaxed)
}

/// Sets the statement count back to zero.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_reset_statement_count() {
    TEST_STATEMENTS.store(0, Ordering::Relaxed);
    TEST_STATEMENT_LOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
}

/// The readers this process has taken since the last reset. A walk that
/// holds one transaction takes one reader, whatever it steps.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_reader_takes() -> u64 {
    TEST_READER_TAKES.load(Ordering::Relaxed)
}

/// Sets the reader-take count back to zero.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_reset_reader_takes() {
    TEST_READER_TAKES.store(0, Ordering::Relaxed);
}

/// How many of this backend's readers sit idle in the pool. A walk that
/// lost its reader to a leaked transaction leaves the pool short.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_idle_readers(pg: &Pg) -> usize {
    pg.inner
        .readers
        .idle
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .len()
}

/// Runs one statement of a job on the client.
fn run_statement(client: &mut Client, job: &Job) -> Result<Reply> {
    #[cfg(any(test, feature = "test-hooks"))]
    {
        TEST_STATEMENTS.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut log) = TEST_STATEMENT_LOG.try_lock() {
            let sql: String = job.sql.chars().take(90).collect();
            log.push(sql.replace('\n', " "));
        }
    }
    let refs = as_refs(&job.params);
    match job.kind {
        Kind::Batch => client.batch_execute(&job.sql).map(|(): ()| Reply::Count(0)),
        Kind::Count => client.execute(&job.sql, &refs).map(Reply::Count),
        Kind::Rows => client.query(&job.sql, &refs).map(Reply::Rows),
        Kind::Raw => unreachable!("a raw job ran in its own branch"),
    }
    .map_err(map_pg_error)
}

/// The reply of a failed setup: a takeover names the loss; an
/// unreachable database answers what a client retries against.
fn setup_error(setup: &Setup, ever_held: bool) -> Error {
    if matches!(setup.outcome, SetupOutcome::NoSchema) {
        return Error::FailedPrecondition(
            "the URL's search_path names no existing schema; create it first".into(),
        );
    }
    if matches!(setup.outcome, SetupOutcome::Unreachable) {
        return Error::Storage(Storage::busy(std::io::Error::other(
            "the database was unreachable at setup",
        )));
    }
    Error::FailedPrecondition(lost_message(ever_held).into())
}

/// The reply of a statement a reconnect robbed of its transaction.
const TX_LOST: &str = "the transaction was lost with the connection; \
     roll it back and run it again";

/// The answer of a setup that does not hold: a process that once held
/// the lock lost it, and a process that never got one may not land
/// beside the holder.
fn lost_message(setup_held: bool) -> &'static str {
    if setup_held {
        "this server process lost the database to another \
         server process; it serves nothing from here on"
    } else {
        HOLDER_MESSAGE
    }
}

/// The text a second open sees while another session still holds the
/// one-server lock. One name, so a reword cannot silently turn the
/// test reopen's wait off.
pub(crate) const HOLDER_MESSAGE: &str = "another server holds this store";

/// Reconnects a dead client, applies the setup again, and hands the new
/// setup back: the caller folds it into `setup_held` and the lost
/// transaction state.
fn reconnect(client: &mut Client, conn: &Conn) -> Result<Setup> {
    let fresh = connect_on(&conn.url, conn.ca.as_deref(), &conn.options)?;
    *client = fresh;
    Ok(Setup::new(conn.schema.as_deref(), conn.exclusive, client))
}

/// Whether the worker's client is inside the transaction a `BEGIN`
/// opened. The worker sees the statements, so it knows when a reconnect
/// has silently dropped one.
#[derive(Clone, Copy)]
enum TxState {
    Idle,
    InTx,
    Lost,
}

impl TxState {
    fn in_progress(self) -> bool {
        matches!(self, TxState::InTx | TxState::Lost)
    }

    fn lost(self) -> bool {
        matches!(self, TxState::Lost)
    }

    fn after(self, sql: &str, ok: bool) -> Self {
        // Only a BEGIN that ran opens a transaction: one the connection
        // refused leaves the worker outside it, and the caller's Drop
        // rolls no transaction back that never opened. A BEGIN that ran
        // after a reconnect opens a real one too: the lost transaction
        // is gone with the old session, and its caller cannot commit it
        // through a new BEGIN, whose statements belong to the client's
        // next holder.
        if sql.starts_with("BEGIN") && ok {
            return TxState::InTx;
        }
        if sql.starts_with("ROLLBACK") || (sql.starts_with("COMMIT") && !self.lost()) {
            return TxState::Idle;
        }
        if self.lost() && !sql.starts_with("COMMIT") {
            return self;
        }
        self
    }
}

/// How a worker reconnects: where to connect, and what setup to apply
/// again. One per [`ClientConfig`].
struct Conn {
    url: String,
    ca: Option<PathBuf>,
    options: String,
    /// The schema a reconnect narrows itself to again.
    schema: Option<String>,
    /// Whether this session re-takes the one-server lock after every
    /// reconnect (an exclusive writer; a reader takes none).
    exclusive: bool,
}

/// What a worker needs to run, and to connect again when its client dies.
struct ClientConfig {
    client: Client,
    conn: Conn,
    /// The process fence of the exclusive client: tripped when a setup
    /// that once held the lock no longer does.
    fence: Option<Arc<Fence>>,
}

/// The advisory-lock key of one test schema, so parallel tests of
/// different schemas do not block each other on the one lock.
pub(crate) fn schema_lock_key(schema: &str) -> i64 {
    let hash = blake3::hash(schema.as_bytes());
    SERVER_LOCK.wrapping_add(i64::from_be_bytes(
        hash.as_bytes()[..8].try_into().expect("8 of 32 bytes"),
    ))
}

/// 16 bytes as 32 lowercase hex digits.
fn hex16(bytes: &[u8; 16]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(32), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// The advisory lock of the one server process of a database, or of one
/// test schema: a second `serve` or `derive` on the same target is
/// refused. Runs on the worker thread, like every statement.
fn server_lock_raw(client: &mut Client, key: i64) -> bool {
    client
        .query_opt("SELECT pg_try_advisory_lock($1)", &[&key])
        .is_ok_and(|row| row.is_some_and(|row| row.get::<_, bool>(0)))
}

/// Connects one client of `url`, with libpq `options` added. The connect
/// runs on a plain thread: `postgres` drives its own runtime, which must
/// not start inside the server's tokio one.
fn connect_with(
    url: &str,
    ca: Option<&Path>,
    options: &str,
    schema: Option<&str>,
) -> Result<PgClient> {
    spawn_client(url, ca, options, schema, false, None)
}

/// How long the schema reset's drop waits for a stale table lock: an
/// admin session or a stuck backend cannot hold a failed restore
/// forever, and a timeout leaves the schema behind, named for the
/// next restore's start to reset. Longer than the writers' 5 s clock:
/// the cleanup must not give up behind a peer that is merely slow.
const RESET_LOCK_TIMEOUT: &str = "60s";
/// The drop statement's own clock, above the lock's.
const RESET_STATEMENT_TIMEOUT: &str = "120s";

/// Undoes the identity a failed restore's open minted in the database:
/// a fresh store names no store, and the next open mints again. The
/// tables the migration made stay (they are empty, and the next open's
/// migration is idempotent); only the name goes.
/// Drops the schema the URL names and makes it again, empty: the
/// restore's staging metadata lives there, and a killed restore's
/// committed batches (a kill runs no failure path) or a replay that
/// failed midway must not survive into the next run. Refuses when
/// another process holds the schema's one-server lock: a live store is
/// never dropped.
///
/// # Errors
///
/// Why the database could not be reached, or another server holds the
/// schema.
pub(crate) fn reset_schema(url: &str, ca: Option<&Path>) -> Result<()> {
    let options = base_options();
    // The one-server lock of the schema the connection lands in: the
    // setup asks the database `current_schema()` and locks that. A
    // refusal here is another server's hold, and the drop must not run.
    let client = connect_exclusive(url, ca, &options, None, Arc::new(Fence::new()))?;
    // The target schema, resolved ONCE and quoted: the guards, the lock
    // and the drop must all act on the same name. A URL whose
    // search_path names another schema resolves current_schema() to
    // that schema, and the guards would read the public schema empty
    // while the drop tore the search_path's schema down - the hole the
    // verifiers found. A search_path whose members all miss answers
    // NULL: the connection would create nothing, so refuse with the
    // fix.
    let schema: Option<String> = client
        .query_opt("SELECT current_schema()::text", &[])?
        .and_then(|row| row.get::<_, Option<String>>(0));
    let Some(schema) = schema else {
        return Err(Error::InvalidArgument(
            "the URL's search_path names no existing schema; create it first".into(),
        ));
    };
    // The drop's only other guard is what the schema holds: this is the
    // one check a stopped store's URL cannot escape - a stopped server
    // holds no lock, so the schema's own rows decide. A schema that
    // carries a store's identity and no restore's staging mark is a
    // real store: refused, unchanged, whatever the lock says. The
    // lookups take the schema as a bind parameter and quote it as an
    // identifier in the database (format %I), so a mixed-case name, a
    // quote or a space is still the one name the guards and the drop
    // act on - never the hard-wired `public`.
    let staging: Option<String> = client
        .query_opt(
            "SELECT to_regclass(format('%I.restore_staging', $1::text))::text",
            &[Param::text(schema.clone())],
        )?
        .and_then(|row| row.get::<_, Option<String>>(0));
    let named: Option<String> = client
        .query_opt(
            "SELECT to_regclass(format('%I.store_uuid', $1::text))::text",
            &[Param::text(schema.clone())],
        )?
        .and_then(|row| row.get::<_, Option<String>>(0));
    if staging.is_none() && named.is_some() {
        return Err(Error::Corrupt(
            "this database names a store, and no restore's staging mark: \
             a restore refuses to drop a store it did not make"
                .to_string(),
        ));
    }
    if staging.is_none() && named.is_none() {
        // Not ours and not a store: a schema with other objects is not
        // a restore's leftover, and nothing here may drop it. The
        // schema rides a bind parameter: no case folding, no literal
        // escape, whatever the name carries.
        let foreign: i64 = client.query(
            "SELECT count(*) FROM pg_catalog.pg_class \
                 WHERE relnamespace = (SELECT oid FROM pg_namespace \
                 WHERE nspname = $1) AND relkind = 'r'",
            &[Param::text(schema.clone())],
        )?[0]
            .get(0);
        if foreign > 0 {
            return Err(Error::Corrupt(format!(
                "this schema holds {foreign} table(s) that no store and no restore made: \
                 a restore refuses to drop what it did not make"
            )));
        }
    }
    // The drop waits out a stale table lock a dead replay's peer left
    // behind - but on a clock, so a stuck backend cannot hold a failed
    // restore forever. A live store is already refused by the
    // one-server lock above, before any drop runs.
    client.batch(&format!(
        "SET lock_timeout = '{RESET_LOCK_TIMEOUT}'; SET statement_timeout = '{RESET_STATEMENT_TIMEOUT}'"
    ))?;
    // One batch: a crash between the drop and the create would leave
    // the next open a database with no schema to land in. The name is
    // quoted as an identifier, so a mixed-case schema drops exactly.
    let quoted = schema.replace('"', "\"\"");
    if let Err(why) = client.batch(&format!(
        "DROP SCHEMA IF EXISTS \"{quoted}\" CASCADE; CREATE SCHEMA \"{quoted}\""
    )) {
        return Err(Error::Corrupt(format!(
            "the staging schema {schema} is left behind: {why}; the next restore's start resets it"
        )));
    }
    // This schema is now the restore's staging, and its mark is what
    // the next restore's start looks for: a schema with the mark is a
    // restore's leftover, whatever a kill did to the process that made
    // it. A store's identity never has it. The mark's table is created
    // in the resolved schema (the connection's search_path already
    // lands there).
    let mut id = [0_u8; 16];
    getrandom::fill(&mut id).expect("system randomness");
    let id: String = id
        .iter()
        .fold(String::new(), |acc, b| acc + &format!("{b:02x}"));
    client.batch(&format!(
        "CREATE TABLE restore_staging (id text primary key, started timestamptz default now()); \
         INSERT INTO restore_staging (id) VALUES ('{id}')"
    ))?;
    Ok(())
}

/// The failure path's reset: the schema this restore reset at its
/// start, marked as staging, holds only this run's rows - a failed
/// replay's committed batches and the identity its open minted. Drop
/// it unconditionally: everything in it is the restore's, and the
/// bounded clocks still apply (W1).
///
/// # Errors
///
/// Why the database could not be reached, or the drop ran out of its
/// clock with the schema left behind (named, for the next restore's
/// start to reset).
pub(crate) fn reset_own_staging(url: &str, ca: Option<&Path>) -> Result<()> {
    let options = base_options();
    let client = connect_exclusive(url, ca, &options, None, Arc::new(Fence::new()))?;
    client.batch(&format!(
        "SET lock_timeout = '{RESET_LOCK_TIMEOUT}'; SET statement_timeout = '{RESET_STATEMENT_TIMEOUT}'"
    ))?;
    let schema: String = client.query("SELECT current_schema()", &[])?[0]
        .try_get(0)
        .map_err(|error| {
            Error::Corrupt(format!("the database answers no current schema: {error}"))
        })?;
    // The name rides as an identifier, quoted the way reset_schema
    // quotes it: a schema whose name holds a quote or a space is still
    // one name, and a raw interpolation here is a SQL injection the
    // URL's search_path reaches.
    let quoted = schema.replace('"', "\"\"");
    if let Err(why) = client.batch(&format!(
        "DROP SCHEMA IF EXISTS \"{quoted}\" CASCADE; CREATE SCHEMA \"{quoted}\""
    )) {
        return Err(Error::Corrupt(format!(
            "the staging schema {schema} is left behind: {why}; the next restore's start resets it"
        )));
    }
    Ok(())
}

/// [`connect_with`], as the one server process of the database: the
/// advisory lock it takes survives as long as the client, and a
/// reconnecting client takes it again. A lost lock trips the fence
/// ([`Fence`]).
fn connect_exclusive(
    url: &str,
    ca: Option<&Path>,
    options: &str,
    schema: Option<&str>,
    fence: Arc<Fence>,
) -> Result<PgClient> {
    // Whether the lock was given is decided at the spawn: one probe job
    // brings a refusal here, to the open, so an operator whose second
    // server starts against a taken database sees it now, and not in the
    // errors of the first call a client makes.
    let client = spawn_client(url, ca, options, schema, true, Some(fence))?;
    client.query_opt("SELECT 1", &[])?;
    Ok(client)
}

/// Puts one client on its worker thread. An exclusive client re-takes
/// the one-server lock after every reconnect, and its loss trips the
/// fence.
fn spawn_client(
    url: &str,
    ca: Option<&Path>,
    options: &str,
    schema: Option<&str>,
    exclusive: bool,
    fence: Option<Arc<Fence>>,
) -> Result<PgClient> {
    let client =
        std::thread::scope(|scope| scope.spawn(|| connect_on(url, ca, options)).join().unwrap())?;
    // The lock is taken on the worker thread, like every statement: the
    // caller may be a tokio one, which must never block on this client.
    PgClient::spawn(ClientConfig {
        client,
        conn: Conn {
            url: url.to_owned(),
            ca: ca.map(Path::to_path_buf),
            options: options.to_owned(),
            schema: schema.map(str::to_owned),
            exclusive,
        },
        fence,
    })
}

/// The libpq `options` a connection starts with: the ones the URL carries
/// first, then the backend's own, which win on a clash. The backend checks
/// `synchronous_commit` itself after the connect, so an operator's
/// `search_path` in the URL survives, and its clashes do not matter.
fn merged_options(from_url: Option<&str>, own: &str) -> String {
    match from_url {
        Some(from_url) if !from_url.is_empty() => format!("{from_url} {own}"),
        _ => own.to_owned(),
    }
}

/// The whole connect's deadline: the socket connect, its TLS, the
/// startup message, the authentication. tokio-postgres's
/// `connect_timeout` covers only the socket connect (the
/// `connect_socket` step of `connect_once`); a database that accepts
/// the connection and never answers its startup would hold the open,
/// the readers' connect and every reconnect forever, so the connect
/// runs on its own thread and the caller stops waiting at the
/// deadline. `tcp_user_timeout` cannot cover the wait: a client that
/// waits for an answer has no unacknowledged data, and the peer's
/// kernel answers the keepalive probes. A connect past the deadline
/// leaves its thread parked on the socket - tokio-postgres offers no
/// cancel - so the connect registers itself as parked, and the server
/// starts no new connect while
/// [`CONNECT_PARKED_MAX`] of them are parked: a connect that would be
/// the next fails at once, naming the host and how long the oldest has
/// been parked. The cap ends the FD storm a reconnecting server would
/// otherwise grow against a database that stalls its connects - one
/// thread and socket a retry, forever - because once the cap fills,
/// every new connect fails at once, and only the connects already in
/// flight when it filled can park: a batch bounded by the worker
/// count, not a thread an hour. The upgrade is a connector whose
/// stream fails at the deadline, so no thread parks at all (SHORTCUT:
/// a cap of [`CONNECT_PARKED_MAX`] registered parks, one socket and
/// thread each, until a tokio-postgres that takes a deadline for the
/// whole connect).
const CONNECT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// How many connects may sit parked on silent sockets at once. Two:
/// one for the writers' path, one for a reader, so an outage still
/// names itself in the log twice before the calls fail fast.
const CONNECT_PARKED_MAX: usize = 2;

/// The connects parked on silent sockets: their tokens and the moments
/// they started, so the next connect past the cap can name how long
/// the oldest has waited.
static PARKED_CONNECTS: std::sync::LazyLock<Mutex<Vec<(u64, std::time::Instant)>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

static PARKED_TOKEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Refuses a new connect while the cap of parked connects is held: the
/// error names the host and how long the oldest parked connect has
/// waited.
fn park_refuse(host: &str) -> Result<()> {
    let parked = PARKED_CONNECTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if parked.len() >= CONNECT_PARKED_MAX {
        let oldest = parked
            .iter()
            .map(|(_, since)| since.elapsed().as_secs())
            .max()
            .unwrap_or(0);
        return Err(Error::Unavailable(format!(
            "the database at {host} already has {} connects parked on a \
             socket that never answers, the oldest for {oldest} s: not starting another",
            parked.len()
        )));
    }
    Ok(())
}

/// Registers a connect whose caller gave up waiting: its thread is
/// still parked on the socket. Past the cap the register keeps no row
/// - the thread parks anyway, one of the batch the cap bounded.
fn park_register(token: u64) {
    let mut parked = PARKED_CONNECTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if parked.len() < CONNECT_PARKED_MAX {
        parked.push((token, std::time::Instant::now()));
    }
}

/// Gives a parked connect's row back: the connect's thread ended, so
/// its socket is no longer held.
fn park_release(token: u64) {
    PARKED_CONNECTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|(held, _)| *held != token);
}

/// [`connect_with`]'s work: parse, then connect.
fn connect_on(url: &str, ca: Option<&Path>, options: &str) -> Result<Client> {
    connect_on_deadline(url, ca, options, CONNECT_DEADLINE)
}

/// [`connect_on`], with the whole connect's deadline a parameter, so
/// the test that pins it does not wait the server's thirty seconds.
#[doc(hidden)]
#[cfg(any(test, feature = "test-hooks"))]
pub fn connect_on_deadline_for_tests(
    url: &str,
    ca: Option<&Path>,
    options: &str,
    deadline: std::time::Duration,
) -> Result<Client> {
    connect_on_deadline(url, ca, options, deadline)
}

/// The tests' reset of the parked-connect slots: every slot goes back,
/// so a test starts its count from empty.
#[doc(hidden)]
#[cfg(any(test, feature = "test-hooks"))]
pub fn test_reset_parked_connects() {
    PARKED_CONNECTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
}

fn connect_on_deadline(
    url: &str,
    ca: Option<&Path>,
    options: &str,
    deadline: std::time::Duration,
) -> Result<Client> {
    let mut config: Config = url
        .parse()
        .map_err(|e| Error::InvalidArgument(format!("cannot parse the database URL: {e}")))?;
    let options = merged_options(config.get_options(), options);
    config.options(&options);
    // A dead connection is noticed in seconds, not held open forever.
    config.connect_timeout(std::time::Duration::from_secs(10));
    config.keepalives(true);
    config.keepalives_idle(std::time::Duration::from_secs(30));
    config.tcp_user_timeout(std::time::Duration::from_secs(30));
    // The host is on this machine only when every host of the URL is, and
    // no `hostaddr` names one by address: an address has no name to check
    // a certificate against.
    let local = !config.get_hosts().is_empty()
        && config.get_hostaddrs().is_empty()
        && config.get_hosts().iter().all(|host| match host {
            postgres::config::Host::Tcp(host) => {
                host == "localhost"
                    || host
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
            }
            // A Unix socket is as local as it gets.
            postgres::config::Host::Unix(_) => true,
        });
    let wants_tls = config.get_ssl_mode() == postgres::config::SslMode::Require;
    // Off this machine the traffic is encrypted, whatever a man in the
    // middle answers to the start: `prefer` falls back to plaintext when
    // the server's answer says no, so it is refused here.
    if !local && !wants_tls {
        return Err(Error::InvalidArgument(
            "the database host is off this machine, so its traffic must be \
             encrypted: name sslmode=require or stronger in the URL"
                .to_owned(),
        ));
    }
    // The host the error names: the first host of the URL, with the
    // port, or the socket's path.
    let host = match config.get_hosts().first() {
        Some(postgres::config::Host::Tcp(host)) => {
            let port = config.get_ports().first().copied().unwrap_or(5432);
            format!("{host}:{port}")
        }
        Some(postgres::config::Host::Unix(path)) => path.display().to_string(),
        None => "no host".to_owned(),
    };
    // A connect that fails is a database the caller may wait for: the
    // same states a statement answers, mapped the same way, so a caller
    // that retries a busy database retries a starting one too. The
    // connect runs on its own thread, and the caller stops waiting at
    // `deadline`: a database that accepts the connection and never
    // answers its startup fails here instead of holding the open, the
    // readers' connect and every reconnect forever.
    let (connected, wait) = std::sync::mpsc::channel();
    park_refuse(&host)?;
    let token = PARKED_TOKEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let attempt = match (local, ca) {
        // The connector upgrades every connection, and checks the
        // certificate the server presents against `ca`.
        (_, Some(ca)) => {
            let tls = make_tls(ca)?;
            std::thread::Builder::new()
                .name("postgres-connect".to_owned())
                .spawn(move || {
                    let answer = config.connect(tls);
                    park_release(token);
                    let _ = connected.send(answer);
                })
        }
        (true, None) => std::thread::Builder::new()
            .name("postgres-connect".to_owned())
            .spawn(move || {
                let answer = config.connect(NoTls);
                park_release(token);
                let _ = connected.send(answer);
            }),
        (false, None) => {
            return Err(Error::InvalidArgument(
                "the database host is off this machine: give --postgres-ca, \
                 so that its certificate can be checked"
                    .to_owned(),
            ));
        }
    }
    .map_err(|e| Error::Internal(format!("could not start the connect thread: {e}")))?;
    if let Ok(answer) = wait.recv_timeout(deadline) {
        answer.map_err(map_pg_error)
    } else {
        // The connect's thread is now parked on the socket: registered,
        // so the next connects past the cap refuse at once.
        park_register(token);
        let why = format!(
            "the database at {host} did not finish its connect within {} s: \
             the socket may have connected, but its startup, its TLS \
             or its authentication never answered",
            deadline.as_secs()
        );
        tracing::error!(
            "{why}; the connect thread {} is left on the socket",
            attempt.thread().name().unwrap_or("?")
        );
        Err(Error::DeadlineExceeded(why))
    }
}

/// A database error that a caller may retry maps to a busy [`Storage`]:
/// the importer retries a batch on it, and the gRPC status becomes
/// UNAVAILABLE. The rest keep the engine's error as the source.
pub(crate) fn map_pg_error(e: postgres::Error) -> Error {
    let code = e
        .code()
        .map(postgres::error::SqlState::code)
        .map(str::to_owned);
    pg_error_of(code.as_deref(), e)
}

/// The mapping on one SQLSTATE, split from [`map_pg_error`] so a unit
/// test can hand it a code: the `postgres` crate's error type cannot be
/// built with a chosen one.
fn pg_error_of(code: Option<&str>, e: postgres::Error) -> Error {
    // A connection the database dropped is as retryable as a busy one.
    if e.is_closed() {
        return Error::Storage(Storage::busy(e));
    }
    match code {
        // Deadlock, serialization failure, an administrative shutdown, a
        // database that starts up, a lock another writer holds.
        Some(code)
            if matches!(code, "40P01" | "40001" | "57P01" | "57P03" | "55P03")
                || code.starts_with("08") =>
        {
            tracing::warn!("the database is busy: {e}");
            Error::Storage(Storage::busy(e))
        }
        // The lock or the statement waited past its clock: the caller
        // has been at it too long, and the client retries past its own
        // deadline.
        Some("57014") => {
            tracing::warn!("the database gave the statement up: {e}");
            Error::DeadlineExceeded("the database kept the caller waiting".to_owned())
        }
        // A violation the code handles (a duplicate key, a check, a
        // foreign key) is one answer among many: it logs quietly.
        Some(code) if code.starts_with("23") => {
            tracing::debug!("the database refused a statement: {e}");
            Error::Storage(Storage::internal(e))
        }
        // Out of space: the same shortage, wherever the metadata lives.
        // 53100 is the database's one disk-full code - errcode_for_file_access
        // maps ENOSPC to it for every file, temp files included (elog.c).
        // The other codes of the same classes stay internal on purpose:
        // 53200 is out of MEMORY (the fixed reply would name a disk and
        // lie), 53000 is the class's catch-all (too vague to promise a
        // disk answer), 53400 is a configured cap - temp_file_limit, or
        // the replication slots - the same refusal every time, so a
        // retry-later reply tells the client to retry what cannot
        // succeed - and 58030 is an io_error - a failing device is the
        // EIO row's internal, not a space shortage.
        Some("53100") => {
            tracing::error!("the server is out of disk space: {e}");
            Error::Storage(Storage::disk_full(e))
        }
        // A statement the database never saw: the connection could not
        // be made or was cut on the way. No SQLSTATE names it, and a
        // client retries it as it retries a busy one.
        None => {
            tracing::warn!("the database is unreachable: {e}");
            Error::Storage(Storage::busy(e))
        }
        // The rest keep the engine's error as the source, and it reaches
        // the log in full: a bug of this backend, such as a bad parameter,
        // must not look busy, or the importer retries it and the log says
        // nothing but "busy".
        _ => {
            tracing::warn!("the database refused a statement: {e}");
            Error::Storage(Storage::internal(e))
        }
    }
}

/// The TLS connector of a database whose host is off this machine: the
/// certificate it presents must be one `ca` signed. There is no mode that
/// accepts any certificate.
fn make_tls(ca: &Path) -> Result<MakeRustlsConnect> {
    use tokio_rustls::rustls::pki_types::{CertificateDer, pem::PemObject as _};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore, crypto::CryptoProvider};

    // The CA must stay whole: a swapped one changes whom the database
    // connection trusts, so it is read through one checked descriptor.
    let ca_bytes = scion_proto::runtime::auth::read_shared_file(ca)
        .map_err(|e| Error::InvalidArgument(format!("cannot read the database CA: {e}")))?;
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(&ca_bytes) {
        let certificate = certificate
            .map_err(|e| Error::InvalidArgument(format!("the database CA is not PEM: {e}")))?;
        roots
            .add(certificate)
            .map_err(|_| Error::InvalidArgument("the database CA is not a root".to_owned()))?;
    }
    let provider: Arc<CryptoProvider> =
        Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::InvalidArgument(format!("no supported TLS version: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(MakeRustlsConnect::new(config))
}

/// What every store opener answers when the database is behind this
/// build and the opener may not migrate: the fix is named.
pub(crate) fn pending_migration_message(db: u32, build: u32) -> String {
    format!(
        "the database is at schema version {db}, this build ships {build}; \
         run `scion-server migrate` first, or pass --migrate to serve"
    )
}

/// One raw connection for preflight: the URL's own TLS and option
/// rules, the pool's connect discipline, and no setup - no schema
/// write, and no lock of any kind: the one-server lock and the
/// schema's shared lock both stay untouched. The session is read-only
/// at the database itself (`default_transaction_read_only=on`), so a
/// check cannot write even by accident.
pub(crate) fn preflight_client(url: &str, ca: Option<&Path>) -> Result<Client> {
    connect_on(
        url,
        ca,
        &format!(
            "-cdefault_transaction_read_only=on -cstatement_timeout=600000 \
             -clock_timeout={LOCK_TIMEOUT_FOR_WRITERS}"
        ),
    )
}

/// The schema version the database stands at, read without writing:
/// version 0 is a database with no schema yet. The check and the open's
/// pending-migration refusal read it; the migration itself runs under
/// the schema's lock, and its first step makes the ledger.
/// The two statement paths one schema-state read may take: the open's
/// migration reads through the worker's client, the preflight's check
/// and `migrate --check`'s read through a plain session of their own.
/// One trait, so the read is one code path, not a copy.
pub(crate) trait SchemaRows {
    /// The statement's rows, in this backend's errors.
    fn schema_query(&mut self, sql: &str) -> Result<Vec<postgres::Row>>;
}

impl SchemaRows for Client {
    fn schema_query(&mut self, sql: &str) -> Result<Vec<postgres::Row>> {
        Client::query(self, sql, &[]).map_err(map_pg_error)
    }
}

impl SchemaRows for PgClient {
    fn schema_query(&mut self, sql: &str) -> Result<Vec<postgres::Row>> {
        self.query(sql, &[])
    }
}

fn schema_version(client: &mut impl SchemaRows) -> Result<u32> {
    let ledger: Option<String> = client
        .schema_query("SELECT to_regclass('schema_migrations')::text")?
        .into_iter()
        .next()
        .and_then(|row| row.get(0));
    if ledger.is_none() {
        return Ok(0);
    }
    let version: i64 = client
        .schema_query("SELECT max(version) FROM schema_migrations")?
        .first()
        .and_then(|row| row.get::<_, Option<i64>>(0))
        .unwrap_or(0);
    u32::try_from(version).map_err(|_| {
        Error::FailedPrecondition(format!(
            "database schema version {version} is not a version"
        ))
    })
}

/// What `migrate --check` reports: the version the database stands at,
/// this build's version, and the names of the migrations the build
/// would run. Read without the schema's lock and without writing, so a
/// check is safe beside a live server.
pub struct SchemaCheck {
    db_version: u32,
    pending: &'static [&'static str],
}

impl SchemaCheck {
    /// Whether the database is behind this build: `migrate --check`
    /// exits non-zero on it, and the names are the steps it would run.
    #[must_use]
    pub fn is_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// The pending migrations' names, in the order `migrate` runs them.
    #[must_use]
    pub fn pending(&self) -> &[&'static str] {
        self.pending
    }

    /// The version the database stands at.
    #[must_use]
    pub fn db_version(&self) -> u32 {
        self.db_version
    }

    /// The version this build ships.
    #[must_use]
    pub fn build_version(&self) -> u32 {
        PG_SCHEMA_VERSION
    }
}

/// Reads the schema state for `migrate --check`, without the schema's
/// lock and without writing.
///
/// # Errors
///
/// What the database returns, and the newer-schema refusal a newer
/// database gives every opener.
pub fn check_schema(url: &str, ca: Option<&Path>) -> Result<SchemaCheck> {
    let mut client = connect_with(url, ca, &base_options(), None)?;
    check_schema_on(&mut client)
}

/// [`check_schema`]'s decision, on a caller's connection: the preflight
/// runs it on its read-only session, so the check opens no second
/// session outside that discipline.
///
/// # Errors
///
/// What the database returns, and the newer-schema refusal a newer
/// database gives every opener.
pub(crate) fn check_schema_on(client: &mut impl SchemaRows) -> Result<SchemaCheck> {
    let db_version = schema_version(client)?;
    if db_version > PG_SCHEMA_VERSION {
        return Err(Error::FailedPrecondition(format!(
            "database schema version {db_version} is newer than this build \
             ({PG_SCHEMA_VERSION}); run this build's older server, or upgrade it"
        )));
    }
    Ok(SchemaCheck {
        db_version,
        pending: &PG_MIGRATION_NAMES[db_version as usize..],
    })
}

/// Runs the migrations on purpose: the same path the open runs, with
/// the same logs and the same schema lock. The caller records the move
/// in its audit log.
///
/// The schema's one-server lock comes first, held for the migration on
/// this connection: a live server holds it for its life, so a migrate
/// beside one refuses before the schema can move under it - a migration
/// an old server does not know (a rename, a drop) would break the
/// process that serves. The wait is none: the operator's next step is
/// stopping the server, not queuing behind it. `serve --migrate` holds
/// the same lock by its open, so an unattended start keeps working.
///
/// # Errors
///
/// What the database returns, the newer-schema refusal, the
/// no-schema refusal, or the one-server lock held by a live server.
pub fn migrate_store(url: &str, ca: Option<&Path>) -> Result<Option<(u32, u32)>> {
    let mut client = connect_with(url, ca, &base_options(), None)?;
    // What the connection actually writes into, whatever the URL
    // spelled: the same current-schema read the open's exclusive setup
    // makes, so a plain URL and one that rides `--search_path=public`
    // take the same lock.
    let current = client
        .query("SELECT current_schema()::text", &[])?
        .into_iter()
        .next()
        .and_then(|row| row.get::<_, Option<String>>(0))
        .ok_or_else(|| {
            Error::FailedPrecondition(
                "the URL's search_path names no existing schema; create it first".into(),
            )
        })?;
    // The one-server lock, tried, not waited on: the operator's next
    // step is stopping the server, not queuing behind it.
    let held = client
        .query(
            "SELECT pg_try_advisory_lock($1)",
            &[P::i64(schema_lock_key(&current))],
        )?
        .into_iter()
        .next()
        .is_some_and(|row| row.get::<_, bool>(0));
    if !held {
        return Err(Error::FailedPrecondition(format!(
            "{HOLDER_MESSAGE}; stop the server first"
        )));
    }
    migrate(&mut client, true)
}

/// Brings the database to this build's schema: runs every migration it has
/// not run yet, in one transaction each, and refuses a schema of a newer
/// build. Returns the `(from, to)` versions when it migrated, and `None`
/// when the database was at this build's version already. A database of
/// version 0 was no schema at all: the first open creates one, and a
/// creation moves nothing.
///
/// When `allow` is false, a database that is behind this build is
/// refused instead of migrated: every store opener but `migrate` and
/// `serve --migrate` passes false, so an upgrade happens when the
/// operator asks for it.
#[allow(clippy::needless_pass_by_ref_mut)]
fn migrate(client: &mut PgClient, allow: bool) -> Result<Option<(u32, u32)>> {
    // One process at a time migrates: a lock of the session, held across
    // the migrations, so a server and an import started together take
    // turns while the migrations are quick, and the version the loser
    // reads is the one the winner left. A lock past its 5 s clock leaves
    // the taker busy: an open that hits one waits and tries again.
    client.query_opt("SELECT pg_advisory_lock($1)", &[P::i64(SCHEMA_LOCK)])?;
    let outcome = migrate_under_lock(client, allow);
    // The lock lives on the client's session: it goes back whatever the
    // migrations did. A client that died mid-migration lost the lock with
    // its connection, and the unlock names a lock the new connection
    // never held, which answers false and costs nothing.
    let _ = client.query_opt("SELECT pg_advisory_unlock($1)", &[P::i64(SCHEMA_LOCK)]);
    outcome
}

/// The migrations themselves, under the session's schema lock. `allow`
/// is the opener's right to move the schema: false turns a behind
/// database into the refusal that names `scion-server migrate`.
fn migrate_under_lock(client: &mut PgClient, allow: bool) -> Result<Option<(u32, u32)>> {
    // The ledger of the versions, made first, in its own transaction: a
    // migration that fails leaves the ledger and every version before it.
    client.batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
             version BIGINT PRIMARY KEY,
             name    TEXT NOT NULL
         )",
    )?;
    let version: i64 = client
        .query("SELECT max(version) FROM schema_migrations", &[])?
        .first()
        .and_then(|row| row.get::<_, Option<i64>>(0))
        .unwrap_or(0);
    let version = u32::try_from(version).map_err(|_| {
        Error::FailedPrecondition(format!(
            "database schema version {version} is not a version"
        ))
    })?;
    if version > PG_SCHEMA_VERSION {
        return Err(Error::FailedPrecondition(format!(
            "database schema version {version} is newer than this build \
             ({PG_SCHEMA_VERSION}); run this build's older server, or upgrade it"
        )));
    }
    // A fresh database (version 0) is made by whatever opens it, as
    // before; a database that is behind this build moves only when the
    // opener asked to migrate.
    if !allow && version != 0 && version < PG_SCHEMA_VERSION {
        return Err(Error::FailedPrecondition(pending_migration_message(
            version,
            PG_SCHEMA_VERSION,
        )));
    }
    if version == PG_SCHEMA_VERSION {
        return Ok(None);
    }
    if version != 0 {
        tracing::info!(
            from_version = version,
            to_version = PG_SCHEMA_VERSION,
            "migrating the metadata schema"
        );
    }
    for (n, sql) in PG_MIGRATIONS.iter().enumerate() {
        let n = u32::try_from(n + 1).expect("a schema version fits in a u32");
        if n <= version {
            continue;
        }
        // One transaction per migration, with its version bump: a
        // migration that fails rolls its own statements back, and the
        // versions before it stand.
        let step = std::time::Instant::now();
        let mut pending = Vec::new();
        let mut derived = DerivedPending::default();
        let mut preload = std::collections::HashMap::new();
        let mut history = HistoryPending::default();
        let mut tx = PgTx::begin(
            client,
            &mut pending,
            &mut derived,
            &mut preload,
            &mut history,
            None,
        )?;
        tx.c.batch(&format!(
            "{sql}\nINSERT INTO schema_migrations (version, name) \
             VALUES ({}, 'postgres schema')",
            i64::from(n)
        ))?;
        tx.commit()?;
        if version != 0 {
            tracing::info!(
                from_version = n - 1,
                to_version = n,
                elapsed_ms = u64::try_from(step.elapsed().as_millis()).unwrap_or(u64::MAX),
                "the metadata schema migrated one step"
            );
        }
    }
    if version == 0 {
        tracing::info!(
            to_version = PG_SCHEMA_VERSION,
            "the metadata schema was created"
        );
    } else {
        tracing::info!(
            from_version = version,
            to_version = PG_SCHEMA_VERSION,
            "the metadata migration is done"
        );
    }
    // Version 0 was no schema at all: the first open creates one, and a
    // creation moves nothing.
    let migrated = if version == 0 {
        None
    } else {
        Some((version, PG_SCHEMA_VERSION))
    };
    Ok(migrated)
}

/// Reads a tree's rows and checks them against its ID.
fn load_tree(c: &PgClient, id: TreeId) -> Result<(Tree, Vec<u8>)> {
    let body: Vec<u8> = c
        .query_opt(
            "SELECT body FROM trees WHERE id = $1",
            &[P::bytes(&id_bytes(id.as_bytes()))],
        )?
        .ok_or_else(|| Error::NotFound(format!("tree {id}")))?
        .get(0);
    let (tree, actual) =
        Tree::decode_with_id(&body).map_err(|e| Error::Corrupt(format!("tree {id}: {e}")))?;
    if actual != id {
        return Err(Error::Corrupt(format!("tree {id} does not match its ID")));
    }
    Ok((tree, body))
}

fn to_id(bytes: &[u8]) -> Result<ChangesetId> {
    ChangesetId::try_from(bytes).map_err(|e| Error::Corrupt(format!("stored changeset ID: {e}")))
}

/// The bytes of a content address, as a BYTEA parameter.
fn id_bytes(id: &[u8; 32]) -> Vec<u8> {
    id.to_vec()
}

impl MetaTx for PgTx<'_> {
    /// Creates a repository, or returns the existing one with that name.
    fn create_repo(&mut self, name: &str) -> Result<RepoId> {
        self.c.execute(
            "INSERT INTO repos (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
            &[P::text(name)],
        )?;
        self.repo_id(name)
    }

    /// Looks up a repository by name.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if no repository has that name.
    fn repo_id(&mut self, name: &str) -> Result<RepoId> {
        let row = self
            .c
            .query_opt(
                "SELECT repo_id FROM repos WHERE name = $1",
                &[P::text(name)],
            )?
            .ok_or_else(|| Error::NotFound(format!("repo {name:?}")))?;
        Ok(RepoId(row.get(0)))
    }

    /// Stores a changeset and adds it to `repo`. Every parent must already
    /// be in `repo`. Safe to repeat.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if a parent is not in `repo`.
    fn insert_changeset(&mut self, repo: RepoId, cs: &Changeset) -> Result<ChangesetId> {
        // This transaction holds the collector's key from its begin, so
        // the collector's check waits for it, and its re-read pins every
        // blob this changeset puts. The files themselves the lens of
        // `store_changeset` has already read inside this transaction: a
        // blob whose file the collector removed fails there, before any
        // write, on both backends.
        let (body, id) = cs.encode_with_id();
        let mut parent_gen = 0;
        let parents: Vec<Vec<u8>> = cs
            .parents()
            .iter()
            .map(|parent| id_bytes(parent.as_bytes()))
            .collect();
        // The generations this transaction already holds answer the
        // parents it inserted itself, so a batch's chain reads the
        // database only for the parents it did not insert, usually the
        // first commit's. The read carries only those misses: a parent
        // this transaction inserted has no membership row yet (the flush
        // holds it back), and the read's join with repo_changesets would
        // drop it.
        let mut misses: Vec<Vec<u8>> = Vec::new();
        for parent in cs.parents() {
            let key = id_bytes(parent.as_bytes());
            match self.gen_cache.get(&(repo.0, key.clone())) {
                Some(cached) => parent_gen = parent_gen.max(*cached),
                None => {
                    if !misses.contains(&key) {
                        misses.push(key);
                    }
                }
            }
        }
        if !misses.is_empty() {
            // One round trip for every parent this transaction did not
            // insert, however many a merge has.
            let rows = self.c.query(
                "SELECT id, gen FROM changesets JOIN repo_changesets ON cs = id
                 WHERE repo_id = $1 AND id = ANY($2)",
                &[P::i64(repo.0), P::bytea_array(misses.clone())],
            )?;
            let gens: std::collections::HashMap<Vec<u8>, i64> = rows
                .into_iter()
                .map(|row| (row.get::<_, Vec<u8>>(0), row.get::<_, i64>(1)))
                .collect();
            for (parent, key) in cs.parents().iter().zip(&parents) {
                // A repeated parent reads the same row twice; `get`, not
                // `remove`, so parents [X, X] store like they did when
                // each parent was its own statement.
                let parent_gen_i = match self.gen_cache.get(&(repo.0, key.clone())) {
                    Some(cached) => *cached,
                    None => *gens
                        .get(key)
                        .ok_or_else(|| Error::NotFound(format!("changeset {parent}")))?,
                };
                parent_gen = parent_gen.max(parent_gen_i);
                self.gen_cache.insert((repo.0, key.clone()), parent_gen_i);
            }
        }
        let inserted = self.c.execute(
            "INSERT INTO changesets (id, gen, body) VALUES ($1, $2, $3)
             ON CONFLICT (id) DO NOTHING",
            &[
                P::bytes(&id_bytes(id.as_bytes())),
                P::i64(parent_gen + 1),
                P::bytes(&body),
            ],
        )?;
        let generation = parent_gen + 1;
        // Only a row this statement wrote is the transaction's own. A
        // conflicting row predates the transaction (a land rebases onto
        // the head its input changeset already hangs from), and the
        // write path asks `inserted_this_tx` to tell the two apart: the
        // stored derived rows of an old changeset are real and must be
        // read, the ones of a new one cannot exist.
        if inserted > 0 {
            self.inserted_gens
                .insert(id_bytes(id.as_bytes()), generation);
        }
        self.gen_cache
            .insert((repo.0, id_bytes(id.as_bytes())), generation);
        // While the import's batching is on, the parents and the repo
        // membership of this transaction's changesets wait for the
        // flush, which writes them one statement per 1,000 rows. The
        // changesets row itself lands per commit: the derived rows of
        // the same commit name it by an immediate foreign key. When the
        // last per-commit writer of a commit moves into the batch, this
        // row moves with it, and every table writes at the end in the
        // order the foreign keys read: changesets, changeset_parents,
        // repo_changesets, derived, history, git_import.
        if self.cs_batching {
            if let Some(pending) = self.cs_pending.as_deref_mut() {
                pending.push((id, cs.parents().to_vec()));
            }
            return Ok(id);
        }
        // Every parent as one statement, however many a merge has: the
        // row number is the parent's place in the array, counting from
        // zero, as the first-parent readers and the seed writer count.
        if !parents.is_empty() {
            self.c.execute(
                "INSERT INTO changeset_parents (cs, n, parent)
                 SELECT $1, ord - 1, parent
                 FROM unnest($2::bytea[]) WITH ORDINALITY AS parents(parent, ord)
                 ON CONFLICT (cs, n) DO NOTHING",
                &[
                    P::bytes(&id_bytes(id.as_bytes())),
                    P::bytea_array(parents.clone()),
                ],
            )?;
        }
        self.c.execute(
            "INSERT INTO repo_changesets (repo_id, cs) VALUES ($1, $2)
             ON CONFLICT (repo_id, cs) DO NOTHING",
            &[P::i64(repo.0), P::bytes(&id_bytes(id.as_bytes()))],
        )?;
        Ok(id)
    }

    /// Turns the import's changeset batching on or off. While it is on,
    /// `insert_changeset` writes the changesets row only, and the parents
    /// and the repo membership of this transaction's changesets wait for
    /// `flush_changeset_rows`.
    fn set_changeset_batching(&mut self, on: bool) {
        self.cs_batching = on;
        if !on && let Some(pending) = self.cs_pending.as_deref_mut() {
            pending.clear();
        }
    }

    /// Whether the import's changeset batching is on.
    fn changeset_batching(&self) -> bool {
        self.cs_batching
    }

    /// Writes the parents and the repo membership of the changesets the
    /// transaction inserted while batching, one statement per 1,000 rows
    /// per table, and returns how many statements it wrote. The parents'
    /// row numbers follow the per-commit insert's contract (the parent's
    /// place in the array, counting from zero). The
    /// transaction's derived rows land after them, in the order the
    /// foreign keys read: `changesets`, `changeset_parents`,
    /// `repo_changesets`, `derived`, `history`, `git_import` - the
    /// history rows join the batch in the slice after this one.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn flush_changeset_rows(&mut self, repo: RepoId) -> Result<()> {
        const CHUNK: usize = 1_000;
        const DELETE_CHUNK: usize = 10_000;
        let Some(pending) = self.cs_pending.as_deref_mut() else {
            return Ok(());
        };
        if pending.is_empty() {
            return Ok(());
        }
        let mut parent_rows: Vec<(Vec<u8>, i64, Vec<u8>)> = Vec::new();
        let mut member_rows: Vec<(i64, Vec<u8>)> = Vec::new();
        for (id, parents) in pending.drain(..) {
            let key = id_bytes(id.as_bytes());
            for (n, parent) in (0_i64..).zip(&parents) {
                parent_rows.push((key.clone(), n, id_bytes(parent.as_bytes())));
            }
            member_rows.push((repo.0, key));
        }
        for chunk in parent_rows.chunks(CHUNK) {
            let css = chunk
                .iter()
                .map(|(cs, _, _)| cs.clone())
                .collect::<Vec<_>>();
            let ns = chunk.iter().map(|(_, n, _)| *n).collect::<Vec<_>>();
            let parents = chunk
                .iter()
                .map(|(_, _, parent)| parent.clone())
                .collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO changeset_parents (cs, n, parent)
                 SELECT * FROM unnest($1::bytea[], $2::int8[], $3::bytea[])
                 ON CONFLICT (cs, n) DO NOTHING",
                &[
                    P::bytea_array(css),
                    P::i64_array(ns),
                    P::bytea_array(parents),
                ],
            )?;
        }
        for chunk in member_rows.chunks(CHUNK) {
            let repos = chunk.iter().map(|(repo, _)| *repo).collect::<Vec<_>>();
            let css = chunk.iter().map(|(_, cs)| cs.clone()).collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO repo_changesets (repo_id, cs)
                 SELECT * FROM unnest($1::int8[], $2::bytea[])
                 ON CONFLICT (repo_id, cs) DO NOTHING",
                &[P::i64_array(repos), P::bytea_array(css)],
            )?;
        }
        // The derived rows the transaction inserted, after the changesets
        // they name: one statement per 1,000 rows, then one delete of the
        // older versions per kind, which the per-commit writes ran as
        // part of each write.
        let Some(derived) = self.derived_pending.as_deref_mut() else {
            return Ok(());
        };
        let mut kinds: Vec<(String, i64)> = Vec::new();
        for (k, v, ..) in derived.iter() {
            match kinds.iter_mut().find(|(kind, _)| kind == k) {
                Some((_, seen)) => *seen = (*seen).max(*v),
                None => kinds.push((k.clone(), *v)),
            }
        }
        for chunk in derived.chunks(CHUNK) {
            let kinds = chunk.iter().map(|(k, ..)| k.clone()).collect::<Vec<_>>();
            let versions = chunk.iter().map(|(_, v, _, _)| *v).collect::<Vec<_>>();
            let css = chunk
                .iter()
                .map(|(_, _, cs, _)| id_bytes(cs.as_bytes()))
                .collect::<Vec<_>>();
            let values = chunk
                .iter()
                .map(|(_, _, _, value)| value.clone())
                .collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO derived (kind, version, cs, value)
                 SELECT * FROM unnest($1::text[], $2::int8[], $3::bytea[], $4::bytea[])
                 ON CONFLICT (kind, version, cs) DO NOTHING",
                &[
                    P::text_array(kinds),
                    P::i64_array(versions),
                    P::bytea_array(css),
                    P::bytea_array(values),
                ],
            )?;
        }
        // One delete of the older versions per kind, as the per-commit
        // write ran as part of each write: below the newest version the
        // batch wrote, bounded like the per-commit delete.
        for (kind, version) in &kinds {
            self.c.execute(
                "DELETE FROM derived WHERE (kind, version, cs) IN (
                     SELECT kind, version, cs FROM derived
                     WHERE kind = $1 AND version < $2 LIMIT $3)",
                &[
                    P::text(kind.clone()),
                    P::i64(*version),
                    P::i64(i64::try_from(DELETE_CHUNK).unwrap_or(i64::MAX)),
                ],
            )?;
        }
        derived.clear();
        self.flush_history_rows()?;
        Ok(())
    }

    /// Holds the derived values of the first-parent chain that starts at
    /// `start`, so the batch's reads of them cost nothing.
    fn preload_derived_chain(
        &mut self,
        kind: &str,
        version: u32,
        start: ChangesetId,
        limit: i64,
    ) -> Result<()> {
        if !self.cs_batching {
            return Ok(());
        }
        let rows = self.get_derived_chain(kind, version, start, limit)?;
        if let Some(preload) = self.derived_preload.as_deref_mut() {
            for (cs, value) in rows {
                preload.insert(
                    (kind.to_owned(), i64::from(version), id_bytes(cs.as_bytes())),
                    value,
                );
            }
        }
        Ok(())
    }

    /// Adds `blob` to the blobs that `repo` may name. Returns whether it is
    /// new.
    fn add_repo_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool> {
        let added = self.c.execute(
            "INSERT INTO repo_blobs (repo_id, blob) VALUES ($1, $2)
             ON CONFLICT (repo_id, blob) DO NOTHING",
            &[P::i64(repo.0), P::bytes(&id_bytes(blob.as_bytes()))],
        )?;
        Ok(added == 1)
    }

    /// Adds every blob as one statement, however many the changeset
    /// puts.
    fn add_repo_blobs(&mut self, repo: RepoId, blobs: &[BlobId]) -> Result<()> {
        if blobs.is_empty() {
            return Ok(());
        }
        let ids: Vec<Vec<u8>> = blobs.iter().map(|blob| id_bytes(blob.as_bytes())).collect();
        self.c.execute(
            "INSERT INTO repo_blobs (repo_id, blob)
             SELECT $1, x FROM unnest($2::bytea[]) AS x
             ON CONFLICT (repo_id, blob) DO NOTHING",
            &[P::i64(repo.0), P::bytea_array(ids)],
        )?;
        Ok(())
    }

    /// Whether `repo` may name `blob`.
    fn repo_has_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool> {
        Ok(self
            .c
            .query_opt(
                "SELECT 1 FROM repo_blobs WHERE repo_id = $1 AND blob = $2",
                &[P::i64(repo.0), P::bytes(&id_bytes(blob.as_bytes()))],
            )?
            .is_some())
    }

    /// The blobs of the list that the repo does not have. One statement
    /// per 1,000 ids, the chunk size of [`add_repo_blobs`](Self::add_repo_blobs):
    /// a changeset may name 100,000 changes, and one query per blob
    /// would cost one round trip each.
    fn repo_missing_blobs(&mut self, repo: RepoId, blobs: &[BlobId]) -> Result<Vec<BlobId>> {
        const CHUNK: usize = 1_000;
        let mut missing = Vec::new();
        for chunk in blobs.chunks(CHUNK) {
            let ids: Vec<Vec<u8>> = chunk.iter().map(|blob| id_bytes(blob.as_bytes())).collect();
            let rows = self.c.query(
                "SELECT x FROM unnest($2::bytea[]) AS x
                 WHERE NOT EXISTS (
                     SELECT 1 FROM repo_blobs WHERE repo_id = $1 AND blob = x
                 )",
                &[P::i64(repo.0), P::bytea_array(ids)],
            )?;
            for row in rows {
                let bytes: Vec<u8> = row.get(0);
                // The bytes come back from the statement's own array (the
                // SELECT reads unnest($2), not the table), so today they
                // are always 32 bytes; the guard keeps a future SELECT
                // from the table honest instead of panicking a request.
                let id = BlobId::try_from(bytes.as_slice()).map_err(|_| {
                    Error::Corrupt(format!(
                        "repo {}: the missing-blob statement read back a \
                         blob id of {} bytes, not 32 - the id is the \
                         statement's own parameter array, not a stored row",
                        repo.0,
                        bytes.len()
                    ))
                })?;
                missing.push(id);
            }
        }
        Ok(missing)
    }

    /// Reads a changeset that `repo` may see and checks it against its ID.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not have it, [`Error::Corrupt`]
    /// if it does not decode or match.
    fn get_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<Changeset> {
        #[cfg(test)]
        CHANGESET_READS.set(CHANGESET_READS.get() + 1);
        let body: Vec<u8> = self
            .c
            .query_opt(
                "SELECT body FROM changesets JOIN repo_changesets ON cs = id
                 WHERE repo_id = $1 AND id = $2",
                &[P::i64(repo.0), P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")))?
            .get(0);
        let (cs, actual) = Changeset::decode_with_id(&body)
            .map_err(|e| Error::Corrupt(format!("changeset {id}: {e}")))?;
        if actual != id {
            return Err(Error::Corrupt(format!(
                "changeset {id} does not match its ID"
            )));
        }
        Ok(cs)
    }

    /// Whether `repo` may see changeset `id`.
    fn has_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<bool> {
        Ok(self
            .c
            .query_opt(
                "SELECT 1 FROM repo_changesets WHERE repo_id = $1 AND cs = $2",
                &[P::i64(repo.0), P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .is_some())
    }

    /// The changesets of `repo` that the ids name, in the order asked,
    /// one statement: the ids travel as one bytea array, the reply has at
    /// most one row per id, and each body is checked against its ID, as
    /// `get_changeset` checks one.
    fn get_changeset_many(&mut self, repo: RepoId, ids: &[ChangesetId]) -> Result<Vec<Changeset>> {
        let cs = ids
            .iter()
            .map(|id| id_bytes(id.as_bytes()))
            .collect::<Vec<_>>();
        let rows = self.c.query(
            "SELECT c.id, c.body FROM changesets c
             JOIN repo_changesets rc ON rc.repo_id = $1 AND rc.cs = c.id
             WHERE c.id = ANY($2)",
            &[P::i64(repo.0), P::bytea_array(cs)],
        )?;
        let bodies = rows
            .into_iter()
            .map(|row| {
                let cs: Vec<u8> = row.get(0);
                Ok((to_id(&cs)?, row.get::<_, Vec<u8>>(1)))
            })
            .collect::<Result<std::collections::HashMap<_, _>>>()?;
        ids.iter()
            .map(|id| {
                #[cfg(test)]
                CHANGESET_READS.set(CHANGESET_READS.get() + 1);
                let body = bodies
                    .get(id)
                    .ok_or_else(|| Error::NotFound(format!("changeset {id}")))?;
                let (cs, actual) = Changeset::decode_with_id(body)
                    .map_err(|e| Error::Corrupt(format!("changeset {id}: {e}")))?;
                if actual != *id {
                    return Err(Error::Corrupt(format!(
                        "changeset {id} does not match its ID"
                    )));
                }
                Ok(cs)
            })
            .collect()
    }

    /// One page of the first-parent chain, one statement: the recursive
    /// term follows the indexed first-parent rows (cs, n = 0) that the
    /// repo may see, so every hop is one index lookup plus one membership
    /// probe, and the join carries the generation and the body. A link
    /// the repo may not see ends the walk, and the page stays short.
    /// Every fetched changeset is a member of `repo` and an ancestor of
    /// `start`. The page is short at the root of the history, and the
    /// walk's last link is checked to tell a root from a tampered row:
    /// one whose parent the repo does not hold is corruption, refused
    /// with [`Error::Corrupt`] naming the changeset, its first parent
    /// and the repo, so a truncated history never reads as a whole one.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not hold `start`.
    /// [`Error::Corrupt`] if a row's id is not a changeset id, or the
    /// page's last changeset names a first parent the repo does not
    /// hold.
    fn first_parent_page(
        &mut self,
        repo: RepoId,
        start: ChangesetId,
        limit: i64,
    ) -> Result<Vec<(ChangesetId, i64, Vec<u8>)>> {
        let rows = self.c.query(
            "WITH RECURSIVE walk(id, depth) AS (
                     SELECT rc.cs, 0::bigint FROM repo_changesets rc
                     WHERE rc.repo_id = $1 AND rc.cs = $2
                     UNION ALL
                     SELECT cp.parent, w.depth + 1
                     FROM walk w
                     JOIN changeset_parents cp ON cp.cs = w.id AND cp.n = 0
                     JOIN repo_changesets rc ON rc.repo_id = $1 AND rc.cs = cp.parent
                     WHERE w.depth < $3
                 )
                 SELECT c.id, c.gen, c.body FROM walk
                 JOIN changesets c ON c.id = walk.id
                 ORDER BY walk.depth",
            &[
                P::i64(repo.0),
                P::bytes(&id_bytes(start.as_bytes())),
                P::i64(limit),
            ],
        )?;
        if rows.is_empty() {
            return Err(Error::NotFound(format!("changeset {start}")));
        }
        // A page short of its limit ended the walk: at a root, whose last
        // changeset has no first-parent row, or at a tampered link, whose
        // last changeset names a parent this repo does not hold. One
        // probe tells the two apart, and the tampered one is corruption:
        // a truncated history must not read as a whole one.
        // `limit` is a link count, far from any wrap.
        #[allow(clippy::cast_possible_wrap)]
        if rows.len() as i64 <= limit {
            let last = rows.last().unwrap();
            let last = to_id(last.get::<_, Vec<u8>>(0).as_slice())?;
            let hop = self.c.query_opt(
                "SELECT cp.parent, rc.cs IS NOT NULL AS member
                 FROM changeset_parents cp
                 LEFT JOIN repo_changesets rc ON rc.repo_id = $1 AND rc.cs = cp.parent
                 WHERE cp.cs = $2 AND cp.n = 0",
                &[P::i64(repo.0), P::bytes(&id_bytes(last.as_bytes()))],
            )?;
            if let Some(hop) = hop
                && !hop.get::<_, bool>(1)
            {
                let parent: Vec<u8> = hop.get(0);
                let parent = to_id(&parent)?;
                return Err(Error::Corrupt(format!(
                    "changeset {last} points at first parent {parent}, which repo {} does not hold",
                    repo.0
                )));
            }
        }
        rows.into_iter()
            .map(|row| {
                let cs: Vec<u8> = row.get(0);
                Ok((to_id(&cs)?, row.get(1), row.get(2)))
            })
            .collect()
    }

    /// The generation of a changeset that `repo` may see.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not have it.
    fn generation(&mut self, repo: RepoId, id: ChangesetId) -> Result<i64> {
        let key = (repo.0, id_bytes(id.as_bytes()));
        if let Some(cached) = self.gen_cache.get(&key) {
            return Ok(*cached);
        }
        let found = self
            .c
            .query_opt(
                "SELECT gen FROM changesets JOIN repo_changesets ON cs = id
                 WHERE repo_id = $1 AND id = $2",
                &[P::i64(repo.0), P::bytes(&key.1)],
            )?
            .map(|row| row.get::<_, i64>(0))
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")));
        if let Ok(found_gen) = &found {
            self.gen_cache.insert(key, *found_gen);
        }
        found
    }

    /// The generation of a changeset, in any repo. For derived data.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if no changeset has this ID.
    fn generation_of(&mut self, id: ChangesetId) -> Result<i64> {
        if let Some(cached) = self.inserted_gens.get(&id_bytes(id.as_bytes())) {
            return Ok(*cached);
        }
        self.c
            .query_opt(
                "SELECT gen FROM changesets WHERE id = $1",
                &[P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .map(|row| row.get::<_, i64>(0))
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")))
    }

    /// The first parent of a changeset, or `None` for a root.
    #[cfg(any(test, feature = "test-hooks"))]
    fn first_parent(&mut self, id: ChangesetId) -> Result<Option<ChangesetId>> {
        let parent = self
            .c
            .query_opt(
                "SELECT parent FROM changeset_parents WHERE cs = $1 AND n = 0",
                &[P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .map(|row| row.get::<_, Vec<u8>>(0));
        parent.map(|bytes| to_id(&bytes)).transpose()
    }

    /// Every parent of changeset `id`, in order, with its generation.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if a parent's generation is not below `id_gen`.
    fn parents(&mut self, id: ChangesetId, id_gen: i64) -> Result<Vec<(ChangesetId, i64)>> {
        let rows = self.c.query(
            "SELECT parent, gen FROM changeset_parents JOIN changesets ON id = parent
             WHERE cs = $1 ORDER BY n",
            &[P::bytes(&id_bytes(id.as_bytes()))],
        )?;
        rows.iter()
            .map(|row| {
                let parent: Vec<u8> = row.get(0);
                let parent_gen: i64 = row.get(1);
                let parent = to_id(&parent)?;
                if parent_gen >= id_gen {
                    return Err(Error::Corrupt(format!(
                        "changeset {id} has generation {id_gen}, its parent {parent} {parent_gen}"
                    )));
                }
                Ok((parent, parent_gen))
            })
            .collect()
    }

    /// The parents of up to `DERIVED_BATCH` merges, in the order asked,
    /// in one statement: the ids travel as one `bytea[]`, and the reply
    /// has at most one row per parent. A merge asked twice gets its
    /// parents twice, once per ask. The generation check is the one
    /// [`MetaTx::parents`] makes, with the asked merge's generation.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if a parent's generation is not below its
    /// merge's.
    fn parents_many(
        &mut self,
        merges: &[(ChangesetId, i64)],
    ) -> Result<Vec<Vec<(ChangesetId, i64)>>> {
        let ids = merges
            .iter()
            .map(|(id, _)| id_bytes(id.as_bytes()))
            .collect::<Vec<_>>();
        let rows = self.c.query(
            "SELECT cs, parent, gen FROM changeset_parents JOIN changesets ON id = parent
             WHERE cs = ANY($1) ORDER BY cs, n",
            &[P::bytea_array(ids)],
        )?;
        let mut out: Vec<Vec<(ChangesetId, i64)>> = vec![Vec::new(); merges.len()];
        // One id can be asked twice: two histories reach the same merge.
        let mut asked: HashMap<&[u8], Vec<usize>> = HashMap::new();
        for (i, (id, _)) in merges.iter().enumerate() {
            asked.entry(id.as_bytes()).or_default().push(i);
        }
        for row in &rows {
            let cs: Vec<u8> = row.get(0);
            let parent: Vec<u8> = row.get(1);
            let parent_gen: i64 = row.get(2);
            let parent = to_id(&parent)?;
            for &i in &asked[cs.as_slice()] {
                let (id, id_gen) = merges[i];
                if parent_gen >= id_gen {
                    return Err(Error::Corrupt(format!(
                        "changeset {id} has generation {id_gen}, its parent {parent} {parent_gen}"
                    )));
                }
                out[i].push((parent, parent_gen));
            }
        }
        Ok(out)
    }

    /// Whether `ancestor` is `descendant` or reachable from it through
    /// parents, within a bounded walk.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not have either changeset.
    /// [`Error::ResourceExhausted`] if the walk needs more than `max_visits`
    /// changesets.
    #[cfg(any(test, feature = "test-hooks"))]
    fn is_ancestor(
        &mut self,
        repo: RepoId,
        ancestor: ChangesetId,
        descendant: ChangesetId,
        max_visits: usize,
    ) -> Result<bool> {
        let floor = self.generation(repo, ancestor)?;
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![(descendant, self.generation(repo, descendant)?)];
        while let Some((id, id_gen)) = stack.pop() {
            if id == ancestor {
                return Ok(true);
            }
            if id_gen <= floor || !seen.insert(id) {
                continue;
            }
            if seen.len() > max_visits {
                return Err(test_hooks::too_deep(max_visits));
            }
            stack.extend(self.parents(id, id_gen)?);
        }
        Ok(false)
    }

    /// Where a ref points, if it exists.
    fn get_ref(&mut self, repo: RepoId, name: &str) -> Result<Option<RefState>> {
        self.c
            .query_opt(
                "SELECT target, version FROM refs WHERE repo_id = $1 AND name = $2",
                &[P::i64(repo.0), P::text(name)],
            )?
            .map(|row| -> Result<RefState> {
                let target: Vec<u8> = row.get(0);
                Ok(RefState {
                    target: to_id(&target)?,
                    version: row.get(1),
                })
            })
            .transpose()
    }

    /// Moves a ref if its version is still `expected`. `None` creates the
    /// ref and expects it not to exist. Returns whether the ref moved.
    ///
    /// # Errors
    ///
    /// Database errors, or [`Error::NotFound`] if `repo` does not have
    /// `target`.
    fn cas_ref(
        &mut self,
        repo: RepoId,
        name: &str,
        expected: Option<i64>,
        target: ChangesetId,
    ) -> Result<bool> {
        self.generation(repo, target)?;
        let changed = match expected {
            None => self.c.execute(
                "INSERT INTO refs (repo_id, name, target, version) VALUES ($1, $2, $3, 1)
                 ON CONFLICT (repo_id, name) DO NOTHING",
                &[
                    P::i64(repo.0),
                    P::text(name),
                    P::bytes(&id_bytes(target.as_bytes())),
                ],
            )?,
            Some(version) => self.c.execute(
                "UPDATE refs SET target = $3, version = version + 1
                 WHERE repo_id = $1 AND name = $2 AND version = $4",
                &[
                    P::i64(repo.0),
                    P::text(name),
                    P::bytes(&id_bytes(target.as_bytes())),
                    P::i64(version),
                ],
            )?,
        };
        Ok(changed == 1)
    }

    fn restore_ref(&mut self, repo: RepoId, name: &str, target: &[u8], version: i64) -> Result<()> {
        self.c.execute(
            "INSERT INTO refs (repo_id, name, target, version) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (repo_id, name) DO UPDATE SET target = $3, version = $4",
            &[
                P::i64(repo.0),
                P::text(name),
                P::bytes(target),
                P::i64(version),
            ],
        )?;
        Ok(())
    }

    /// The stored bytes of a derived value, if computed. While the
    /// importer's batching is on, the values this transaction inserted
    /// answer first (newest insert wins), then the chains the importer
    /// preloaded, then the database: the batch's derives chain through
    /// the values it inserted, and the reads of the ancestors outside
    /// the transaction cost what the preload already paid for.
    fn get_derived(
        &mut self,
        kind: &str,
        version: u32,
        cs: ChangesetId,
    ) -> Result<Option<Vec<u8>>> {
        let version = i64::from(version);
        let key = id_bytes(cs.as_bytes());
        if self.cs_batching
            && let Some(pending) = self.derived_pending.as_deref()
        {
            for (k, v, c, value) in pending.iter().rev() {
                if *k == kind && *v == version && id_bytes(c.as_bytes()) == key {
                    return Ok(Some(value.clone()));
                }
            }
        }
        // The transaction's own memory of the derived rows it preloaded
        // or wrote: one statement filled it at the start of a write path,
        // and every put of this transaction keeps it true. A row it
        // deleted is of an older version, which no read of this kind
        // asks for, so no stale answer can hide there.
        if let Some(value) = self
            .derived_preload
            .as_deref()
            .and_then(|preload| preload.get(&(kind.to_owned(), version, key.clone())))
        {
            return Ok(Some(value.clone()));
        }
        Ok(self
            .c
            .query_opt(
                "SELECT value FROM derived WHERE kind = $1 AND version = $2 AND cs = $3",
                &[P::text(kind), P::i64(version), P::bytes(&key)],
            )?
            .map(|row| row.get(0)))
    }

    fn get_derived_batch(
        &mut self,
        kind: &str,
        version: u32,
        ids: &[ChangesetId],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        // One statement for the whole batch: the ids travel as one bytea
        // array, the reply has at most one row per id. The caller's read
        // transaction holds for the batch, so a land between two ids of the
        // batch cannot mix two states.
        let version = i64::from(version);
        let cs = ids
            .iter()
            .map(|id| id_bytes(id.as_bytes()))
            .collect::<Vec<_>>();
        let rows = self.c.query(
            "SELECT cs, value FROM derived
             WHERE kind = $1 AND version = $2 AND cs = ANY($3)",
            &[P::text(kind), P::i64(version), P::bytea_array(cs)],
        )?;
        let found = rows
            .into_iter()
            .map(|row| {
                let cs: Vec<u8> = row.get(0);
                Ok((to_id(&cs)?, row.get::<_, Option<Vec<u8>>>(1)))
            })
            .collect::<Result<std::collections::HashMap<_, _>>>()?;
        // Answered from the map without removing it: a batch may name the
        // same id twice (two merges can share a side parent), and each
        // ask gets the row - the reply has one entry per ASKED id.
        ids.iter()
            .map(|id| Ok(found.get(id).cloned().flatten()))
            .collect()
    }

    fn get_derived_chain(
        &mut self,
        kind: &str,
        version: u32,
        start: ChangesetId,
        limit: i64,
    ) -> Result<Vec<(ChangesetId, Vec<u8>)>> {
        // One statement for the whole chain: the recursive term follows the
        // indexed first-parent rows (cs, n = 0), so every hop is one index
        // lookup, and the join keeps only the rows that carry a stored
        // value. Every fetched changeset is an ancestor of `start`, so no
        // repo filter is needed. The reply is in walk order: newest
        // first, from the asked node, the way first_parent_page orders.
        let version = i64::from(version);
        let rows = self.c.query(
            "WITH RECURSIVE walk(id, depth) AS (
                 SELECT $1::bytea, 0::bigint
                 UNION ALL
                 SELECT cp.parent, w.depth + 1
                 FROM walk w
                 JOIN changeset_parents cp ON cp.cs = w.id AND cp.n = 0
                 WHERE w.depth < $2
             )
             SELECT d.cs, d.value FROM walk
             JOIN derived d ON d.cs = walk.id
             WHERE d.kind = $3 AND d.version = $4
             ORDER BY walk.depth",
            &[
                P::bytes(id_bytes(start.as_bytes()).as_slice()),
                P::i64(limit),
                P::text(kind),
                P::i64(version),
            ],
        )?;
        rows.into_iter()
            .map(|row| {
                let cs: Vec<u8> = row.get(0);
                Ok((to_id(&cs)?, row.get(1)))
            })
            .collect()
    }

    /// Every link of the first-parent chain that starts at `start`, up to
    /// `limit` of them, newest first, each with its stored value or `None`
    /// where none is stored. One statement: the recursive term follows the
    /// indexed first-parent rows, as `get_derived_chain` follows them, and
    /// the left join keeps every link. The caller decides what a missing
    /// value means.
    fn get_derived_chain_links(
        &mut self,
        kind: &str,
        version: u32,
        start: ChangesetId,
        limit: i64,
    ) -> Result<Vec<(ChangesetId, Option<Vec<u8>>)>> {
        let version = i64::from(version);
        let rows = self.c.query(
            "WITH RECURSIVE walk(id, depth) AS (
                 SELECT $1::bytea, 0::bigint
                 UNION ALL
                 SELECT cp.parent, w.depth + 1
                 FROM walk w
                 JOIN changeset_parents cp ON cp.cs = w.id AND cp.n = 0
                 WHERE w.depth < $2
             )
             SELECT walk.id, d.value FROM walk
             LEFT JOIN derived d ON d.cs = walk.id
                 AND d.kind = $3 AND d.version = $4
             ORDER BY walk.depth",
            &[
                P::bytes(id_bytes(start.as_bytes()).as_slice()),
                P::i64(limit),
                P::text(kind),
                P::i64(version),
            ],
        )?;
        rows.into_iter()
            .map(|row| {
                let cs: Vec<u8> = row.get(0);
                Ok((to_id(&cs)?, row.get::<_, Option<Vec<u8>>>(1)))
            })
            .collect()
    }

    /// The test seed, in batches: the changesets, their parents, the repo
    /// membership, and the derived rows, 1,000 rows per statement.
    /// Parents must come before their children in `css`, so the
    /// generations compute in order; the derived rows must find an empty
    /// schema, like the trait doc says.
    #[cfg(any(test, feature = "test-hooks"))]
    fn bulk_seed(
        &mut self,
        repo: RepoId,
        css: &[scion_core::Changeset],
        derived: &[(&'static str, u32, ChangesetId, Vec<u8>)],
    ) -> Result<Vec<ChangesetId>> {
        let mut gens: std::collections::HashMap<Vec<u8>, i64> = std::collections::HashMap::new();
        let mut ids = Vec::with_capacity(css.len());
        let mut cs_rows: Vec<(Vec<u8>, i64, Vec<u8>)> = Vec::new();
        let mut parent_rows: Vec<(Vec<u8>, i64, Vec<u8>)> = Vec::new();
        let mut member_rows: Vec<(i64, Vec<u8>)> = Vec::new();
        for cs in css {
            let (body, id) = cs.encode_with_id();
            let key = id_bytes(id.as_bytes());
            let mut parent_gen = 0;
            for parent in cs.parents() {
                let parent_gen_i =
                    gens.get(&id_bytes(parent.as_bytes()))
                        .copied()
                        .ok_or_else(|| {
                            Error::Corrupt(format!("the seed's {parent} comes after its child"))
                        })?;
                parent_gen = parent_gen.max(parent_gen_i);
            }
            let generation = 1 + parent_gen;
            gens.insert(key.clone(), generation);
            for (n, parent) in cs.parents().iter().enumerate() {
                let n = i64::try_from(n).map_err(|_| {
                    Error::Corrupt(format!(
                        "the seed's {id} holds more parents than i64 counts"
                    ))
                })?;
                parent_rows.push((key.clone(), n, id_bytes(parent.as_bytes())));
            }
            member_rows.push((repo.0, key.clone()));
            cs_rows.push((key, generation, body));
            ids.push(id);
        }
        self.insert_seed_changesets(&cs_rows)?;
        self.insert_seed_parents(&parent_rows)?;
        self.insert_seed_members(&member_rows)?;
        let derived_rows: Vec<(&str, i64, Vec<u8>, Vec<u8>)> = derived
            .iter()
            .map(|(kind, version, cs, value)| {
                (
                    *kind,
                    i64::from(*version),
                    id_bytes(cs.as_bytes()),
                    value.clone(),
                )
            })
            .collect();
        self.insert_seed_derived(&derived_rows)?;
        Ok(ids)
    }

    /// Records a derived value. A second write must agree with the first:
    /// derivation is deterministic, so a mismatch means corruption.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if a different value is already stored.
    fn put_derived(
        &mut self,
        kind: &str,
        version: u32,
        cs: ChangesetId,
        value: &[u8],
    ) -> Result<()> {
        // While the importer's batching is on, the row waits for the
        // flush, which writes it in batches and checks nothing: the
        // batch's changesets are new, so no stored row can disagree, and
        // the flush lands inside this transaction. The buffer answers
        // this transaction's reads of the same value.
        if self.cs_batching
            && let Some(pending) = self.derived_pending.as_deref_mut()
        {
            pending.push((kind.to_owned(), i64::from(version), cs, value.to_vec()));
            return Ok(());
        }
        let version64 = i64::from(version);
        let inserted = self.c.execute(
            "INSERT INTO derived (kind, version, cs, value) VALUES ($1, $2, $3, $4)
             ON CONFLICT (kind, version, cs) DO NOTHING",
            &[
                P::text(kind),
                P::i64(version64),
                P::bytes(&id_bytes(cs.as_bytes())),
                P::bytes(value),
            ],
        )?;
        if inserted == 0 && self.get_derived(kind, version, cs)?.as_deref() != Some(value) {
            return Err(Error::Corrupt(format!(
                "derived {kind} v{version} for {cs} differs from the stored value"
            )));
        }
        // The transaction's memory follows its writes, so a later read
        // of the same row answers with what was written. It follows
        // after the check above: the check must see the stored row, not
        // the value just written.
        if let Some(preload) = self.derived_preload.as_deref_mut() {
            preload.insert(
                (kind.to_owned(), version64, id_bytes(cs.as_bytes())),
                value.to_vec(),
            );
        }
        Ok(())
    }

    /// Deletes up to `limit` derived values of `kind` with a version below
    /// `version`, and returns how many. While the importer's batching is
    /// on, this waits for the flush, which deletes once per kind for the
    /// whole transaction: the end state is the same, and the old rows of
    /// a version a batch is still writing are readable garbage until the
    /// flush lands.
    fn delete_derived_below(&mut self, kind: &str, version: u32, limit: usize) -> Result<usize> {
        if self.cs_batching {
            return Ok(0);
        }
        let version64 = i64::from(version);
        // A write path that deferred its deletes collects this call; the
        // commit deletes for the whole transaction with one statement,
        // honoring the summed limits. The callers that count the rows
        // (the old-value sweep) never turn the deferral on.
        if self.defer_derived {
            self.deferred_deletes.push((
                kind.to_owned(),
                version64,
                i64::try_from(limit).unwrap_or(i64::MAX),
            ));
            return Ok(0);
        }
        let deleted = self.c.execute(
            "DELETE FROM derived WHERE (kind, version, cs) IN (
                 SELECT kind, version, cs FROM derived
                 WHERE kind = $1 AND version < $2 ORDER BY version, cs LIMIT $3)",
            &[
                P::text(kind),
                P::i64(version64),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        Ok(usize::try_from(deleted).unwrap_or(0))
    }

    /// The stored value of the kind for the changeset the transaction
    /// itself inserted cannot exist unless the transaction wrote it, and
    /// its own memory answers that: the stored check reads nothing.
    fn inserted_this_tx(&self, cs: ChangesetId) -> bool {
        self.inserted_gens.contains_key(&id_bytes(cs.as_bytes()))
    }

    /// Reads the derived values of the write path's kinds for the
    /// changeset's parents into the transaction's memory, one statement:
    /// every per-value read of the path then answers without a round
    /// trip. A value written later in the same transaction updates the
    /// memory, so the answers stay the transaction's own.
    fn preload_derived(
        &mut self,
        kinds: &[(&'static str, u32)],
        css: &[ChangesetId],
    ) -> Result<()> {
        if kinds.is_empty() || css.is_empty() {
            return Ok(());
        }
        let Some(preload) = self.derived_preload.as_deref_mut() else {
            return Ok(());
        };
        let kind_names: Vec<String> = kinds.iter().map(|(name, _)| (*name).to_owned()).collect();
        let versions: Vec<i64> = kinds
            .iter()
            .map(|(_, version)| i64::from(*version))
            .collect();
        let keys: Vec<Vec<u8>> = css.iter().map(|cs| id_bytes(cs.as_bytes())).collect();
        let rows = self.c.query(
            "SELECT kind, version, cs, value FROM derived
             WHERE cs = ANY($3)
               AND (kind, version) IN (SELECT kind, version
                                       FROM unnest($1::text[], $2::int8[]) AS t(kind, version))",
            &[
                P::text_array(kind_names),
                P::i64_array(versions),
                P::bytea_array(keys),
            ],
        )?;
        for row in rows {
            preload.insert((row.get(0), row.get(1), row.get(2)), row.get(3));
        }
        Ok(())
    }

    /// Defers the write path's per-row flushes to the commit: the
    /// collected `delete_derived_below` calls wait for the commit's
    /// one-statement flush, and the history nodes the write path
    /// buffers do too. The deferral lasts to the commit. One-way -
    /// there is no off, because an off would silently drop deletes the
    /// transaction already promised.
    fn defer_derived_deletes(&mut self) {
        self.defer_derived = true;
    }

    /// The view that the per-tree memo holds for a (rule set, version,
    /// real tree, context), if derived.
    ///
    /// # Errors
    ///
    /// Database errors, or a stored view that is not a tree ID.
    fn get_view_tree(
        &mut self,
        repo: RepoId,
        digest: &[u8; 32],
        version: u32,
        tree: TreeId,
        ctx: [u8; 32],
    ) -> Result<Option<TreeId>> {
        let bytes: Option<Vec<u8>> = self
            .c
            .query_opt(
                "SELECT view FROM view_trees \
                 WHERE repo_id = $1 AND digest = $2 AND version = $3 AND tree = $4 AND ctx = $5",
                &[
                    P::i64(repo.0),
                    P::bytes(digest),
                    P::i64(i64::from(version)),
                    P::bytes(&id_bytes(tree.as_bytes())),
                    P::bytes(&ctx),
                ],
            )?
            .map(|row| row.get(0));
        bytes
            .map(|b| TreeId::try_from(b.as_slice()).map_err(|_| Error::Corrupt("bad view".into())))
            .transpose()
    }

    /// Records one row of the per-tree memo. A second write must agree
    /// with the first: derivation is deterministic, so a mismatch means
    /// corruption.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if a different view is already stored.
    fn put_view_tree(
        &mut self,
        repo: RepoId,
        digest: &[u8; 32],
        version: u32,
        tree: TreeId,
        ctx: [u8; 32],
        view: TreeId,
    ) -> Result<()> {
        let inserted = self.c.execute(
            "INSERT INTO view_trees (repo_id, digest, version, tree, ctx, view) \
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (repo_id, digest, version, tree, ctx) DO NOTHING",
            &[
                P::i64(repo.0),
                P::bytes(digest),
                P::i64(i64::from(version)),
                P::bytes(&id_bytes(tree.as_bytes())),
                P::bytes(&ctx),
                P::bytes(&id_bytes(view.as_bytes())),
            ],
        )?;
        if inserted == 0 && self.get_view_tree(repo, digest, version, tree, ctx)? != Some(view) {
            return Err(Error::Corrupt(format!(
                "the view of tree {tree} under its context differs from the stored value"
            )));
        }
        Ok(())
    }

    /// Deletes at most `limit` dead rows of the per-tree memo: rows of an
    /// older projection version, then rows of a digest that no grant of
    /// this repo hashes to any more, as far as this server's access file
    /// knows. The LIMIT lives in the subquery over the primary key, as the
    /// derived-row expiry does. A digest that another server still needs
    /// is re-derived, at that server's next read.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn delete_stale_view_trees(
        &mut self,
        repo: RepoId,
        live: &[[u8; 32]],
        version: u32,
        limit: usize,
    ) -> Result<usize> {
        let limit = u64::try_from(limit).map_err(|_| Error::InvalidArgument("limit".into()))?;
        let version = i64::from(version);
        // Rows of an older projection version are dead, whoever used them.
        let mut removed = self.c.execute(
            "DELETE FROM view_trees WHERE (repo_id, digest, version, tree, ctx) IN (
                 SELECT repo_id, digest, version, tree, ctx FROM view_trees
                 WHERE repo_id = $1 AND version < $2 LIMIT $3)",
            &[
                P::i64(repo.0),
                P::i64(version),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        // Rows of a digest that no grant of this repo hashes to any more.
        // `live` is never empty here: the deriving caller's digest is among
        // the digests of its access snapshot (the assert in `view_root`),
        // and a caller that cannot prove that has no business sweeping.
        if removed < limit && !live.is_empty() {
            let live: Vec<Vec<u8>> = live.iter().map(|d| d.to_vec()).collect();
            let left = limit - removed;
            removed += self.c.execute(
                "DELETE FROM view_trees WHERE (repo_id, digest, version, tree, ctx) IN (
                     SELECT repo_id, digest, version, tree, ctx FROM view_trees
                     WHERE repo_id = $1 AND version = $2
                       AND NOT (digest = ANY($3))
                     LIMIT $4)",
                &[
                    P::i64(repo.0),
                    P::i64(version),
                    P::bytea_array(live),
                    P::i64(i64::try_from(left).unwrap_or(i64::MAX)),
                ],
            )?;
        }
        Ok(usize::try_from(removed).unwrap_or(0))
    }

    /// Stores a tree. Safe to repeat.
    fn put_tree(&mut self, tree: &Tree) -> Result<TreeId> {
        let (body, id) = tree.encode_with_id();
        self.c.execute(
            "INSERT INTO trees (id, body) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            &[P::bytes(&id_bytes(id.as_bytes())), P::bytes(&body)],
        )?;
        Ok(id)
    }

    /// Stores pre-encoded trees, whose ids are their content hashes, as
    /// one statement however many the chain built.
    fn put_trees(&mut self, trees: &[(TreeId, Vec<u8>)]) -> Result<()> {
        if trees.is_empty() {
            return Ok(());
        }
        let ids: Vec<Vec<u8>> = trees
            .iter()
            .map(|(id, _)| id_bytes(id.as_bytes()))
            .collect();
        let bodies: Vec<Vec<u8>> = trees.iter().map(|(_, body)| body.clone()).collect();
        self.c.execute(
            "INSERT INTO trees (id, body)
             SELECT * FROM unnest($1::bytea[], $2::bytea[])
             ON CONFLICT (id) DO NOTHING",
            &[P::bytea_array(ids), P::bytea_array(bodies)],
        )?;
        Ok(())
    }

    /// Reads a tree and checks it against its ID.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if absent, [`Error::Corrupt`] if it does not
    /// decode or match.
    fn get_tree(&mut self, id: TreeId) -> Result<Tree> {
        load_tree(self.c, id).map(|(tree, _)| tree)
    }

    /// The stored bytes of the trees the ids name, in the order asked.
    /// One statement per 1,000 ids: the ids travel as one bytea array.
    fn get_trees(&mut self, ids: &[TreeId]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(crate::meta::DERIVED_BATCH) {
            let keys: Vec<Vec<u8>> = chunk.iter().map(|id| id_bytes(id.as_bytes())).collect();
            let rows = self.c.query(
                "SELECT id, body FROM trees WHERE id = ANY($1)",
                &[P::bytea_array(keys.clone())],
            )?;
            let bodies: std::collections::HashMap<Vec<u8>, Vec<u8>> = rows
                .into_iter()
                .map(|row| (row.get::<_, Vec<u8>>(0), row.get::<_, Vec<u8>>(1)))
                .collect();
            // Answered from the map without removing it: one tree can sit
            // under two paths, so a batch may name the same id twice, and
            // each ask gets the body.
            for key in keys {
                out.push(bodies.get(&key).cloned());
            }
        }
        Ok(out)
    }

    /// Reads a tree's stored `TreeV1` bytes and checks them against its ID.
    ///
    /// # Errors
    ///
    /// As [`MetaTx::get_tree`].
    fn get_tree_body(&mut self, id: TreeId) -> Result<Vec<u8>> {
        load_tree(self.c, id).map(|(_, body)| body)
    }

    /// The length of a tree's stored `TreeV1` bytes, without reading them.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if absent.
    fn tree_size(&mut self, id: TreeId) -> Result<usize> {
        let size: i64 = self
            .c
            .query_opt(
                // octet_length of a bytea is int4; the read wants int8.
                "SELECT octet_length(body)::bigint FROM trees WHERE id = $1",
                &[P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .ok_or_else(|| Error::NotFound(format!("tree {id}")))?
            .get(0);
        usize::try_from(size).map_err(|_| Error::Corrupt(format!("tree {id}: length {size}")))
    }

    /// Inserts a land, or finds the land for the same request key. A land
    /// to a ref that does not exist is recorded as rejected at once. A new
    /// land also expires old ones.
    fn record_land(
        &mut self,
        caller: &Caller,
        req: &LandRequest,
        pending_limit: Option<usize>,
    ) -> Result<(RepoId, LandOutcome)> {
        // The outcome's columns ride the lookup the record already runs:
        // the caller learns a settled twin without a second read of the
        // row, and the repo's id rides the same statement, so the record
        // pays no separate repos read. A row that is still pending must
        // be finished: the land's own next step reads the outcome in its
        // first transaction, where a twin that settles between the
        // record and the finish answers.
        let (repo, recorded) = self.repo_and_land_by_key(caller, req)?;
        self.generation(repo, req.changeset)?;
        let recorded = if let Some(recorded) = recorded {
            recorded
        } else {
            self.record_new_land(repo, caller, req, pending_limit)?
        };
        Ok((repo, recorded))
    }

    /// The pending lands that started more than `stale_secs` ago.
    fn stale_lands(&mut self, stale_secs: i64) -> Result<Vec<StaleLand>> {
        let rows = self.c.query(
            "SELECT l.repo_id, l.land_id, l.target_ref, l.caller, l.request_key, r.name
             FROM lands l JOIN repos r ON r.repo_id = l.repo_id
             WHERE l.state = 'pending' AND l.created_at > 0
                 AND l.created_at < now_epoch() - $1",
            &[P::i64(stale_secs)],
        )?;
        Ok(rows
            .iter()
            .map(|row| StaleLand {
                repo: RepoId(row.get(0)),
                land_id: row.get(1),
                target_ref: row.get(2),
                caller: row.get(3),
                request_key: row.get(4),
                repo_name: row.get(5),
            })
            .collect())
    }

    /// Rejects a land that is still pending; 1 if it changed, else 0.
    fn reject_stale_land(&mut self, land_id: i64, detail: String) -> Result<u64> {
        let changed = self.c.execute(
            "UPDATE lands SET state = 'rejected', detail = $1, \
                 reject_reason = 'abandoned', finished_at = now_epoch()
             WHERE land_id = $2 AND state = 'pending'",
            &[P::text(&detail), P::i64(land_id)],
        )?;
        Ok(changed)
    }

    /// Deletes up to [`EXPIRE_PER_LAND`] lands that finished more than
    /// [`LAND_RETENTION_SECS`] ago, oldest first. The newest land always
    /// stays; its ID is the largest, and the sequence never hands an ID
    /// out twice.
    fn expire_lands(&mut self) -> Result<()> {
        let deleted = self.c.execute(
            "DELETE FROM lands WHERE land_id IN (
                 SELECT land_id FROM lands
                 WHERE finished_at < now_epoch() - $1
                     AND land_id < (SELECT max(land_id) FROM lands)
                 ORDER BY finished_at LIMIT $2)",
            &[P::i64(LAND_RETENTION_SECS), P::i64(EXPIRE_PER_LAND)],
        )?;
        if deleted > 0 {
            tracing::debug!(deleted, "expired finished lands");
        }
        Ok(())
    }

    /// A land that `caller` started in `repo`. With `ref_name`, the ref's
    /// state rides the same statement: the finish's outcome check and the
    /// plan's ref read run in one read-only transaction, and one
    /// statement answers both from the same snapshot.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the repo has no such land.
    fn land_outcome(
        &mut self,
        repo: RepoId,
        caller: &Caller,
        land_id: i64,
        ref_name: Option<&str>,
    ) -> Result<(LandOutcome, Option<RefState>)> {
        // The ref rides a LEFT JOIN: a ref deleted between the record and
        // the finish answers no ref columns, and the land's outcome
        // stands alone - the plan then sees a missing ref, as a separate
        // read would have.
        let row = self
            .c
            .query_opt(
                "SELECT l.state, l.result_cs, l.detail, l.conflict_paths, l.reject_reason,
                        r.target, r.version
                 FROM lands l
                 LEFT JOIN refs r ON r.repo_id = l.repo_id AND r.name = $4
                 WHERE l.repo_id = $1 AND l.land_id = $2 AND l.caller = $3",
                &[
                    P::i64(repo.0),
                    P::i64(land_id),
                    P::text(&caller.0),
                    P::text(ref_name.unwrap_or_default()),
                ],
            )?
            .ok_or_else(|| Error::NotFound(format!("land {land_id}")))?;
        let state: String = row.get(0);
        let result: Option<Vec<u8>> = row.get(1);
        let detail: String = row.get(2);
        let paths: Vec<u8> = row.get(3);
        let reason: String = row.get(4);
        let ref_target: Option<Vec<u8>> = row.get(5);
        let ref_version: Option<i64> = row.get(6);
        let head = match (ref_target, ref_version) {
            (Some(target), Some(version)) => Some(RefState {
                target: to_id(&target)?,
                version,
            }),
            (None, None) => None,
            _ => return Err(Error::Corrupt(format!("land {land_id} ref half-read"))),
        };
        let result = result
            .map(|bytes| ChangesetId::try_from(bytes.as_slice()))
            .transpose()
            .map_err(|e| Error::Corrupt(format!("land {land_id} result: {e}")))?;
        let out = LandOutcome {
            moved: false,
            land_id,
            state: LandState::parse(&state)?,
            result,
            previous: None,
            detail,
            conflict_paths: decode_paths(&paths)
                .ok_or_else(|| Error::Corrupt(format!("land {land_id} conflict paths")))?,
            reject_reason: LandRejectReason::parse(&reason)?,
        };
        Ok((out, head))
    }

    /// Moves a pending land to its final state and returns the outcome.
    #[allow(clippy::too_many_arguments)] // the one row's own columns: state, result, detail, paths and reason land together
    fn set_land_state(
        &mut self,
        repo: RepoId,
        land_id: i64,
        state: LandState,
        result: Option<ChangesetId>,
        detail: String,
        conflict_paths: Vec<RepoPath>,
        reject_reason: LandRejectReason,
    ) -> Result<LandOutcome> {
        let updated = self.c.execute(
            "UPDATE lands SET state = $3, result_cs = $4, detail = $5,
                 conflict_paths = $6, reject_reason = $7, finished_at = now_epoch()
             WHERE repo_id = $1 AND land_id = $2 AND state = 'pending'",
            &[
                P::i64(repo.0),
                P::i64(land_id),
                P::text(state.as_str()),
                P::opt_bytes(result.as_ref().map(|id| id_bytes(id.as_bytes()))),
                P::text(&detail),
                P::bytes(&encode_paths(&conflict_paths)),
                P::text(reject_reason.as_str()),
            ],
        )?;
        if updated != 1 {
            return Err(Error::Corrupt(format!(
                "land {land_id} is no longer pending"
            )));
        }
        Ok(LandOutcome {
            moved: false,
            land_id,
            state,
            result,
            previous: None,
            detail,
            conflict_paths,
            reject_reason,
        })
    }

    /// Records that the git commits became the changesets. One statement
    /// per 1,000 pairs: the ids travel as one bytea array each, and the
    /// conflict rule is the single row's, so a repeated pair is skipped.
    fn put_git_imports(&mut self, repo: RepoId, pairs: &[(ObjectId, ChangesetId)]) -> Result<()> {
        for chunk in pairs.chunks(crate::meta::DERIVED_BATCH) {
            let oids: Vec<Vec<u8>> = chunk
                .iter()
                .map(|(oid, _)| oid.as_slice().to_vec())
                .collect();
            let css: Vec<Vec<u8>> = chunk
                .iter()
                .map(|(_, cs)| id_bytes(cs.as_bytes()))
                .collect();
            self.c.execute(
                "INSERT INTO git_import (repo_id, git_algo, git_oid, cs)
                 SELECT $1, $2, u.oid, u.cs
                 FROM unnest($3::bytea[], $4::bytea[]) AS u(oid, cs)
                 ON CONFLICT (repo_id, git_algo, git_oid) DO NOTHING",
                &[
                    P::i64(repo.0),
                    P::i64(SHA1),
                    P::bytea_array(oids),
                    P::bytea_array(css),
                ],
            )?;
        }
        Ok(())
    }

    /// Every git commit already imported into `repo`.
    fn imported_commits(
        &mut self,
        repo: RepoId,
    ) -> Result<std::collections::HashMap<ObjectId, ChangesetId>> {
        let rows = self.c.query(
            "SELECT git_oid, cs FROM git_import WHERE repo_id = $1 AND git_algo = $2",
            &[P::i64(repo.0), P::i64(SHA1)],
        )?;
        let mut out = std::collections::HashMap::new();
        for row in &rows {
            let oid: Vec<u8> = row.get(0);
            let cs: Vec<u8> = row.get(1);
            let corrupt = || Error::Corrupt("git_import row".to_owned());
            let oid = ObjectId::try_from(oid.as_slice()).map_err(|_| corrupt())?;
            let cs = ChangesetId::try_from(cs.as_slice()).map_err(|_| corrupt())?;
            out.insert(oid, cs);
        }
        Ok(out)
    }

    /// Stores a history node under its content address. Safe to repeat.
    fn put_history_node(&mut self, id: NodeId, body: Vec<u8>) -> Result<NodeId> {
        // While the importer's batching is on, the row waits for the
        // flush, which writes it in batches after the derived rows; the
        // buffer answers this transaction's reads of the same node. A
        // deferring write path collects the same way: its commit writes
        // the transaction's nodes with one statement, however many
        // levels the history tree grew. A row the buffer already holds
        // takes the newer body, the same bytes under the same address:
        // the content-addressed body makes the repeat free.
        if let Some(pending) = self.history_pending.as_deref_mut()
            && (self.cs_batching || self.defer_derived)
        {
            let added = body.len();
            if let Some(old) = pending.rows.insert(id.0.to_vec(), body) {
                pending.bytes -= old.len();
            }
            pending.bytes += added;
            #[cfg(any(test, feature = "test-hooks"))]
            HISTORY_PENDING_PEAK.fetch_max(pending.bytes, Ordering::Relaxed);
            if pending.bytes > history_pending_max_bytes() {
                self.flush_history_rows()?;
            }
            return Ok(id);
        }
        self.c.execute(
            "INSERT INTO history_nodes (id, body) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            &[P::bytes(&id.0), P::bytes(&body)],
        )?;
        Ok(id)
    }

    /// Reads a history node and checks it against its ID. Also returns the
    /// length of its stored bytes.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if absent or if it does not match its ID.
    fn get_history_node(&mut self, id: NodeId) -> Result<(Vec<u8>, usize)> {
        // A row the transaction holds for its flush answers from the
        // buffer, batching or not: a deferred write is the transaction's
        // own state.
        if let Some(body) = self
            .history_pending
            .as_deref()
            .and_then(|pending| pending.rows.get(id.0.as_slice()))
        {
            let len = body.len();
            return Ok((body.clone(), len));
        }
        let body: Vec<u8> = self
            .c
            .query_opt(
                "SELECT body FROM history_nodes WHERE id = $1",
                &[P::bytes(&id.0)],
            )?
            .ok_or_else(|| Error::Corrupt(format!("history node {id} is missing")))?
            .get(0);
        if NodeId::of(&body) != id {
            return Err(Error::Corrupt(format!(
                "history node {id} does not match its ID"
            )));
        }
        let len = body.len();
        Ok((body, len))
    }
    /// Every repo with its name, in ID order.
    fn repos(&mut self) -> Result<Vec<(RepoId, String)>> {
        let rows = self
            .c
            .query("SELECT repo_id, name FROM repos ORDER BY repo_id", &[])?;
        Ok(rows
            .iter()
            .map(|row| (RepoId(row.get(0)), row.get(1)))
            .collect())
    }

    /// The `(repo, changeset ID, body)` of the stored changesets after
    /// `(repo, cs)`, in that order, at most `limit`.
    fn repo_changesets_after(
        &mut self,
        from_repo: RepoId,
        from_cs: &[u8],
        limit: usize,
    ) -> Result<Vec<RepoChangeset>> {
        let rows = self.c.query(
            "SELECT rc.repo_id, rc.cs, ch.body
             FROM repo_changesets rc JOIN changesets ch ON ch.id = rc.cs
             WHERE (rc.repo_id, rc.cs) > ($1, $2)
             ORDER BY rc.repo_id, rc.cs LIMIT $3",
            &[
                P::i64(from_repo.0),
                P::bytes(from_cs),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        Ok(rows
            .iter()
            .map(|row| RepoChangeset(RepoId(row.get(0)), row.get(1), row.get(2)))
            .collect())
    }

    /// The changeset IDs that `repo` holds, all of them.
    fn repo_changeset_ids(&mut self, repo: RepoId) -> Result<Vec<Vec<u8>>> {
        let rows = self.c.query(
            "SELECT cs FROM repo_changesets WHERE repo_id = $1 ORDER BY cs",
            &[P::i64(repo.0)],
        )?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    fn repo_changesets_with_bodies(
        &mut self,
        repo: RepoId,
    ) -> Result<Vec<(Vec<u8>, i64, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT c.id, c.gen, c.body FROM repo_changesets r \
             JOIN changesets c ON c.id = r.cs WHERE r.repo_id = $1 ORDER BY c.gen, c.id",
            &[P::i64(repo.0)],
        )?;
        Ok(rows
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect())
    }

    fn list_refs(&mut self, repo: RepoId) -> Result<Vec<(String, Vec<u8>, i64)>> {
        let rows = self.c.query(
            "SELECT name, target, version FROM refs WHERE repo_id = $1 ORDER BY name",
            &[P::i64(repo.0)],
        )?;
        Ok(rows
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect())
    }

    fn pending_lands(&mut self, repo: RepoId) -> Result<Vec<crate::meta::PendingLand>> {
        let rows = self.c.query(
            "SELECT caller, request_key, target_ref, input_cs FROM lands \
             WHERE repo_id = $1 AND state = 'pending' ORDER BY land_id",
            &[P::i64(repo.0)],
        )?;
        Ok(rows
            .iter()
            .map(|row| crate::meta::PendingLand {
                caller: row.get(0),
                request_key: row.get(1),
                target_ref: row.get(2),
                input_cs: row.get(3),
            })
            .collect())
    }

    /// The bodies of the changesets the ids name, as `(id, body)` for
    /// every id that has a stored body. One statement: the ids travel as
    /// one bytea array, and the reply has at most one row per id.
    fn stored_changeset_bodies(&mut self, ids: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT id, body FROM changesets WHERE id = ANY($1)",
            &[P::bytea_array(ids.to_vec())],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect())
    }

    /// The stored length of every id's body. `octet_length` of a bytea is
    /// int4; the read wants i64, so the cast.
    fn stored_changeset_body_lengths(&mut self, ids: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, i64)>> {
        let rows = self.c.query(
            "SELECT id, octet_length(body)::bigint FROM changesets WHERE id = ANY($1)",
            &[P::bytea_array(ids.to_vec())],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect())
    }

    /// The blobs that `repo` may name, all of them.
    fn repo_blob_ids(&mut self, repo: RepoId) -> Result<Vec<Vec<u8>>> {
        let rows = self.c.query(
            "SELECT blob FROM repo_blobs WHERE repo_id = $1 ORDER BY blob",
            &[P::i64(repo.0)],
        )?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    /// Removes the rows that name any of `blobs` for `repo`. One
    /// statement: the blobs travel as one bytea array.
    fn delete_repo_blobs(&mut self, repo: RepoId, blobs: &[Vec<u8>]) -> Result<()> {
        self.c.execute(
            "DELETE FROM repo_blobs WHERE repo_id = $1 AND blob = ANY($2)",
            &[P::i64(repo.0), P::bytea_array(blobs.to_vec())],
        )?;
        Ok(())
    }

    /// Whether any repo names `blob`. Driven from `repos`:
    /// `repo_blobs` has no index on `blob` alone.
    fn any_repo_names(&mut self, blob: &[u8]) -> Result<bool> {
        Ok(self
            .c
            .query_opt(
                "SELECT 1 FROM repos r
                 WHERE EXISTS (SELECT 1 FROM repo_blobs rb
                               WHERE rb.repo_id = r.repo_id AND rb.blob = $1)
                 LIMIT 1",
                &[P::bytes(blob)],
            )?
            .is_some())
    }

    /// See the data directory's `repos_naming`: one probe per repo, same
    /// shape.
    fn repos_naming(&mut self, blob: &[u8]) -> Result<Vec<RepoId>> {
        let rows = self.c.query(
            "SELECT r.repo_id FROM repos r
             WHERE EXISTS (SELECT 1 FROM repo_blobs rb
                           WHERE rb.repo_id = r.repo_id AND rb.blob = $1)
             ORDER BY r.repo_id",
            &[P::bytes(blob)],
        )?;
        Ok(rows.iter().map(|row| RepoId(row.get(0))).collect())
    }

    /// The `(changeset ID, body)` of the changesets after `from`, in ID
    /// order, at most `limit`.
    fn changeset_bodies_after(
        &mut self,
        from: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT id, body FROM changesets WHERE id > $1 ORDER BY id LIMIT $2",
            &[
                P::bytes(from),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
    }

    /// The `(tree ID, body)` of the trees after `from`, in ID order, at
    /// most `limit`.
    fn tree_bodies_after(&mut self, from: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT id, body FROM trees WHERE id > $1 ORDER BY id LIMIT $2",
            &[
                P::bytes(from),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
    }

    /// The `(node ID, body)` of the history nodes after `from`, in ID
    /// order, at most `limit`.
    fn history_node_bodies_after(
        &mut self,
        from: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT id, body FROM history_nodes WHERE id > $1 ORDER BY id LIMIT $2",
            &[
                P::bytes(from),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
    }

    /// The `repo_blobs` rows after `(repo, blob)`, at most `limit`.
    fn repo_blob_rows_after(
        &mut self,
        from_repo: RepoId,
        from_blob: &[u8],
        limit: usize,
    ) -> Result<Vec<(RepoId, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT repo_id, blob FROM repo_blobs
             WHERE (repo_id, blob) > ($1, $2) ORDER BY repo_id, blob LIMIT $3",
            &[
                P::i64(from_repo.0),
                P::bytes(from_blob),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        Ok(rows
            .iter()
            .map(|row| (RepoId(row.get(0)), row.get(1)))
            .collect())
    }

    /// The refs whose target no `repo_changesets` row of their repo holds.
    fn dangling_refs(&mut self) -> Result<Vec<(RepoId, String, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT repo_id, name, target FROM refs r
             WHERE NOT EXISTS (
                 SELECT 1 FROM repo_changesets rc
                 WHERE rc.repo_id = r.repo_id AND rc.cs = r.target
             )",
            &[],
        )?;
        Ok(rows
            .iter()
            .map(|row| (RepoId(row.get(0)), row.get(1), row.get(2)))
            .collect())
    }

    /// Every land whose input or result changeset is not stored, as
    /// `(land, changeset, which column)`.
    fn lands_with_missing_changesets(&mut self) -> Result<Vec<(i64, Vec<u8>, &'static str)>> {
        let mut out = Vec::new();
        for (column, when) in [
            ("input_cs", "1 = 1"),
            ("result_cs", "result_cs IS NOT NULL"),
        ] {
            let sql = format!(
                "SELECT land_id, {column} FROM lands l
                 WHERE {when} AND NOT EXISTS (
                     SELECT 1 FROM changesets c WHERE c.id = l.{column}
                 )"
            );
            let rows = self.c.query(&sql, &[])?;
            for row in rows {
                out.push((row.get(0), row.get(1), column));
            }
        }
        Ok(out)
    }

    /// The derived rows after `(kind, version, cs)`, at most `limit`, with
    /// whether their changeset is stored.
    fn derived_rows_after(
        &mut self,
        kind: &str,
        version: u32,
        cs: &[u8],
        limit: usize,
    ) -> Result<Vec<FsckDerivedRow>> {
        let rows = self.c.query(
            "SELECT d.kind, d.version, d.cs, d.value,
                    EXISTS (SELECT 1 FROM changesets c WHERE c.id = d.cs)
             FROM derived d
             WHERE (d.kind, d.version, d.cs) > ($1, $2, $3)
             ORDER BY d.kind, d.version, d.cs LIMIT $4",
            &[
                P::text(kind),
                P::i64(i64::from(version)),
                P::bytes(cs),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        rows.iter()
            .map(|row| -> Result<FsckDerivedRow> {
                // d.version is BIGINT here and u32 in the row: read the
                // int8 first, like every numeric expression here.
                let version: i64 = row.get(1);
                Ok(FsckDerivedRow(
                    row.get(0),
                    u32::try_from(version)
                        .map_err(|_| Error::Corrupt(format!("derived version {version}")))?,
                    row.get(2),
                    row.get(3),
                    row.get(4),
                ))
            })
            .collect()
    }

    fn clear_restore_mark(&mut self) -> Result<()> {
        // Publication is atomic with this: the caller's transaction
        // holds, so the store that answers reads never carries the
        // mark, and a mark that survives only survives a kill.
        self.c.batch("DROP TABLE IF EXISTS restore_staging")?;
        Ok(())
    }
}

/// The statement helpers of [`PgTx::bulk_seed`], outside the trait impl:
/// each inserts its rows in batches of 1,000 through one `unnest`.
#[cfg(any(test, feature = "test-hooks"))]
impl PgTx<'_> {
    fn insert_seed_changesets(&mut self, rows: &[(Vec<u8>, i64, Vec<u8>)]) -> Result<()> {
        const CHUNK: usize = 1_000;
        for chunk in rows.chunks(CHUNK) {
            let ids = chunk
                .iter()
                .map(|(id, _, _)| id.clone())
                .collect::<Vec<_>>();
            let gens = chunk
                .iter()
                .map(|(_, generation, _)| *generation)
                .collect::<Vec<_>>();
            let bodies = chunk
                .iter()
                .map(|(_, _, body)| body.clone())
                .collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO changesets (id, gen, body)
                 SELECT * FROM unnest($1::bytea[], $2::int8[], $3::bytea[])
                 ON CONFLICT (id) DO NOTHING",
                &[
                    P::bytea_array(ids),
                    P::i64_array(gens),
                    P::bytea_array(bodies),
                ],
            )?;
        }
        Ok(())
    }

    fn insert_seed_parents(&mut self, rows: &[(Vec<u8>, i64, Vec<u8>)]) -> Result<()> {
        const CHUNK: usize = 1_000;
        for chunk in rows.chunks(CHUNK) {
            let css = chunk
                .iter()
                .map(|(cs, _, _)| cs.clone())
                .collect::<Vec<_>>();
            let ns = chunk.iter().map(|(_, n, _)| *n).collect::<Vec<_>>();
            let parents = chunk
                .iter()
                .map(|(_, _, parent)| parent.clone())
                .collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO changeset_parents (cs, n, parent)
                 SELECT * FROM unnest($1::bytea[], $2::int8[], $3::bytea[])
                 ON CONFLICT (cs, n) DO NOTHING",
                &[
                    P::bytea_array(css),
                    P::i64_array(ns),
                    P::bytea_array(parents),
                ],
            )?;
        }
        Ok(())
    }

    fn insert_seed_members(&mut self, rows: &[(i64, Vec<u8>)]) -> Result<()> {
        const CHUNK: usize = 1_000;
        for chunk in rows.chunks(CHUNK) {
            let repos = chunk.iter().map(|(repo, _)| *repo).collect::<Vec<_>>();
            let css = chunk.iter().map(|(_, cs)| cs.clone()).collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO repo_changesets (repo_id, cs)
                 SELECT * FROM unnest($1::int8[], $2::bytea[])
                 ON CONFLICT (repo_id, cs) DO NOTHING",
                &[P::i64_array(repos), P::bytea_array(css)],
            )?;
        }
        Ok(())
    }

    fn insert_seed_derived(
        &mut self,
        rows: &[(&'static str, i64, Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        const CHUNK: usize = 1_000;
        for chunk in rows.chunks(CHUNK) {
            let kinds = chunk
                .iter()
                .map(|(kind, ..)| (*kind).to_owned())
                .collect::<Vec<_>>();
            let versions = chunk
                .iter()
                .map(|(_, version, ..)| *version)
                .collect::<Vec<_>>();
            let css = chunk
                .iter()
                .map(|(_, _, cs, _)| cs.clone())
                .collect::<Vec<_>>();
            let values = chunk
                .iter()
                .map(|(_, _, _, value)| value.clone())
                .collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO derived (kind, version, cs, value)
                 SELECT * FROM unnest($1::text[], $2::int8[], $3::bytea[], $4::bytea[])
                 ON CONFLICT (kind, version, cs) DO NOTHING",
                &[
                    P::text_array(kinds),
                    P::i64_array(versions),
                    P::bytea_array(css),
                    P::bytea_array(values),
                ],
            )?;
        }
        Ok(())
    }
}

impl Meta for Pg {
    type Tx<'a>
        = PgTx<'a>
    where
        Self: 'a;

    fn read<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        // Every commit is durable when it returns, so what `f` sees is
        // durable, and the read need not wait.
        Pg::read(self, f)
    }

    fn read_counted<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> (Result<T>, u64) {
        // The count is for a caller that waits before it replies; on this
        // backend there is nothing to wait for.
        (Pg::read(self, f), 0)
    }

    fn wait_durable(&self, _seen: u64) -> Result<()> {
        Ok(())
    }

    fn read_unflushed<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        Pg::read_unflushed(self, f)
    }

    fn commit<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        Pg::commit(self, f)
    }

    fn write<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        // Durable on return, so `write` is `commit`.
        Pg::write(self, f)
    }

    fn flush(&self) -> Result<()> {
        // synchronous_commit is on: a commit is durable when it returns.
        // The blob files are the backend's own disk state, though: on a
        // drive whose cache fsync does not clear, they reach the drive on
        // their own schedule.
        if crate::store::FSYNC_LEAVES_DRIVE_CACHE {
            self.inner.blobs.full_flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, NoTls, delete_prefixes, merged_options};
    use crate::pg::{Pairing, Pg, pairing_decision};
    use std::sync::Arc;

    /// The pairing decision's whole table: every arm of the match, on
    /// both sides of the repos flag. A rename or an arm that flips
    /// shows here, not in a store that refuses or adopts wrongly.
    #[test]
    fn the_pairing_table_covers_every_case() {
        let marker = [1u8; 16];
        let held = [2u8; 16];
        let refuse = |pairing: Pairing| matches!(pairing, Pairing::Refuse(_));
        // One store on both sides agrees, whatever the schema holds.
        assert!(matches!(
            pairing_decision(Some(marker), Some(marker), true),
            Pairing::Agree
        ));
        assert!(matches!(
            pairing_decision(Some(marker), Some(marker), false),
            Pairing::Agree
        ));
        // Two stores on one disk refuse, whatever the schema holds.
        assert!(refuse(pairing_decision(Some(marker), Some(held), true)));
        assert!(refuse(pairing_decision(Some(marker), Some(held), false)));
        // A served database with no marker refuses; a schema that
        // never served is a first open's cut, and the marker is
        // written.
        assert!(refuse(pairing_decision(None, Some(held), true)));
        assert!(matches!(
            pairing_decision(None, Some(held), false),
            Pairing::AdoptCut
        ));
        // A marker over a schema that holds no store and no repos is
        // the wrong-URL refusal; over one that holds repos it is a
        // dump beside its own directory, and the open mints the row.
        assert!(refuse(pairing_decision(Some(marker), None, false)));
        assert!(matches!(
            pairing_decision(Some(marker), None, true),
            Pairing::Fresh
        ));
        // Neither side names a store: a first boot, whatever the
        // schema holds.
        assert!(matches!(pairing_decision(None, None, true), Pairing::Fresh));
        assert!(matches!(
            pairing_decision(None, None, false),
            Pairing::Fresh
        ));
    }

    #[test]
    fn the_delete_prefix_folds_the_calls_of_one_kind_in_order() {
        // Two versions of one kind, the higher version called first:
        // the first call takes 2 of the 6 rows below it, the second 1
        // of the 3 rows its lower version still sees. The per-call
        // deletes would leave 6 - 2 - 1 = 3 rows.
        let kind = "k".to_owned();
        let (kinds, totals) = delete_prefixes(&[(kind.clone(), 2), (kind, 2)], &[6, 3]);
        assert_eq!(kinds, vec!["k"]);
        assert_eq!(totals, vec![3]);
        // Kinds interleave in the call order; each kind folds alone: a's
        // first call takes the 2 rows below it, its second 5 of the 8
        // still below, so 7; b sees no rows below its version.
        let (kinds, totals) = delete_prefixes(
            &[
                ("a".to_owned(), 5),
                ("b".to_owned(), 1),
                ("a".to_owned(), 5),
            ],
            &[2, 0, 10],
        );
        assert_eq!(kinds, vec!["a", "b"]);
        assert_eq!(totals, vec![7, 0]);
        // A later call's version sees fewer rows than the calls before
        // it took: its share is nothing.
        let kind = "k".to_owned();
        let (_, totals) = delete_prefixes(&[(kind.clone(), 5), (kind, 5)], &[10, 2]);
        assert_eq!(totals, vec![5]);
    }

    #[test]
    fn a_worker_panic_logs_its_payload_and_the_error_names_the_log() {
        // The shared subscriber is one and global, so the line the
        // worker thread logs lands where this test reads it.
        crate::service::test_init_log();
        let url = crate::backend::test_hooks::test_url().unwrap();
        let client = super::test_hooks::connect(&url, None).unwrap();
        let error = client
            .raw(Box::new(|_| -> crate::error::Result<()> {
                panic!("the worker's test panic");
            }))
            .unwrap_err();
        assert!(
            error.to_string().contains("its log line names why"),
            "{error}"
        );
        let text = crate::service::test_log_text();
        assert!(
            text.contains("the worker's test panic"),
            "the log holds the panic payload: {text}"
        );
        assert!(
            text.contains("the postgres worker thread panicked and stopped"),
            "the log says the worker stopped: {text}"
        );
    }

    /// A `store_uuid` row that is not 16 bytes refuses the reopen with the
    /// row's length, instead of panicking it: a database of another
    /// backend's shape is corrupt data, and corrupt data is refused.
    #[test]
    fn a_store_uuid_row_of_the_wrong_length_refuses_the_open() {
        let _slot = super::damage::slot();
        let dir = crate::test_support::private_tempdir();
        let store = crate::Backend::open_test(dir.path()).unwrap();
        super::damage::corrupt_store_uuid(&store).unwrap();
        drop(store);
        // The corrupt row is the answer the database gives once the old
        // session has ended; the reopen waits for that session the way
        // the one-server lock test does, instead of racing the worker's
        // close.
        let Err(error) = crate::Backend::reopen_test(dir.path()) else {
            panic!("a store_uuid row of 1 byte refuses the open");
        };
        assert!(
            matches!(&error, crate::error::Error::Corrupt(message)
                if message.contains("the store_uuid row holds 1 bytes, not 16")),
            "{error:?}"
        );
    }

    const OWN: &str = "-csynchronous_commit=on -cstatement_timeout=600000 -clock_timeout=5000";

    #[test]
    fn the_urls_options_survive_the_backends_own() {
        assert_eq!(
            merged_options(Some("--search_path=scion_test_1"), OWN),
            format!("--search_path=scion_test_1 {OWN}")
        );
    }

    /// A URL whose `search_path` names no existing schema refuses with
    /// the fix named, not 'unreachable': the database answered, and
    /// only the schema is missing.
    #[test]
    fn a_search_path_that_names_no_schema_refuses_with_the_fix() {
        let url = crate::backend::test_hooks::test_url().unwrap();
        let mut client = Client::connect(&url, NoTls).unwrap();
        let setup = super::Setup::new(Some("scion_test_missing"), true, &mut client);
        assert!(!setup.ok(), "a missing schema cannot hold the setup");
        let error = super::setup_error(&setup, false);
        assert!(
            error
                .to_string()
                .contains("the URL's search_path names no existing schema; create it first"),
            "{error}"
        );
    }

    #[test]
    fn a_53100_database_error_is_disk_full_with_the_fixed_reply() {
        use super::pg_error_of;
        use crate::{Error, StorageClass};
        // A source of the right type with no SQLSTATE of its own: a
        // connect to port 1, which every kernel refuses. The mapping
        // runs on the SQLSTATE the test hands it, not on this source.
        let Err(refused) = Client::connect("postgres://127.0.0.1:1/x", NoTls) else {
            panic!("a connect to port 1 is refused");
        };
        assert!(!refused.is_closed());
        assert!(refused.code().is_none());
        let error = pg_error_of(Some("53100"), refused);
        assert!(
            matches!(&error, Error::Storage(s) if s.class() == StorageClass::DiskFull),
            "{error:?}"
        );
        // The reply the client sees is the fixed one: it names the
        // shortage and the retry, not the server path.
        let status = tonic::Status::from(error);
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            status.message(),
            "the server is out of disk space; retry later"
        );
    }

    #[test]
    fn on_a_clash_the_backend_options_win() {
        // The backend's option comes last, and a `-c` of the startup
        // options overrides one before it.
        let merged = merged_options(Some("-cstatement_timeout=1"), OWN);
        assert!(merged.ends_with(OWN), "{merged}");
    }

    #[test]
    fn a_url_without_options_gets_the_backends_only() {
        assert_eq!(merged_options(None, OWN), OWN);
        assert_eq!(merged_options(Some(""), OWN), OWN);
    }

    /// The shipped Postgres migrations are frozen: a database an old
    /// build made was built from these files as they shipped, and an
    /// edit would cut it
    /// off ([#318]).
    #[test]
    fn the_shipped_migrations_stay_as_they_shipped() {
        for (name, file, hex) in [
            (
                "0001_init",
                include_bytes!("../migrations/postgres/0001_init.sql").as_slice(),
                "9f95c671379b0d1c8e2ac2af34d95badca7929d379edcf0f8e8a02572f4a0c63",
            ),
            (
                "0002_view_roots",
                include_bytes!("../migrations/postgres/0002_view_roots.sql").as_slice(),
                "483a299895752535dbe3856ed496894687f4840b5edfcc2fdc24b6eece02dff2",
            ),
            (
                "0003_view_trees",
                include_bytes!("../migrations/postgres/0003_view_trees.sql").as_slice(),
                "3935e99d760f8848860acf33aeb33633a288b03aadd856a65cbe474d88989716",
            ),
            (
                "0004_store_uuid",
                include_bytes!("../migrations/postgres/0004_store_uuid.sql").as_slice(),
                "6ac9d420e9499b40e38196c598d4c7c75c88180ed729261844b4e44f368dfab4",
            ),
        ] {
            assert_eq!(
                blake3::hash(file).to_hex().as_str(),
                hex,
                "migration {name} shipped: add a new migration, never edit one",
            );
        }
    }

    /// A write that waits for the collector's key records the wait in the
    /// metrics: the gauge an operator watches for a busy database.
    #[test]
    fn a_commit_records_its_wait_for_the_collectors_key() {
        let url = crate::backend::test_hooks::test_url().unwrap();
        let dir = crate::test_support::private_tempdir();
        let schema = crate::backend::test_hooks::test_schema(dir.path());
        let metrics = std::sync::Arc::new(crate::metrics::Metrics::default());
        let pg = Pg::open_test(
            &url,
            &schema,
            2,
            dir.path(),
            crate::backend::Marker::read(dir.path()),
        )
        .unwrap();
        pg.set_metrics(Arc::clone(&metrics));
        assert!(
            !metrics.render().contains("scion_pg_collector_key_wait"),
            "nothing waited yet"
        );
        // Another client holds the key, so the commit below really waits.
        let mut holder = Client::connect(&url, NoTls).unwrap();
        holder
            .batch_execute(&format!(
                "SET search_path TO {schema};
                 BEGIN;
                 SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':gc'));"
            ))
            .unwrap();
        let writer = std::thread::spawn(move || {
            pg.commit(|_tx| Ok(())).unwrap();
        });
        // Give the writer time to reach the key, then let it in.
        for _ in 0..2_000 {
            std::thread::yield_now();
        }
        holder.batch_execute("COMMIT").unwrap();
        let mut spins = 0;
        while !writer.is_finished() {
            spins += 1;
            assert!(spins < 1_000_000, "the writer never finished");
            std::thread::yield_now();
        }
        writer.join().unwrap();
        let text = metrics.render();
        assert!(
            text.contains("scion_pg_collector_key_wait_seconds_count 1\n"),
            "{text}"
        );
    }
}

#[cfg(test)]
pub(crate) mod pool_tests {
    use super::*;
    use crate::store::READERS;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Mutex, MutexGuard, PoisonError};

    /// The tests that hold a pool of the server's size run one at a
    /// time: each holds a write connection and `READERS` readers, and
    /// the suite's other backends fill the rest of the database's
    /// `max_connections` (E53300, "too many clients").
    pub(crate) static SERVER_SIZED_POOL: Mutex<()> = Mutex::new(());

    fn server_sized_pool() -> MutexGuard<'static, ()> {
        SERVER_SIZED_POOL
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// A Postgres backend of a test database, with the pool gate: the
    /// caller holds the pair for its whole test body.
    /// An open whose batch fails after its BEGIN ran leaves no open
    /// transaction behind: the helper rolls the aborted transaction back
    /// before the error returns, and the client answers its next holder
    /// cleanly. Without the rollback, the session would sit in an
    /// aborted transaction - "current transaction is aborted" on every
    /// statement until someone rolls it back.
    /// The finish's joined read answers a land whose ref vanished after
    /// the record: the land's outcome stands alone (LEFT JOIN), the ref
    /// half answers None, and the plan then sees a missing ref - the
    /// same answer a separate ref read would have given. An inner join
    /// would lose the land's row with its ref.
    #[test]
    fn a_finish_still_reads_the_land_when_its_ref_vanished() {
        let data = crate::test_support::private_tempdir();
        let store = crate::Backend::open_test(data.path()).unwrap();
        let cs = store
            .commit(|c| {
                let repo = c.create_repo("r")?;
                let cs = c.insert_changeset(
                    repo,
                    &Changeset::new(
                        vec![],
                        scion_core::IdentityTime::new(
                            b"Ada".to_vec(),
                            b"ada@example.com".to_vec(),
                            0,
                            0,
                        )
                        .expect("identity fits the format"),
                        None,
                        b"root".to_vec(),
                        std::collections::BTreeMap::new(),
                    )
                    .expect("changeset fits the format"),
                )?;
                assert!(c.cas_ref(repo, "main", None, cs)?);
                Ok(cs)
            })
            .unwrap();
        let req = LandRequest {
            repo: "r".into(),
            changeset: cs,
            target_ref: "main".into(),
            request_key: "k".into(),
            deadline: crate::deadline::Deadline::new(Instant::now() + Duration::from_secs(60)),
        };
        let (_, recorded) = store
            .commit(|c| c.record_land(&Caller("t".into()), &req, None))
            .unwrap();
        assert_eq!(recorded.state, LandState::Pending);
        let pg = store.into_pg();
        // The join answers the ref while it exists.
        let (row, head) = crate::meta::Meta::read(&pg, |c| {
            let repo = c.repo_id("r")?;
            c.land_outcome(repo, &Caller("t".into()), recorded.land_id, Some("main"))
        })
        .unwrap();
        assert_eq!(row.state, LandState::Pending);
        assert!(head.is_some());
        // The ref goes away (the API never deletes refs; an operator or a
        // future gc can), and the land's outcome must survive it.
        let mut reader = pg.inner.readers.take(READER_WAIT).unwrap();
        reader
            .client()
            .execute("DELETE FROM refs WHERE name = 'main'", &[])
            .unwrap();
        drop(reader);
        let (row, head) = crate::meta::Meta::read(&pg, |c| {
            let repo = c.repo_id("r")?;
            c.land_outcome(repo, &Caller("t".into()), recorded.land_id, Some("main"))
        })
        .unwrap();
        assert_eq!(row.state, LandState::Pending, "the land row stands alone");
        assert!(head.is_none(), "the ref half answers None");
    }

    #[test]
    fn an_open_whose_set_local_failed_leaves_no_open_transaction() {
        let (_pool, pg) = pg();
        let mut reader = pg.inner.readers.take(READER_WAIT).unwrap();
        let client = reader.client();
        // A statement_timeout above the bigint range fails the batch's
        // second command; the BEGIN in the same batch has already run.
        let huge = std::time::Duration::from_millis(u64::MAX);
        let Err(_) = PgTx::begin_read(
            client,
            ReadIsolation::ReadCommitted,
            huge,
            std::time::Duration::from_secs(30),
        ) else {
            panic!("the out-of-range timeout must fail the open");
        };
        // The session answers its next holder: no aborted transaction.
        let one: i32 = client.query("SELECT 1", &[]).unwrap()[0].get(0);
        assert_eq!(one, 1);
        client.execute("BEGIN", &[]).unwrap();
        client.execute("COMMIT", &[]).unwrap();
    }

    fn pg() -> (MutexGuard<'static, ()>, Pg) {
        let gate = server_sized_pool();
        let data = crate::test_support::private_tempdir();
        // The pool tests hold the whole reader pool: they need the
        // server's pool size, not the suite's small test pool.
        let pg = crate::backend::Backend::open_test_with_readers(data.path(), READERS)
            .unwrap()
            .into_pg();
        (gate, pg)
    }

    /// A transaction whose connection dies mid-way leaves the worker's
    /// client usable: the next read on the same reader takes a fresh
    /// session, and a BEGIN on it opens a real transaction. A build that
    /// keeps the lost state on a running BEGIN bricks the reader: every
    /// later read on it fails, and its session holds an open transaction
    /// forever.
    #[test]
    fn a_read_whose_connection_died_mid_transaction_recovers() {
        let (_pool, pg) = pg();
        // One reader client, out of the pool, in a transaction.
        let mut reader = pg.inner.readers.take(READER_WAIT).unwrap();
        let client = reader.client();
        client.execute("BEGIN", &[]).unwrap();
        let pid: i32 = client.query("SELECT pg_backend_pid()", &[]).unwrap()[0].get(0);
        // The session dies under the transaction, from another
        // connection. The next statement reconnects, finds the
        // transaction lost, and answers what a caller rolls back.
        pg.commit(|tx| {
            tx.c.execute(
                "SELECT pg_terminate_backend($1::bigint::int)",
                &[P::i64(i64::from(pid))],
            )?;
            Ok(())
        })
        .unwrap();
        let err = client.query_opt("SELECT 1", &[]).unwrap_err();
        assert!(
            matches!(err, Error::Unavailable(ref m) if m.contains("lost")),
            "the lost transaction answered {err}"
        );
        drop(reader);
        // The same reader, back from the pool: a full read on it runs.
        let again = pg.read(|tx| tx.c.query_opt("SELECT 1", &[]).map(|row| row.is_some()));
        assert!(again.unwrap(), "the reader never recovered");
    }

    /// One statement that runs past its bound is cancelled, and the
    /// reader comes back to the pool.
    #[test]
    fn a_read_statement_past_its_bound_is_cancelled() {
        let (_pool, pg) = pg();
        let err = pg
            .read_bounded(
                |tx| {
                    tx.c.query_opt("SELECT pg_sleep(2)", &[])?;
                    Ok(())
                },
                Duration::from_millis(50),
                Duration::from_secs(30),
                READER_WAIT,
            )
            .unwrap_err();
        // SQLSTATE 57014 is the statement clock, and map_pg_error gives
        // it the deadline answer.
        assert!(
            matches!(err, Error::DeadlineExceeded(_)),
            "the cancelled read came back as {err}"
        );
        // The reader returned: the next read runs at once.
        let again = pg.read(|tx| {
            tx.c.query_opt("SELECT 1", &[])
                .map(|row| row.map(|row| row.get::<_, i32>(0)))
        });
        assert_eq!(again.unwrap(), Some(1));
    }

    /// A call whose wait for a reader passes the bound hears
    /// `Unavailable`, instead of blocking to its deadline.
    #[test]
    fn a_call_that_waits_past_the_pool_bound_is_refused() {
        let (_pool, pg) = pg();
        let pg = Arc::new(pg);
        let busy = Arc::new(AtomicUsize::new(0));
        let holds: Vec<_> = (0..READERS)
            .map(|_| {
                let pg = Arc::clone(&pg);
                let busy = Arc::clone(&busy);
                std::thread::spawn(move || {
                    pg.read_bounded(
                        |_| {
                            busy.fetch_add(1, AtomicOrdering::SeqCst);
                            std::thread::sleep(Duration::from_millis(600));
                            Ok(())
                        },
                        Duration::from_secs(300),
                        Duration::from_secs(30),
                        Duration::from_secs(30),
                    )
                    .unwrap();
                })
            })
            .collect::<Vec<_>>();
        // Every reader is out of the pool.
        let wait = std::time::Instant::now() + Duration::from_secs(10);
        while busy.load(AtomicOrdering::SeqCst) < READERS && std::time::Instant::now() < wait {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            busy.load(AtomicOrdering::SeqCst),
            READERS,
            "all readers busy"
        );
        let refused = pg
            .read_bounded(
                |tx| {
                    tx.c.query_opt("SELECT 1", &[])?;
                    Ok(())
                },
                Duration::from_secs(300),
                Duration::from_secs(30),
                Duration::from_millis(50),
            )
            .unwrap_err();
        assert!(matches!(refused, Error::Unavailable(_)), "{refused}");
        for hold in holds {
            hold.join().unwrap();
        }
    }

    /// A reader that goes back to the pool is taken again. The take's
    /// tail reports the busy gauge, and the report takes the idle lock
    /// that the take itself still held: the first read of a store then
    /// hung forever, with no panic and nothing on the database. A 10 s
    /// ceiling keeps this a fast failure that names the step.
    #[test]
    fn a_reader_taken_and_given_back_is_taken_again() {
        let (_pool, pg) = pg();
        let pg = Arc::new(pg);
        let done = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            {
                let pg = Arc::clone(&pg);
                let done = Arc::clone(&done);
                scope.spawn(move || {
                    for round in 1..=2 {
                        pg.read_bounded(
                            |tx| {
                                tx.c.query_opt("SELECT 1", &[])?;
                                Ok(())
                            },
                            Duration::from_secs(30),
                            Duration::from_secs(30),
                            Duration::from_secs(5),
                        )
                        .unwrap();
                        done.store(round, AtomicOrdering::SeqCst);
                    }
                });
            }
            let wait = std::time::Instant::now() + Duration::from_secs(10);
            while done.load(AtomicOrdering::SeqCst) < 2 && std::time::Instant::now() < wait {
                std::thread::sleep(Duration::from_millis(50));
            }
            assert_eq!(
                done.load(AtomicOrdering::SeqCst),
                2,
                "the pool take deadlocked at step {}",
                done.load(AtomicOrdering::SeqCst) + 1
            );
        });
    }

    /// The pool wait lands in the histogram, and the busy gauge returns
    /// to zero when the reads are done.
    #[test]
    fn the_pool_records_its_waits_and_its_busy_readers() {
        let (_pool, pg) = pg();
        let metrics = Arc::new(Metrics::default());
        pg.set_metrics(Arc::clone(&metrics));
        let pg = Arc::new(pg);
        let busy = Arc::new(AtomicUsize::new(0));
        let holds: Vec<_> = (0..READERS)
            .map(|_| {
                let pg = Arc::clone(&pg);
                let busy = Arc::clone(&busy);
                std::thread::spawn(move || {
                    pg.read_bounded(
                        |_| {
                            busy.fetch_add(1, AtomicOrdering::SeqCst);
                            std::thread::sleep(Duration::from_millis(500));
                            Ok(())
                        },
                        Duration::from_secs(30),
                        Duration::from_secs(30),
                        Duration::from_millis(400),
                    )
                    .unwrap();
                })
            })
            .collect::<Vec<_>>();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while busy.load(AtomicOrdering::SeqCst) < READERS && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        // One more call waits ~400 ms for a reader, then takes one as the
        // holders return theirs.
        let waited = std::time::Instant::now();
        pg.read_bounded(
            |tx| {
                tx.c.query_opt("SELECT 1", &[])?;
                Ok(())
            },
            Duration::from_secs(300),
            Duration::from_secs(30),
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(
            waited.elapsed() >= Duration::from_millis(100),
            "the wait was real"
        );
        for hold in holds {
            hold.join().unwrap();
        }
        // The gauges read the rendered text, as a scraper would: the
        // busy gauge is back to zero, and the wait is in the histogram.
        let text = metrics.render();
        let gauge = text
            .lines()
            .find(|line| line.starts_with("scion_pg_readers_busy "))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(u64::MAX);
        assert_eq!(gauge, 0, "{text}");
        let count = text
            .lines()
            .find(|line| line.starts_with("scion_pg_pool_wait_seconds_count "))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);
        assert!(count >= 1, "the wait was recorded: {text}");
        let sum = text
            .lines()
            .find(|line| line.starts_with("scion_pg_pool_wait_seconds_sum "))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|n| n.parse::<f64>().ok())
            .unwrap_or(0.0);
        assert!(sum > 0.0, "sum {sum}");
        assert_eq!(pg.tx_age(), Duration::ZERO);
    }

    /// The tx-age gauge says how old the oldest open read is, and goes
    /// back to zero when it ends.
    #[test]
    fn the_tx_age_gauge_tracks_the_oldest_open_read() {
        let (_pool, pg) = pg();
        let pg = Arc::new(pg);
        assert_eq!(pg.tx_age(), Duration::ZERO);
        let hold = {
            let pg = Arc::clone(&pg);
            std::thread::spawn(move || {
                pg.read_bounded(
                    |_| {
                        std::thread::sleep(Duration::from_millis(300));
                        Ok(())
                    },
                    Duration::from_secs(30),
                    Duration::from_secs(30),
                    Duration::from_secs(5),
                )
                .unwrap();
            })
        };
        std::thread::sleep(Duration::from_millis(100));
        assert!(pg.tx_age() > Duration::ZERO, "{:?}", pg.tx_age());
        hold.join().unwrap();
        assert_eq!(pg.tx_age(), Duration::ZERO);
    }

    /// Two lands of one principal that record at the same time, from two
    /// independent backends over one database, cannot both pass the
    /// pending-lands cap. The count and the insert are one step: every
    /// write transaction of the schema holds the schema's advisory key
    /// (`Pg::commit`) until it commits, so the second land counts the
    /// first one's row and hears the limit's answer.
    #[test]
    fn two_lands_that_record_together_respect_the_pending_cap() {
        let data = crate::test_support::private_tempdir();
        let a = crate::backend::Backend::open_test(data.path()).unwrap();
        let b = crate::backend::Backend::open_test_shared(data.path()).unwrap();
        let cs = a
            .commit(|c| {
                let repo = c.create_repo("r")?;
                let cs = c.insert_changeset(
                    repo,
                    &Changeset::new(
                        vec![],
                        scion_core::IdentityTime::new(
                            b"Ada".to_vec(),
                            b"ada@example.com".to_vec(),
                            0,
                            0,
                        )
                        .expect("identity fits the format"),
                        None,
                        b"root".to_vec(),
                        std::collections::BTreeMap::new(),
                    )
                    .expect("changeset fits the format"),
                )?;
                assert!(c.cas_ref(repo, "main", None, cs)?);
                Ok(cs)
            })
            .unwrap();
        let req = move |key: &str| LandRequest {
            repo: "r".into(),
            changeset: cs,
            target_ref: "main".into(),
            request_key: key.into(),
            deadline: crate::deadline::Deadline::new(Instant::now() + Duration::from_secs(60)),
        };

        // Both backends release together, one caller, a cap of one, two
        // new keys: exactly one land records, and the other is over the
        // cap. Every wait has a ceiling that names its stall.
        let go = Arc::new(std::sync::Barrier::new(2));
        let done = Arc::new(AtomicUsize::new(0));
        let results = Arc::new(Mutex::new(Vec::new()));
        let mut threads = Vec::new();
        for (store, key) in [(a, "k1"), (b, "k2")] {
            let (go, done, results) = (Arc::clone(&go), Arc::clone(&done), Arc::clone(&results));
            threads.push(std::thread::spawn(move || {
                go.wait();
                let out = store.commit(|c| c.record_land(&Caller("t".into()), &req(key), Some(1)));
                results
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(out);
                done.fetch_add(1, AtomicOrdering::SeqCst);
            }));
        }
        let ceiling = Instant::now() + Duration::from_secs(30);
        while done.load(AtomicOrdering::SeqCst) < 2 {
            assert!(
                Instant::now() < ceiling,
                "the two record_land calls never finished within 30 s"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        let results = std::mem::take(&mut *results.lock().unwrap_or_else(PoisonError::into_inner));
        let recorded = results
            .iter()
            .filter(|out| matches!(out, Ok((_, row)) if row.state == LandState::Pending))
            .count();
        let refused = results
            .iter()
            .filter(|out| matches!(out, Err(Error::ResourceExhausted(_))))
            .count();
        assert_eq!((recorded, refused), (1, 1), "{results:?}");
    }

    /// The out-of-space SQLSTATEs of the Postgres docs' class 53 and 58:
    /// which map to the fixed `ResourceExhausted` shortage and which
    /// stay internal, and that the decision is the same for every code.
    #[test]
    fn the_disk_full_sqlstates_answer_the_fixed_shortage() {
        use super::pg_error_of;
        use crate::{Error, StorageClass};
        // A source of the right type with no SQLSTATE of its own: a
        // connect to port 1, which every kernel refuses. The mapping
        // runs on the SQLSTATE the test hands it, not on this source.
        let source = || match Client::connect("postgres://127.0.0.1:1/x", NoTls) {
            Ok(_) => panic!("a connect to port 1 is refused"),
            Err(refused) => refused,
        };
        let full = |code: &str| {
            let error = pg_error_of(Some(code), source());
            (
                matches!(&error, Error::Storage(s) if s.class() == StorageClass::DiskFull),
                tonic::Status::from(error),
            )
        };
        // 53100: the database's one disk-full code, for every file kind
        // (elog.c maps ENOSPC to it). The client sees the fixed
        // retry-later reply.
        {
            let (full, status) = full("53100");
            assert!(full, "53100 must map to the disk-full storage class");
            assert_eq!(status.code(), tonic::Code::ResourceExhausted, "53100");
            assert_eq!(
                status.message(),
                "the server is out of disk space; retry later",
                "53100"
            );
        }
        // The codes of the same classes that are NOT a disk shortage
        // stay internal, on purpose: 53200 is out of memory, 53000 is
        // the class's catch-all, 53400 is a configured cap (a
        // temp_file_limit or the replication slots - the same refusal
        // every time, so a retry-later reply would tell the client to
        // retry what cannot succeed), and 58030 is an io_error (a
        // failing device is the EIO row's internal, not a space
        // shortage). Pinning them keeps a later "helpful" widening from
        // answering a memory error with a disk reply.
        for code in ["53200", "53000", "53400", "58030"] {
            let (full, status) = full(code);
            assert!(!full, "{code} must not map to the disk-full class");
            assert_eq!(status.code(), tonic::Code::Internal, "{code}");
        }
    }

    /// The importer's batching holds a transaction's parents and repo
    /// membership for the flush: a transaction that ends without the
    /// flush refuses, rolls back, and writes nothing ([#545] review).
    #[test]
    fn a_batch_that_never_flushes_refuses_to_commit() {
        use crate::test_support::meta::{Meta as _, MetaTx as _};

        let dir = crate::test_support::private_tempdir();
        let store = crate::Backend::open_test(dir.path()).unwrap();
        let repo = store.write(|c| c.create_repo("main")).unwrap();
        let cs = Changeset::new(
            Vec::new(),
            scion_core::IdentityTime::new(
                b"Ada".to_vec(),
                b"ada@example.com".to_vec(),
                1_700_000_000,
                0,
            )
            .unwrap(),
            None,
            b"t\n".to_vec(),
            std::collections::BTreeMap::new(),
        )
        .unwrap();
        let (_body, id) = cs.encode_with_id();
        let error = store
            .write(|c| {
                c.set_changeset_batching(true);
                c.insert_changeset(repo, &cs)
            })
            .unwrap_err();
        assert!(
            error.to_string().contains("never flushed"),
            "the refusal names the flush: {error}"
        );
        // The transaction rolled back: the changeset row is not in the
        // database.
        assert!(
            !store.read(|c| c.has_changeset(repo, id)).unwrap(),
            "the changeset row of a transaction that never flushed survived"
        );
        // A normal write after the refusal writes as before.
        let again = store.write(|c| c.insert_changeset(repo, &cs)).unwrap();
        assert_eq!(again, id);
    }

    /// The lane's admission rule, on its own: a slot holder goes ahead
    /// of other waiting writers, an other writer goes when no slot
    /// holder waits or after the bound's run of slot holders, and with
    /// no slot holder in the lane the rule is today's plain queue.
    #[test]
    fn the_write_lane_admits_by_the_bound() {
        let gate = |holders_waiting: usize, plain_waiting: usize, holders_ahead: usize| {
            super::WriterGate {
                taken: false,
                waiting: holders_waiting + plain_waiting,
                holders_waiting,
                plain_waiting,
                holders_ahead,
            }
        };
        // A slot holder passes queued other writers while the bound is
        // not reached.
        assert!(super::lane_admits(&gate(0, 3, 0), true));
        assert!(super::lane_admits(&gate(0, 3, 3), true));
        // At the bound, the next other writer goes first.
        assert!(!super::lane_admits(
            &gate(0, 1, super::WRITER_LANE_AHEAD),
            true
        ));
        assert!(super::lane_admits(
            &gate(1, 1, super::WRITER_LANE_AHEAD),
            false
        ));
        // After that writer, the holders go on.
        assert!(super::lane_admits(&gate(2, 0, 0), true));
        // An other writer with no slot holder waiting goes at once: the
        // lane is the plain queue when no land holds a slot.
        assert!(super::lane_admits(&gate(0, 1, 0), false));
        // A slot holder with no other writer waiting goes at once.
        assert!(super::lane_admits(&gate(1, 0, 0), true));
    }

    /// A slot holder's commit enters ahead of the other writers that
    /// wait for the write client: the write client is held by a slow
    /// transaction, five other writers and one slot holder queue behind
    /// it, and the slot holder's commit completes before any of the
    /// five.
    #[test]
    fn a_slot_holders_commit_enters_ahead_of_the_write_queue() {
        use crate::test_support::meta::Meta as _;
        let _slot = super::damage::slot();
        let dir = crate::test_support::private_tempdir();
        let store = Arc::new(crate::Backend::open_test(dir.path()).unwrap());
        let hold = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let holder = {
            let store = Arc::clone(&store);
            let hold = Arc::clone(&hold);
            let release = Arc::clone(&release);
            std::thread::spawn(move || {
                store
                    .write(|_| {
                        hold.store(true, std::sync::atomic::Ordering::Release);
                        while !release.load(std::sync::atomic::Ordering::Acquire) {
                            std::thread::sleep(std::time::Duration::from_millis(2));
                        }
                        Ok(())
                    })
                    .unwrap();
            })
        };
        while !hold.load(std::sync::atomic::Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        // Five other writers and one slot holder queue; the slot holder
        // goes first, then the other writers.
        let order = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
        let mut threads = Vec::new();
        for class in ["plain", "plain", "plain", "plain", "plain", "holder"] {
            let store = Arc::clone(&store);
            let order = Arc::clone(&order);
            threads.push(std::thread::spawn(move || {
                let done = |_c: &mut crate::pg::PgTx<'_>| -> crate::error::Result<()> {
                    order
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(class);
                    Ok(())
                };
                if class == "holder" {
                    store.write_slot_holder(done).unwrap();
                } else {
                    store.write(done).unwrap();
                }
            }));
        }
        // All six wait: one slot holder and five other writers.
        for _ in 0..2_000 {
            let (holders, plain) = store.writer_lane_waiting();
            if holders == 1 && plain == 5 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let (holders, plain) = store.writer_lane_waiting();
        assert_eq!((holders, plain), (1, 5), "the six writers did not queue");
        release.store(true, std::sync::atomic::Ordering::Release);
        holder.join().unwrap();
        for thread in threads {
            thread.join().unwrap();
        }
        let order = order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(order.len(), 6, "every writer ran: {order:?}");
        assert_eq!(order[0], "holder", "the slot holder went first: {order:?}");
    }

    /// The bound: after four slot holders in a row, the next turn is
    /// the other writer's, so a stream of settles cannot starve the
    /// record commits that feed it.
    #[test]
    fn a_non_slot_writer_is_admitted_within_the_bound() {
        use crate::test_support::meta::Meta as _;
        let _slot = super::damage::slot();
        let dir = crate::test_support::private_tempdir();
        let store = Arc::new(crate::Backend::open_test(dir.path()).unwrap());
        let hold = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let holder = {
            let store = Arc::clone(&store);
            let hold = Arc::clone(&hold);
            let release = Arc::clone(&release);
            std::thread::spawn(move || {
                store
                    .write(|_| {
                        hold.store(true, std::sync::atomic::Ordering::Release);
                        while !release.load(std::sync::atomic::Ordering::Acquire) {
                            std::thread::sleep(std::time::Duration::from_millis(2));
                        }
                        Ok(())
                    })
                    .unwrap();
            })
        };
        while !hold.load(std::sync::atomic::Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        // Four slot holders, then one other writer, then one more slot
        // holder, all queued before the held transaction ends.
        let order = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
        let mut threads = Vec::new();
        for class in ["holder", "holder", "holder", "holder", "plain", "holder"] {
            let store = Arc::clone(&store);
            let order = Arc::clone(&order);
            threads.push(std::thread::spawn(move || {
                let done = |_c: &mut crate::pg::PgTx<'_>| -> crate::error::Result<()> {
                    order
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(class);
                    Ok(())
                };
                if class == "holder" {
                    store.write_slot_holder(done).unwrap();
                } else {
                    store.write(done).unwrap();
                }
            }));
        }
        for _ in 0..2_000 {
            let (holders, plain) = store.writer_lane_waiting();
            if holders == 5 && plain == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let (holders, plain) = store.writer_lane_waiting();
        assert_eq!((holders, plain), (5, 1), "the six writers did not queue");
        release.store(true, std::sync::atomic::Ordering::Release);
        holder.join().unwrap();
        for thread in threads {
            thread.join().unwrap();
        }
        let order = order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // The first four slot holders go, then the other writer, then
        // the fifth slot holder: the bound held the fifth back.
        assert_eq!(
            order.as_slice(),
            ["holder", "holder", "holder", "holder", "plain", "holder"],
            "the lane ran the writers in the bound's order"
        );
    }

    /// A read that runs while a write transaction holds the write
    /// client: the land's outcome check and plan ride it, so a land in
    /// its slot must not wait for another writer's statements. The
    /// writer is held here, so a read that queues behind it would hang
    /// the test.
    #[test]
    fn a_read_latest_runs_while_a_write_holds_the_write_client() {
        let (_pool, pg) = pg();
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let writer = {
            let pg = pg.clone();
            std::thread::spawn(move || {
                pg.commit(|_tx| {
                    held_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .unwrap();
            })
        };
        held_rx.recv().unwrap();
        // The land's shape: a per-statement read while the writer is
        // held. It answers.
        let seen = pg
            .read_latest(|tx| Ok(tx.c.query_opt("SELECT 1", &[])?.is_some()))
            .unwrap();
        assert!(seen);
        // A read on the write client queues behind it, as it always did:
        // the same hold keeps `read_unflushed` waiting.
        let queued = {
            let pg = pg.clone();
            std::thread::spawn(move || pg.read_unflushed(|_| Ok(())))
        };
        for _ in 0..2_000 {
            std::thread::yield_now();
        }
        assert!(
            !queued.is_finished(),
            "the writer-bound read did not queue behind the held writer"
        );
        release_tx.send(()).unwrap();
        queued.join().unwrap().unwrap();
        writer.join().unwrap();
    }

    /// A per-statement read sees the newest committed state: a commit
    /// that lands between its statements is visible to the later one,
    /// which is the shape of the land's outcome check and plan.
    #[test]
    fn a_read_latest_sees_a_commit_that_lands_between_its_statements() {
        let (_pool, pg) = pg();
        pg.commit(|tx| tx.c.execute("CREATE TABLE read_latest_watch (n INT)", &[]))
            .unwrap();
        pg.commit(|tx| {
            tx.c.execute("INSERT INTO read_latest_watch VALUES (1)", &[])
        })
        .unwrap();
        let (counted_tx, counted_rx) = std::sync::mpsc::channel();
        let (commit_tx, commit_rx) = std::sync::mpsc::channel::<()>();
        let writer = {
            let pg = pg.clone();
            std::thread::spawn(move || {
                counted_rx.recv().unwrap();
                pg.commit(|tx| {
                    tx.c.execute("INSERT INTO read_latest_watch VALUES (2)", &[])
                })
                .unwrap();
                commit_tx.send(()).unwrap();
            })
        };
        let seen = pg
            .read_latest(|tx| {
                let first: i64 =
                    tx.c.query("SELECT count(*) FROM read_latest_watch", &[])
                        .unwrap()[0]
                        .get(0);
                assert_eq!(first, 1, "the read started from an empty table");
                counted_tx.send(()).unwrap();
                commit_rx.recv().unwrap();
                let second: i64 =
                    tx.c.query("SELECT count(*) FROM read_latest_watch", &[])
                        .unwrap()[0]
                        .get(0);
                Ok(second)
            })
            .unwrap();
        assert_eq!(seen, 2, "the read did not see the commit it raced");
        writer.join().unwrap();
        pg.commit(|tx| tx.c.execute("DROP TABLE read_latest_watch", &[]))
            .unwrap();
    }

    /// A write inside `read_latest` fails: the read is READ ONLY, so a
    /// write that slips onto this path cannot land outside the writer's
    /// lock and the collector's key (SQLSTATE 25006,
    /// `read_only_sql_transaction`).
    #[test]
    fn a_write_inside_read_latest_fails() {
        let (_pool, pg) = pg();
        let error = pg
            .read_latest(|tx| {
                tx.c.execute("CREATE TABLE read_latest_read_only (n INT)", &[])
                    .map(|_| ())
            })
            .unwrap_err();
        // The cause chain carries the SQLSTATE text only in the debug
        // form: the storage error's own display names the class, not
        // the database's message.
        assert!(
            format!("{error:?}").contains("read-only transaction"),
            "{error:?}"
        );
    }
}
