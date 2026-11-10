//! `backend.rs`'s test hooks, as its own child module: `backend.rs`
//! declares this file with `#[path]`, so everything here keeps the
//! privacy of its module unchanged, and the file sits under
//! `src/test_support/` for the coverage exclusion.

#![forbid(unsafe_code)]

use std::path::Path;
use std::time::Duration;

use super::{Backend, Marker, marker_agrees, refuse_damaged_marker, refuse_sqlite_dir};
use crate::error::Result;
#[cfg(any(test, feature = "test-hooks"))]
use crate::pg::test_hooks::TEST_SCHEMA_PREFIX;
use crate::pg::{Pg, map_pg_error};

/// The reader-pool size of a test backend. Tests run many backends at
/// once - the lib suite runs its tests on threads, and each backend
/// holds one write connection plus its readers - so the pool is far
/// under the server's [`READERS`](crate::store::READERS): a pool of the
/// server's size saturates the test database's `max_connections` before
/// the suite finishes (E53300, "too many clients").
const PG_TEST_READERS: usize = 2;

impl Backend {
    /// Runs the stale-schema sweep out of band, without the
    /// once-per-process gate: the run's own sweep runs once, at the
    /// first test open; a test calls this to run the sweep again
    /// against a database it shaped. It waits for the cleanup lock,
    /// and answers the schemas it dropped and the eligible ones it
    /// left for a later sweep.
    ///
    /// # Errors
    ///
    /// A missing test URL, or what the database returns.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn sweep_stale_test_schemas() -> Result<(usize, usize)> {
        crate::pg::test_hooks::sweep_stale_test_schemas(&test_url()?)
    }

    /// The advisory-lock key of the test schema's one-server lock, for
    /// the test that takes the lock from its own connection, as a
    /// second server process would.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_server_lock(data: &Path) -> Result<i64> {
        Ok(crate::pg::schema_lock_key(&test_schema(data)))
    }
    /// The advisory-lock key of the test schema's migrations, for the
    /// test that takes it from its own connection, as a wedged
    /// migration would.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_migration_lock(data: &Path) -> Result<i64> {
        Ok(crate::pg::migration_lock_key(&test_schema(data)))
    }
    /// The database's bare migration key, which an older build takes
    /// exclusive: a test holds it to stand in for an older migrator
    /// mid-migration.
    #[doc(hidden)]
    #[must_use]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_bare_migration_lock() -> i64 {
        crate::pg::SCHEMA_LOCK
    }
    /// The migration key of a schema name, for the test that pins the
    /// key spaces apart.
    #[doc(hidden)]
    #[must_use]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_migration_lock_for(schema: &str) -> i64 {
        crate::pg::migration_lock_key(schema)
    }
    /// The one-server key of a schema name, for the same pin.
    #[doc(hidden)]
    #[must_use]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_server_lock_for(schema: &str) -> i64 {
        crate::pg::schema_lock_key(schema)
    }
    /// A test backend of `data`, as a process that does not land: it
    /// takes no one-server lock, like an import beside a server.
    ///
    /// # Errors
    ///
    /// What the database returns, and a missing test URL.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn open_test_shared(data: &Path) -> Result<Self> {
        Self::open_test_in_schema_with(
            data,
            &test_schema(data),
            PG_TEST_READERS,
            Pg::open_test_shared,
        )
    }
    /// [`Backend::open_test_shared`], for a store the suite reopens from
    /// another path than the one its first open used: the power suite
    /// reopens from the backing directory, whose path would name a
    /// fresh, empty schema, while the metadata lives in the database the
    /// first open filled. The schema follows the store, not the path.
    ///
    /// # Errors
    ///
    /// What the database returns, and a missing test URL.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn open_test_shared_in_schema(data: &Path, schema: &str) -> Result<Self> {
        Self::open_test_in_schema_with(data, schema, PG_TEST_READERS, Pg::open_test_shared)
    }
    /// Makes an existing test data directory private, as the operator's
    /// `chmod 700` does: the tests pre-create their directories with the
    /// process umask, and the open refuses one that other users can reach.
    /// The refusal tests set the open modes after this runs, so they still
    /// exercise it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    fn ensure_private_dir(data: &Path) {
        use std::os::unix::fs::PermissionsExt as _;
        // A fresh open makes the data directory it names: the first open
        // on a new host writes its marker into a directory that exists.
        if !data.exists() {
            let _ = scion_proto::runtime::auth::create_private_dir_all(data);
            return;
        }
        if data.is_dir() {
            let _ = std::fs::set_permissions(data, std::fs::Permissions::from_mode(0o700));
        }
    }

    /// Opens a test backend on `data`, with the reader pool the suite
    /// runs: far under the server's pool, so the test database's
    /// `max_connections` holds.
    ///
    /// # Errors
    ///
    /// What the database returns, and a missing test URL.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn open_test(data: &Path) -> Result<Self> {
        Self::open_test_with_readers(data, PG_TEST_READERS)
    }

    /// Sets the test's own short cleanup-lock bound, in seconds, so the
    /// red proof of the bound stays fast; 0 restores the const's. The
    /// caller holds the override until it sets 0 back - drop a guard.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_clean_lock_wait_secs(secs: u64) {
        crate::pg::test_hooks::set_clean_lock_wait_for_test(secs);
    }

    /// [`Backend::open_test`], with the reader pool the caller names: the
    /// tests of a pool's cap need the server's pool size, while the rest
    /// of the suite runs far under it so the test database's
    /// `max_connections` holds.
    ///
    /// # Errors
    ///
    /// As [`Backend::open_test`].
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn open_test_with_readers(data: &Path, readers: usize) -> Result<Self> {
        Self::open_test_in_schema_with(data, &test_schema(data), readers, Pg::open_test)
    }

    /// [`Backend::open_test`], after a drop of a store on the same
    /// directory: the one-server lock rides the dropped store's write
    /// connection, and the worker thread closes that connection after
    /// the drop has returned. Until the session ends, the database
    /// answers "another server holds this store". This reopen retries
    /// that one error for a short bound and returns the last result, so
    /// a test that drops and reopens at once waits for the real
    /// condition instead of racing the worker's clock.
    ///
    /// # Errors
    ///
    /// What [`Backend::open_test`] returns: any error that is not the
    /// holder message at once, and the holder message if no reopen
    /// succeeds within the bound.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn reopen_test(data: &Path) -> Result<Self> {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            match Self::open_test(data) {
                Err(crate::error::Error::FailedPrecondition(message))
                    if message.contains(crate::pg::HOLDER_MESSAGE)
                        && std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(20));
                }
                result => return result,
            }
        }
    }

    /// [`Backend::open_test`], with the opener chosen: the one seam the
    /// open shapes share.
    fn open_test_in_schema_with(
        data: &Path,
        schema: &str,
        readers: usize,
        open: impl FnOnce(&str, &str, usize, &Path, Marker) -> Result<Pg>,
    ) -> Result<Self> {
        Self::ensure_private_dir(data);
        refuse_damaged_marker(data)?;
        let marker = Marker::read(data);
        refuse_sqlite_dir(data)?;
        let pg = open(&test_url()?, schema, readers, data, marker)?;
        marker_agrees(data, pg.store_uuid())?;
        Ok(Self { pg })
    }

    /// [`Backend::open_test`], with the derive walks built for `walks`
    /// in flight and `walk_bytes` of shared budget, for the tests of the
    /// walk caps. The flights are built before the backend opens:
    /// production types stay immutable.
    ///
    /// # Errors
    ///
    /// What `open_test` returns.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn open_test_with_walks(
        data: &Path,
        walks: usize,
        walk_bytes: crate::memory::Bytes,
    ) -> Result<Self> {
        Self::ensure_private_dir(data);
        let flights = crate::derived::DeriveFlights::with_walk_bytes(walks, walk_bytes);
        refuse_damaged_marker(data)?;
        let marker = Marker::read(data);
        refuse_sqlite_dir(data)?;
        let pg = Pg::open_test_shared_with_flights(
            &test_url()?,
            &test_schema(data),
            PG_TEST_READERS,
            data,
            marker,
            flights,
        )?;
        marker_agrees(data, pg.store_uuid())?;
        Ok(Self { pg })
    }

    /// The URL a server, an import or a restore on `data` takes: the
    /// test database, with the data directory's own schema as the
    /// search path, which this creates first. The name is the one
    /// [`Backend::open_test`] gives the same directory, so a pre-seeded
    /// store and the process that serves it share one schema.
    ///
    /// # Errors
    ///
    /// A missing test URL.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_postgres_url(data: &Path) -> Result<String> {
        // The connect is one blocking call at setup; some callers run
        // inside an async runtime, and the client blocks its own thread,
        // so the call runs on a thread of its own to stay out of the
        // runtime's way.
        let data = data.to_path_buf();
        std::thread::spawn(move || {
            let url = test_url()?;
            let schema = test_schema(&data);
            let dir = canonical_test_dir(&data);
            let mut client =
                postgres::Client::connect(&url, postgres::NoTls).map_err(map_pg_error)?;
            client
                .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
                .map_err(map_pg_error)?;
            // Register the schema and record the directory in its
            // comment, exactly as an open does: a store opened through
            // this URL leaves a schema the sweep can name and decide
            // about, not an orphan the registry never saw.
            crate::pg::test_hooks::register_test_schema(&mut client, &schema, &dir)
                .map_err(map_pg_error)?;
            let sep = if url.contains('?') { '&' } else { '?' };
            Ok(format!("{url}{sep}options=--search_path%3D{schema}"))
        })
        .join()
        .expect("the URL helper never panics")
    }

    /// The Postgres statements this process has run since the last
    /// reset. The land round-trip guard measures the delta around one
    /// land.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_statement_count() -> u64 {
        crate::pg::test_statement_count()
    }

    /// Sets the statement count back to zero.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_reset_statement_count() {
        crate::pg::test_reset_statement_count();
    }

    /// The body fetches the gc's prune made since the last reset: one
    /// call to `stored_changeset_bodies` is one fetch. A test with a
    /// small byte budget asserts this is more than one, so the prune
    /// really goes through the byte-budget chunker.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_gc_body_fetch_count() -> u64 {
        crate::gc::test_body_fetch_count()
    }

    /// Sets the gc's body-fetch count back to zero.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_reset_gc_body_fetch_count() {
        crate::gc::test_reset_body_fetch_count();
    }

    /// Shrinks the byte budget of the gc's body fetches, for a test that
    /// exercises the chunker on small bodies. Pass zero to reset.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_set_gc_body_budget(bytes: usize) {
        crate::gc::test_set_body_budget(bytes);
    }

    /// Whether the server's first stale-land sweep has finished: a test
    /// that counts a call's statements waits for it, so the sweep's own
    /// statements cannot fall inside its window.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_first_sweep_done() -> bool {
        crate::service::TEST_FIRST_SWEEP_DONE.load(std::sync::atomic::Ordering::Acquire)
    }

    /// The SQL of the statements run since the last reset.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_statement_log() -> Vec<String> {
        crate::pg::test_statement_log()
    }

    /// The readers this backend has taken since the last reset. A walk
    /// that holds one transaction takes one reader, whatever it steps.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_reader_takes(&self) -> u64 {
        crate::pg::test_reader_takes()
    }

    /// Sets the reader-take count back to zero.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_reset_reader_takes() {
        crate::pg::test_reset_reader_takes();
    }

    /// How many of this backend's readers sit idle in its pool. A walk
    /// that leaked its transaction leaves the pool short.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_idle_readers(&self) -> usize {
        crate::pg::test_idle_readers(&self.pg)
    }
    /// The schema that [`Backend::open_test`] gave this data directory:
    /// a test that counts rows through its own Postgres client names it
    /// the same way. A test holds a lock in it from a connection of its
    /// own.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_schema(data: &Path) -> String {
        test_schema(data)
    }
    /// Write transactions run so far: the tests of the batching watch
    /// the count.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn txes(&self) -> u64 {
        self.pg.txes()
    }
    /// Commits made so far: the transactions of the write connection.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn commits(&self) -> u64 {
        self.pg.commits()
    }
    /// Sets how many of the next commits fail as a busy database: the
    /// transaction ran and rolled back, so the retry re-runs it whole.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn fail_next_commits(&self, n: u32) {
        self.pg.fail_next_commits(n);
    }
    /// Sets how many of the next commits fail as a database out of
    /// space: the transaction ran and rolled back, so the retry re-runs
    /// it whole. The audit's why reads disk_full, not busy.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn fail_next_commits_disk_full(&self, n: u32) {
        self.pg.fail_next_commits_disk_full(n);
    }
    /// The import's test hooks and busy-retry count.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    fn hooks(&self) -> &crate::store::test_hooks::ImportHooks {
        self.pg.hooks()
    }
    /// Sets the step the tests run after each batch of the import commits.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_after_batch(&self, step: Option<crate::store::test_hooks::AfterBatch>) {
        self.hooks().set_after_batch(step);
    }
    /// How many busy retries of the import's batches ran so far.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn busy_retries(&self) -> u64 {
        self.hooks().busy_retries()
    }
    /// Runs the test step of a batch boundary, with the count of batches
    /// done so far.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn run_after_batch(&self, batches: usize) {
        self.hooks().run_after_batch(batches);
    }
    /// Counts one busy retry of the import's batch.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn note_busy_retry(&self) {
        self.hooks().note_busy_retry();
    }
}

/// The URL of the test database. Its callers sit behind the test-hooks
/// feature, and so does this.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_url() -> Result<String> {
    std::env::var("SCION_TEST_POSTGRES_URL").map_err(|_| {
        crate::error::Error::InvalidArgument(
            "SCION_TEST_POSTGRES_URL must name the test database".into(),
        )
    })
}

/// A database schema name that belongs to `data`, so the tests of one
/// directory share it and no others see it. Its callers sit behind the
/// test-hooks feature, and so does this.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_schema(data: &Path) -> String {
    let path = canonical_test_dir(data);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&path, &mut hasher);
    format!(
        "{TEST_SCHEMA_PREFIX}{:016x}",
        std::hash::Hasher::finish(&hasher)
    )
}

/// The canonical form `test_schema` hashes, spelled the same way the
/// schema's comment records it: the canonical form of the deepest
/// existing ancestor, with the missing tail spelled as the caller
/// spelled it. A first open of a data directory that does not exist yet
/// and every open after the directory exists see one name.
#[cfg(any(test, feature = "test-hooks"))]
fn canonical_test_dir(data: &Path) -> std::path::PathBuf {
    // The hash must not move when the leaf directory appears: a first
    // open of a data directory that does not exist yet would otherwise
    // hash the raw path (on macOS /var is a symlink to /private/var, so
    // the raw and the canonical spellings hash apart), and every open
    // after the directory exists would hash the canonical one.
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    let mut cur = data.to_path_buf();
    let base = loop {
        match cur.canonicalize() {
            Ok(base) => break base,
            Err(_) => match cur.file_name() {
                Some(name) => {
                    tail.push(name.to_owned());
                    cur.pop();
                }
                None => break cur,
            },
        }
    };
    let tail: std::path::PathBuf = tail.into_iter().rev().collect();
    base.join(tail)
}

/// Arms the marker write's directory fsync of `data` to fail with EIO,
/// once: the next open that writes `data`'s marker must refuse with the
/// disk's error. The store's own fsync gate cannot carry this hook: the
/// marker writes before any store exists to arm.
#[doc(hidden)]
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn fail_marker_dir_fsync(data: &Path) {
    super::arm_marker_dir_fsync_failure(data);
}
