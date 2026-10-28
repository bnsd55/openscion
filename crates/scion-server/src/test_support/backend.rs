//! `backend.rs`'s test hooks, as its own child module: `backend.rs`
//! declares this file with `#[path]`, so everything here keeps the
//! privacy of its module unchanged, and the file sits under
//! `src/test_support/` for the coverage exclusion.

#![forbid(unsafe_code)]

use std::path::Path;

use super::{Backend, Marker, PG_READERS, marker_agrees, refuse_damaged_marker, refuse_sqlite_dir};
use crate::error::Result;
use crate::pg::Pg;

impl Backend {
    /// The advisory-lock key of the test schema's one-server lock, for
    /// the test that takes the lock from its own connection, as a
    /// second server process would.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_server_lock(data: &Path) -> Result<i64> {
        Ok(crate::pg::schema_lock_key(&test_schema(data)))
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
        Self::open_test_in_schema_with(data, test_schema(data), |url, schema, readers, data, marker| {
            Pg::open_test_shared(url, schema, readers, data, marker)
        })
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
        if data.is_dir() {
            let _ = std::fs::set_permissions(data, std::fs::Permissions::from_mode(0o700));
        }
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
        Self::open_test_in_schema_with(data, schema, Pg::open_test_shared)
    }

    /// Opens a test backend on `data`, with the opener `open` (the
    /// one-server or the shared one): the shared shape of every test
    /// open.
    ///
    /// # Errors
    ///
    /// What the database returns, and a missing test URL.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn open_test(data: &Path) -> Result<Self> {
        Self::open_test_in_schema_with(data, test_schema(data), Pg::open_test)
    }

    /// [`Backend::open_test`], with the opener chosen: the one seam the
    /// open shapes share.
    fn open_test_in_schema_with(
        data: &Path,
        schema: String,
        open: impl FnOnce(&str, &str, usize, &Path, Marker) -> Result<Pg>,
    ) -> Result<Self> {
        Self::ensure_private_dir(data);
        refuse_damaged_marker(data)?;
        let marker = Marker::read(data);
        refuse_sqlite_dir(data)?;
        let pg = open(&test_url()?, &schema, PG_READERS, data, marker)?;
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
            PG_READERS,
            data,
            marker,
            flights,
        )?;
        marker_agrees(data, pg.store_uuid())?;
        Ok(Self { pg })
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
    // The hash must not move when the leaf directory appears: a first
    // open of a data directory that does not exist yet would otherwise
    // hash the raw path (on macOS /var is a symlink to /private/var, so
    // the raw and the canonical spellings hash apart), and every open
    // after the directory exists would hash the canonical one. The
    // canonical form of the deepest existing ancestor, with the missing
    // tail spelled as the caller spelled it, is one name for both.
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
    let path = base.join(tail);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&path, &mut hasher);
    format!("scion_test_{:016x}", std::hash::Hasher::finish(&hasher))
}
