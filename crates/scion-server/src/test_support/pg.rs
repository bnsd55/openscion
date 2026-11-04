//! `pg.rs`'s test hooks, as its own child module: `pg.rs` declares
//! this file with `#[path]`, so everything here keeps the privacy of
//! its module unchanged, and the file sits under `src/test_support/`
//! for the coverage exclusion.

#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicBool, Ordering};

use super::{
    Client, Error, Job, Kind, Marker, P, Path, Pg, PgClient, Reply, Result, connect_with,
    map_pg_error,
};

impl PgClient {
    /// Runs one closure with the client on the worker thread. The sync
    /// client must never be touched from a tokio thread, which this
    /// keeps true for the few setup statements a caller needs. Only the
    /// test schema's preparation calls it.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn as_raw(
        &self,
        f: impl FnOnce(&mut Client) -> Result<()> + Send + 'static,
    ) -> Result<()> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.jobs
            .send(Job {
                sql: String::new(),
                params: Vec::new(),
                kind: Kind::Raw,
                reply,
                raw: Some(Box::new(f)),
            })
            .map_err(|_| Error::Corrupt(super::WORKER_STOPPED.into()))?;
        match rx.recv() {
            Ok(Ok(Reply::Count(_))) => Ok(()),
            Ok(Ok(_)) => unreachable!("a raw job replies with a count"),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::Corrupt(super::WORKER_STOPPED.into())),
        }
    }
}

/// The name prefix of every schema the test helpers make. The sweep
/// drops nothing outside it, and the test schema helper spells it: a
/// test pins the two to one prefix.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) const TEST_SCHEMA_PREFIX: &str = "scion_test_";

/// The advisory lock of the test-schema cleanup.
#[cfg(any(test, feature = "test-hooks"))]
const CLEAN_LOCK: i64 = 0x5343_494F_4E53_5445;

/// How many stale schemas one process drops, oldest registration
/// first: a database with hundreds of stale schemas must not stall the
/// first test of a run for minutes. The next run, or the next process,
/// continues.
#[cfg(any(test, feature = "test-hooks"))]
const SWEEP_CAP: usize = 50;

/// How many stale schemas one test-schema preparation drops, oldest
/// registration first: the suite's cases churn directories faster
/// than the once-per-process sweep ever could, and a schema whose
/// directory is gone serves nothing (#849). The cap bounds one
/// preparation's work; the next preparation, the next case, continues,
/// so the suite's leftover schemas drain at the rate the cases make
/// them and the database's population follows the suite's concurrency
/// instead of its case count.
#[cfg(any(test, feature = "test-hooks"))]
const DRAIN_CAP: usize = 8;

/// Whether this process has swept: the sweep runs at the start of the
/// run, on the first test open, and not again.
#[cfg(any(test, feature = "test-hooks"))]
static SWEPT: AtomicBool = AtomicBool::new(false);

/// Drops the test schemas whose tests are over: a test run leaves them
/// behind, because a server holds its schema while it runs. One
/// process at a time holds the cleanup lock, so the test binaries that
/// run in parallel clean nothing under each other's feet.
///
/// A schema whose owner still runs is named in `pg_stat_activity`
/// under its `application_name`, and never drops. A schema whose test
/// directory still exists never drops either: the open records the
/// directory in the schema's comment, so a suite that reopens a fixed
/// directory in a later run keeps its data. Schemas registered before
/// the comment existed carry no directory: for those, the old rule
/// holds - older than ten minutes and no live connection. At most
/// `cap` drops per call, oldest registration first, and one line says
/// what dropped and what stayed.
#[cfg(any(test, feature = "test-hooks"))]
fn drop_stale_schemas(maker: &PgClient, automatic: bool, cap: usize) -> (usize, usize) {
    if automatic && SWEPT.swap(true, Ordering::SeqCst) {
        return (0, 0);
    }
    let got = if automatic {
        maker.query_opt("SELECT pg_try_advisory_lock($1)", &[P::i64(CLEAN_LOCK)])
    } else {
        // A test-driven sweep waits for the cleanup lock: it must run
        // against the database the test shaped, even while a run's own
        // sweep holds the lock.
        maker.query_opt("SELECT pg_advisory_lock($1)", &[P::i64(CLEAN_LOCK)])
    };
    let Ok(got) = got else {
        // This connection cannot even ask: another open's cleanup runs, or
        // the database is in trouble. The next open tries again.
        if automatic {
            SWEPT.store(false, Ordering::SeqCst);
        }
        return (0, 0);
    };
    // The try-lock's answer is a row that holds the verdict: a `false`
    // row means another cleanup runs, and this one stands down. The
    // blocking lock's row always grants.
    if automatic && !got.is_some_and(|row| row.get::<_, bool>(0)) {
        SWEPT.store(false, Ordering::SeqCst);
        return (0, 0);
    }
    // The registry names every test schema with its start time, in
    // `public`, spelled in full: the maker's search path is the
    // database's default, and no test schema holds the table. A
    // connection whose `application_name` is the schema's name keeps
    // its schema; so does a schema whose recorded directory exists.
    //
    // The read pages past the schemas it keeps: a head that stops at
    // the oldest rows would read the same kept rows at every call and
    // never reach the stale ones behind them - a developer database
    // with a few dozen live directories would stall every drain (#849
    // review). Each page starts strictly after the last row the page
    // before read, kept or dropped, and the walk stops when the cap is
    // reached or a bounded number of pages is read, so one call's work
    // stays bounded whatever the registry holds; the next call
    // continues from the front.
    let page_size = (cap * 2) as i64;
    let page_bound = 64_usize;
    let now = extract_epoch_now(maker);
    let mut dropped = 0_usize;
    let mut left = 0_usize;
    let mut cursor_started = i64::MIN;
    let mut cursor_schema = String::new();
    for _page in 0..page_bound {
        let eligible = maker.query(
            &format!(
                "SELECT r.schema, obj_description(n.oid, 'pg_namespace'), r.started
                 FROM public.scion_test_schemas AS r
                 JOIN pg_catalog.pg_namespace AS n ON n.nspname = r.schema
                 WHERE r.schema NOT IN (
                     SELECT application_name FROM pg_catalog.pg_stat_activity
                     WHERE application_name LIKE 'scion_test_%')
                   AND (r.started, r.schema) > ($1, $2)
                 ORDER BY r.started ASC, r.schema ASC
                 LIMIT {page_size}"
            ),
            &[P::i64(cursor_started), P::text(&cursor_schema)],
        );
        let Ok(rows) = eligible else {
            // The registry cannot be read: the next open tries again.
            break;
        };
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            let name: String = row.get(0);
            let dir: Option<String> = row.get(1);
            let started: i64 = row.get(2);
            cursor_started = started;
            cursor_schema = name.clone();
            // Only the names this backend makes: no other schema drops,
            // whatever a row of the registry names.
            if !name.starts_with(TEST_SCHEMA_PREFIX) {
                continue;
            }
            // The recorded directory decides: gone means the test that
            // made the schema is over, whatever the clock says. No
            // directory recorded - the schema is older than the comment
            // - falls back to the registration's age, but only when the
            // clock answers: a database that cannot tell the time keeps
            // every schema it cannot date.
            let stale = match &dir {
                Some(recorded) => !Path::new(recorded).exists(),
                None => now.is_some_and(|now| started < now.saturating_sub(600)),
            };
            if !stale || dropped >= cap {
                left += 1;
                continue;
            }
            // The snapshot's connection check is stale by drop time: a
            // test may have named the schema after the read, while this
            // sweep worked through its list. The lock closes that
            // window: every connection that names a schema holds a
            // SHARED session lock on it (hashtext of the name), so a
            // schema a live connection names never yields the
            // EXCLUSIVE try below, whatever the snapshot saw. A failed
            // check keeps the schema: a check that cannot answer never
            // drops.
            let free = maker
                .query_opt(
                    "SELECT pg_try_advisory_lock(hashtext($1))",
                    &[P::text(&name)],
                )
                .ok()
                .flatten()
                .map(|row| row.get::<_, bool>(0));
            if free != Some(true) {
                left += 1;
                continue;
            }
            if let Err(why) = maker.batch(&format!(
                "DROP SCHEMA IF EXISTS {name} CASCADE;
                 DELETE FROM public.scion_test_schemas WHERE schema = '{name}'"
            )) {
                tracing::warn!(schema = %name, error = %why, "cannot drop a stale test schema");
                left += 1;
            } else {
                dropped += 1;
            }
            // The lock was this session's own try: release it, so the
            // next row's try decides on its own name.
            let _ = maker.query_opt("SELECT pg_advisory_unlock(hashtext($1))", &[P::text(&name)]);
        }
        if dropped >= cap || (rows.len() as i64) < page_size {
            // The cap is reached, or the page was the registry's tail.
            break;
        }
    }
    tracing::info!(
        dropped,
        left,
        clock = now.is_some(),
        "dropped stale test schemas at the run's start"
    );
    // The lock frees when this connection closes: a failed unlock needs
    // nothing but the drop of the maker.
    if let Err(why) = maker.query_opt("SELECT pg_advisory_unlock($1)", &[P::i64(CLEAN_LOCK)]) {
        tracing::warn!(error = %why, "cannot unlock the test-schema cleanup");
    }
    (dropped, left)
}

/// The database server's clock, as the registry's `started` column
/// spells it, so the age check reads one clock. `None` when the clock
/// does not answer: the sweep then keeps every schema it cannot date,
/// rather than date them all stale.
#[cfg(any(test, feature = "test-hooks"))]
fn extract_epoch_now(maker: &PgClient) -> Option<i64> {
    maker
        .query_opt("SELECT extract(epoch from now())::bigint", &[])
        .ok()
        .and_then(|row| row.and_then(|row| row.get(0)))
}

/// A plain connection. Only the test schema's preparation calls it.
#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn connect(url: &str, ca: Option<&Path>) -> Result<PgClient> {
    connect_with(url, ca, "", None)
}

/// The error of a graph walk that reached its bound.
#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn too_deep(max_visits: usize) -> Error {
    Error::ResourceExhausted(format!(
        "the history is too deep: the answer needs more than {max_visits} changesets"
    ))
}

/// The statements that register a test schema: the registry row with
/// its start time, and the schema's comment, which records the
/// directory the schema serves - the fact the sweep's drop decision
/// reads. One maker at a time: two concurrent `CREATE TABLE IF NOT
/// EXISTS` of the registry race on the type catalog, and the loser
/// dies - the transaction's advisory lock serializes them. The name is
/// spelled by the test helpers alone (`scion_test_` plus hex), so the
/// literal interpolations carry no user text.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn register_test_schema(
    client: &mut Client,
    name: &str,
    dir: &Path,
) -> std::result::Result<(), postgres::Error> {
    let comment = dir.to_string_lossy().replace('\'', "''");
    client.batch_execute(&format!(
        "SELECT pg_advisory_xact_lock({CLEAN_LOCK});
         CREATE TABLE IF NOT EXISTS public.scion_test_schemas (
             schema  TEXT PRIMARY KEY,
             started BIGINT NOT NULL
         );
         INSERT INTO public.scion_test_schemas (schema, started)
         VALUES ('{name}', extract(epoch from now())::bigint)
         ON CONFLICT (schema) DO UPDATE SET started = excluded.started;
         COMMENT ON SCHEMA {name} IS '{comment}'"
    ))
}

/// Makes the test schema, and registers it with its directory, before
/// any connection narrows its search path to it. Runs the stale-schema
/// sweep once per process, at the first open.
#[cfg(any(test, feature = "test-hooks"))]
fn prepare_test_schema(url: &str, schema: &str, dir: &Path) -> Result<()> {
    let preparer = connect(url, None)?;
    preparer.as_raw({
        let name = schema.to_owned();
        move |client| {
            client
                .batch_execute(&format!(
                    "SET application_name TO '{name}';
                     SELECT pg_advisory_lock_shared(hashtext('{name}'))"
                ))
                .map_err(map_pg_error)
        }
    })?;
    // The run's own sweep runs once; every preparation then drains a
    // few stale schemas, so the suite's leftovers never outgrow its
    // concurrency (#849).
    drop_stale_schemas(&preparer, true, SWEEP_CAP);
    drop_stale_schemas(&preparer, false, DRAIN_CAP);
    preparer.as_raw({
        let name = schema.to_owned();
        let dir = dir.to_path_buf();
        move |client| {
            client
                .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {name};"))
                .map_err(map_pg_error)?;
            register_test_schema(client, &name, &dir).map_err(map_pg_error)
        }
    })?;
    Ok(())
}

/// Runs the stale-schema sweep out of band, without the
/// once-per-process gate: the run's own sweep runs once, at the first
/// test open; a test calls this to run the sweep again against a
/// database it shaped. It waits for the cleanup lock, and answers the
/// schemas it dropped and the eligible ones it left.
///
/// # Errors
///
/// What the database returns for the maker's connect.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn sweep_stale_test_schemas(url: &str) -> Result<(usize, usize)> {
    let preparer = connect(url, None)?;
    preparer.as_raw(move |client| {
        client
            .batch_execute("SET application_name TO ''")
            .map_err(map_pg_error)
    })?;
    Ok(drop_stale_schemas(&preparer, false, SWEEP_CAP))
}

impl Pg {
    /// [`Pg::open`], in `schema` instead of the database's default one, for
    /// tests: each data directory gets its own corner of one database. The
    /// schema is created if missing, and the migrations run in it.
    ///
    /// # Errors
    ///
    /// As [`Pg::open`].
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn open_test(
        url: &str,
        schema: &str,
        readers: usize,
        data: &Path,
        marker: Marker,
    ) -> Result<Self> {
        prepare_test_schema(url, schema, data)?;
        // The test store is the one server of its schema: it takes the
        // per-schema one-server lock, and its loss fences the process,
        // exactly as a serve does on the database key.
        Self::open_in(url, None, readers, data, Some(schema), true, marker, None)
    }
    /// [`Pg::open_test`], as a process that does not land: it makes the
    /// schema the same way and takes no one-server lock, and its loss
    /// fences nothing, like a `gc`, `fsck` or `import` beside a live
    /// server.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn open_test_shared(
        url: &str,
        schema: &str,
        readers: usize,
        data: &Path,
        marker: Marker,
    ) -> Result<Self> {
        prepare_test_schema(url, schema, data)?;
        Self::open_in(url, None, readers, data, Some(schema), false, marker, None)
    }
    /// [`PgClient::open_test_shared`], with the derive walks built for
    /// `flights`: the tests of the walk caps, which change the numbers,
    /// never the machinery.
    pub(crate) fn open_test_shared_with_flights(
        url: &str,
        schema: &str,
        readers: usize,
        data: &Path,
        marker: Marker,
        flights: crate::derived::DeriveFlights,
    ) -> Result<Self> {
        prepare_test_schema(url, schema, data)?;
        Self::open_in(
            url,
            None,
            readers,
            data,
            Some(schema),
            false,
            marker,
            Some(flights),
        )
    }
    /// The import's test hooks and busy-retry count.
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub(crate) fn hooks(&self) -> &crate::store::test_hooks::ImportHooks {
        &self.inner.hooks
    }
}
