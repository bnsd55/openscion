//! Damage for tests: one function per kind, and each takes the
//! backend, so the caller never spells the database's form.
//!
//! Every function makes its row the way an older build may have -
//! through the database, behind the server's rules. It opens its own
//! client on the directory's schema (the same registration `test_hooks`
//! uses).
//!
//! The schema forbids most of these rows with its keys on, so the
//! helpers that need it drop the rule for the insert: one transaction
//! drops the constraint, makes the row, and adds the constraint back
//! `NOT VALID`.

#![forbid(unsafe_code)]

use crate::Backend;
use crate::backend::test_hooks::{test_schema, test_url};
use crate::error::Result;

use super::map_pg_error;
use crate::meta::RepoId;
use scion_core::ChangesetId;
use std::fmt::Write as _;
use std::path::Path;
/// The gate the Postgres tests hold while a fixture is open. The
/// database's connection slots are the bound: one open store is 1
/// writer + [`PG_TEST_READERS`](crate::test_support::backend) 2 readers
/// = 3 connections, so [`slot`] allows 4 stores at once (12 of the
/// 100 `max_connections`), and `--test-threads=4` runs four gated
/// tests together where the old serial lock ran one at a time.
/// [`exclusive`] is the full stop a test takes when it reads a
/// process-global counter: another test's statements must not land in
/// its delta.
const SLOTS: usize = 4;

struct GateState {
    /// Stores holding a permit.
    permits_out: usize,
    /// Tests waiting for the full stop, so new permits queue behind
    /// them instead of starving them.
    waiting_exclusive: usize,
    /// Whether the full stop is held.
    exclusive_out: bool,
}

static GATE: std::sync::Mutex<GateState> = std::sync::Mutex::new(GateState {
    permits_out: 0,
    waiting_exclusive: 0,
    exclusive_out: false,
});
static GATE_CV: std::sync::Condvar = std::sync::Condvar::new();

/// One permit of the gate: the default for a test whose fixture opens
/// a store.
pub(crate) struct SlotGuard;

impl Drop for SlotGuard {
    fn drop(&mut self) {
        let mut state = GATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.permits_out -= 1;
        GATE_CV.notify_all();
    }
}

/// One permit of the gate, one of 4.
pub(crate) fn slot() -> SlotGuard {
    let mut state = GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while state.exclusive_out || state.waiting_exclusive > 0 || state.permits_out >= SLOTS {
        state = GATE_CV
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    state.permits_out += 1;
    SlotGuard
}

/// The full stop: no permit is out, and no new one goes out, until the
/// guard drops. For the tests that read a process-global counter (the
/// statement count of `pg.rs`, the body-fetch count of `gc.rs`), whose
/// delta another test's work would corrupt.
pub(crate) struct ExclusiveGuard;

impl Drop for ExclusiveGuard {
    fn drop(&mut self) {
        let mut state = GATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.exclusive_out = false;
        GATE_CV.notify_all();
    }
}

/// Takes the full stop, one of none.
pub(crate) fn exclusive() -> ExclusiveGuard {
    let mut state = GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.waiting_exclusive += 1;
    while state.exclusive_out || state.permits_out > 0 {
        state = GATE_CV
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    state.waiting_exclusive -= 1;
    state.exclusive_out = true;
    ExclusiveGuard
}

/// Updates the schema's planner statistics, the way autovacuum does on a
/// real server: a shared test schema grows across cases, and an
/// un-analyzed planner keeps the small-table plans it picked on the
/// first case.
pub(crate) fn analyze(backend: &Backend) -> Result<()> {
    exec(data_dir(backend), "ANALYZE")
}

/// The kind of a derived row.
pub(crate) type DerivedRow = (String, i64, Vec<u8>, Vec<u8>);

// -- damage: one function per kind -------------------------------------

/// Corrupts the stored body of one changeset: the id stays, the bytes lie.
pub(crate) fn corrupt_changeset_body(backend: &Backend, id: &ChangesetId) -> Result<()> {
    let data = data_dir(backend);
    exec(
        data,
        &format!(
            "UPDATE changesets SET body = {} WHERE id = {}",
            pg_bytes(&[0_u8]),
            pg_bytes(id.as_bytes())
        ),
    )
}

/// Points the first-parent row of `cs` at `to`, which another repo may
/// hold: a hand-edited or older-build row may cross repos, and the walks
/// that follow first parents must stop at the repo's own membership
/// instead of serving the other repo's changeset.
pub(crate) fn redirect_first_parent(
    backend: &Backend,
    cs: &ChangesetId,
    to: &ChangesetId,
) -> Result<()> {
    let data = data_dir(backend);
    exec(
        data,
        &format!(
            "UPDATE changeset_parents SET parent = {} WHERE cs = {} AND n = 0;",
            pg_bytes(to.as_bytes()),
            pg_bytes(cs.as_bytes())
        ),
    )
}

/// Corrupts the stored body of one tree: the id stays, the bytes lie.
pub(crate) fn corrupt_tree_body(backend: &Backend, id: &[u8; 32], body: &[u8]) -> Result<()> {
    let data = data_dir(backend);
    exec(
        data,
        &format!(
            "UPDATE trees SET body = {} WHERE id = {}",
            pg_bytes(body),
            pg_bytes(id)
        ),
    )
}

/// Corrupts the stored body of one history node: the id stays, the
/// bytes no longer decode.
pub(crate) fn corrupt_history_node_body(
    backend: &Backend,
    id: &[u8; 32],
    body: &[u8],
) -> Result<()> {
    let data = data_dir(backend);
    exec(
        data,
        &format!(
            "UPDATE history_nodes SET body = {} WHERE id = {}",
            pg_bytes(body),
            pg_bytes(id)
        ),
    )
}

/// Inserts a tree row: the id stays the tree's own hash, the body is
/// whatever the test crafted (an entry whose size lies, for one).
pub(crate) fn insert_tree_row(backend: &Backend, id: &[u8], body: &[u8]) -> Result<()> {
    let data = data_dir(backend);
    exec(
        data,
        &format!(
            "INSERT INTO trees (id, body) VALUES ({}, {})",
            pg_bytes(id),
            pg_bytes(body)
        ),
    )
}

/// Deletes the row that names `cs` as one repo's changeset: the
/// changeset stays, the repo's membership dangles.
pub(crate) fn delete_repo_membership(backend: &Backend, cs: &ChangesetId) -> Result<()> {
    let data = data_dir(backend);
    // `refs` names the membership row: one transaction drops that
    // key, deletes the row, and puts the key back.
    exec(
        data,
        &format!(
            "BEGIN;
             ALTER TABLE refs DROP CONSTRAINT refs_repo_id_target_fkey;
             DELETE FROM repo_changesets WHERE cs = {};
             ALTER TABLE refs ADD CONSTRAINT refs_repo_id_target_fkey
                 FOREIGN KEY (repo_id, target) REFERENCES repo_changesets (repo_id, cs) NOT VALID;
             COMMIT;",
            pg_bytes(cs.as_bytes())
        ),
    )
}

/// Inserts a pending land row whose changeset does not exist: the
/// schema forbids it, an older build may have made it.
pub(crate) fn insert_land_row(
    backend: &Backend,
    repo: RepoId,
    request_key: &str,
    target_ref: &str,
    input_cs: &[u8; 32],
) -> Result<()> {
    let data = data_dir(backend);
    // The insert needs the foreign key gone: one transaction drops
    // it, makes the row, and puts the constraint back.
    exec(
        data,
        &format!(
            "BEGIN;
             ALTER TABLE lands DROP CONSTRAINT lands_input_cs_fkey;
             INSERT INTO lands (repo_id, caller, request_key, target_ref, input_cs, state)
             VALUES ({}, {}, {}, {}, {}, 'pending');
             ALTER TABLE lands ADD CONSTRAINT lands_input_cs_fkey
                 FOREIGN KEY (input_cs) REFERENCES changesets (id) NOT VALID;
             COMMIT;",
            repo.0,
            pg_text("test"),
            pg_text(request_key),
            pg_text(target_ref),
            pg_bytes(input_cs),
        ),
    )
}

/// Inserts a derived row, even one whose changeset does not exist: the
/// schema forbids that, an older build may have made it.
pub(crate) fn insert_derived_row(
    backend: &Backend,
    kind: &str,
    version: i64,
    cs: &[u8],
    value: &[u8],
) -> Result<()> {
    let data = data_dir(backend);
    exec(
        data,
        &format!(
            "BEGIN;
             ALTER TABLE derived DROP CONSTRAINT derived_cs_fkey;
             INSERT INTO derived (kind, version, cs, value)
             VALUES ({}, {}, {}, {});
             ALTER TABLE derived ADD CONSTRAINT derived_cs_fkey
                 FOREIGN KEY (cs) REFERENCES changesets (id) NOT VALID;
             COMMIT;",
            pg_text(kind),
            version,
            pg_bytes(cs),
            pg_bytes(value),
        ),
    )
}

/// Swaps the stored value of every derived row of `kind`: the kind
/// stays, the bytes no longer decode.
pub(crate) fn set_derived_value(backend: &Backend, kind: &str, value: &[u8]) -> Result<()> {
    let data = data_dir(backend);
    exec(
        data,
        &format!(
            "UPDATE derived SET value = {} WHERE kind = {}",
            pg_bytes(value),
            pg_text(kind)
        ),
    )
}

/// The id of the first stored history node, when one exists. A land
/// stores history nodes, so a landed store has one.
pub(crate) fn first_history_node_id(backend: &Backend) -> Result<Option<[u8; 32]>> {
    let data = data_dir(backend);
    pg_with(data, |client| {
        let row = client
            .query_opt("SELECT id FROM history_nodes LIMIT 1", &[])
            .map_err(map_pg_error)?;
        Ok(row.map(|r| id_of(&r.get::<_, Vec<u8>>(0))))
    })
}

/// Every derived row, as `(kind, version, cs, value)`: what the
/// derived tests assert about.
pub(crate) fn derived_rows(backend: &Backend) -> Result<Vec<DerivedRow>> {
    let data = data_dir(backend);
    pg_with(data, |client| {
        let rows = client
            .query(
                "SELECT kind, version, cs, value FROM derived ORDER BY kind, version, cs",
                &[],
            )
            .map_err(map_pg_error)?;
        Ok(rows
            .iter()
            .map(|r| {
                (
                    r.get::<_, String>(0),
                    r.get::<_, i64>(1),
                    r.get::<_, Vec<u8>>(2),
                    r.get::<_, Vec<u8>>(3),
                )
            })
            .collect())
    })
}

/// Downgrades every derived row of `kind` to version 0: the shape an
/// older build left behind, which the next write must remove. Returns
/// the count it made.
pub(crate) fn downgrade_derived_rows(backend: &Backend, kind: &str) -> Result<u64> {
    let data = data_dir(backend);
    pg_with(data, |client| {
        client
            .execute(
                "INSERT INTO derived (kind, version, cs, value)
                 SELECT kind, 0, cs, value FROM derived WHERE kind = $1",
                &[&kind],
            )
            .map_err(map_pg_error)
    })
}

// -- the raw form ------------------------------------------------------

/// The data directory of a backend: its blob files sit two folders
/// in, under `objects/blob`.
fn data_dir(backend: &Backend) -> &Path {
    backend
        .blobs()
        .root()
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the blob root sits in the data directory")
}

/// The raw client of one schema, kept for the whole test run: every
/// damage call reuses it, so a parallel suite cannot exhaust the
/// database's connection slots. The schema of the data directory keys
/// the cache; one global lock keeps the map itself safe, and the
/// damage of one call holds it, so damages of one schema never race.
fn pg_with<T>(data: &Path, f: impl FnOnce(&mut postgres::Client) -> Result<T>) -> Result<T> {
    static CLIENTS: std::sync::LazyLock<
        std::sync::Mutex<std::collections::HashMap<String, postgres::Client>>,
    > = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let schema = test_schema(data);
    let mut cache = CLIENTS.lock().expect("the client cache never poisons");
    if !cache.contains_key(&schema) {
        let mut client =
            postgres::Client::connect(&test_url()?, postgres::NoTls).map_err(map_pg_error)?;
        client
            .batch_execute(&format!("SET search_path = \"{schema}\""))
            .map_err(map_pg_error)?;
        cache.insert(schema.clone(), client);
    }
    f(cache.get_mut(&schema).unwrap())
}

/// Runs one statement on the raw client of the directory's schema.
fn exec(data: &Path, sql: &str) -> Result<()> {
    pg_with(data, |client| {
        client.batch_execute(sql).map_err(map_pg_error)
    })
}

/// A byte string as a Postgres hex literal.
fn pg_bytes(v: &[u8]) -> String {
    let mut s = String::from("'\\x");
    for b in v {
        let _ = write!(s, "{b:02x}");
    }
    s.push_str("'::bytea");
    s
}

/// A text string as a quoted Postgres literal.
fn pg_text(v: &str) -> String {
    format!("'{}'", v.replace('\'', "''"))
}

/// The first 32 bytes of an id row, as an array.
fn id_of(v: &[u8]) -> [u8; 32] {
    let mut id = [0_u8; 32];
    id.copy_from_slice(&v[..32]);
    id
}

/// How many view rows the store holds: the tests of the view memo count
/// the rows a derivation adds.
pub(crate) fn view_tree_rows(backend: &Backend) -> Result<usize> {
    let data = data_dir(backend);
    pg_with(data, |client| {
        let row = client
            .query_one("SELECT count(*) FROM view_trees", &[])
            .map_err(map_pg_error)?;
        Ok(usize::try_from(row.get::<_, i64>(0)).expect("a row count fits"))
    })
}

/// The count of the lands whose state is `landed`.
pub(crate) fn landed_rows(backend: &Backend) -> Result<i64> {
    let data = data_dir(backend);
    pg_with(data, |client| {
        let row = client
            .query_one("SELECT count(*) FROM lands WHERE state = 'landed'", &[])
            .map_err(map_pg_error)?;
        Ok(row.get::<_, i64>(0))
    })
}

/// The state of the land row of `request_key`.
pub(crate) fn land_state_by_key(backend: &Backend, key: &str) -> Result<String> {
    let data = data_dir(backend);
    pg_with(data, |client| {
        let row = client
            .query_opt("SELECT state FROM lands WHERE request_key = $1", &[&key])
            .map_err(map_pg_error)?;
        Ok(row
            .map(|r| r.get::<_, String>(0))
            .expect("the land row exists"))
    })
}

/// The state and detail of the land row of `request_key`.
pub(crate) fn land_state_detail_by_key(backend: &Backend, key: &str) -> Result<(String, String)> {
    let data = data_dir(backend);
    pg_with(data, |client| {
        let row = client
            .query_opt(
                "SELECT state, detail FROM lands WHERE request_key = $1",
                &[&key],
            )
            .map_err(map_pg_error)?;
        Ok(row
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .expect("the land row exists"))
    })
}

/// The count of the land rows of one caller and request key.
pub(crate) fn land_rows_by_caller_key(backend: &Backend, caller: &str, key: &str) -> Result<i64> {
    let data = data_dir(backend);
    pg_with(data, |client| {
        let row = client
            .query_one(
                "SELECT count(*) FROM lands WHERE caller = $1 AND request_key = $2",
                &[&caller, &key],
            )
            .map_err(map_pg_error)?;
        Ok(row.get::<_, i64>(0))
    })
}

/// Moves the time one land finished `secs` into the past.
pub(crate) fn age_land(backend: &Backend, land_id: i64, secs: i64) -> Result<usize> {
    let data = data_dir(backend);
    pg_with(data, |client| {
        client
            .execute(
                "UPDATE lands SET finished_at = finished_at - $2 WHERE land_id = $1",
                &[&land_id, &secs],
            )
            .map_err(map_pg_error)
            .map(|n| usize::try_from(n).expect("a row count fits"))
    })
}

/// Moves the time every pending land was created `secs` into the past.
pub(crate) fn age_pending_lands(backend: &Backend, secs: i64) -> Result<()> {
    let data = data_dir(backend);
    pg_with(data, |client| {
        client
            .execute(
                "UPDATE lands SET created_at = created_at - $1 WHERE state = 'pending'",
                &[&secs],
            )
            .map_err(map_pg_error)?;
        Ok(())
    })
}
