//! Damage for tests: one function per kind, and each takes the
//! backend, so the caller never spells either backend's form.
//!
//! Every function makes its row the way an older build may have -
//! through the database, behind the server's rules. On SQLite it opens
//! `scion.db` raw; on Postgres it opens its own client on the
//! directory's schema (the same registration `test_hooks` uses). The
//! SQLite half goes away with the engine, and no caller changes.
//!
//! The schema forbids most of these rows with its keys on, so the
//! helpers that need it drop the rule for the insert: SQLite turns
//! `foreign_keys` off on its own connection, Postgres drops the
//! constraint and adds it back in the same transaction.

#![forbid(unsafe_code)]

use crate::Backend;
use crate::backend::test_hooks::{test_schema, test_url};
use crate::error::{Result, from_sqlite};

use super::map_pg_error;
use crate::meta::RepoId;
use scion_core::ChangesetId;
use std::fmt::Write as _;
use std::path::Path;
/// The gate the Postgres tests hold while a fixture is open: one test
/// at a time against the database, whose connection slots a parallel
/// suite's writer-plus-readers would exhaust. SQLite has no such bound,
/// so the lock skips itself there.
pub(crate) fn serial() -> Option<std::sync::MutexGuard<'static, ()>> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    if Backend::test_is_postgres() {
        Some(
            SERIAL
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    } else {
        None
    }
}

/// The kind of a derived row.
pub(crate) type DerivedRow = (String, i64, Vec<u8>, Vec<u8>);

// -- damage: one function per kind -------------------------------------

/// Corrupts the stored body of one changeset: the id stays, the bytes lie.
pub(crate) fn corrupt_changeset_body(backend: &Backend, id: &ChangesetId) -> Result<()> {
    let data = data_dir(backend);
    if Backend::test_is_postgres() {
        exec(
            data,
            &format!(
                "UPDATE changesets SET body = {} WHERE id = {}",
                pg_bytes(&[0_u8]),
                pg_bytes(id.as_bytes())
            ),
        )
    } else {
        raw_sqlite(data, true, |c| {
            c.execute(
                "UPDATE changesets SET body = X'00' WHERE id = ?1",
                rusqlite::params![id.as_bytes()],
            )
        })
        .map(|_| ())
    }
}

/// Corrupts the stored body of one tree: the id stays, the bytes lie.
pub(crate) fn corrupt_tree_body(backend: &Backend, id: &[u8; 32], body: &[u8]) -> Result<()> {
    let data = data_dir(backend);
    if Backend::test_is_postgres() {
        exec(
            data,
            &format!(
                "UPDATE trees SET body = {} WHERE id = {}",
                pg_bytes(body),
                pg_bytes(id)
            ),
        )
    } else {
        raw_sqlite(data, false, |c| {
            c.execute(
                "UPDATE trees SET body = ?1 WHERE id = ?2",
                rusqlite::params![body, id],
            )
        })
        .map(|_| ())
    }
}

/// Corrupts the stored body of one history node: the id stays, the
/// bytes no longer decode.
pub(crate) fn corrupt_history_node_body(
    backend: &Backend,
    id: &[u8; 32],
    body: &[u8],
) -> Result<()> {
    let data = data_dir(backend);
    if Backend::test_is_postgres() {
        exec(
            data,
            &format!(
                "UPDATE history_nodes SET body = {} WHERE id = {}",
                pg_bytes(body),
                pg_bytes(id)
            ),
        )
    } else {
        raw_sqlite(data, false, |c| {
            c.execute(
                "UPDATE history_nodes SET body = ?1 WHERE id = ?2",
                rusqlite::params![body, id],
            )
        })
        .map(|_| ())
    }
}

/// Inserts a tree row: the id stays the tree's own hash, the body is
/// whatever the test crafted (an entry whose size lies, for one).
pub(crate) fn insert_tree_row(backend: &Backend, id: &[u8], body: &[u8]) -> Result<()> {
    let data = data_dir(backend);
    if Backend::test_is_postgres() {
        exec(
            data,
            &format!(
                "INSERT INTO trees (id, body) VALUES ({}, {})",
                pg_bytes(id),
                pg_bytes(body)
            ),
        )
    } else {
        raw_sqlite(data, false, |c| {
            c.execute(
                "INSERT INTO trees (id, body) VALUES (?1, ?2)",
                rusqlite::params![id, body],
            )
        })
        .map(|_| ())
    }
}

/// Deletes the row that names `cs` as one repo's changeset: the
/// changeset stays, the repo's membership dangles.
pub(crate) fn delete_repo_membership(backend: &Backend, cs: &ChangesetId) -> Result<()> {
    let data = data_dir(backend);
    if Backend::test_is_postgres() {
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
    } else {
        raw_sqlite(data, true, |c| {
            c.execute(
                "DELETE FROM repo_changesets WHERE cs = ?1",
                rusqlite::params![cs.as_bytes()],
            )
        })
        .map(|_| ())
    }
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
    if Backend::test_is_postgres() {
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
    } else {
        raw_sqlite(data, true, |c| {
            c.execute(
                "INSERT INTO lands (repo_id, caller, request_key, target_ref, input_cs, state)
                 VALUES (?1, 'test', ?2, ?3, ?4, 'pending')",
                rusqlite::params![repo.0, request_key, target_ref, input_cs],
            )
        })
        .map(|_| ())
    }
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
    if Backend::test_is_postgres() {
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
    } else {
        raw_sqlite(data, true, |c| {
            c.execute(
                "INSERT INTO derived (kind, version, cs, value) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![kind, version, cs, value],
            )
        })
        .map(|_| ())
    }
}

/// Swaps the stored value of every derived row of `kind`: the kind
/// stays, the bytes no longer decode.
pub(crate) fn set_derived_value(backend: &Backend, kind: &str, value: &[u8]) -> Result<()> {
    let data = data_dir(backend);
    if Backend::test_is_postgres() {
        exec(
            data,
            &format!(
                "UPDATE derived SET value = {} WHERE kind = {}",
                pg_bytes(value),
                pg_text(kind)
            ),
        )
    } else {
        raw_sqlite(data, false, |c| {
            c.execute(
                "UPDATE derived SET value = ?1 WHERE kind = ?2",
                rusqlite::params![value, kind],
            )
        })
        .map(|_| ())
    }
}

/// The id of the first stored history node, when one exists. A land
/// stores history nodes, so a landed store has one.
pub(crate) fn first_history_node_id(backend: &Backend) -> Result<Option<[u8; 32]>> {
    let data = data_dir(backend);
    if Backend::test_is_postgres() {
        pg_with(data, |client| {
            let row = client
                .query_opt("SELECT id FROM history_nodes LIMIT 1", &[])
                .map_err(map_pg_error)?;
            Ok(row.map(|r| id_of(&r.get::<_, Vec<u8>>(0))))
        })
    } else {
        raw_sqlite(data, false, |c| {
            let mut stmt = c.prepare("SELECT id FROM history_nodes LIMIT 1")?;
            let mut rows = stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
            match rows.next() {
                Some(row) => Ok(Some(id_of(&row?))),
                None => Ok(None),
            }
        })
    }
}

/// Every derived row, as `(kind, version, cs, value)`: what the
/// derived tests assert about.
pub(crate) fn derived_rows(backend: &Backend) -> Result<Vec<DerivedRow>> {
    let data = data_dir(backend);
    if Backend::test_is_postgres() {
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
    } else {
        raw_sqlite(data, false, |c| {
            let mut stmt = c.prepare(
                "SELECT kind, version, cs, value FROM derived ORDER BY kind, version, cs",
            )?;
            let mapped = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            })?;
            let mut rows = Vec::new();
            for row in mapped {
                rows.push(row?);
            }
            Ok(rows)
        })
    }
}

/// Downgrades every derived row of `kind` to version 0: the shape an
/// older build left behind, which the next write must remove. Returns
/// the count it made.
pub(crate) fn downgrade_derived_rows(backend: &Backend, kind: &str) -> Result<u64> {
    let data = data_dir(backend);
    if Backend::test_is_postgres() {
        pg_with(data, |client| {
            client
                .execute(
                    "INSERT INTO derived (kind, version, cs, value)
                     SELECT kind, 0, cs, value FROM derived WHERE kind = $1",
                    &[&kind],
                )
                .map_err(map_pg_error)
        })
    } else {
        raw_sqlite(data, false, |c| {
            c.execute(
                "INSERT INTO derived (kind, version, cs, value)
                 SELECT kind, 0, cs, value FROM derived WHERE kind = ?1",
                rusqlite::params![kind],
            )
            .map(|n| n as u64)
        })
    }
}

// -- the two forms ------------------------------------------------------

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

/// Runs one closure on a raw SQLite connection of the data directory.
/// `no_foreign_keys` turns the keys off first, the way the raw tests
/// made schema-forbidden rows.
fn raw_sqlite<T>(
    data: &Path,
    no_foreign_keys: bool,
    f: impl FnOnce(&mut rusqlite::Connection) -> rusqlite::Result<T>,
) -> Result<T> {
    let mut c = rusqlite::Connection::open(data.join("scion.db")).map_err(from_sqlite)?;
    if no_foreign_keys {
        c.pragma_update(None, "foreign_keys", false)
            .map_err(from_sqlite)?;
    }
    let out = f(&mut c).map_err(from_sqlite)?;
    drop(c);
    Ok(out)
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
/// the rows a derivation adds. SQLite reads its table; Postgres reads
/// the schema's table off the runtime's raw client.
pub(crate) fn view_tree_rows(backend: &Backend) -> Result<usize> {
    let data = data_dir(backend);
    if Backend::test_is_postgres() {
        pg_with(data, |client| {
            let row = client
                .query_one("SELECT count(*) FROM view_trees", &[])
                .map_err(map_pg_error)?;
            Ok(usize::try_from(row.get::<_, i64>(0)).expect("a row count fits"))
        })
    } else {
        raw_sqlite(data, false, |c| {
            c.query_row("SELECT count(*) FROM view_trees", [], |row| {
                row.get::<_, i64>(0)
            })
            .map(|n| usize::try_from(n).expect("a row count fits"))
        })
    }
}
