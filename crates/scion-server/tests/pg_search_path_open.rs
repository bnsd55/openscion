//! An empty schema opens as its own store: the open's version probe and
//! identity reads are qualified to the schema the connection resolves,
//! so a later `search_path` member that holds a migrated store is never
//! read (#1429). The scripts and the operators connect with
//! `options=--search_path=<schema>,public`; a fresh schema on a shared
//! database must migrate ITSELF, and the later member's rows must come
//! out untouched.
//!
//! The victim here is a second test-owned schema, not `public` itself:
//! the shared test database forbids writes to `public` (its owner is
//! unknown), and the fallthrough is the same mechanism whatever the
//! later member's name is. Both schemas this test makes are named here
//! and dropped here.

use postgres::Client;
use scion_server::Backend;

/// The test database, as the other Postgres suites take it.
fn pg_url() -> String {
    std::env::var("SCION_TEST_POSTGRES_URL").expect("SCION_TEST_POSTGRES_URL")
}

/// The URL with the scripts' options shape: the target schema first,
/// the victim second, in `public`'s role - the member an unqualified
/// name falls through to.
fn url_with_search_path(target: &str, victim: &str) -> String {
    format!(
        "{}?options=--search_path%3D{target}%2C{victim}",
        pg_url().trim_end_matches('/')
    )
}

fn raw() -> Client {
    Client::connect(&pg_url(), postgres::NoTls).expect("the test database is up")
}

fn drop_schema(schema: &str) {
    raw()
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("the test's own schema drops");
}

fn create_schema(schema: &str) {
    raw()
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .expect("the test's own schema is created");
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::new(), |s, b| format!("{s}{b:02x}"))
}

/// The schemas one test run made. Dropped on every exit path, panics
/// included: the test only ever drops names it minted itself.
struct Schemas(Vec<String>);

impl Drop for Schemas {
    fn drop(&mut self) {
        for name in &self.0 {
            drop_schema(name);
        }
    }
}

/// The qualified name of `schema`.`table`, or `None` when the schema
/// holds no such table. The name is the database's own serialization of
/// the identifier, safe to embed in the query that follows.
fn table_of(c: &mut Client, schema: &str, table: &str) -> Option<String> {
    let sql = format!("SELECT to_regclass(format('%I.{table}', $1::text))::text");
    // The table name is a fixed literal of this file, never caller data.
    c.query(&sql, &[&schema]).expect("the table probe reads")[0]
        .get::<_, Option<String>>(0)
        .map(|_| format!("{schema}.{table}"))
}

/// One test-owned schema name per call, in the suite's swept namespace,
/// unique per process and per call.
fn schema_name(kind: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    format!(
        "scion_test_1429_{}_{}_{n:04}",
        kind,
        std::process::id() % 100_000
    )
}

/// The victim's store UUID, read from its schema, qualified.
fn store_uuid_of(schema: &str) -> Vec<u8> {
    let mut c = raw();
    let name = table_of(&mut c, schema, "store_uuid").expect("the victim's store_uuid exists");
    c.query(&format!("SELECT uuid FROM {name}"), &[])
        .expect("the victim's store UUID reads")[0]
        .get(0)
}

/// One count from the victim's schema, 0 when the table is absent.
fn count_of(c: &mut Client, schema: &str, table: &str) -> i64 {
    match table_of(c, schema, table) {
        None => 0,
        Some(name) => c
            .query(&format!("SELECT count(*) FROM {name}"), &[])
            .expect("the count reads")[0]
            .get(0),
    }
}

/// What the victim holds, as one string: the rows the open must leave
/// alone. Every lookup is qualified to the victim's schema.
fn snapshot(schema: &str) -> String {
    let uuid = hex(&store_uuid_of(schema));
    let mut c = raw();
    let version = match table_of(&mut c, schema, "schema_migrations") {
        None => "?".to_string(),
        Some(name) => c
            .query(&format!("SELECT max(version) FROM {name}"), &[])
            .expect("the version reads")[0]
            .get::<_, Option<i64>>(0)
            .map_or("?".into(), |v| v.to_string()),
    };
    format!(
        "uuid={uuid} repos={} refs={} trees={} version={version}",
        count_of(&mut c, schema, "repos"),
        count_of(&mut c, schema, "refs"),
        count_of(&mut c, schema, "trees"),
    )
}

/// A victim: a store the standard test path migrated in its own schema,
/// with the rows a served store carries when `served` (one repo, one
/// changeset, one ref at `main`). Returns (schema, uuid hex).
fn make_victim(served: bool) -> (String, String, tempfile::TempDir) {
    let dir = scion_server::test_support::private_tempdir();
    let victim = schema_name("victim");
    create_schema(&victim);
    // The shared open migrates the schema and mints its store UUID; it
    // takes no one-server lock, which this test's process does not need.
    let store =
        Backend::open_test_shared_in_schema(dir.path(), &victim).expect("the victim store opens");
    if served {
        let mut c = raw();
        c.batch_execute(&format!(
            "SET search_path TO {victim};
             INSERT INTO repos (name) VALUES ('r');
             INSERT INTO changesets (id, gen, body)
               VALUES (repeat('x', 32)::bytea, 1, 'b');
             INSERT INTO repo_changesets (repo_id, cs)
               SELECT repo_id, id FROM repos, changesets WHERE name = 'r';
             INSERT INTO refs (repo_id, name, target, version)
               SELECT repo_id, 'main', id, 1 FROM repos, changesets WHERE name = 'r';"
        ))
        .expect("the victim's served rows are made");
    }
    let uuid = hex(&store_uuid_of(&victim));
    let _ = store; // the schema outlives this function; the dir keeps the marker
    (victim, uuid, dir)
}

/// A served victim (its repos row makes the pairing refuse): the open
/// of a fresh, empty target must NOT take the victim's store - it
/// refuses on today's code, naming the victim's UUID, and after the fix
/// it opens, migrates its own schema, and mints its own UUID, with the
/// victim's rows byte-identical.
#[test]
fn an_empty_schema_next_to_a_served_store_opens_as_its_own_store() {
    let (victim, victim_uuid, _victim_dir) = make_victim(true);
    let before = snapshot(&victim);
    let target = schema_name("target");
    create_schema(&target);
    let _schemas = Schemas(vec![victim.clone(), target.clone()]);
    let target_dir = scion_server::test_support::private_tempdir();

    let opened = Backend::open_migrating(
        target_dir.path(),
        &url_with_search_path(&target, &victim),
        None,
    );
    let store = match opened {
        Ok(store) => store,
        Err(error) => panic!(
            "the empty target refused, which on today's code means the \
             version probe fell through to the victim's ledger; the \
             refusal was: {error}"
        ),
    };
    // The target's own ledger exists, at this build's version.
    let mut c = raw();
    let version = match table_of(&mut c, &target, "schema_migrations") {
        None => panic!("the target migrated no ledger of its own"),
        Some(name) => c
            .query(&format!("SELECT max(version) FROM {name}"), &[])
            .expect("the target's ledger reads")[0]
            .get::<_, Option<i64>>(0)
            .expect("the target migrated its own schema"),
    };
    assert_eq!(version, 6, "the target's own ledger is at this build");
    // The target minted its own identity, not the victim's.
    let target_uuid = hex(&store_uuid_of(&target));
    assert_ne!(
        target_uuid, victim_uuid,
        "the empty target took the victim's store identity"
    );
    // The marker the target's directory carries names the same store.
    let marker = std::fs::read_to_string(target_dir.path().join("backend"))
        .expect("the target's marker is written");
    assert!(
        marker.contains(&target_uuid),
        "the target's marker names the target's store, not {marker}"
    );
    // The victim's rows are what they were.
    let after = snapshot(&victim);
    assert_eq!(after, before, "the victim's rows are untouched");
    let _ = store;
}

/// An unserved victim (a store UUID, no repos row - the state a first
/// open cut before its first repo leaves): today's code ADOPTS the
/// victim's identity and serves a schema with no tables - the silent
/// corruption the issue names. The open must instead migrate the target
/// and mint a fresh identity.
#[test]
fn an_empty_schema_next_to_an_unserved_store_never_adopts_its_identity() {
    let (victim, victim_uuid, _victim_dir) = make_victim(false);
    let before = snapshot(&victim);
    let target = schema_name("target");
    create_schema(&target);
    let _schemas = Schemas(vec![victim.clone(), target.clone()]);
    let target_dir = scion_server::test_support::private_tempdir();

    let store = Backend::open_migrating(
        target_dir.path(),
        &url_with_search_path(&target, &victim),
        None,
    )
    .unwrap_or_else(|error| panic!("the empty target refused: {error}"));

    // The target's own ledger exists (the open migrated the empty
    // schema; on today's code the probe fell through, skipped the
    // migration, and this probe answers None).
    let ledger = table_of(&mut raw(), &target, "schema_migrations");
    assert!(
        ledger.is_some(),
        "the empty target never migrated its own schema: the version \
         probe read the victim's ledger through the search_path"
    );
    // The target minted its own identity, not the victim's.
    let target_uuid = hex(&store_uuid_of(&target));
    assert_ne!(
        target_uuid, victim_uuid,
        "the empty target adopted the victim's store identity"
    );
    let marker = std::fs::read_to_string(target_dir.path().join("backend"))
        .expect("the target's marker is written");
    assert!(
        marker.contains(&target_uuid),
        "the target's marker names the target's store, not {marker}"
    );
    // The victim's rows are what they were.
    assert_eq!(snapshot(&victim), before, "the victim's rows are untouched");
    let _ = store;
}
