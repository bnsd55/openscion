//! The stale-schema sweep: a test run leaves its Postgres schemas
//! behind, because a server holds its schema while it runs. The sweep
//! runs at the start of a run and drops the schemas of tests that are
//! over - the data directory is gone and no connection still names the
//! schema - and never a schema whose directory or owner is alive.
//! These proofs fail if the sweep drops a live schema or keeps a dead
//! one. The tests share one registry and one database, and nextest runs
//! them as concurrent processes, so every test that drives the sweep
//! holds one advisory lock for its whole body: a test's fixture is
//! never another test's sweep's input (#863).

use std::path::Path;

use scion_server::Backend;
use scion_server::test_support::private_tempdir;

/// The database's URL, and a client that reaches it. Every test here
/// needs no store of its own, only the registry and the catalog.
fn connect() -> postgres::Client {
    let url = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL must name the test database");
    postgres::Client::connect(&url, postgres::NoTls).unwrap()
}

/// One client whose session holds the sweep tests' advisory lock for
/// the caller's whole body. The sweep is global - it reads every
/// registry row in the database - so a test that shapes the registry
/// while another test's sweep reads it makes that sweep see a database
/// the test never shaped; the cap test's fifty rows would spend the
/// sweep's whole cap before another test's younger schema came up. The
/// lock releases when the client drops, a panic included.
fn locked() -> postgres::Client {
    let mut client = connect();
    client
        .batch_execute("SELECT pg_advisory_lock(hashtext('scion_test_sweep_tests'))")
        .unwrap();
    client
}

/// Whether the database still has the schema.
fn schema_exists(client: &mut postgres::Client, schema: &str) -> bool {
    client
        .query_opt(
            "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1",
            &[&schema],
        )
        .unwrap()
        .is_some()
}

/// The schema's comment, which records the directory the open served.
fn schema_comment(client: &mut postgres::Client, schema: &str) -> Option<String> {
    client
        .query_opt(
            "SELECT obj_description(n.oid, 'pg_namespace')
             FROM pg_catalog.pg_namespace AS n WHERE n.nspname = $1",
            &[&schema],
        )
        .unwrap()
        .and_then(|row| row.get(0))
}

/// A registry row for `schema`, aged `age_secs` behind the clock, so a
/// test shapes the eligibility of a schema it made just now. A day's
/// age makes the row the oldest one the sweep sees, so the sweep
/// reaches it before any older leftover of an earlier run.
fn register(client: &mut postgres::Client, schema: &str, age_secs: i64) {
    client
        .batch_execute(&format!(
            "INSERT INTO public.scion_test_schemas (schema, started)
         VALUES ('{schema}', extract(epoch from now())::bigint - {age_secs})
         ON CONFLICT (schema) DO UPDATE SET started = excluded.started"
        ))
        .unwrap();
}

/// Waits for a closed client's server backend to leave
/// `pg_stat_activity`: the client's drop returns before the server
/// tears the backend down, so a sweep that runs at once can still see
/// the closed connection's name and keep the schema. Twenty-millisecond
/// ticks, a two-second bound; the panic prints what the database still
/// sees.
fn wait_unnamed(client: &mut postgres::Client, schema: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        let named: i64 = client
            .query_one(
                "SELECT count(*) FROM pg_catalog.pg_stat_activity
                 WHERE application_name = $1",
                &[&schema],
            )
            .unwrap()
            .get(0);
        if named == 0 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    panic!(
        "the closed client's backend left pg_stat_activity within two seconds; it still sees: {}",
        snapshot(client, schema)
    );
}

/// The key `register_test_schema` and the drain serialize on: the same
/// constant test_support/pg.rs names CLEAN_LOCK, spelled here so the
/// test holds exactly that lock.
const CLEAN_LOCK: i64 = 0x5343_494F_4E53_5445;

/// One open whose registration waits for the cleanup lock: a test
/// session holds CLEAN_LOCK for six seconds while an open runs, longer
/// than the writer's 5 s lock clock. The registration runs on the
/// preparer's session, which carries no lock clock, so the open must
/// succeed - slow, not failed. A 55P03 here names the statement that
/// waited (#905).
#[test]
fn an_open_whose_cleanup_lock_is_held_for_six_seconds_succeeds() {
    let _lock = locked();
    let holder = std::thread::spawn(|| {
        let mut client = connect();
        client
            .execute("SELECT pg_advisory_lock($1)", &[&CLEAN_LOCK])
            .unwrap();
        std::thread::sleep(std::time::Duration::from_secs(6));
        client
            .execute("SELECT pg_advisory_unlock($1)", &[&CLEAN_LOCK])
            .unwrap();
    });
    std::thread::sleep(std::time::Duration::from_millis(200));
    let dir = private_tempdir();
    let started = std::time::Instant::now();
    let store = Backend::open_test(dir.path());
    if let Err(err) = &store {
        panic!("the open failed while the cleanup lock was held: {err}");
    }
    drop(store);
    holder.join().unwrap();
    let took = started.elapsed();
    assert!(
        took >= std::time::Duration::from_secs(5),
        "the open did not wait for the cleanup lock: {took:?}"
    );
}

/// Eight parallel opens while stale schemas wait to be drained: the
/// drain holds the cleanup lock across its drops, and every open's
/// writer must start inside its own 5 s lock clock (#905).
#[test]
fn eight_parallel_opens_succeed_while_stale_schemas_wait_to_drain() {
    let _lock = locked();
    let mut client = connect();
    let pid = std::process::id();
    for i in 0..10 {
        let stale = format!("scion_test_stall{pid}_{i}");
        let mut tables = String::new();
        for t in 0..60 {
            tables.push_str(&format!("CREATE TABLE {stale}.t{t} (a int); "));
        }
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {stale}; {tables}
                 COMMENT ON SCHEMA {stale} IS '/the/directory/is/gone'"
            ))
            .unwrap();
        register(&mut client, &stale, 86_400_000 * 10 - 100);
    }
    std::thread::scope(|scope| {
        let results: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    let dir = private_tempdir();
                    let opened = Backend::open_test(dir.path()).is_ok();
                    (opened, dir)
                })
            })
            .collect();
        for result in results {
            let (opened, _dir) = result.join().unwrap();
            assert!(opened, "a parallel open failed");
        }
    });
}

/// A finished test's schema drops at a later sweep: the backend closed,
/// its directory is gone, and no connection names the schema. The
/// registry row goes with it.
#[test]
fn a_finished_tests_schema_drops_at_a_later_sweep() {
    let _lock = locked();
    let dir = private_tempdir();
    let schema = Backend::test_schema(dir.path());
    {
        let store = Backend::open_test(dir.path()).unwrap();
        let mut client = connect();
        assert_eq!(
            schema_comment(&mut client, &schema).as_deref(),
            Some(dir.path().to_string_lossy().as_ref()),
            "the open records the data directory in the schema's comment"
        );
        drop(store);
    }
    let mut client = connect();
    wait_unnamed(&mut client, &schema);
    dir.close().unwrap();
    // The run's own first-open sweep may drop it first; whichever sweep
    // runs, the finished test's schema is gone after this one.
    let _ = Backend::sweep_stale_test_schemas().unwrap();
    assert!(
        !schema_exists(&mut client, &schema),
        "a finished test's schema drops"
    );
    let left: i64 = client
        .query_one(
            "SELECT count(*) FROM public.scion_test_schemas WHERE schema = $1",
            &[&schema],
        )
        .unwrap()
        .get(0);
    assert_eq!(left, 0, "the registry row goes with the schema");
}

/// A schema whose directory still exists survives the sweep, whatever
/// its age: a suite that reopens a fixed directory in a later run
/// keeps its data. This is the check the age-only rule got wrong.
#[test]
fn a_schema_whose_directory_lives_survives_the_sweep() {
    let _lock = locked();
    let dir = private_tempdir();
    let schema = Backend::test_schema(dir.path());
    {
        let _store = Backend::open_test(dir.path()).unwrap();
    }
    let mut client = connect();
    register(&mut client, &schema, 86_400);
    Backend::sweep_stale_test_schemas().unwrap();
    assert!(
        schema_exists(&mut client, &schema),
        "a live directory's schema never drops, however old the registration"
    );
    // The store still opens: its schema and its rows are whole.
    let _store = Backend::open_test(dir.path()).unwrap();
}

/// A schema a live connection still names survives the sweep, and
/// drops once the connection is gone: a test that runs in another
/// process keeps its schema for its whole life.
#[test]
fn a_schema_a_connection_still_names_survives_the_sweep() {
    let _lock = locked();
    let dir = private_tempdir();
    let schema = Backend::test_schema(dir.path());
    {
        let _store = Backend::open_test(dir.path()).unwrap();
    }
    let mut client = connect();
    register(&mut client, &schema, 86_400);
    let mut holder = connect();
    holder
        .batch_execute(&format!(
            "SET application_name TO '{schema}';
             SELECT pg_advisory_lock_shared(hashtext('{schema}'))"
        ))
        .unwrap();
    dir.close().unwrap();
    Backend::sweep_stale_test_schemas().unwrap();
    let mut client = connect();
    if schema_exists(&mut client, &schema) {
        // The holder's name protected the schema through the sweep.
    } else {
        panic!(
            "a named connection keeps its schema alive; after the drop: {}",
            snapshot(&mut client, &schema)
        );
    }
    drop(holder);
    wait_unnamed(&mut client, &schema);
    Backend::sweep_stale_test_schemas().unwrap();
    assert!(
        !schema_exists(&mut client, &schema),
        "the schema drops once its last connection is gone: {}",
        snapshot(&mut client, &schema)
    );
}

/// What the sweep saw when a named schema vanished anyway: the
/// registry row, the comment, and the connections that name test
/// schemas. A test prints this before its own failure, so a parallel
/// sweep's read is on the record.
fn snapshot(client: &mut postgres::Client, schema: &str) -> String {
    let row = client
        .query_opt(
            "SELECT started, obj_description(n.oid, 'pg_namespace')
             FROM public.scion_test_schemas AS r
             JOIN pg_catalog.pg_namespace AS n ON n.nspname = r.schema
             WHERE r.schema = $1",
            &[&schema],
        )
        .unwrap();
    let named: Vec<String> = client
        .query(
            "SELECT application_name FROM pg_catalog.pg_stat_activity
             WHERE application_name LIKE 'scion_test_%'
               AND application_name <> ''",
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    format!("registry {row:?}; connections naming test schemas: {named:?}")
}

/// The sweep drops only the names the test schema helper spells: a
/// registered schema outside the prefix never drops, whatever its age,
/// and the helper's own names always carry the prefix.
#[test]
fn the_sweep_drops_only_the_names_the_test_helper_makes() {
    let _lock = locked();
    // The registry comes with the first open, and this test shapes it
    // before any store of its own exists: it opens one first, so the
    // table is there under any scheduling.
    let dir = private_tempdir();
    {
        let _store = Backend::open_test(dir.path()).unwrap();
    }
    dir.close().unwrap();
    let mut client = connect();
    let foreign = "not_a_test_schema_of_the_sweep";
    client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {foreign};
             COMMENT ON SCHEMA {foreign} IS '/the/directory/is/gone'"
        ))
        .unwrap();
    register(&mut client, foreign, 86_400);
    Backend::sweep_stale_test_schemas().unwrap();
    assert!(
        schema_exists(&mut client, foreign),
        "a schema outside the test prefix never drops"
    );
    client
        .batch_execute(&format!("DROP SCHEMA {foreign} CASCADE; DELETE FROM public.scion_test_schemas WHERE schema = '{foreign}'"))
        .unwrap();
    let dir = private_tempdir();
    let own = Backend::test_schema(dir.path());
    assert!(
        own.starts_with("scion_test_"),
        "the helper's names carry the prefix the sweep filters on"
    );
    dir.close().unwrap();
}

/// At most fifty schemas drop in one sweep, oldest registration first,
/// and the rest wait for the next sweep: a database with hundreds of
/// stale schemas must not stall the first test of a run for minutes.
/// The registry rows are aged far beyond any leftover, so this test's
/// own schemas are the oldest ones the sweep sees.
#[test]
fn a_sweep_drops_at_most_fifty_oldest_first() {
    let _lock = locked();
    // The registry comes with the first open, and this test shapes it
    // before any store of its own exists: it opens one first, so the
    // table is there under any scheduling.
    let dir = private_tempdir();
    {
        let _store = Backend::open_test(dir.path()).unwrap();
    }
    dir.close().unwrap();
    let mut client = connect();
    let pid = std::process::id();
    // A failed earlier run of this test must not shape this one: its
    // rows and schemas go first.
    client
        .batch_execute(&format!(
            "DELETE FROM public.scion_test_schemas WHERE schema LIKE 'scion_test_cap{pid}_%'"
        ))
        .unwrap();
    let made: Vec<String> = (0..55)
        .map(|i| {
            let name = format!("scion_test_cap{pid}_{i}");
            client
                .batch_execute(&format!(
                    "CREATE SCHEMA IF NOT EXISTS {name};
                     COMMENT ON SCHEMA {name} IS '/the/directory/is/gone'"
                ))
                .unwrap();
            // The oldest registration is the smallest index, and far
            // older than any leftover of an earlier run.
            register(&mut client, &name, 86_400_000 - i);
            name
        })
        .collect();
    let (dropped, _) = Backend::sweep_stale_test_schemas().unwrap();
    assert!(dropped <= 50, "one sweep drops at most fifty");
    let survivors: Vec<String> = client
        .query(
            "SELECT schema FROM public.scion_test_schemas
             WHERE schema LIKE $1 ORDER BY started ASC, schema ASC",
            &[&format!("scion_test_cap{pid}_%")],
        )
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    // The run's own first-open sweep may have eaten some of these rows
    // already (they are the oldest eligible ones while it runs); what
    // the test asserts is the cap and the order: at most fifty dropped
    // here, and whatever stayed is the newest tail.
    assert!(
        survivors.len() <= 5,
        "the sweep counts its own: at most fifty drop"
    );
    assert_eq!(
        survivors,
        made[55 - survivors.len()..],
        "the oldest registrations drop first, so what stays is the newest"
    );
    let (dropped2, _) = Backend::sweep_stale_test_schemas().unwrap();
    assert!(dropped2 <= 50, "the next sweep's cap holds too");
    let left: i64 = client
        .query_one(
            "SELECT count(*) FROM public.scion_test_schemas WHERE schema LIKE $1",
            &[&format!("scion_test_cap{pid}_%")],
        )
        .unwrap()
        .get(0);
    assert_eq!(left, 0, "the next sweep takes the rest");
    assert!(!schema_exists(&mut client, &made[54]));
}

/// A schema registered before the comment existed carries no
/// directory: the ten-minute rule covers it, so a database that
/// predates the comment still cleans.
#[test]
fn a_schema_the_comment_predates_drops_when_it_is_old_and_unused() {
    let _lock = locked();
    // The registry comes with the first open, and this test shapes it
    // before any store of its own exists: it opens one first, so the
    // table is there under any scheduling.
    let dir = private_tempdir();
    {
        let _store = Backend::open_test(dir.path()).unwrap();
    }
    dir.close().unwrap();
    let mut client = connect();
    let pid = std::process::id();
    let legacy = format!("scion_test_legacy{pid}");
    client
        .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {legacy}"))
        .unwrap();
    register(&mut client, &legacy, 86_400);
    assert_eq!(schema_comment(&mut client, &legacy), None);
    Backend::sweep_stale_test_schemas().unwrap();
    assert!(
        !schema_exists(&mut client, &legacy),
        "an old, unused, commentless test schema drops"
    );
}

/// The comment's directory is the one the open hashed: a path spelled
/// differently but naming the same directory gives the same schema and
/// the same recorded directory, so a reopen never orphans a schema.
#[test]
fn the_recorded_directory_is_the_one_the_name_hashes() {
    let dir = private_tempdir();
    let schema = Backend::test_schema(dir.path());
    {
        let _store = Backend::open_test(dir.path()).unwrap();
    }
    // The same directory through a symlinked ancestor names the same
    // schema on macOS, where /var is a symlink to /private/var: the
    // helper hashes the canonical form.
    let canonical = dir.path().canonicalize().unwrap();
    assert_eq!(Backend::test_schema(&canonical), schema);
    assert!(Path::new(dir.path()).exists());
    let _store = Backend::open_test(dir.path()).unwrap();
}

/// The drain reaches past the schemas it keeps: a head of kept rows
/// (directories that exist) must not hide the stale row behind it.
/// A read that stops at the oldest rows reads the same kept rows at
/// every call, drops nothing, and the pile-up behind the head never
/// drains. The kept rows here are far older than any other registry
/// row, so the oldest-rows window of a read without paging holds only
/// them - and this test fails on such a read.
#[test]
fn the_drain_reaches_past_the_schemas_it_keeps() {
    let _lock = locked();
    // The registry comes with the first open, and this test shapes it
    // before any store of its own exists: it opens one first, so the
    // table is there under any scheduling.
    let dir = private_tempdir();
    {
        let _store = Backend::open_test(dir.path()).unwrap();
    }
    dir.close().unwrap();
    let mut client = connect();
    let pid = std::process::id();
    // A failed earlier run of this test must not shape this one.
    client
        .batch_execute(&format!(
            "DELETE FROM public.scion_test_schemas WHERE schema LIKE 'scion_test_stall{pid}_%'"
        ))
        .unwrap();
    // One directory that exists: every kept schema's comment names it,
    // so the sweep keeps them whatever their age. Their registrations
    // are older than any other row the database can hold, so a read
    // without paging sees them first, all of them.
    let dir = private_tempdir();
    let recorded = dir.path().to_string_lossy().replace('\'', "''");
    let kept: Vec<String> = (0..20)
        .map(|i| {
            let name = format!("scion_test_stall{pid}_kept{i}");
            client
                .batch_execute(&format!(
                    "CREATE SCHEMA IF NOT EXISTS {name};
                     COMMENT ON SCHEMA {name} IS '{recorded}'"
                ))
                .unwrap();
            register(&mut client, &name, 86_400_000 * 10 - i);
            name
        })
        .collect();
    // The stale one: its directory never existed, and it registered
    // behind every kept row.
    let gone = format!("scion_test_stall{pid}_gone");
    client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {gone};
             COMMENT ON SCHEMA {gone} IS '/the/directory/is/gone'"
        ))
        .unwrap();
    register(&mut client, &gone, 86_400_000 * 10 - 100);
    Backend::sweep_stale_test_schemas().unwrap();
    assert!(
        !schema_exists(&mut client, &gone),
        "the drain drops the stale schema behind the kept head"
    );
    for name in &kept {
        assert!(
            schema_exists(&mut client, name),
            "a schema whose directory exists never drops: {name}"
        );
    }
}
