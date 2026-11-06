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

use scion_server::test_support::private_tempdir;
use scion_server::{Backend, Error};

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
/// constant of the test schema preparation names `CLEAN_LOCK`, spelled
/// here so the
/// test holds exactly that lock.
const CLEAN_LOCK: i64 = 0x5343_494F_4E53_5445;

/// One open whose registration waits for the cleanup lock: a test
/// session holds `CLEAN_LOCK` just over the writer's 5 s lock clock while
/// an open runs. The test guards the preparer's absence of a lock
/// clock: the registration runs on the preparer's session, which
/// carries none, so the open must succeed - slow, not failed. A 55P03
/// here would name the statement that waited (#905).
#[test]
fn an_open_whose_cleanup_lock_is_held_over_the_writer_s_clock_succeeds() {
    let _lock = locked();
    let holder = std::thread::spawn(|| {
        let mut client = connect();
        client
            .execute("SELECT pg_advisory_lock($1)", &[&CLEAN_LOCK])
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5_500));
        client
            .execute("SELECT pg_advisory_unlock($1)", &[&CLEAN_LOCK])
            .unwrap();
    });
    std::thread::sleep(std::time::Duration::from_millis(200));
    let dir = private_tempdir();
    let started = std::time::Instant::now();
    let store = Backend::open_test(dir.path());
    let took = started.elapsed();
    if let Err(err) = &store {
        panic!("the open failed while the cleanup lock was held: {err}");
    }
    drop(store);
    holder.join().unwrap();
    assert!(
        took >= std::time::Duration::from_millis(5_000),
        "the open did not wait for the cleanup lock: {took:?}"
    );
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
    // Leftover registry rows from earlier runs - some older than this
    // test's rows - would eat the fifty slots and shift the survivors.
    // The backlog goes first: sweeps of fifty until one drops fewer,
    // so this run's rows are the oldest the measured sweep sees.
    for _ in 0..20 {
        let (dropped, _) = Backend::sweep_stale_test_schemas().unwrap();
        if dropped < 50 {
            break;
        }
    }
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

/// One open whose migration waits for its schema's migration lock: a
/// session holds the schema's own migration key until the open has
/// returned, so the writer's 5 s clock - not a fixed sleep - is the
/// race. The open of the held schema fails with the named error, the
/// migration's lock step and its statement; an open of another schema
/// during the same hold succeeds: the migrations touch only their own
/// schema, so two schemas never take turns (#905).
#[test]
fn an_open_waits_for_its_own_schemas_migration_lock_and_not_for_another_s() {
    let _lock = locked();
    let dir = private_tempdir();
    let held = Backend::test_migration_lock(dir.path()).unwrap();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let mut client = connect();
        client
            .execute("SELECT pg_advisory_lock($1)", &[&held])
            .unwrap();
        // The hold lasts until the open has returned: the writer's
        // clock is the race, not a sleep.
        let _ = released.recv();
        client
            .execute("SELECT pg_advisory_unlock($1)", &[&held])
            .unwrap();
    });
    std::thread::sleep(std::time::Duration::from_millis(200));
    let started = std::time::Instant::now();
    let Err(err) = Backend::open_test(dir.path()) else {
        panic!("the open succeeded while its schema's migration lock was held");
    };
    let took = started.elapsed();
    // Another schema's open is not the holder's business: its migrate
    // takes its own schema's key, inside the hold.
    let other = private_tempdir();
    let other_open = Backend::open_test(other.path());
    drop(release);
    holder.join().unwrap();
    assert!(
        format!("{err:?}").contains("the schema migration lock")
            && format!("{err:?}").contains("SELECT pg_advisory_lock($1)"),
        "the error names the migration's lock step: {err:?}"
    );
    assert!(
        took >= std::time::Duration::from_millis(5_000),
        "the open did not wait out its schema's clock: {took:?}"
    );
    assert!(
        other_open.is_ok(),
        "another schema's open succeeds during the hold: {:?}",
        other_open.err()
    );
}

/// An older build's migrator and this build still take turns: the older
/// build takes the database's bare migration key alone, exclusive, and
/// this build takes that key shared beside its own schema's key - so a
/// hold of the bare key keeps this build's open waiting on its shared
/// take, and the open answers after the hold lifts (#905).
#[test]
fn an_open_waits_while_an_older_builds_migration_holds_the_bare_key() {
    let _lock = locked();
    let held = Backend::test_bare_migration_lock();
    let (held_hi, held_lo) = (held >> 32, held & 0xFFFF_FFFF);
    let (release, released) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let mut client = connect();
        client
            .execute("SELECT pg_advisory_lock($1)", &[&held])
            .unwrap();
        // The hold lasts until the main thread releases it, about a
        // second after the open started: an older build's quick
        // migration window, well under the writer's clock, so no other
        // open in the window reaches it.
        let _ = released.recv();
        client
            .execute("SELECT pg_advisory_unlock($1)", &[&held])
            .unwrap();
    });
    // The hold is seen in pg_locks, not assumed from a sleep: the open
    // starts only once the older build's turn is real. The holder may
    // still be connecting, so the poll waits for the row; a poll that
    // gives up names whatever holds the key instead.
    let mut holding = Vec::new();
    let poll = std::time::Instant::now();
    while poll.elapsed() < std::time::Duration::from_secs(10) {
        holding = connect()
            .query(
                &format!(
                    "SELECT 1 FROM pg_locks WHERE locktype = 'advisory'
                     AND classid = {held_hi} AND objid = {held_lo}
                     AND mode = 'ExclusiveLock' AND granted"
                ),
                &[],
            )
            .unwrap();
        if !holding.is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    if holding.is_empty() {
        let holders = connect()
            .query(
                &format!(
                    "SELECT l.pid, l.mode, l.granted, a.application_name, a.query
                     FROM pg_locks l LEFT JOIN pg_stat_activity a ON a.pid = l.pid
                     WHERE l.locktype = 'advisory'
                       AND l.classid = {held_hi} AND l.objid = {held_lo}"
                ),
                &[],
            )
            .unwrap();
        for row in &holders {
            eprintln!(
                "DEBUG the bare key: pid {} mode {} granted {} app {} query {}",
                row.get::<_, i32>(0),
                row.get::<_, String>(1),
                row.get::<_, bool>(2),
                row.get::<_, Option<String>>(3).unwrap_or_default(),
                row.get::<_, String>(4),
            );
        }
    }
    assert_eq!(holding.len(), 1, "the older build's turn is real");
    let dir = private_tempdir();
    let started = std::time::Instant::now();
    let opened = std::thread::spawn(move || Backend::open_test(dir.path()));
    std::thread::sleep(std::time::Duration::from_millis(1_000));
    drop(release);
    let opened = opened.join().unwrap();
    holder.join().unwrap();
    let took = started.elapsed();
    assert!(
        opened.is_ok(),
        "the open answers after the older build's migration: {:?}",
        opened.err()
    );
    assert!(
        took >= std::time::Duration::from_millis(1_000),
        "the open waited inside the hold to its release: {took:?}"
    );
}

/// The two key spaces never meet for the same schema: the one-server
/// lock a server holds for its whole life and the migration lock an
/// open takes mix the schema's hash differently, and a debug build's
/// overflow panic cannot happen on an XOR. Pinned over a thousand
/// names (#905).
#[test]
fn the_migration_key_never_meets_the_one_server_key() {
    for i in 0..1_000u64 {
        let schema = format!("scion_test_sweep_{i}");
        let migration = Backend::test_migration_lock_for(&schema);
        let one_server = Backend::test_server_lock_for(&schema);
        assert_ne!(
            migration, one_server,
            "the migration key met the one-server key for {schema}"
        );
        assert_ne!(
            migration,
            Backend::test_bare_migration_lock(),
            "the migration key met the database's bare key for {schema}"
        );
    }
}

/// The shared take goes back with the open: a writer that ran its
/// migrations keeps no advisory lock on the database's bare migration
/// key, or an older build's exclusive take would wait for as long as
/// any new server runs (#905).
#[test]
fn an_open_keeps_no_lock_on_the_bare_migration_key() {
    let _lock = locked();
    let dir = private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    // The writer's session is the one holding the store's one-server
    // key for its whole life.
    let server_key = Backend::test_server_lock(dir.path()).unwrap();
    let bare = Backend::test_bare_migration_lock();
    // Postgres splits a one-argument advisory key into the two ints
    // pg_locks shows: the high half in classid, the low in objid.
    let (server_hi, server_lo) = (server_key >> 32, server_key & 0xFFFF_FFFF);
    let (bare_hi, bare_lo) = (bare >> 32, bare & 0xFFFF_FFFF);
    let rows = connect()
        .query(
            &format!(
                "SELECT l.pid FROM pg_locks l
                 WHERE l.locktype = 'advisory' AND l.classid = {server_hi}
                   AND l.objid = {server_lo} AND l.granted"
            ),
            &[],
        )
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the writer's session is on the one-server key"
    );
    let pid: i32 = rows[0].get(0);
    let leaked = connect()
        .query(
            &format!(
                "SELECT l.mode FROM pg_locks l
                 WHERE l.locktype = 'advisory' AND l.classid = {bare_hi}
                   AND l.objid = {bare_lo}
                   AND l.pid = {pid} AND l.granted"
            ),
            &[],
        )
        .unwrap();
    assert!(
        leaked.is_empty(),
        "the writer keeps no lock on the bare migration key, holds: {leaked:?}"
    );
    drop(store);
}

/// The lane key's holder row, polled until it appears; a poll that
/// gives up prints every granted advisory row beside the split the
/// confirm computed, so a red names the two numbers.
fn lane_key_holder(schema: &str) -> Vec<postgres::Row> {
    let confirm = "SELECT 1 FROM pg_locks WHERE locktype = 'advisory'
                 AND classid = ((hashtext($1::text || ':gc')::bigint >> 32)
                                & 4294967295)
                 AND objid = (hashtext($1::text || ':gc') & 4294967295)
                 AND granted";
    let mut held = Vec::new();
    for _ in 0..250 {
        held = connect().query(confirm, &[&schema]).unwrap();
        if !held.is_empty() {
            return held;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let seen = connect()
        .query(
            "SELECT classid, objid, objsubid, mode FROM pg_locks
             WHERE locktype = 'advisory' AND granted",
            &[],
        )
        .unwrap();
    for row in &seen {
        eprintln!(
            "DEBUG the lane key's holder: classid {} objid {} objsubid {} mode {}",
            row.get::<_, u32>(0),
            row.get::<_, u32>(1),
            row.get::<_, u32>(2),
            row.get::<_, String>(3),
        );
    }
    let want = connect()
        .query(
            "SELECT ((hashtext($1::text || ':gc')::bigint >> 32) & 4294967295),
                    (hashtext($1::text || ':gc') & 4294967295)",
            &[&schema],
        )
        .unwrap();
    eprintln!(
        "DEBUG the confirm wants: classid {} objid {}",
        want[0].get::<_, u32>(0),
        want[0].get::<_, u32>(1)
    );
    held
}

/// A write that loses the lane key answers the same busy as before the
/// key rode the open batch, and the transaction the batch opened is
/// rolled back: the writer's session holds no transaction when the
/// error lands (#782).
#[test]
fn a_write_that_loses_the_lane_key_answers_busy_and_leaves_no_transaction_open() {
    let _lock = locked();
    let dir = private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    // The lane key of the store's schema, held in a transaction past
    // the writer's 5 s clock, as a rival writer would.
    let schema = Backend::test_schema(dir.path());
    let escaped = schema.replace('\'', "''");
    let (release, released) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let mut client = connect();
        client
            .batch_execute(&format!(
                "BEGIN; SELECT pg_advisory_xact_lock(hashtext('{}' || ':gc'));",
                escaped
            ))
            .unwrap();
        let _ = released.recv();
        client
            .batch_execute("ROLLBACK")
            .expect("the holder's transaction ends");
    });
    // The hold is seen in pg_locks before the write runs: hashtext is a
    // signed 32-bit value, and its bigint promotion splits into the
    // lock table's two ints, so the confirm computes the split it
    // expects and a poll that gives up prints the row beside it.
    let held = lane_key_holder(&schema);
    use scion_server::test_support::meta::Meta as _;
    assert_eq!(held.len(), 1, "the rival writer holds the lane key");
    let error = store
        .write(|_c| Ok(()))
        .expect_err("the write loses the lane key");
    drop(release);
    holder.join().unwrap();
    assert!(
        matches!(
            &error,
            Error::Storage(storage)
                if storage.class() == scion_server::StorageClass::Busy
        ),
        "the write answers busy: {error:?}"
    );
    // The batch's ROLLBACK ran: the writer's session - the one on the
    // one-server key - holds no transaction.
    let server_key = Backend::test_server_lock(dir.path()).unwrap();
    let rows = connect()
        .query(
            &format!(
                "SELECT a.state FROM pg_locks l
                 JOIN pg_stat_activity a ON a.pid = l.pid
                 WHERE l.locktype = 'advisory'
                   AND l.classid = {}
                   AND l.objid = {}
                   AND l.granted",
                server_key >> 32,
                server_key & 0xFFFF_FFFF
            ),
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 1, "the writer's session is found");
    let state: String = rows[0].get(0);
    assert_ne!(
        state, "idle in transaction",
        "the failed write left no transaction open"
    );
}
