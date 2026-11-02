//! The write transaction's collector key: the wait for it runs under
//! the timeout that is in force before the statement starts, and the
//! restore that rides the advisory's own statement governs the
//! statements that follow. Postgres reads a GUC when a statement
//! starts, so a `set_config` in the same statement cannot guard that
//! statement's own lock wait — which is why the 60 s guard for the key
//! stays its own statement, and only the 5 s restore folds into the
//! advisory's statement. These proofs fail if that order is reversed.

use std::time::{Duration, Instant};

use postgres::{Client, NoTls};

/// Each test holds its own key: the tests run in parallel, and a key
/// two of them wait on would measure each other, not the timeout.
const KEY_GUARD: i64 = 0x5E57_0000_0000_0001;
const KEY_FOLD: i64 = 0x5E57_0000_0000_0002;
const KEY_RESTORE: i64 = 0x5E57_0000_0000_0003;
/// The 60 s guard the write transaction gives its key wait.
const KEY_GUARD_TIMEOUT: &str = "60s";

fn connect() -> Client {
    let url = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL names the test database");
    Client::connect(&url, NoTls).expect("the test database is up")
}

/// A held key on its own open transaction: the holder stays in scope
/// for about 1.2 s, so a waiter under a 1 s timeout fails fast and a
/// waiter under a longer one eventually gets the key. The holder is a
/// spawned thread because the key must stay taken while the test's own
/// connection waits.
fn hold_key_for(key: i64, secs: u64) {
    std::thread::spawn(move || {
        let mut client = connect();
        let mut tx = client
            .transaction()
            .expect("the holder's transaction begins");
        tx.execute("SELECT pg_advisory_xact_lock($1)", &[&key])
            .expect("the holder takes the key");
        std::thread::sleep(Duration::from_secs(secs));
        tx.commit().expect("the holder releases the key");
    });
}

/// Waits for `KEY` on `client` under `lock_timeout`, and answers what
/// happened: the error, or how long the wait took when the key came.
fn wait_for_key(client: &mut Client, key: i64, timeout: &str) -> Result<Duration, postgres::Error> {
    client
        .batch_execute(&format!("SET lock_timeout = '{timeout}'"))
        .expect("the session timeout is set");
    let started = Instant::now();
    match client.query_opt("SELECT pg_advisory_xact_lock($1)", &[&key]) {
        Ok(_) => Ok(started.elapsed()),
        Err(error) => Err(error),
    }
}

#[test]
fn the_key_wait_runs_under_the_timeout_set_before_it() {
    hold_key_for(KEY_GUARD, 3);
    std::thread::sleep(Duration::from_millis(500));
    let mut waiter = connect();
    // 1 s is shorter than the holder's 3 s: only if the 1 s timeout
    // governs the wait does it fail.
    let wait = wait_for_key(&mut waiter, KEY_GUARD, "1s");
    let error = wait.expect_err("the wait for a held key succeeded");
    let code = error.as_db_error().map_or("none", |db| db.code().code());
    assert_eq!(
        code, "55P03",
        "the wait failed with something other than the lock timeout: {error}"
    );
}

#[test]
fn the_key_wait_runs_under_the_transactions_60s_guard() {
    // The shipped shape: the 60 s guard is its own statement, and the
    // restore reads FROM the subquery that takes the key. The session
    // says 1 s, the restore says 5 s, the holder keeps the key for 7 s:
    // the wait succeeds only if the transaction's 60 s governed it. A
    // restore that ever ran before the lock (a target-list neighbor
    // happens to evaluate first on today's Postgres) would put the wait
    // under 5 s, and the wait would fail at 5 s - this proof fails
    // then. And a dropped 60 s statement fails it at the session's 1 s.
    hold_key_for(KEY_FOLD, 7);
    std::thread::sleep(Duration::from_millis(500));
    let mut waiter = connect();
    waiter
        .batch_execute("SET lock_timeout = '1s'")
        .expect("the session timeout is set");
    let started = Instant::now();
    let taken = waiter
        .batch_execute("BEGIN")
        .and_then(|()| {
            waiter.batch_execute(&format!("SET LOCAL lock_timeout = '{KEY_GUARD_TIMEOUT}'"))
        })
        .and_then(|()| waiter.query_opt(SHIPPED_SHAPE, &[&KEY_FOLD]).map(|_| ()));
    let elapsed = started.elapsed();
    assert!(
        taken.is_ok(),
        "the wait failed after {elapsed:?}: the transaction's 60 s guard \
         did not govern it: {:?}",
        taken.unwrap_err()
    );
    assert!(
        elapsed >= Duration::from_secs(5),
        "the wait took {elapsed:?}: a timeout under 5 s governed it, the \
         restore ran before the lock"
    );
    waiter
        .batch_execute("COMMIT")
        .expect("the waiter's transaction ends");
}

/// The statement the write transaction runs: the restore reads FROM the
/// subquery that takes the key, so the lock's wait cannot run under it.
const SHIPPED_SHAPE: &str = "SELECT set_config('lock_timeout', '5s', true) \
     FROM (SELECT pg_advisory_xact_lock($1) OFFSET 0) AS held";
/// The same statement with the key as a literal: EXPLAIN cannot infer a
/// parameter's type, and the plan's shape is what the proof reads.
const SHIPPED_SHAPE_PLAN: &str = "SELECT set_config('lock_timeout', '5s', true) \
     FROM (SELECT pg_advisory_xact_lock(6799890) OFFSET 0) AS held";

#[test]
fn the_plan_takes_the_key_before_the_restore_reads_it() {
    // The data dependency must survive the planner: OFFSET 0 keeps the
    // subquery unflattened, so the restore (the outer select's target
    // list) sits above the key's function scan. If a planner change
    // flattens them back into one target list, this proof fails and the
    // fold's order is no longer promised.
    let mut client = connect();
    // The key rides as a literal: EXPLAIN cannot infer a parameter's
    // type. The text plan prints root first, each level indented past
    // its parent, so the nesting reads straight off the indents.
    let rows = client
        .query(&format!("EXPLAIN {SHIPPED_SHAPE_PLAN}"), &[])
        .expect("the plan explains");
    let plan: Vec<String> = rows.iter().map(|row| row.get::<_, String>(0)).collect();
    // The planner folds both calls into Result nodes, so the shape the
    // proof reads is the subquery itself: the outer scan (which
    // evaluates the restore) survives unflattened, and its child (the
    // inner select that takes the key) runs first - a node's target
    // list evaluates after its child returns a row. A planner that
    // flattens the subquery away merges both into one node, and the
    // data dependency is gone.
    assert_eq!(
        plan.len(),
        2,
        "the plan is the outer scan and its child: {plan:?}"
    );
    assert!(
        plan[0].starts_with("Subquery Scan on held"),
        "the outer scan that evaluates the restore is the plan's top: {plan:?}"
    );
    let (child_at, child_indent) = {
        let at = plan[1].find("->").expect("the child marker: {plan:?}");
        (at, plan[1].len() - plan[1].trim_start().len())
    };
    assert!(
        child_at == 2 && child_indent > 0,
        "the inner select that takes the key is the outer's child: {plan:?}"
    );
}

#[test]
fn the_restore_in_the_keys_statement_governs_what_follows() {
    let mut client = connect();
    // The product runs the guard inside its write transaction; a local
    // set_config lasts to the transaction's end, so the proof lives in
    // one too.
    client
        .batch_execute("BEGIN")
        .expect("the transaction begins");
    // No other test holds this key, so the wait returns at once.
    client
        .query_one(
            "SELECT pg_advisory_xact_lock($1), \
             set_config('lock_timeout', '5s', true)",
            &[&KEY_RESTORE],
        )
        .expect("the key is taken and the restore rides it");
    // pg_advisory_xact_lock answers void: the row's existence is the
    // proof, the restore is what SHOW reads next.
    // The restore is in force for the statements after the advisory's:
    // the same statement's snapshot still ran under the old value.
    let shown: String = client
        .query_one("SHOW lock_timeout", &[])
        .expect("the GUC reads back")
        .get(0);
    assert_eq!(shown, "5s", "the restore did not govern the next statement");
    client
        .batch_execute("COMMIT")
        .expect("the transaction ends");
}
