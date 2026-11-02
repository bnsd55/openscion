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
    // restore rides the advisory's. The session says 1 s, the holder
    // keeps the key for 3 s: the wait succeeds only if the
    // transaction's 60 s governed it, not the session's 1 s. A fold
    // that drops the 60 s statement and moves it into the advisory's
    // own statement appears to work on today's Postgres (the executor
    // happens to evaluate the set_config first), which rests on
    // evaluation order the SQL standard does not promise - so the 60 s
    // stays its own statement, and this proof fails if it stops being
    // one.
    hold_key_for(KEY_FOLD, 3);
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
        .and_then(|()| {
            waiter
                .query_opt(
                    "SELECT pg_advisory_xact_lock($1), \
                     set_config('lock_timeout', '1s', true)",
                    &[&KEY_FOLD],
                )
                .map(|_| ())
        });
    let elapsed = started.elapsed();
    assert!(
        taken.is_ok(),
        "the wait failed after {elapsed:?}: the transaction's 60 s guard \
         did not govern it: {:?}",
        taken.unwrap_err()
    );
    assert!(
        elapsed >= Duration::from_secs(2),
        "the wait took {elapsed:?}: the key was free, the proof held nothing"
    );
    waiter
        .batch_execute("COMMIT")
        .expect("the waiter's transaction ends");
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
