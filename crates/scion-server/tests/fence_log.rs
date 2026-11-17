//! A takeover says so in the log, exactly once: the fence's first trip
//! writes the one error line that tells the operator why every call
//! fails from here on. One test in one binary, so the count between the
//! mark and the read is the takeover's own and nothing else's.

use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use std::sync::{Arc, Mutex};

/// The log of the test binary, into this one buffer. One global
/// subscriber, set once: a per-thread default cannot serve a test whose
/// events another test's first open has already poisoned the callsite
/// cache for (a callsite's interest is computed once, against whatever
/// dispatch first evaluated it — a thread-local default that was not
/// there at that moment means the events stay off for everyone).
static LOG: std::sync::LazyLock<Captured> = std::sync::LazyLock::new(Captured::default);

impl Default for Captured {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }
}

#[derive(Clone)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn init_log() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::level_filters::LevelFilter::TRACE)
            .with_writer(Captured::clone(&*LOG))
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
    });
}

/// The buffer's length at a moment: the test marks before it acts, and
/// reads only what came after.
fn mark() -> usize {
    LOG.0.lock().unwrap().len()
}

fn since(at: usize) -> String {
    String::from_utf8(LOG.0.lock().unwrap()[at..].to_vec()).unwrap()
}

#[test]
fn a_takeover_logs_its_cause_once() {
    init_log();
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    store
        .write(|c| c.create_repo("before").map(|_| ()))
        .unwrap();
    assert!(!Backend::fenced(&store));
    // The takeover: kill the store's session, then take the lock before
    // the store's next statement can - a second server serving now.
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let schema = Backend::test_schema(dir.path());
    let key = Backend::test_server_lock(dir.path()).unwrap();
    let at = mark();
    let mut taker = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    taker
        .batch_execute(&format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = '{schema}' AND pid <> pg_backend_pid()"
        ))
        .unwrap();
    taker.query("SELECT pg_advisory_lock($1)", &[&key]).unwrap();
    // The first write goes through the reconnect and the lock check: it
    // answers that the lock is gone, and the fence trips.
    let error = store
        .write(|c| c.create_repo("after").map(|_| ()))
        .expect_err("a backend that lost its lock fails writes");
    assert!(
        matches!(error, scion_server::Error::FailedPrecondition(_)),
        "{error:?}"
    );
    assert!(Backend::fenced(&store), "the fence tripped");
    // The cause is in the log, exactly once: the one line the operator
    // reads to learn why every call started failing.
    let logged = since(at)
        .matches("lost the database to another server")
        .count();
    assert_eq!(
        logged,
        1,
        "the takeover logged its cause once, not {logged}:\n{}",
        since(at)
    );
    drop(taker);
}
