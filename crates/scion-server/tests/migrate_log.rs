//! A fresh database's first open logs one honest line: it says it
//! created the schema, and nothing about migrating.

use scion_server::test_support::backend::schema_version;
use scion_server::Backend;
use std::sync::{Arc, Mutex};

/// The version this build's Postgres schema climbs to.
const POSTGRES_TOP: u32 = schema_version::POSTGRES;

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

/// Installs the one global subscriber; the first test that runs wins
/// the `Once`, and every test reads the same buffer from its own mark.
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

/// The buffer's length at a moment: a test marks before it acts, and
/// reads only what came after.
fn mark() -> usize {
    LOG.0.lock().unwrap().len()
}

fn since(mark: usize) -> String {
    String::from_utf8(LOG.0.lock().unwrap()[mark..].to_vec()).unwrap()
}

/// The lines that name a message, with the values of `field` in order.
fn fields(log: &str, message: &str, field: &str) -> Vec<u32> {
    log.lines()
        .filter(|line| line.contains(message))
        .filter_map(|line| {
            let at = line.find(field)? + field.len();
            let rest = &line[at..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            digits.parse().ok()
        })
        .collect()
}

/// A fresh database says it created the schema - the same honest
/// one-liner the log always gave - and nothing about migrating.
#[test]
fn a_postgres_open_of_a_fresh_database_logs_its_creation() {
    init_log();
    let dir = scion_server::test_support::private_tempdir();
    let at = mark();
    let store = Backend::open_test(dir.path()).unwrap();
    drop(store);
    let log = since(at);

    assert_eq!(
        fields(&log, "the metadata schema was created", "to_version="),
        vec![POSTGRES_TOP],
        "a fresh database says what it created; the log was:\n{log}"
    );
    assert!(
        !log.contains("migrat"),
        "a fresh database's story is a creation, not a migration: {log}"
    );
}
