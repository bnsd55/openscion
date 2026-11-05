//! The `preflight` command: every check a first boot needs, read-only,
//! one line each, the same words `serve`'s own refusals use.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use scion_proto::runtime::auth::{checked_read_error, read_secret_file, read_shared_file};

use crate::error::Result;

/// The inputs of a `preflight` run: the flags `serve` takes, as the
/// shell hands them over. The token, TLS and ticket flags are not
/// here: their rules are serve's own, and the binary runs them through
/// `serve_flags` before any check prints.
pub struct PreflightOptions {
    /// The data directory the checks read, and whose filesystem the
    /// disk check gauges.
    pub data: PathBuf,
    /// The file holding the Postgres URL, read like any secret; the
    /// URL may ride `SCION_POSTGRES_URL` instead.
    pub postgres_url_file: Option<PathBuf>,
    /// The CA that verifies the database's certificate, when the URL
    /// names a host that is not local.
    pub postgres_ca: Option<PathBuf>,
    /// The address the gRPC listener would bind.
    pub listen: SocketAddr,
    /// The address the metrics listener would bind, when given.
    pub metrics_listen: Option<SocketAddr>,
}

/// What a run answers: one line per check, and the shell's exit code -
/// 0 all pass, 1 one warns, 2 one fails.
#[must_use = "a preflight run answers with its report: print the lines, exit with the code"]
pub struct Report {
    /// One line per check, `ok`, `warn` or `fail`, in check order.
    pub lines: Vec<String>,
    /// The shell's exit code: 0, 1 or 2.
    pub code: i32,
}

/// One check's answer: the line preflight prints, and the level the
/// exit code reads.
enum Answer {
    Ok(String),
    Warn(String),
    Fail(String),
}

/// Every check, in the order an operator fixes them: the directory,
/// the database, the schema, the store's pairing, the address, the
/// host. The flags' rules are serve's own (`serve_flags`, in the
/// binary); nothing here copies them. Nothing is written: no marker,
/// no schema move, no lock.
pub fn run(opts: &PreflightOptions) -> Report {
    let mut answers: Vec<(&'static str, Answer)> = Vec::new();

    // The data directory: the open's own refusals, the same words.
    answers.push((
        "data directory",
        match crate::backend::check_dir(&opts.data) {
            Ok(()) => Answer::Ok(format!(
                "{} is a data directory of the right kind",
                opts.data.display()
            )),
            Err(error) => Answer::Fail(error.to_string()),
        },
    ));

    // The database: one raw connection, no setup, no lock of any kind.
    // The URL's rules are the pool's own connect rules.
    let mut db = match postgres_url(opts)
        .and_then(|url| crate::pg::preflight_client(&url, opts.postgres_ca.as_deref()))
    {
        Err(error) => {
            answers.push((
                "database",
                Answer::Fail(format!("the database does not answer: {error}")),
            ));
            None
        }
        Ok(client) => Some(client),
    };

    let url = postgres_url(opts);
    if let Some(db) = db.as_mut() {
        database_checks(&url, opts, db, &mut answers);
    }

    // The listen address: bound and dropped, the way `serve` binds it.
    answers.push(("listen address", listen_free(opts.listen)));
    if let Some(metrics_listen) = opts.metrics_listen {
        answers.push(("metrics listen address", listen_free(metrics_listen)));
    }

    // The host's memory: the same read the start-up check makes. The
    // floor stays a warning, as it is in `serve`.
    answers.push(("memory floor", memory_floor()));

    // The data directory's filesystem: free bytes against the margin
    // the store's own disk evidence names, and the inodes free.
    answers.push(("disk and inodes", disk_and_inodes(&opts.data)));

    finish(answers)
}

/// The database's checks: the build's version, the schema's state
/// through `check_schema`, the store's pairing, and a live server on
/// the lock. A database that does not answer stops here; the later
/// checks still print.
fn database_checks(
    url: &Result<String, crate::Error>,
    opts: &PreflightOptions,
    db: &mut postgres::Client,
    answers: &mut Vec<(&'static str, Answer)>,
) {
    let data = &opts.data;
    let version = match db
        .query_one("SHOW server_version_num", &[])
        .map(|row| row.get::<_, String>(0).parse().unwrap_or(0))
    {
        Ok(num) if (16..=18).contains(&(num / 10000)) => {
            let (major, minor) = (num / 10000, num % 10000 / 100);
            Answer::Ok(format!("PostgreSQL {major}.{minor} is supported"))
        }
        Ok(num) => Answer::Fail(format!(
            "this build supports PostgreSQL 16 to 18; the database runs {}",
            num / 10000
        )),
        Err(error) => Answer::Fail(format!("cannot read the server version: {error}")),
    };
    answers.push(("Postgres version", version));

    // The schema, through `check_schema`'s own decision: a fresh
    // database is fine (serve creates it), behind names the migrate
    // fix, newer refuses - the open's own words.
    let check = match url {
        Ok(url) => crate::pg::check_schema(url, opts.postgres_ca.as_deref()),
        Err(error) => Err(crate::Error::InvalidArgument(error.to_string())),
    };
    let check = match check {
        Ok(check) => check,
        Err(error) => {
            answers.push(("schema version", Answer::Fail(error.to_string())));
            answers.push((
                "store pairing",
                Answer::Fail("the schema's state does not read".to_owned()),
            ));
            answers.push(("one-server lock", live_server(db)));
            return;
        }
    };
    if check.db_version() == 0 {
        answers.push((
            "schema version",
            Answer::Ok("a fresh database; serve creates the schema".to_owned()),
        ));
        answers.push((
            "store pairing",
            Answer::Ok("a first boot: the open mints the marker and the store together".to_owned()),
        ));
    } else {
        answers.push((
            "schema version",
            if check.is_pending() {
                Answer::Fail(format!(
                    "the schema stands at version {}, this build ships {}; \
                     run `scion-server migrate` first, or pass --migrate to serve",
                    check.db_version(),
                    check.build_version()
                ))
            } else {
                Answer::Ok(format!(
                    "the schema stands at version {}, this build's",
                    check.db_version()
                ))
            },
        ));
        // The store's pairing: the open's own decision, on plain
        // selects the read-only session allows.
        answers.push(("store pairing", store_pairing(data, db)));
    }

    // A live server on this store: the one-server lock's key, read
    // from `pg_locks` - asked, never taken. It stays a warning, as the
    // approved design says; the open's own lock is the enforcement.
    answers.push(("one-server lock", live_server(db)));
}

/// The exit code and the lines, from the answers in check order.
fn finish(answers: Vec<(&'static str, Answer)>) -> Report {
    let mut code = 0;
    let mut lines = Vec::new();
    for (name, answer) in answers {
        let (level, text) = match answer {
            Answer::Ok(text) => ("ok", text),
            Answer::Warn(text) => {
                code = code.max(1);
                ("warn", text)
            }
            Answer::Fail(text) => {
                code = 2;
                ("fail", text)
            }
        };
        lines.push(format!("{level} {name}: {text}"));
    }
    Report { lines, code }
}

/// The Postgres URL, as `serve` reads it: a checked file named by the
/// flag, or `SCION_POSTGRES_URL`.
fn postgres_url(opts: &PreflightOptions) -> Result<String> {
    if let Some(file) = &opts.postgres_url_file {
        return checked_text(file, true);
    }
    std::env::var("SCION_POSTGRES_URL")
        .ok()
        .filter(|url| !url.is_empty())
        .ok_or_else(|| {
            crate::Error::InvalidArgument(
                "no Postgres URL: give --postgres-url-file or SCION_POSTGRES_URL".into(),
            )
        })
}

/// A file's trimmed text, read the way `serve` reads its secrets: the
/// owner alone, no symlink, the directories above it checked.
fn checked_text(file: &Path, secret: bool) -> Result<String> {
    let bytes = if secret {
        read_secret_file(file)
    } else {
        read_shared_file(file)
    }
    .map_err(|why| crate::Error::InvalidArgument(checked_read_error(file, why).to_string()))?;
    String::from_utf8(bytes)
        .map(|text| text.trim().to_owned())
        .map_err(|_| crate::Error::InvalidArgument(format!("{} is not UTF-8", file.display())))
}

fn store_pairing(data: &Path, db: &mut postgres::Client) -> Answer {
    // Any query error is a fail: "cannot read" is not "no store".
    let marker = crate::backend::store_uuid_of(data);
    let held: Option<[u8; 16]> =
        match db.query_opt("SELECT uuid FROM store_uuid WHERE one = 1", &[]) {
            Ok(row) => row.map(|row| {
                let bytes: Vec<u8> = row.get(0);
                bytes.try_into().expect("the store's uuid is 16 bytes")
            }),
            Err(error) => {
                return Answer::Fail(format!("cannot read the database's store row: {error}"));
            }
        };
    // The repos probe decides the no-marker case, as the open's own
    // pairing does: a schema that holds repos was served.
    let served = held.is_some()
        && !db
            .query("SELECT 1 FROM repos LIMIT 1", &[])
            .map_err(|error| Answer::Fail(format!("cannot read the schema's repos: {error}")))
            .unwrap_or_default()
            .is_empty();
    // The open's own decision, in the open's own words.
    match crate::pg::pairing_decision(marker, held, served) {
        crate::pg::Pairing::Agree => {
            Answer::Ok("the data directory and the database name one store".to_owned())
        }
        crate::pg::Pairing::AdoptCut => Answer::Warn(
            "the database names a store the directory carries no marker of, and \
             the schema holds no repos: the first open adopts it and writes \
             the marker"
                .to_owned(),
        ),
        crate::pg::Pairing::Fresh if marker.is_some() => Answer::Ok(
            "the database holds no store yet; the first open takes the \
             directory's marker as its store"
                .to_owned(),
        ),
        crate::pg::Pairing::Fresh => {
            Answer::Ok("a first boot: the open mints the marker and the store".to_owned())
        }
        crate::pg::Pairing::Refuse(error) => Answer::Fail(error.to_string()),
    }
}

/// A live server on this store, read from `pg_locks`: the advisory
/// key the one-server lock takes, asked, never taken.
fn live_server(db: &mut postgres::Client) -> Answer {
    let schema: Option<String> = match db
        .query_one("SELECT current_schema()::text", &[])
        .map(|row| row.get::<_, Option<String>>(0))
    {
        Ok(schema) => schema,
        Err(error) => return Answer::Fail(format!("cannot read the schema: {error}")),
    };
    let Some(schema) = schema else {
        return Answer::Warn(
            "the URL's search_path names no existing schema; create it first".to_owned(),
        );
    };
    let holders = db
        .query_opt(
            // A 64-bit advisory key sits in pg_locks as two 32-bit
            // halves: classid carries the high half, objid the low.
            "SELECT count(*) FROM pg_locks
             WHERE locktype = 'advisory'
               AND classid = (($1::bigint >> 32) & 4294967295)::oid
               AND objid = ($1::bigint & 4294967295)::oid
               AND database = (SELECT oid FROM pg_database
                               WHERE datname = current_database())
               AND granted",
            &[&crate::pg::schema_lock_key(&schema)],
        )
        .map(|row| row.map_or(0, |row| row.get::<_, i64>(0)));
    match holders {
        Ok(0) => Answer::Ok("no server holds this store".to_owned()),
        Ok(n) => Answer::Warn(format!(
            "{n} server(s) hold this store; the listen check below refuses \
             this build's own address while one runs"
        )),
        Err(error) => Answer::Fail(format!("cannot read pg_locks: {error}")),
    }
}

/// The listen address, bound and dropped: `serve` binds the same way.
fn listen_free(listen: SocketAddr) -> Answer {
    match std::net::TcpListener::bind(listen) {
        Ok(listener) => {
            drop(listener);
            Answer::Ok(format!("{listen} is free"))
        }
        Err(error) => Answer::Fail(format!(
            "cannot listen on {listen}: {error}; free the port or pass --listen"
        )),
    }
}

/// The host's memory against the floor: a warning, as in `serve` - the
/// floor never refuses.
fn memory_floor() -> Answer {
    match crate::floor::host_memory(&crate::floor::production_paths()) {
        Some((bytes, source)) if bytes < crate::floor::FLOOR => Answer::Warn(format!(
            "the host has {} GiB ({source}), below the server's floor of \
             {} GiB: the page cache gives way first, and under load the \
             process can reach the OOM killer",
            bytes / (1 << 30),
            crate::floor::FLOOR / (1 << 30)
        )),
        Some((bytes, source)) => {
            Answer::Ok(format!("the host has {} GiB ({source})", bytes / (1 << 30)))
        }
        None => Answer::Warn("cannot read the host's memory limit".to_owned()),
    }
}

/// # SHORTCUT: 12 MiB, the `disk_full` run's own margin, as a warn; a
/// real disk budget needs a doc row naming the store's spend per blob
/// and per land. The `disk_full` wet run leaves 12 MiB for the open,
/// the repo and the first lands; a filesystem below it cannot boot a
/// store.
const DISK_MARGIN: u64 = 12 << 20;

/// The data directory's filesystem: free bytes against the margin the
/// store's own disk evidence names, and the inodes free. Inodes have no
/// threshold: the budget is one file per blob, and the blob count is
/// the operator's, so the number is the answer.
fn disk_and_inodes(data: &Path) -> Answer {
    let bytes = match crate::store::free_bytes(data) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Answer::Fail(format!(
                "cannot read the filesystem of {}: {error}",
                data.display()
            ));
        }
    };
    let inodes = match crate::store::free_inodes(data) {
        Ok(inodes) => inodes,
        Err(error) => {
            return Answer::Fail(format!(
                "cannot read the filesystem of {}: {error}",
                data.display()
            ));
        }
    };
    if bytes < DISK_MARGIN {
        return Answer::Warn(format!(
            "{} free on {}, below the {} MiB the store's open and first \
             lands spend: grow the filesystem before the first boot",
            human_bytes(bytes),
            data.display(),
            DISK_MARGIN / (1 << 20)
        ));
    }
    Answer::Ok(format!(
        "{} free and {inodes} inodes on {}",
        human_bytes(bytes),
        data.display()
    ))
}

/// Bytes as the unit an operator reads at a glance.
fn human_bytes(bytes: u64) -> String {
    if bytes >= 1 << 30 {
        format!("{} GiB", bytes / (1 << 30))
    } else if bytes >= 1 << 20 {
        format!("{} MiB", bytes / (1 << 20))
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::{Mutex, MutexGuard};

    /// The memory override is process-global, so the tests that use it
    /// take turns, as the statement log's lock does.
    static MEMORY_LOCK: Mutex<()> = Mutex::new(());

    /// A temp directory the run owns outright: mode 0700, whatever the
    /// umask. The data-directory check refuses one group or others can
    /// read.
    fn private_tempdir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let mut mode = std::fs::metadata(dir.path()).unwrap().permissions();
        mode.set_mode(0o700);
        std::fs::set_permissions(dir.path(), mode).unwrap();
        dir
    }

    /// A URL file the preflight reads: mode 0600, as a secret wants.
    fn url_file(dir: &Path, url: &str) -> PathBuf {
        let file = dir.join("postgres-url");
        std::fs::write(&file, url).unwrap();
        let mut mode = std::fs::metadata(&file).unwrap().permissions();
        mode.set_mode(0o600);
        std::fs::set_permissions(&file, mode).unwrap();
        file
    }

    /// A store ready for a first boot: the test database, a marker, a
    /// schema at this build's version, no server holding the lock.
    fn ready_store() -> (tempfile::TempDir, PathBuf) {
        let dir = private_tempdir();
        let url = crate::Backend::test_postgres_url(dir.path()).unwrap();
        let store = crate::Backend::open_test(dir.path()).unwrap();
        drop(store);
        let file = url_file(dir.path(), &url);
        (dir, file)
    }

    /// The preflight of `data` against `url_file`, as the shell sees
    /// it: the exit code and the lines.
    fn run_cli(data: &Path, url_file: Option<&PathBuf>) -> Report {
        super::run(&PreflightOptions {
            data: data.to_path_buf(),
            postgres_url_file: url_file.cloned(),
            postgres_ca: None,
            listen: "127.0.0.1:0".parse().unwrap(),
            metrics_listen: None,
        })
    }

    fn line<'a>(report: &'a Report, name: &str) -> &'a str {
        report
            .lines
            .iter()
            .find(|line| line.contains(name))
            .unwrap_or_else(|| panic!("no {name} line in\n{:#?}", report.lines))
    }

    /// The host's memory read points at a meminfo with room to spare:
    /// the floor's answer must not depend on the runner's own memory.
    /// The override is process-global: the tests that touch it hold
    /// [`MEMORY_LOCK`], and the drop resets it under the same lock.
    struct MemoryOverride(#[allow(dead_code)] tempfile::TempDir);
    impl MemoryOverride {
        fn set(lock: &MutexGuard<'_, ()>) -> (Self, ()) {
            let _ = lock;
            let dir = private_tempdir();
            std::fs::write(dir.path().join("meminfo"), "MemTotal:       67108864 kB\n").unwrap();
            crate::floor::test_set_paths(Some(crate::floor::Paths {
                self_cgroup: dir.path().join("no-cgroup"),
                cgroup_root: dir.path().join("no-root"),
                meminfo: dir.path().join("meminfo"),
            }));
            (Self(dir), ())
        }

        /// The override pointed at a small host: a floor warning.
        fn set_small(lock: &MutexGuard<'_, ()>) -> (Self, ()) {
            let _ = lock;
            let dir = private_tempdir();
            std::fs::write(dir.path().join("meminfo"), "MemTotal:         1048576 kB\n").unwrap();
            crate::floor::test_set_paths(Some(crate::floor::Paths {
                self_cgroup: dir.path().join("no-cgroup"),
                cgroup_root: dir.path().join("no-root"),
                meminfo: dir.path().join("meminfo"),
            }));
            (Self(dir), ())
        }
    }
    impl Drop for MemoryOverride {
        fn drop(&mut self) {
            crate::floor::test_set_paths(None);
        }
    }

    /// The files of `dir`, every file's bytes, recursively, sorted:
    /// the bytes a run must not change.
    fn file_bytes(dir: &Path) -> Vec<(String, u64)> {
        fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, u64)>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let entry = entry.unwrap();
                let name = format!("{prefix}{}", entry.file_name().to_string_lossy());
                let kind = entry.file_type().unwrap();
                if kind.is_dir() {
                    walk(&entry.path(), &format!("{name}/"), out);
                } else {
                    out.push((name, entry.metadata().unwrap().len()));
                }
            }
        }
        let mut out = Vec::new();
        walk(dir, "", &mut out);
        out.sort();
        out
    }

    /// Row counts of every table in the store's schema.
    fn table_rows(url: &str) -> Vec<(String, i64)> {
        let mut raw = postgres::Client::connect(url, postgres::NoTls).unwrap();
        let schema: String = raw
            .query_one("SELECT current_schema()::text", &[])
            .unwrap()
            .get(0);
        let mut out = Vec::new();
        for row in raw
            .query(
                "SELECT c.relname FROM pg_class c
                 WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
                   AND c.relkind = 'r'
                 ORDER BY c.relname",
                &[&schema],
            )
            .unwrap()
        {
            let name: String = row.get(0);
            let count = raw
                .query_one(&format!("SELECT count(*) FROM {name}"), &[])
                .unwrap()
                .get::<_, i64>(0);
            out.push((name, count));
        }
        out
    }

    /// A directory that holds a sqlite store fails with the open's own
    /// refusal, and the exit code is 2.
    #[test]
    fn a_sqlite_directory_fails_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let dir = private_tempdir();
        std::fs::write(dir.path().join("scion.db"), b"sqlite bytes\n").unwrap();
        let url = crate::Backend::test_postgres_url(dir.path()).unwrap();
        let url = url_file(dir.path(), &url);
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 2);
        assert!(
            line(&report, "data directory").starts_with("fail"),
            "the sqlite directory fails:\n{:#?}",
            report.lines
        );
    }

    /// A ready store passes everything, and the run writes nothing:
    /// every table's rows and every file's bytes stand unchanged.
    #[test]
    fn a_ready_store_passes_preflight_and_writes_nothing() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        let url_text = std::fs::read_to_string(&url).unwrap();
        let tables_before = table_rows(&url_text);
        let files_before = file_bytes(dir.path());
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(
            report.code, 0,
            "a ready store passes everything:\n{:#?}",
            report.lines
        );
        for name in [
            "ok data directory",
            "ok Postgres version",
            "ok schema version",
            "ok store pairing",
            "ok one-server lock",
            "ok listen address",
            "ok memory floor",
            "ok disk and inodes",
        ] {
            assert!(
                report.lines.iter().any(|l| l.starts_with(name)),
                "no {name} in\n{:#?}",
                report.lines
            );
        }
        let tables_after = table_rows(&url_text);
        assert_eq!(tables_before, tables_after, "the rows stand unchanged");
        let files_after: Vec<_> = file_bytes(dir.path());
        assert_eq!(files_before, files_after, "the files stand unchanged");
    }

    /// A database that does not answer fails, and the later checks
    /// still print their lines.
    #[test]
    fn a_database_that_does_not_answer_fails_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let dir = private_tempdir();
        let url = url_file(dir.path(), "postgres://127.0.0.1:1/scion");
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 2);
        assert!(
            line(&report, "database").starts_with("fail"),
            "the dead database fails:\n{:#?}",
            report.lines
        );
        assert!(
            line(&report, "listen address").starts_with("ok"),
            "the later checks still run:\n{:#?}",
            report.lines
        );
    }

    /// A schema behind this build fails with the fix named: migrate.
    #[test]
    fn a_schema_behind_the_build_fails_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        let mut raw = postgres::Client::connect(
            std::fs::read_to_string(&url).unwrap().as_str(),
            postgres::NoTls,
        )
        .unwrap();
        // Behind, not fresh: an empty ledger reads as a database serve
        // would create, so the test stands the ledger at an old
        // version instead.
        raw.execute("DELETE FROM schema_migrations", &[]).unwrap();
        raw.execute(
            "INSERT INTO schema_migrations (version, name) VALUES (3, 'an old step')",
            &[],
        )
        .unwrap();
        drop(raw);
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 2);
        let schema = line(&report, "schema version");
        assert!(
            schema.starts_with("fail") && schema.contains("migrate"),
            "the behind schema fails with the fix:\n{schema}"
        );
    }

    /// A schema newer than this build fails: the open's own refusal,
    /// run the build that made the database.
    #[test]
    fn a_schema_newer_than_the_build_fails_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        let mut raw = postgres::Client::connect(
            std::fs::read_to_string(&url).unwrap().as_str(),
            postgres::NoTls,
        )
        .unwrap();
        raw.execute(
            "INSERT INTO schema_migrations (version, name) VALUES (99, 'a future step')",
            &[],
        )
        .unwrap();
        drop(raw);
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 2);
        let schema = line(&report, "schema version");
        assert!(
            schema.starts_with("fail") && schema.contains("newer"),
            "the newer schema fails:\n{schema}"
        );
    }

    /// A data directory group or others can read fails: the open's own
    /// rule, so a preflight pass cannot promise what the open refuses.
    #[test]
    fn a_shared_data_directory_fails_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        let mut mode = std::fs::metadata(dir.path()).unwrap().permissions();
        mode.set_mode(0o777);
        std::fs::set_permissions(dir.path(), mode).unwrap();
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 2);
        assert!(
            line(&report, "data directory").starts_with("fail"),
            "the shared directory fails:\n{:#?}",
            report.lines
        );
    }

    /// A symlinked data directory fails: the target and its mode can
    /// change behind the checks.
    #[test]
    fn a_symlinked_data_directory_fails_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        let link = private_tempdir();
        std::os::unix::fs::symlink(dir.path(), link.path().join("store")).unwrap();
        let report = run_cli(&link.path().join("store"), Some(&url));
        assert_eq!(report.code, 2);
        assert!(
            line(&report, "data directory").starts_with("fail"),
            "the symlink fails:\n{:#?}",
            report.lines
        );
    }

    /// A damaged marker fails with the open's own refusal.
    #[test]
    fn a_damaged_marker_fails_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        std::fs::write(dir.path().join("backend"), b"not a marker\n").unwrap();
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 2);
        assert!(
            line(&report, "data directory").starts_with("fail"),
            "the damaged marker fails:\n{:#?}",
            report.lines
        );
    }

    /// A directory whose marker names another store than the
    /// database's row fails: one was restored over the other.
    #[test]
    fn a_pairing_mismatch_fails_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        let mut raw = postgres::Client::connect(
            std::fs::read_to_string(&url).unwrap().as_str(),
            postgres::NoTls,
        )
        .unwrap();
        raw.execute(
            "UPDATE store_uuid SET uuid = $1 WHERE one = 1",
            &[&[9u8; 16].to_vec()],
        )
        .unwrap();
        drop(raw);
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 2);
        let pairing = line(&report, "store pairing");
        assert!(
            pairing.starts_with("fail") && pairing.contains("restored over"),
            "the mismatch fails:\n{pairing}"
        );
    }

    /// A database that cannot read its store row fails: "cannot read"
    /// is not "no store".
    #[test]
    fn a_pairing_error_fails_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        let mut raw = postgres::Client::connect(
            std::fs::read_to_string(&url).unwrap().as_str(),
            postgres::NoTls,
        )
        .unwrap();
        raw.execute("DROP TABLE store_uuid", &[]).unwrap();
        drop(raw);
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 2);
        let pairing = line(&report, "store pairing");
        assert!(
            pairing.starts_with("fail") && pairing.contains("cannot read"),
            "the read error fails:\n{pairing}"
        );
    }

    /// A server on the store warns, and the exit code is 1: the memory
    /// floor is the only other warn, and the override keeps it ok.
    #[test]
    fn a_server_on_the_store_warns_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        let mut holder = postgres::Client::connect(
            std::fs::read_to_string(&url).unwrap().as_str(),
            postgres::NoTls,
        )
        .unwrap();
        let schema: String = holder
            .query_one("SELECT current_schema()::text", &[])
            .unwrap()
            .get(0);
        holder
            .query_one(
                "SELECT pg_advisory_lock($1)",
                &[&crate::pg::schema_lock_key(&schema)],
            )
            .unwrap();
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(
            report.code, 1,
            "a held lock warns, not fails:\n{:#?}",
            report.lines
        );
        assert!(
            line(&report, "one-server lock").starts_with("warn"),
            "the hold is the warn:\n{:#?}",
            report.lines
        );
        holder
            .query_one(
                "SELECT pg_advisory_unlock($1)",
                &[&crate::pg::schema_lock_key(&schema)],
            )
            .unwrap();
    }

    /// A host under the floor warns: the floor never refuses, as in
    /// `serve`.
    #[test]
    fn a_host_under_the_floor_warns_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set_small(&lock);
        let (dir, url) = ready_store();
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(
            report.code, 1,
            "the floor warns, not fails:\n{:#?}",
            report.lines
        );
        assert!(
            line(&report, "memory floor").starts_with("warn"),
            "the small meminfo warns:\n{:#?}",
            report.lines
        );
    }

    /// An occupied listen address fails with the fix named.
    #[test]
    fn an_occupied_listen_address_fails_preflight() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let taken = held.local_addr().unwrap();
        let report = run(&PreflightOptions {
            data: dir.path().to_path_buf(),
            postgres_url_file: Some(url.clone()),
            postgres_ca: None,
            listen: taken,
            metrics_listen: None,
        });
        assert_eq!(report.code, 2);
        assert!(
            line(&report, "listen address").starts_with("fail"),
            "the occupied address fails:\n{:#?}",
            report.lines
        );
    }

    /// The disk check names the numbers: free bytes and free inodes on
    /// the data directory's filesystem.
    #[test]
    fn the_disk_check_names_the_numbers() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 0);
        let disk = line(&report, "disk and inodes");
        assert!(
            disk.contains("inodes on") && disk.contains("free"),
            "the numbers are the answer:\n{disk}"
        );
    }

    /// A filesystem that does not answer fails: the numbers are the
    /// answer, and an error is not a number.
    #[test]
    fn a_missing_filesystem_fails_the_disk_check() {
        let lock = MEMORY_LOCK.lock().unwrap();
        let (_memory, ()) = MemoryOverride::set(&lock);
        let (dir, url) = ready_store();
        // The directory goes away before the run: the directory check
        // fails first, and the disk check still names its own error.
        let path = dir.path().to_path_buf();
        drop(dir);
        let report = run_cli(&path, Some(&url));
        assert_eq!(report.code, 2);
        assert!(
            line(&report, "disk and inodes").starts_with("fail"),
            "the gone filesystem fails:\n{:#?}",
            report.lines
        );
    }
}
