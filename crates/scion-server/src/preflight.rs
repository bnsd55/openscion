//! The `preflight` command: every check a first boot needs, read-only,
//! one line each, the same words `serve`'s own refusals use.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use scion_proto::runtime::auth::{checked_read_error, read_secret_file, read_shared_file};
use scion_proto::runtime::tls::ServerTls;

use crate::error::Result;

/// The inputs of a `preflight` run: the flags `serve` takes, as the
/// shell hands them over. The Postgres URL stays unresolved, so the
/// URL's own check can name the missing piece.
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
    /// The file holding the one principal's token, checked like a
    /// secret.
    pub token_file: Option<PathBuf>,
    /// The access file: tokens and rules for many principals.
    pub access: Option<PathBuf>,
    /// The server's certificate chain (PEM).
    pub tls_cert: Option<PathBuf>,
    /// The server's private key (PEM).
    pub tls_key: Option<PathBuf>,
    /// The CA that verifies a client's certificate, when mTLS is on.
    pub tls_client_ca: Option<PathBuf>,
    /// The ticket signing key's seed file.
    pub ticket_key: Option<PathBuf>,
    /// The previous ticket key's public file, which a rotation keeps
    /// alive.
    pub ticket_key_old: Option<PathBuf>,
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
/// the database, the schema, the keys and files, the address, the
/// host. Nothing is written: no marker, no schema move, no lock.
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

    // The Postgres build: 16 to 18, as docs/support.md supports.
    if let Some(db) = db.as_mut() {
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

        // synchronous_commit, the database's own setting: the session
        // carries no option for it (see preflight_client), so the SHOW
        // answers what every other client would get.
        let sync = match db.query_one("SHOW synchronous_commit", &[]) {
            Ok(row) if row.get::<_, String>(0) == "on" => {
                Answer::Ok("synchronous_commit is on".to_owned())
            }
            Ok(row) => Answer::Fail(format!(
                "the database runs synchronous_commit={}; this backend \
                 promises that a returned commit is durable, so it needs \
                 synchronous_commit=on",
                row.get::<_, String>(0)
            )),
            Err(error) => Answer::Fail(format!("cannot read synchronous_commit: {error}")),
        };
        answers.push(("synchronous_commit", sync));

        // The schema's version, read-only: the ledger's presence, then
        // its top version. Never the open's migrate path, which writes.
        answers.push((
            "schema version",
            match crate::pg::read_schema_version_raw(db) {
                Ok(version) if version == crate::pg::PG_SCHEMA_VERSION => Answer::Ok(format!(
                    "the schema stands at version {version}, this build's"
                )),
                Ok(version) if version < crate::pg::PG_SCHEMA_VERSION => Answer::Fail(format!(
                    "the schema stands at version {version}, this build needs {}; \
                     run migrate (serve --migrate)",
                    crate::pg::PG_SCHEMA_VERSION
                )),
                Ok(version) => Answer::Fail(format!(
                    "the schema stands at version {version}, this build speaks {}; \
                     run the build that made the database",
                    crate::pg::PG_SCHEMA_VERSION
                )),
                Err(error) => Answer::Fail(format!("cannot read the schema version: {error}")),
            },
        ));

        // The store's pairing: the marker's UUID beside the database's
        // own row, plain selects only.
        answers.push(("store pairing", store_pairing(&opts.data, db)));

        // A live server on this store: the one-server lock's key, read
        // from `pg_locks` - never taken.
        answers.push(("one-server lock", live_server(db)));
    }

    // The token and the access file: the rules of `serve`'s own load.
    answers.push(("token and access", token_source(opts)));

    // The TLS files, through the protocol's own loader.
    answers.push(("TLS", tls(opts)));

    // The ticket key files, through the keys' own loader.
    answers.push(("ticket key", tickets(opts)));

    // The listen address: bound and dropped, the way `serve` binds it.
    answers.push(("listen address", listen_free(opts.listen)));
    if let Some(metrics_listen) = opts.metrics_listen {
        answers.push(("metrics listen address", listen_free(metrics_listen)));
    }

    // The host's memory: the same read the start-up check makes. The
    // floor stays a warning, as it is in `serve`.
    answers.push(("memory floor", memory_floor()));

    // The data directory's filesystem: free bytes and free inodes, the
    // two walls of a blob store (one file, one inode, per blob).
    answers.push(("disk and inodes", disk_and_inodes(&opts.data)));

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

/// The rules of `serve`'s token load: exactly one of `--token-file`,
/// `SCION_TOKEN` or `--access`, the same words serve's start answers.
fn token_source(opts: &PreflightOptions) -> Answer {
    let token = std::env::var("SCION_TOKEN").ok();
    let given = [
        token.is_some(),
        opts.token_file.is_some(),
        opts.access.is_some(),
    ]
    .into_iter()
    .filter(|given| *given)
    .count();
    if given != 1 {
        return Answer::Fail(if given == 0 {
            "give --token-file, SCION_TOKEN or --access: serve has no token source".to_owned()
        } else {
            "give --token-file, SCION_TOKEN or --access, not two".to_owned()
        });
    }
    if let Some(file) = &opts.token_file {
        return match checked_text(file, true) {
            Ok(_) => Answer::Ok("the token source loads".to_owned()),
            Err(error) => Answer::Fail(error.to_string()),
        };
    }
    if token.is_some() {
        return Answer::Ok("the token source loads".to_owned());
    }
    let file = opts.access.as_ref().expect("exactly one source is given");
    match checked_text(file, true) {
        Ok(text) => match crate::Access::parse(&text) {
            Ok(_) => Answer::Ok("the token source loads".to_owned()),
            Err(why) => Answer::Fail(format!("bad access file {}: {why}", file.display())),
        },
        Err(error) => Answer::Fail(error.to_string()),
    }
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

/// The TLS files: the protocol's own loader, as `serve` loads them.
fn tls(opts: &PreflightOptions) -> Answer {
    match (&opts.tls_cert, &opts.tls_key) {
        (None, None) => Answer::Ok(
            "no TLS is configured; the listen check refuses a \
                        non-loopback address without it"
                .to_owned(),
        ),
        (Some(cert), Some(key)) => {
            match ServerTls::load(cert, key, opts.tls_client_ca.as_deref()) {
                Ok(_) => Answer::Ok("the certificates load".to_owned()),
                Err(error) => Answer::Fail(error.to_string()),
            }
        }
        _ => Answer::Fail("--tls-cert and --tls-key come together".to_owned()),
    }
}

/// The ticket keys, as `serve` loads them: a signing key, and the
/// public key a rotation keeps alive.
fn tickets(opts: &PreflightOptions) -> Answer {
    let Some(seed) = &opts.ticket_key else {
        return if opts.ticket_key_old.is_some() {
            Answer::Fail("--ticket-key-old needs --ticket-key".to_owned())
        } else {
            Answer::Ok("no ticket key is configured".to_owned())
        };
    };
    let seed = match checked_text(seed, true) {
        Ok(text) => text,
        Err(error) => return Answer::Fail(error.to_string()),
    };
    let mut old = Vec::new();
    if let Some(file) = &opts.ticket_key_old {
        match checked_text(file, false) {
            Ok(text) => old.push(text),
            Err(error) => return Answer::Fail(error.to_string()),
        }
    }
    match crate::Keys::load(&seed, &old) {
        Ok(_) => Answer::Ok("the ticket key loads".to_owned()),
        Err(why) => Answer::Fail(why.clone()),
    }
}

/// The marker's UUID beside the database's `store_uuid` row, plain
/// selects: a mismatch is a store restored over a directory, the
/// open's own refusal to name.
fn store_pairing(data: &Path, db: &mut postgres::Client) -> Answer {
    let marker = crate::backend::store_uuid_of(data);
    let held: Option<Vec<u8>> = db
        .query_opt("SELECT uuid FROM store_uuid WHERE one = 1", &[])
        .ok()
        .flatten()
        .map(|row| row.get(0));
    match (marker, held) {
        (Some(marker), Some(held)) if marker == *held => {
            Answer::Ok("the data directory and the database name one store".to_owned())
        }
        (Some(marker), Some(held)) => Answer::Fail(format!(
            "this data directory belongs to the store whose UUID is {}, \
             but the database names the store {}: one was restored over \
             the other; give the directory of the database's own store, \
             or a directory the database has never served",
            hex(&marker),
            hex(&held)
        )),
        (Some(_), None) => {
            Answer::Ok("the database has no store yet; the first open mints one".to_owned())
        }
        (None, Some(_)) => {
            Answer::Ok("the directory carries no marker yet; the first open writes one".to_owned())
        }
        (None, None) => Answer::Ok("neither a marker nor a store: a first boot".to_owned()),
    }
}

/// 16 bytes as 32 lowercase hex digits, for the pairing refusal.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
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
            "SELECT count(*) FROM pg_locks
             WHERE locktype = 'advisory' AND objid = $1 AND granted",
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

/// The data directory's filesystem: free bytes and free inodes, the
/// store's own gauges. No threshold: the docs size the budget from the
/// blob count, so the numbers are the answer.
fn disk_and_inodes(data: &Path) -> Answer {
    match (
        crate::store::free_bytes(data),
        crate::store::free_inodes(data),
    ) {
        (Ok(bytes), Ok(inodes)) => Answer::Ok(format!(
            "{} free and {inodes} inodes on {}",
            human_bytes(bytes),
            data.display()
        )),
        (Err(error), _) | (_, Err(error)) => Answer::Fail(format!(
            "cannot read the filesystem of {}: {error}",
            data.display()
        )),
    }
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

    /// A token file the preflight reads: mode 0600.
    fn token_file(dir: &Path) -> PathBuf {
        let file = dir.join("token");
        std::fs::write(&file, "t".repeat(32)).unwrap();
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
            token_file: Some(token_file(data)),
            access: None,
            tls_cert: None,
            tls_key: None,
            tls_client_ca: None,
            ticket_key: None,
            ticket_key_old: None,
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
    struct MemoryOverride(#[allow(dead_code)] tempfile::TempDir);
    impl MemoryOverride {
        fn set() -> Self {
            let dir = private_tempdir();
            std::fs::write(dir.path().join("meminfo"), "MemTotal:       67108864 kB\n").unwrap();
            crate::floor::test_set_paths(Some(crate::floor::Paths {
                self_cgroup: dir.path().join("no-cgroup"),
                cgroup_root: dir.path().join("no-root"),
                meminfo: dir.path().join("meminfo"),
            }));
            Self(dir)
        }
    }
    impl Drop for MemoryOverride {
        fn drop(&mut self) {
            crate::floor::test_set_paths(None);
        }
    }

    /// A ready store passes everything, and the run writes nothing: the
    /// data directory's files and the schema's catalog stand unchanged.
    #[test]
    fn a_ready_store_passes_preflight_and_writes_nothing() {
        let _memory = MemoryOverride::set();
        let (dir, url) = ready_store();
        let before: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        let mut raw = postgres::Client::connect(
            std::fs::read_to_string(&url).unwrap().as_str(),
            postgres::NoTls,
        )
        .unwrap();
        let schema: String = raw
            .query_one("SELECT current_schema()::text", &[])
            .unwrap()
            .get(0);
        let catalog_before = raw
            .query_one(
                "SELECT count(*) FROM pg_class WHERE relnamespace = $1::regnamespace",
                &[&schema],
            )
            .unwrap()
            .get::<_, i64>(0);
        drop(raw);
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(
            report.code, 0,
            "a ready store passes everything:\n{:#?}",
            report.lines
        );
        for name in [
            "ok data directory",
            "ok Postgres version",
            "ok synchronous_commit",
            "ok schema version",
            "ok store pairing",
            "ok one-server lock",
            "ok token and access",
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
        let after: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(before, after, "the data directory stands unchanged");
        let mut raw = postgres::Client::connect(
            std::fs::read_to_string(&url).unwrap().as_str(),
            postgres::NoTls,
        )
        .unwrap();
        let catalog_after = raw
            .query_one(
                "SELECT count(*) FROM pg_class WHERE relnamespace = $1::regnamespace",
                &[&schema],
            )
            .unwrap()
            .get::<_, i64>(0);
        assert_eq!(
            catalog_before, catalog_after,
            "the catalog stands unchanged"
        );
    }

    /// A directory that holds a sqlite store fails with the open's own
    /// refusal, and the exit code is 2.
    #[test]
    fn a_sqlite_directory_fails_preflight() {
        let _memory = MemoryOverride::set();
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

    /// A database that does not answer fails, and the later checks
    /// still print their lines.
    #[test]
    fn a_database_that_does_not_answer_fails_preflight() {
        let _memory = MemoryOverride::set();
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
            line(&report, "token and access").starts_with("ok"),
            "the later checks still run:\n{:#?}",
            report.lines
        );
    }

    /// A schema behind this build fails with the fix named: migrate.
    #[test]
    fn a_schema_behind_the_build_fails_preflight() {
        let _memory = MemoryOverride::set();
        let (dir, url) = ready_store();
        let mut raw = postgres::Client::connect(
            std::fs::read_to_string(&url).unwrap().as_str(),
            postgres::NoTls,
        )
        .unwrap();
        raw.execute("DELETE FROM schema_migrations", &[]).unwrap();
        drop(raw);
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 2);
        let schema = line(&report, "schema version");
        assert!(
            schema.starts_with("fail") && schema.contains("migrate"),
            "the behind schema fails with the fix:\n{schema}"
        );
    }

    /// A server on the store warns, and the exit code is 1: the memory
    /// floor is the only other warn, and the override keeps it ok.
    #[test]
    fn a_server_on_the_store_warns_preflight() {
        let _memory = MemoryOverride::set();
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

    /// No token source anywhere: the token check fails, exit 2.
    #[test]
    fn a_missing_token_source_fails_preflight() {
        let _memory = MemoryOverride::set();
        let (dir, url) = ready_store();
        std::fs::remove_file(dir.path().join("token")).unwrap();
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 2);
        assert!(
            line(&report, "token and access").starts_with("fail"),
            "the missing token fails:\n{:#?}",
            report.lines
        );
    }

    /// An occupied listen address fails with the fix named.
    #[test]
    fn an_occupied_listen_address_fails_preflight() {
        let _memory = MemoryOverride::set();
        let (dir, url) = ready_store();
        let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let taken = held.local_addr().unwrap();
        let report = run(&PreflightOptions {
            data: dir.path().to_path_buf(),
            postgres_url_file: Some(url.clone()),
            postgres_ca: None,
            token_file: Some(token_file(dir.path())),
            access: None,
            tls_cert: None,
            tls_key: None,
            tls_client_ca: None,
            ticket_key: None,
            ticket_key_old: None,
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
        let _memory = MemoryOverride::set();
        let (dir, url) = ready_store();
        let report = run_cli(dir.path(), Some(&url));
        assert_eq!(report.code, 0);
        let disk = line(&report, "disk and inodes");
        assert!(
            disk.contains("inodes on") && disk.contains("free"),
            "the numbers are the answer:\n{disk}"
        );
    }
}
