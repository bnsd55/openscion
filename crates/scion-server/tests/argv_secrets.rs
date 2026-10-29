//! The secret flags of the binaries: a token can arrive through a
//! checked file, the environment, stdin (`hash-token`), or the command
//! line, which the process list exposes. The file works end to end, a
//! file the group or others can read is refused, and the command line
//! prints its deprecation warning.

use std::io::{BufRead as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use scion_proto::runtime::auth::Bearer;
use scion_proto::v1::{PutBlobRequest, scion_client::ScionClient};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use tonic::transport::Endpoint;

/// The digest the `hash-token` command prints for `token`.
fn expected_digest(token: &str) -> String {
    blake3::Hash::from(scion_proto::runtime::auth::token_digest(token))
        .to_hex()
        .to_string()
}

/// Writes `text` to a file of `mode` under `dir` and returns its path.
fn write_secret(dir: &Path, name: &str, mode: u32, text: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let file = dir.join(name);
    std::fs::write(&file, text).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode)).unwrap();
    file
}

/// Runs `scion-server hash-token` with `args`, feeds `stdin` in, and
/// returns (exit code, stdout, stderr).
fn hash_token(args: &[&str], stdin: Option<&str>, env: &[(&str, &str)]) -> (bool, String, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    command
        .args(["hash-token"])
        .args(args)
        .env_remove("SCION_TOKEN")
        // spawn() inherits the terminal's stdio by default; the test
        // reads what the child printed, so every stream is piped.
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (name, value) in env {
        command.env(name, value);
    }
    let mut child = command.spawn().expect("the test binary runs");
    if let Some(text) = stdin {
        let mut pipe = child.stdin.take().expect("stdin is piped");
        pipe.write_all(text.as_bytes()).unwrap();
    }
    let out = child.wait_with_output().expect("the command ends");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn a_token_file_works_end_to_end() {
    let dir = scion_server::test_support::private_tempdir();
    let file = write_secret(dir.path(), "token", 0o600, " the-token \n");
    let (ok, stdout, stderr) = hash_token(&["--token-file", file.to_str().unwrap()], None, &[]);
    assert!(ok, "{stderr}");
    assert_eq!(stdout.trim(), expected_digest("the-token"));
    // The file form is the replacement: it says nothing about argv.
    assert!(!stderr.contains("deprecated"), "{stderr}");
}

#[test]
fn stdin_works_end_to_end() {
    let (ok, stdout, stderr) = hash_token(&[], Some(" the-token \n"), &[]);
    assert!(ok, "{stderr}");
    assert_eq!(stdout.trim(), expected_digest("the-token"));
    assert!(!stderr.contains("deprecated"), "{stderr}");
}

#[test]
fn the_env_works_end_to_end() {
    let (ok, stdout, stderr) = hash_token(&[], None, &[("SCION_TOKEN", "the-token")]);
    assert!(ok, "{stderr}");
    assert_eq!(stdout.trim(), expected_digest("the-token"));
    assert!(!stderr.contains("deprecated"), "{stderr}");
}

#[test]
fn the_argv_form_warns_and_still_answers() {
    let (ok, stdout, stderr) = hash_token(&["--token", "the-token"], None, &[]);
    assert!(ok, "{stderr}");
    assert_eq!(stdout.trim(), expected_digest("the-token"));
    // The warning names the exposure (the process list, whose ps and
    // /proc view every local account has) and the replacement, and says
    // the form is deprecated.
    assert!(stderr.contains("/proc"), "{stderr}");
    assert!(stderr.contains("every local account"), "{stderr}");
    assert!(stderr.contains("--token-file"), "{stderr}");
    assert!(stderr.contains("deprecated"), "{stderr}");
}

#[test]
fn a_group_readable_token_file_is_refused() {
    let dir = scion_server::test_support::private_tempdir();
    let file = write_secret(dir.path(), "token", 0o644, "the-token\n");
    let (ok, stdout, stderr) = hash_token(&["--token-file", file.to_str().unwrap()], None, &[]);
    assert!(!ok, "a 0644 token file must be refused: {stdout}{stderr}");
    assert!(stderr.contains("read by group or others"), "{stderr}");
}

#[test]
fn an_empty_token_file_is_refused() {
    let dir = scion_server::test_support::private_tempdir();
    let file = write_secret(dir.path(), "token", 0o600, "  \n");
    let (ok, stdout, stderr) = hash_token(&["--token-file", file.to_str().unwrap()], None, &[]);
    assert!(!ok, "{stdout}{stderr}");
    assert!(stderr.contains("holds no secret"), "{stderr}");
}

#[test]
fn a_group_readable_serve_token_file_is_refused() {
    // The same refusal on the `serve` path: the flag is parsed by the
    // serve command, which checks the file before the data directory
    // opens, so nothing is created.
    let data = scion_server::test_support::private_tempdir();
    let dir = scion_server::test_support::private_tempdir();
    let file = write_secret(dir.path(), "token", 0o644, "the-token\n");
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data.path())
        .args(["serve", "--token-file", file.to_str().unwrap()])
        .env_remove("SCION_TOKEN")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(stderr.contains("read by group or others"), "{stderr}");
    assert!(
        !data.path().join("scion.db").exists(),
        "a bad flag must not create the store"
    );
}

#[test]
fn a_serve_token_file_starts_the_server() {
    // The file form reaches the same single-principal access the argv
    // form built: the server starts and says so on its log.
    let data = scion_server::test_support::private_tempdir();
    let dir = scion_server::test_support::private_tempdir();
    let file = write_secret(dir.path(), "token", 0o600, "the-token\n");
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data.path())
        .args([
            "serve",
            "--token-file",
            file.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
        ])
        .env_remove("SCION_TOKEN")
        .stderr(Stdio::piped())
        .spawn()
        .expect("the test binary runs");
    let stderr = child.stderr.take().expect("stderr is piped");
    // The server exits only when killed, so read until its log says it
    // serves, then kill it: the file form loaded the access and the
    // listener bound.
    let mut text = String::new();
    let mut line = String::new();
    let mut reader = std::io::BufReader::new(stderr);
    loop {
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                text.push_str(&line);
                if line.contains("serving") {
                    break;
                }
                line.clear();
            }
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(text.contains("serving"), "{text}");
    assert!(!text.contains("deprecated"), "{text}");
}

// The Postgres URL carries the database's credentials, so no output of a
// serving server may hold it: not the standard streams, not the log, not
// /metrics. The two tests here serve for real, once with the URL in
// `SCION_POSTGRES_URL` and once on the command line (whose deprecation
// warning must print and must not quote the URL), and check every
// surface a leak could reach. The database role they connect as exists
// for the test alone, and its password is a canary no log line could
// carry by accident.
//
// Postgres only: the URL comes from `SCION_TEST_POSTGRES_URL`, and every
// test skips itself when `SCION_TEST_BACKEND` does not name `postgres`.

/// Whether the Postgres mode of the URL tests can run.
fn is_pg() -> bool {
    std::env::var("SCION_TEST_BACKEND").as_deref() == Ok("postgres")
}

/// How the serve process receives the database URL.
#[derive(Clone, Copy)]
enum UrlForm {
    /// The environment, the documented second-best form.
    Env,
    /// The command line, whose warning must show and must not quote it.
    Argv,
}

/// The database side of one test: a login role whose password is the
/// canary, and a schema of its own, both named for this run. The URL it
/// returns reaches the server through the form the test is proving; the
/// admin URL stays here only, and drops the schema and the role when the
/// guard falls, panic or not.
struct Canary {
    admin: String,
    schema: String,
    role: String,
}

impl Canary {
    /// Makes the role, the schema, and the grants, and returns the guard
    /// with the server URL and the canary password.
    fn new(name: &str) -> (Self, String, String) {
        let admin = std::env::var("SCION_TEST_POSTGRES_URL")
            .expect("SCION_TEST_POSTGRES_URL must name the test database");
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is past the epoch")
            .as_nanos();
        let schema = format!("scion_leak_{name}_{}", suffix & 0xffff);
        let role = format!("scion_leak_{name}_{}", suffix & 0xffff);
        let password = format!("canary-{name}-{suffix}");
        // The host, port and name the canary URL keeps from the admin
        // one, taken before the guard takes the string.
        let host = host_of(&admin).to_owned();
        let db = db_of(&admin).to_owned();
        // The connect is one blocking call at setup; the test may sit in
        // a runtime elsewhere, so the call runs on a thread of its own,
        // the way the shared harness's URL helper does.
        std::thread::spawn({
            let (admin, schema, role, password) = (
                admin.clone(),
                schema.clone(),
                role.clone(),
                password.clone(),
            );
            move || {
                let mut client = postgres::Client::connect(&admin, postgres::NoTls)
                    .expect("the test database connects");
                client
                    .batch_execute(&format!(
                        "DROP SCHEMA IF EXISTS {schema} CASCADE; \
                         DROP ROLE IF EXISTS {role}; \
                         CREATE ROLE {role} LOGIN PASSWORD '{password}'; \
                         CREATE SCHEMA {schema}; \
                         GRANT USAGE, CREATE ON SCHEMA {schema} TO {role};"
                    ))
                    .expect("the canary role and schema are made");
            }
        })
        .join()
        .unwrap();
        let guard = Self {
            admin,
            schema: schema.clone(),
            role,
        };
        // The schema rides the URL as the libpq `options` search path,
        // the same shape the shared harness's URL helper builds.
        let url = format!(
            "postgres://{schema}:{password}@{host}/{db}{sep}options=--search_path%3D{schema}",
            sep = if db.contains('?') { '&' } else { '?' },
        );
        (guard, url, password)
    }
}

impl Drop for Canary {
    fn drop(&mut self) {
        // Best effort: a failed test may leave its name behind, and the
        // next run makes its own.
        let admin = self.admin.clone();
        let (schema, role) = (self.schema.clone(), self.role.clone());
        let _ = std::thread::spawn(move || {
            let Ok(mut client) = postgres::Client::connect(&admin, postgres::NoTls) else {
                return;
            };
            let _ = client.batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {schema} CASCADE; DROP ROLE IF EXISTS {role};"
            ));
        })
        .join();
    }
}

/// The authority of `url` without its userinfo: the host and port, which
/// the canary URL keeps, and only the login it swaps.
fn host_of(url: &str) -> &str {
    let rest = url.split("://").nth(1).expect("the URL has a scheme");
    let authority = rest.split('/').next().expect("the URL names a database");
    authority.split('@').next_back().expect("a non-empty URL")
}

/// The path of `url`: the database name and any parameters it rides.
fn db_of(url: &str) -> &str {
    let rest = url.split("://").nth(1).expect("the URL has a scheme");
    let (_authority, path) = rest.split_once('/').expect("the URL names a database");
    path
}

/// What a serve run left on the surfaces a leak could reach. The server
/// logs to stderr, so the log file is the stderr capture; stdout is its
/// own file, because a command's outputs need not share a stream.
struct Serve {
    stdout: String,
    stderr: String,
    metrics: String,
}

impl Serve {
    /// Serves once with the URL in the form `form` names, uploads one
    /// blob through the gRPC API, scrapes /metrics, stops the server,
    /// and returns the three surfaces. Every wait is bounded.
    fn run(form: UrlForm, url: &str) -> Self {
        let dir = scion_server::test_support::private_tempdir();
        let data = dir.path().join("data");
        scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
        seed_repo(&data, url);
        let secret = write_secret(dir.path(), "token", 0o600, "the-token\n");
        let stdout_path = dir.path().join("stdout");
        let stderr_path = dir.path().join("stderr");
        let mut child = spawn_server(form, url, &data, &secret, &stdout_path, &stderr_path);
        let log = wait_listening(&mut child, &stderr_path);
        let metrics_addr = logged_addr(&log, |line| line.contains("serving metrics"));
        // One real upload: the call touches the database through the
        // write path, so the run covers more than the startup's queries.
        let url = server_url(&log);
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let channel = Endpoint::from_shared(url)
                .unwrap()
                .connect_timeout(Duration::from_secs(5))
                .connect()
                .await
                .expect("the client connects");
            let mut scion = ScionClient::with_interceptor(
                channel,
                Bearer::new("the-token").expect("a visible token"),
            );
            scion
                .put_blob(tokio_stream::once(PutBlobRequest {
                    repo: "r".into(),
                    data: b"# url secret\n".to_vec(),
                }))
                .await
                .expect("the upload lands");
        });
        let metrics = scrape(&format!("http://{metrics_addr}/metrics"));
        let _ = child.kill();
        let _ = child.wait();
        Self {
            stdout: std::fs::read_to_string(&stdout_path).unwrap(),
            stderr: std::fs::read_to_string(&stderr_path).unwrap(),
            metrics,
        }
    }
}

/// The repo the upload lands in, made through the same URL the server
/// will get, before the server opens it exclusively: an upload into a
/// missing repo is refused, and a migration made by another login would
/// leave the canary role a store it cannot touch.
fn seed_repo(data: &Path, url: &str) {
    let store = scion_server::Backend::open(data, Some(url), None).unwrap();
    store
        .write(|c| {
            let repo = c.create_repo("r")?;
            let root = c.insert_changeset(
                repo,
                &scion_core::Changeset::new(
                    vec![],
                    scion_core::IdentityTime::new(
                        b"Ada".to_vec(),
                        b"ada@example.com".to_vec(),
                        1_700_000_000,
                        0,
                    )
                    .expect("identity fits the format"),
                    None,
                    b"root\n".to_vec(),
                    std::collections::BTreeMap::new(),
                )
                .expect("changeset fits the format"),
            )?;
            c.cas_ref(repo, "main", None, root)?;
            Ok(())
        })
        .unwrap();
}

/// Starts `serve` on `data`, with the database URL in the form the test
/// is proving, and both outputs in their own files.
fn spawn_server(
    form: UrlForm,
    url: &str,
    data: &Path,
    secret: &Path,
    stdout: &Path,
    stderr: &Path,
) -> std::process::Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    cmd.arg("--data").arg(data);
    match form {
        UrlForm::Env => {
            cmd.env("SCION_POSTGRES_URL", url);
        }
        UrlForm::Argv => {
            cmd.arg("--postgres-url").arg(url);
        }
    }
    cmd.args([
        "serve",
        "--token-file",
        secret.to_str().unwrap(),
        "--listen",
        "127.0.0.1:0",
        "--metrics-listen",
        "127.0.0.1:0",
    ])
    .env("NO_COLOR", "1")
    .env("RUST_LOG", "info")
    .stdout(std::fs::File::create(stdout).unwrap())
    .stderr(std::fs::File::create(stderr).unwrap());
    cmd.spawn().expect("the server starts")
}

/// Waits, bounded, until the server logs that it serves, and returns its
/// log so far. A server that exits early fails here, with its own lines.
fn wait_listening(child: &mut std::process::Child, stderr: &Path) -> String {
    let started = Instant::now();
    loop {
        let text = std::fs::read_to_string(stderr).unwrap_or_default();
        if text.contains("serving") {
            return text;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("the server exited early with {status}: {text}");
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the server never listened: {text}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The address the server logged for the listener `want` names: every
/// listener binds port 0, so the OS picks a free port no other process
/// can take in between.
fn logged_addr(log: &str, want: fn(&str) -> bool) -> SocketAddr {
    log.lines()
        .rev()
        .find(|line| want(line))
        .and_then(|line| line.split("listen=").nth(1))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|addr| addr.parse().ok())
        .expect("the server logged its listener's address")
}

/// The server's gRPC URL: the address the newest `serving` line names.
fn server_url(log: &str) -> String {
    let addr = logged_addr(log, |line| {
        line.contains("serving") && !line.contains("metrics")
    });
    format!("http://{addr}")
}

/// One plain HTTP GET, body only: /metrics answers a scraper that holds
/// no client library.
fn scrape(url: &str) -> String {
    use std::io::Read as _;
    let addr: SocketAddr = url
        .trim_start_matches("http://")
        .split('/')
        .next()
        .expect("the URL names a path")
        .parse()
        .expect("the metrics URL names an address");
    let mut stream = TcpStream::connect(addr).expect("the metrics listener answers");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(format!("GET /metrics HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes())
        .unwrap();
    let mut whole = Vec::new();
    stream.read_to_end(&mut whole).expect("the body ends");
    let text = String::from_utf8_lossy(&whole).into_owned();
    // A scrape of the wrong listener answers no metrics at all: the
    // checks after this must not pass vacuously.
    let body = match text.split_once("\r\n\r\n") {
        Some((_, body)) => body,
        None => text.as_str(),
    };
    assert!(
        body.contains("scion_"),
        "/metrics served no scion metrics: {body}"
    );
    body.to_owned()
}

/// The shared check of the two serve forms: the password and the whole
/// URL reach none of the surfaces. The argv form alone must carry the
/// deprecation warning, and must print it without the URL.
fn assert_no_leak(form: UrlForm, run: &Serve, url: &str, password: &str) {
    let surfaces = [
        ("stdout", &run.stdout),
        ("the log (stderr)", &run.stderr),
        ("/metrics", &run.metrics),
    ];
    for (name, text) in surfaces {
        assert!(!text.contains(password), "the password leaked to {name}");
        assert!(!text.contains(url), "the database URL leaked to {name}");
    }
    let warning = matches!(form, UrlForm::Argv);
    assert_eq!(
        run.stderr.contains("deprecated"),
        warning,
        "the argv warning must print for the argv form alone"
    );
    if warning {
        assert!(
            run.stderr.contains("--postgres-url-file"),
            "the warning names the replacement: {}",
            run.stderr
        );
    }
}

#[test]
fn the_env_url_and_its_password_reach_no_output() {
    if !is_pg() {
        println!("skipping: Postgres only");
        return;
    }
    let (canary, url, password) = Canary::new("env");
    let run = Serve::run(UrlForm::Env, &url);
    assert_no_leak(UrlForm::Env, &run, &url, &password);
    drop(canary);
}

#[test]
fn the_argv_url_warns_without_quoting_itself() {
    if !is_pg() {
        println!("skipping: Postgres only");
        return;
    }
    let (canary, url, password) = Canary::new("argv");
    let run = Serve::run(UrlForm::Argv, &url);
    assert_no_leak(UrlForm::Argv, &run, &url, &password);
    drop(canary);
}
