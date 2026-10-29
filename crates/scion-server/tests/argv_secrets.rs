//! The secret flags of the binaries: a token can arrive through a
//! checked file, the environment, stdin (`hash-token`), or the command
//! line, which the process list exposes. The file works end to end, a
//! file the group or others can read is refused, and the command line
//! prints its deprecation warning.

use std::io::{BufRead as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::str::FromStr as _;
use std::time::{Duration, Instant};

mod common;

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
// surface a leak could reach: the whole URL, the password it carries,
// and a canary of system randomness that rides the URL as an
// `application_name` parameter no log line could carry by accident.
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

/// The URL the test serves with, and the secrets it must not leak: the
/// shared harness's URL for `data` (the test database, the data
/// directory's own schema) plus a canary of system randomness as the
/// application name, which the server only ever holds as part of the
/// URL. The password is the test database's own, as it arrives from the
/// environment; no constant in this file reads as a credential.
fn leak_url(data: &Path) -> (String, String, Option<String>) {
    let url = common::postgres_url(data);
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("system randomness");
    let canary = bytes.iter().fold(String::new(), |mut text, byte| {
        use std::fmt::Write as _;
        let _ = write!(text, "{byte:02x}");
        text
    });
    let url = format!("{url}&application_name=leak-{canary}");
    // The credentials the URL carries, parsed the way the server parses
    // it: through the same library, not a hand-split that a port or an
    // escaped character would fool. A URL without a password has none to
    // leak.
    let password = postgres::Config::from_str(&url)
        .expect("the URL parses as a database URL")
        .get_password()
        .map(|secret| String::from_utf8_lossy(secret).into_owned());
    (url, canary, password)
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
    fn run(form: UrlForm, url: &str, data: &Path) -> Self {
        seed_repo(data);
        let secret = write_secret(
            data.parent().expect("the data directory has a parent"),
            "token",
            0o600,
            "the-token\n",
        );
        let stdout_path = data.parent().unwrap().join("stdout");
        let stderr_path = data.parent().unwrap().join("stderr");
        let mut child = spawn_server(form, url, data, &secret, &stdout_path, &stderr_path);
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

/// The repo the upload lands in, made through the shared harness's own
/// open before the server starts: an upload into a missing repo is
/// refused, and this is the seeding every suite of the harness does.
fn seed_repo(data: &Path) {
    let store = common::open_store(data);
    store
        .write(|c| {
            let repo = c.create_repo(common::REPO)?;
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

/// The log line that names the gRPC listener: it says `serving` and
/// carries the bound address. The metrics line says "serving metrics"
/// and carries another listener's address, so it is not the gate - a
/// bare `serving` match returns one log line early, and the address
/// read that follows fails (the #715 train's Postgres 18 run).
fn is_the_listener_line(line: &str) -> bool {
    line.contains("serving") && !line.contains("metrics") && line.contains("listen=")
}

/// Waits, bounded, until the server logs that it serves, and returns its
/// log so far. A server that exits early fails here, with its own lines.
fn wait_listening(child: &mut std::process::Child, stderr: &Path) -> String {
    let started = Instant::now();
    loop {
        let text = std::fs::read_to_string(stderr).unwrap_or_default();
        if text.lines().any(is_the_listener_line) {
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

/// The shared check of the two serve forms: the URL and the run's
/// canary reach none of the surfaces, and so does the credential form
/// `:<password>@`, the only shape a leak can print a password in
/// without a URL around it. The bare word is checked only when it is
/// distinctive (12 characters or more): a short one, such as the test
/// database's, is a word the log and the metrics body carry anyway, and
/// the canary covers the URL-shaped leaks either way.
fn assert_no_leak(form: UrlForm, run: &Serve, url: &str, canary: &str, password: Option<&str>) {
    let surfaces = [
        ("stdout", &run.stdout),
        ("the log (stderr)", &run.stderr),
        ("/metrics", &run.metrics),
    ];
    for (name, text) in surfaces {
        assert!(!text.contains(canary), "the canary leaked to {name}");
        assert!(!text.contains(url), "the database URL leaked to {name}");
    }
    if let Some(password) = password {
        let form = format!(":{password}@");
        for (name, text) in surfaces {
            assert!(
                !text.contains(&form),
                "the credential form of the password leaked to {name}"
            );
            if password.len() >= 12 {
                assert!(!text.contains(password), "the password leaked to {name}");
            }
        }
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
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    let (url, canary, password) = leak_url(&data);
    let run = Serve::run(UrlForm::Env, &url, &data);
    assert_no_leak(UrlForm::Env, &run, &url, &canary, password.as_deref());
}

#[test]
fn the_argv_url_warns_without_quoting_itself() {
    if !is_pg() {
        println!("skipping: Postgres only");
        return;
    }
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    scion_proto::runtime::auth::create_private_dir_all(&data).unwrap();
    let (url, canary, password) = leak_url(&data);
    let run = Serve::run(UrlForm::Argv, &url, &data);
    assert_no_leak(UrlForm::Argv, &run, &url, &canary, password.as_deref());
}

/// Only the listener's own line opens the gate: the metrics line says
/// `serving` too and carries a `listen=` of its own, so a run where it
/// is the newest line must keep waiting for the listener's own line.
#[test]
fn the_gate_is_the_listener_line_not_any_serving_line() {
    let metrics_line =
        "2026-10-29T16:00:00.000Z INFO scion_server: serving metrics listen=127.0.0.1:54321";
    let listener_line =
        "2026-10-29T16:00:00.001Z INFO scion_server: serving listen=127.0.0.1:49152";
    assert!(!is_the_listener_line(metrics_line));
    assert!(is_the_listener_line(listener_line));
}
