//! The serve limits the operator sets ride to the running server: the
//! `--max-connections`, `--max-streams` and `--max-stalled-bytes` flags
//! land in the serve's `Limits`, and losing one of them to a mutation
//! would silently serve on the default instead. Each test starts the
//! binary with a limit no default shares, and observes the limit's
//! effect from the client's side: a refused connection, the
//! `SETTINGS_MAX_CONCURRENT_STREAMS` value, and a connection closed for
//! holding too many stalled bytes.

use std::io::{BufRead as _, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use prost::Message as _;
use scion_core::{Changeset, FileState, IdentityTime, RepoPath};
use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use tokio::net::TcpStream;

mod common;

const REPO: &str = "main";
const TOKEN: &str = "secret";
/// Names in the head changeset: a page of the diff is over the h2 send
/// buffer, so an unread reply parks in the server and holds its share.
const NAMES: usize = 40_000;

fn author() -> IdentityTime {
    IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 1_700_000_000, 0)
        .expect("identity fits the format")
}

fn token_file(dir: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let file = dir.join("token");
    std::fs::write(&file, format!("{TOKEN}\n")).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    file
}

/// A spawned server that cannot outlive its test: `Drop` kills and reaps
/// on every path out, panics included.
struct Guarded(Child);

impl Drop for Guarded {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts the binary with `extra` serve arguments, waits bounded for the
/// serving line, and answers the gRPC address and the metrics address.
fn spawn_server(data: &Path, token: &Path, extra: &[&str]) -> (Guarded, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    cmd.arg("--data").arg(data);
    cmd.env("SCION_POSTGRES_URL", common::postgres_url(data));
    cmd.env_remove("SCION_TOKEN");
    cmd.args(["serve", "--token-file", token.to_str().unwrap()]);
    cmd.args(["--listen", "127.0.0.1:0", "--metrics-listen", "127.0.0.1:0"]);
    cmd.args(extra);
    cmd.env("NO_COLOR", "1");
    cmd.stderr(Stdio::piped());
    cmd.stdout(Stdio::null());
    let mut child = cmd.spawn().expect("the server starts");
    let started = Instant::now();
    let mut reader = std::io::BufReader::new(child.stderr.take().expect("stderr is piped"));
    let mut serving = String::new();
    let mut metrics = String::new();
    loop {
        let mut line = String::new();
        match std::io::BufRead::read_line(&mut reader, &mut line) {
            Ok(0) | Err(_) => panic!("the server ended before it served"),
            Ok(_) => {
                if line.contains("serving") && !line.contains("metrics") && line.contains("listen=")
                {
                    serving.clone_from(&line);
                }
                if line.contains("serving metrics") {
                    metrics.clone_from(&line);
                }
                if !serving.is_empty() && !metrics.is_empty() {
                    break;
                }
            }
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the server never served both listeners"
        );
    }
    // The stderr reader must keep draining for the child's life: a
    // closed pipe ends the child on its next line.
    std::thread::spawn(move || {
        let mut rest = String::new();
        loop {
            match reader.read_line(&mut rest) {
                Ok(0) | Err(_) => break,
                Ok(_) => rest.clear(),
            }
        }
    });
    let addr = serving
        .split("listen=")
        .nth(1)
        .unwrap_or_else(|| panic!("the serving line names its address: {serving}"))
        .split_whitespace()
        .next()
        .expect("the serving line names an address")
        .to_owned();
    let metrics_addr = metrics
        .split("listen=")
        .nth(1)
        .unwrap_or_else(|| panic!("the metrics line names its address: {metrics}"))
        .split_whitespace()
        .next()
        .expect("the metrics line names an address")
        .to_owned();
    (Guarded(child), addr, metrics_addr)
}

/// The client preface, so a raw connection speaks HTTP/2 from byte one.
const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// One plain HTTP GET, body only.
fn scrape(addr: &str) -> String {
    let mut stream = std::net::TcpStream::connect(addr).expect("the metrics listener answers");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(format!("GET /metrics HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes())
        .unwrap();
    let mut body = String::new();
    stream.read_to_string(&mut body).unwrap();
    body
}

/// A connection beyond `--max-connections 1` waits the connection wait
/// and is closed, and the refusal is counted: the operator's limit, not
/// the default's, decides who is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_over_the_connections_limit_is_refused_and_counted() {
    use tokio::io::AsyncWriteExt as _;
    let dir = scion_server::test_support::private_tempdir();
    let secrets = scion_server::test_support::private_tempdir();
    let token = token_file(secrets.path());
    let (guard, addr, metrics) = spawn_server(dir.path(), &token, &["--max-connections", "1"]);
    let _guard = guard;

    // The first connection is served and holds the one place.
    let tcp = TcpStream::connect(&addr).await.expect("the first connects");
    let (mut h2, connection) = h2::client::handshake(tcp).await.expect("the first serves");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let built = http::Request::builder()
        .method(http::Method::POST)
        .uri("http://x/scion.v1.Scion/ChangesetReads")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(())
        .unwrap();
    let (first, _send) = h2.send_request(built, true).expect("the first sends");
    let _ = tokio::time::timeout(Duration::from_secs(60), first).await;

    // The second connection: the server takes its preface, holds it for
    // a place, and closes it when none frees. A connection that is
    // served instead answers with its own SETTINGS bytes.
    let mut second = TcpStream::connect(&addr)
        .await
        .expect("the second connects");
    second.write_all(PREFACE).await.expect("the preface writes");
    let mut byte = [0_u8; 1];
    let read = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::io::AsyncReadExt::read(&mut second, &mut byte),
    )
    .await;
    match read {
        // The close rides a FIN or a RST, whichever the socket picks
        // with the preface's bytes still unread.
        Ok(Ok(0)) => {}
        Ok(Err(err)) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
        Ok(Ok(_)) => panic!("a connection over the limit was served"),
        Ok(Err(err)) => panic!("the second connection errored instead of closing: {err}"),
        Err(elapsed) => {
            panic!("the second connection was neither served nor closed in 60 s: {elapsed}")
        }
    }
    drop(h2);

    let body = scrape(&metrics);
    let count = body
        .lines()
        .find(|line| line.starts_with("scion_connections_refused_total"))
        .unwrap_or_else(|| panic!("the refusal count is served: {body}"))
        .split_whitespace()
        .last()
        .expect("the counter names its value")
        .to_owned();
    assert_eq!(count, "1", "the served-over connection is counted once");
}

/// The operator's `--max-streams` rides to the client as the server's
/// `SETTINGS_MAX_CONCURRENT_STREAMS`: a client reads it off the first
/// frame the server sends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_streams_limit_rides_the_settings_frame() {
    let dir = scion_server::test_support::private_tempdir();
    let secrets = scion_server::test_support::private_tempdir();
    let token = token_file(secrets.path());
    let (_guard, addr, _metrics) = spawn_server(dir.path(), &token, &["--max-streams", "1"]);

    let tcp = TcpStream::connect(&addr)
        .await
        .expect("the client connects");
    tcp.try_write(PREFACE).expect("the preface writes");
    // The preface's SETTINGS frame: the server sends its own once the
    // client's arrives.
    let empty_settings: [u8; 9] = [0, 0, 0, 0x4, 0, 0, 0, 0, 0];
    tcp.try_write(&empty_settings)
        .expect("the client's settings write");
    let mut stream = tcp.into_std().expect("the socket is std");
    // tokio's into_std leaves the socket non-blocking: a std read then
    // answers EAGAIN at once. This test reads synchronously.
    stream
        .set_nonblocking(false)
        .expect("the blocking mode sets");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();

    // The first frame of an HTTP/2 connection is the server's SETTINGS:
    // a 9-byte header, then 6-byte entries of id and value.
    let mut header = [0_u8; 9];
    read_exact(&mut stream, &mut header);
    let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
    let kind = header[3];
    assert_eq!(kind, 0x4, "the first frame is a SETTINGS frame");
    let mut payload = vec![0_u8; len];
    read_exact(&mut stream, &mut payload);
    let Some((_, value)) = (0..len / 6)
        .map(|i| {
            let id = u16::from_be_bytes([payload[i * 6], payload[i * 6 + 1]]);
            let value = u32::from_be_bytes([
                payload[i * 6 + 2],
                payload[i * 6 + 3],
                payload[i * 6 + 4],
                payload[i * 6 + 5],
            ]);
            (id, value)
        })
        .find(|(id, _)| *id == 0x3)
    else {
        panic!("the settings name MAX_CONCURRENT_STREAMS: {payload:?}");
    };
    assert_eq!(value, 1, "the operator's streams limit is announced");
}

fn read_exact(stream: &mut std::net::TcpStream, buf: &mut [u8]) {
    let mut at = 0;
    while at < buf.len() {
        match stream.read(&mut buf[at..]) {
            Ok(0) => panic!("the server closed before its settings were read"),
            Ok(n) => at += n,
            Err(err) => panic!("the settings read failed: {err}"),
        }
    }
}

/// A reply whose parked bytes cross the operator's
/// `--max-stalled-bytes` loses its connection: the client that stopped
/// reading sees the stream end, and a limit a default would not cross.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_over_the_stalled_bytes_cap_ends_its_connection() {
    let dir = scion_server::test_support::private_tempdir();
    let secrets = scion_server::test_support::private_tempdir();
    let token = token_file(secrets.path());
    let (_root_id, head_id) = seed(dir.path());
    let (guard, addr, _metrics) = spawn_server(dir.path(), &token, &["--max-stalled-bytes", "1"]);

    let tcp = TcpStream::connect(&addr)
        .await
        .expect("the client connects");
    let (mut h2, connection) = h2::client::handshake(tcp)
        .await
        .expect("the handshake lands");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let request = scion_proto::v1::GetChangesetRequest {
        repo: REPO.into(),
        changeset_id: head_id.as_bytes().to_vec(),
    };
    let built = http::Request::builder()
        .method(http::Method::POST)
        .uri("http://x/scion.v1.Scion/GetChangeset")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(())
        .unwrap();
    let message = request.encode_to_vec();
    let mut body = vec![0_u8; 5];
    body[0] = 0;
    let [a, b, c, d] = u32::try_from(message.len())
        .expect("the message fits u32")
        .to_be_bytes();
    body[1..5].copy_from_slice(&[a, b, c, d]);
    body.extend_from_slice(&message);
    let (reply, mut send) = h2.send_request(built, false).expect("the call opens");
    send.send_data(body.into(), true).expect("the body sends");
    let response = tokio::time::timeout(Duration::from_secs(60), reply)
        .await
        .expect("the reply's headers arrive in 60 s")
        .expect("the reply's headers arrive");
    let mut reply = response.into_body();
    // The client never releases the flow-control window: the reply's
    // parts stop being taken, the server's own 120 s stall timeout
    // parks the reply with its 4 MiB estimate, and that count crosses
    // one byte at once, so the connection goes. Reading a frame without
    // releasing keeps the window shut, so the park is real for as long
    // as it lasts. The bound rides the server's own timing: 120 s of
    // stall, then the close.
    let ended = tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            match reply.data().await {
                // The stream ended, or the connection's close reached
                // the client: either way the park is over.
                None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    assert!(
        ended.is_ok(),
        "a reply holding more than one stalled byte kept its connection"
    );
    drop(guard);
}

/// A root and a head whose root-to-head diff names `NAMES` long
/// entries: one unread reply is far over the h2 send buffer, so it
/// parks in the server and holds its share.
fn seed(data: &Path) -> (scion_core::ChangesetId, scion_core::ChangesetId) {
    let store = Backend::open_test(data).unwrap();
    let blob = store.blobs().put(b"x".to_vec().as_slice()).unwrap();
    let root = Changeset::new(
        vec![],
        author(),
        None,
        b"root\n".to_vec(),
        [(
            RepoPath::new(b"seed".as_slice()).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    let root_id = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            c.insert_changeset(repo, &root)
        })
        .unwrap();
    let head = Changeset::new(
        vec![root_id],
        author(),
        None,
        b"big\n".to_vec(),
        (0..NAMES)
            .map(|i| {
                let name = format!("entry-{i:06}.{}", "n".repeat(120));
                (
                    RepoPath::new(name.as_bytes()).unwrap(),
                    Some(FileState::Regular {
                        content: blob,
                        executable: false,
                    }),
                )
            })
            .collect(),
    )
    .expect("changeset fits the format");
    let head_id = store
        .write(|c| {
            let repo = c.repo_id(REPO).expect("the seeded repo is there");
            c.insert_changeset(repo, &head)
        })
        .unwrap();
    drop(store);
    (root_id, head_id)
}
