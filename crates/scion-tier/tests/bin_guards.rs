//! Guards on the two tier binaries' own logic, at the level a starter
//! or an operator sees: the removed-flag refusal, the seed's warning
//! about bad objects, the ticket keys a cache serves with, the TLS
//! pair, the snapshot task, and the operator's limits riding to the
//! running serve. Each test drives the real binary, so a change to the
//! guarded lines turns the test red.

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::io::Write as _;
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ed25519_dalek::SigningKey;
use prost::Message as _;
use scion_core::{BlobId, PathElem, Tree};
use scion_proto::Bytes;
use scion_proto::runtime::object;
use scion_proto::ticket;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind, ObjectPart,
    ObjectRef, ResolveRefRequest, ResolveRefResponse,
};
use scion_tier::{Key, snapshot};
use tempfile::TempDir;
use tokio_stream::StreamExt as _;
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status};
use tower::service_fn;

/// A temp directory the run owns outright: mode 0700, whatever the
/// umask. The daemon refuses a store under a directory that group or
/// others can write.
fn private_dir() -> TempDir {
    let dir = TempDir::new().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

/// A started real binary, killed when the handle drops.
struct Bin {
    child: Child,
    log: PathBuf,
}

impl Drop for Bin {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

/// Starts `bin` with its output in `log`, uncolored, with the tier's
/// tokens in the environment.
fn spawn_bin(bin: &str, args: &[&str], log: &Path) -> Bin {
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    let child = Command::new(bin)
        .args(args)
        .env("NO_COLOR", "1")
        .env("RUST_LOG", "info")
        .env("SCION_TOKEN", "t")
        .env("SCION_UPSTREAM_TOKEN", "t")
        .stdout(file.try_clone().unwrap())
        .stderr(file)
        .spawn()
        .unwrap();
    Bin {
        child,
        log: log.to_owned(),
    }
}

/// First `needle` line in the log, polled until it shows or 60 s pass.
fn wait_for_log(log: &Path, needle: &str) -> String {
    let started = std::time::Instant::now();
    loop {
        if let Some(line) = fs::read_to_string(log).ok().and_then(|text| {
            text.lines()
                .find(|line| line.contains(needle))
                .map(str::to_owned)
        }) {
            return line;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "timed out waiting for {needle}; log: {:?}",
            fs::read_to_string(log).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The address in the newest `serving` line's `listen=` field.
fn serving_addr(log: &Path) -> SocketAddr {
    let started = std::time::Instant::now();
    let line = loop {
        if let Some(line) = fs::read_to_string(log).ok().and_then(|text| {
            text.lines()
                .find(|line| line.contains("serving") && !line.contains("metrics"))
                .map(str::to_owned)
        }) {
            break line;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the serving line never came"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    line.split("listen=")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap_or_else(|| panic!("the serving line names an address: {line}"))
        .parse()
        .expect("the serving line names a socket address")
}

/// A fake upstream of one blob at `f`, whose tickets its key signs. In
/// the stalls mode its `GetObjects` reply never ends, so a connection
/// that holds a place keeps holding it.
#[derive(Clone)]
struct Fake {
    parts: Arc<Mutex<HashMap<Vec<u8>, Bytes>>>,
    key: SigningKey,
    root: scion_core::TreeId,
    blob: Vec<u8>,
    stalls: bool,
}

/// The object's bytes: incompressible, and big enough that its reply
/// needs far more parts than any client window holds (the chunks are
/// 1 MiB), so a client that stops reading parks the reply with bytes
/// still to send. Random-looking bytes: a repetitive blob would pack
/// to one small part, and nothing would stall.
fn guard_blob() -> Vec<u8> {
    let mut data = vec![0_u8; 16 << 20];
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    for byte in &mut data {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = ((state >> 24) & 0xff) as u8;
    }
    data
}

impl Fake {
    fn new(stalls: bool) -> Self {
        let data = guard_blob();
        let blob = BlobId::of(&data).as_bytes().to_vec();
        let (tree_bytes, root) = Tree::new(
            [(
                PathElem::new(b"f".to_vec()).unwrap(),
                scion_core::Entry::File(scion_core::FileEntry::new(
                    scion_core::FileState::Regular {
                        content: BlobId::of(&data),
                        executable: false,
                    },
                    data.len() as u64,
                )),
            )]
            .into_iter()
            .collect(),
        )
        .unwrap()
        .encode_with_id();
        let parts = HashMap::from([
            (root.as_bytes().to_vec(), Bytes::from(tree_bytes)),
            (blob.clone(), Bytes::from(data)),
        ]);
        Self {
            parts: Arc::new(Mutex::new(parts)),
            key: SigningKey::from_bytes(&[7u8; 32]),
            root,
            blob,
            stalls,
        }
    }

    /// The signed ticket for the root tree, in repo `r`.
    fn ticket(&self) -> Vec<u8> {
        ticket::sign(&self.key, "r", "test", *self.root.as_bytes(), ticket::now()).unwrap()
    }

    /// The hex of the public key that verifies its tickets.
    fn public_key_hex(&self) -> String {
        let mut out = String::new();
        for byte in self.key.verifying_key().as_bytes() {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
        }
        out
    }
}

#[tonic::async_trait]
impl Objects for Fake {
    async fn resolve_ref(
        &self,
        _: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        Err(Status::unimplemented("the fake has no refs"))
    }

    async fn get_root_tree(
        &self,
        _: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
        Err(Status::unimplemented("the fake has no refs"))
    }

    type GetObjectsStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<ObjectPart, Status>> + Send>>;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<Self::GetObjectsStream>, Status> {
        let refs = request.into_inner().refs;
        let parts = self.parts.lock().unwrap().clone();
        let mut out = Vec::new();
        for r in &refs {
            let data = parts.get(&r.id).cloned().unwrap_or_default();
            let header = scion_proto::runtime::object::Header {
                kind: r.kind.try_into().unwrap_or(ObjectKind::Blob),
                id: r.id.clone().try_into().unwrap_or([0; 32]),
                encoding: Encoding::Raw,
                encoded_size: data.len(),
                size: data.len(),
            };
            let chunks = scion_proto::runtime::object::read_chunks(&data[..], data.len()).unwrap();
            out.extend(header.parts(chunks).into_iter().map(Ok));
        }
        let tail: std::pin::Pin<
            Box<dyn tokio_stream::Stream<Item = Result<ObjectPart, Status>> + Send>,
        > = if self.stalls {
            Box::pin(tokio_stream::pending())
        } else {
            Box::pin(tokio_stream::empty())
        };
        let stream = tokio_stream::iter(out).chain(tail);
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Serves `fake` on a loopback port the OS chose.
async fn serve_fake(fake: &Fake) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served = fake.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(ObjectsServer::new(served))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    addr
}

/// A gRPC client on a TCP address, as the starter's own client dials.
async fn tcp_client(addr: SocketAddr) -> ObjectsClient<tonic::transport::Channel> {
    tcp_client_with(addr, Some(Duration::from_secs(60))).await
}

/// The same, with the call timeout optional: the stall test's client
/// must not time its call out, or the cancel would end the reply the
/// test waits on.
async fn tcp_client_with(
    addr: SocketAddr,
    timeout: Option<Duration>,
) -> ObjectsClient<tonic::transport::Channel> {
    let mut endpoint = Endpoint::try_from(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(5));
    if let Some(timeout) = timeout {
        endpoint = endpoint.timeout(timeout);
    }
    let channel = endpoint.connect().await.unwrap();
    ObjectsClient::new(channel)
}

/// A `GetObjects` call for `fake`'s blob, with the caller's token and
/// the operator's ticket.
fn blob_request(fake: &Fake) -> Request<GetObjectsRequest> {
    let mut request = Request::new(GetObjectsRequest {
        refs: vec![ObjectRef {
            kind: ObjectKind::Blob.into(),
            id: fake.blob.clone(),
            path: b"f".to_vec(),
        }],
        accept: Vec::new(),
        ticket: fake.ticket(),
    });
    request
        .metadata_mut()
        .insert("authorization", "Bearer t".parse().unwrap());
    request
}

/// A real cache on a loopback port, answering to `upstream`, with its
/// log where the caller can read it.
/// A started cache and everything that must outlive it.
struct CacheRun {
    _dir: TempDir,
    /// The child lives while the run does; the tests only read the log.
    _bin: Bin,
    log: PathBuf,
}

fn spawn_cache(upstream: SocketAddr, extra: &[&str]) -> CacheRun {
    let dir = private_dir();
    let log = dir.path().join("cache.log");
    let mut args: Vec<&str> = vec!["--listen", "127.0.0.1:0"];
    let url = format!("http://{upstream}");
    args.push("--upstream");
    args.push(&url);
    args.extend_from_slice(extra);
    let bin = spawn_bin(env!("CARGO_BIN_EXE_scion-cache"), &args, &log);
    CacheRun {
        _dir: dir,
        _bin: bin,
        log,
    }
}

// --- R1: the removed secret flags still refuse, with the removal's own
// words, before the parser sees the line.

#[test]
fn the_cache_refuses_a_removed_secret_flag_in_its_own_words() {
    let out = Command::new(env!("CARGO_BIN_EXE_scion-cache"))
        .arg("--token")
        .arg("x")
        .output()
        .expect("scion-cache runs");
    assert_eq!(out.status.code(), Some(2), "the removed flag exits 2");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("flag is gone"),
        "the refusal names the removal: {err}"
    );
    assert!(
        err.contains("SCION_TOKEN"),
        "the refusal says where the token goes: {err}"
    );
}

#[test]
fn the_daemon_refuses_a_removed_secret_flag_in_its_own_words() {
    let out = Command::new(env!("CARGO_BIN_EXE_scion-daemon"))
        .arg("--upstream-token")
        .arg("x")
        .output()
        .expect("scion-daemon runs");
    assert_eq!(out.status.code(), Some(2), "the removed flag exits 2");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("flag is gone"),
        "the refusal names the removal: {err}"
    );
    assert!(
        err.contains("SCION_UPSTREAM_TOKEN"),
        "the refusal says where the token goes: {err}"
    );
}

// --- R5: the daemon's seed warns about a snapshot that holds bad
// objects, and stays quiet about a good one.

/// A snapshot of two blobs, as a scion-cache writes it. When `torn`,
/// the second object's key names other bytes, so the seed skips it.
fn write_seed_snapshot(dir: &Path, torn: bool) -> PathBuf {
    let good = b"seed me\n";
    let other = b"other bytes\n";
    let stored: &[u8] = if torn { good } else { other };
    let objects = [
        (
            Key::Blob(BlobId::of(good)),
            Bytes::from(object::pack(good).unwrap()),
        ),
        (
            Key::Blob(BlobId::of(other)),
            Bytes::from(object::pack(stored).unwrap()),
        ),
    ];
    let path = dir.join("r.scnsnap");
    snapshot::write(&path, "r", &objects).unwrap();
    path
}

/// Starts the daemon seeding `snapshot` into a cold store, and waits
/// until it serves.
fn seeded_daemon(dir: &TempDir, snapshot: &Path) -> Bin {
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::set_permissions(&data, fs::Permissions::from_mode(0o700)).unwrap();
    let log = dir.path().join("daemon.log");
    let bin = spawn_bin(
        env!("CARGO_BIN_EXE_scion-daemon"),
        &[
            "--data",
            data.to_str().unwrap(),
            "--upstream",
            "http://127.0.0.1:1",
            "--seed",
            snapshot.to_str().unwrap(),
            "--seed-full-repo",
        ],
        &log,
    );
    wait_for_log(&log, "serving");
    bin
}

#[test]
fn a_seed_with_bad_objects_warns_the_operator() {
    let dir = private_dir();
    let torn = write_seed_snapshot(dir.path(), true);
    let bin = seeded_daemon(&dir, &torn);
    let log = fs::read_to_string(&bin.log).unwrap();
    assert!(
        log.contains("the snapshot holds bad objects or is torn"),
        "a seed that skips an object must warn: {log}"
    );
}

#[test]
fn a_seed_of_good_objects_does_not_warn() {
    let dir = private_dir();
    let clean = write_seed_snapshot(dir.path(), false);
    let bin = seeded_daemon(&dir, &clean);
    let log = fs::read_to_string(&bin.log).unwrap();
    assert!(
        !log.contains("the snapshot holds bad objects or is torn"),
        "a clean seed must not warn: {log}"
    );
    assert!(
        log.contains("seeded the store from a snapshot"),
        "the clean seed is logged as seeded: {log}"
    );
}

// --- R6: the cache verifies tickets against the operator's keys, and a
// call that presents one of theirs is served.

#[tokio::test]
async fn a_call_with_a_ticket_the_operator_key_signed_is_served() {
    let fake = Fake::new(false);
    let upstream = serve_fake(&fake).await;
    let key = fake.public_key_hex();
    let run = spawn_cache(upstream, &["--ticket-public-key", &key]);
    let log = run.log.clone();
    let addr = serving_addr(&log);
    let mut client = tcp_client(addr).await;
    let mut stream = client
        .get_objects(blob_request(&fake))
        .await
        .expect("the call with the operator's ticket is served")
        .into_inner();
    let part = stream
        .message()
        .await
        .expect("the reply reads")
        .expect("the blob's part arrives");
    assert_eq!(part.id, fake.blob, "the served part is the asked blob");
}

// --- R4: the cache starts with a TLS cert and key, and serves.

#[tokio::test]
async fn the_cache_serves_with_a_tls_cert_and_key() {
    let fake = Fake::new(false);
    let dir = private_dir();
    let tls = dir.path().join("tls");
    fs::create_dir_all(&tls).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(vec!["localhost".to_owned()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let cert_path = tls.join("tls.pem");
    let key_path = tls.join("tls.key");
    fs::write(&cert_path, cert.pem()).unwrap();
    fs::write(&key_path, key.serialize_pem()).unwrap();
    fs::set_permissions(&cert_path, fs::Permissions::from_mode(0o644)).unwrap();
    fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600)).unwrap();

    let log = dir.path().join("cache.log");
    let upstream = serve_fake(&fake).await;
    let _bin = spawn_bin(
        env!("CARGO_BIN_EXE_scion-cache"),
        &[
            "--listen",
            "127.0.0.1:0",
            "--upstream",
            &format!("http://{upstream}"),
            "--ticket-public-key",
            &fake.public_key_hex(),
            "--tls-cert",
            cert_path.to_str().unwrap(),
            "--tls-key",
            key_path.to_str().unwrap(),
        ],
        &log,
    );
    let addr = serving_addr(&log);
    assert_ne!(addr.port(), 0, "the TLS cache serves");
    TcpStream::connect(addr).expect("the TLS cache accepts connections");
}

// --- R3: the snapshot task writes one file per --snapshot-repo, holding
// the objects that went into the cache's replies.

#[tokio::test]
async fn the_cache_writes_a_snapshot_of_each_repo() {
    let fake = Fake::new(false);
    let upstream = serve_fake(&fake).await;
    let dir = private_dir();
    let snaps = dir.path().join("snaps");
    let log = dir.path().join("cache.log");
    let bin = spawn_bin(
        env!("CARGO_BIN_EXE_scion-cache"),
        &[
            "--listen",
            "127.0.0.1:0",
            "--upstream",
            &format!("http://{upstream}"),
            "--ticket-public-key",
            &fake.public_key_hex(),
            "--snapshot",
            snaps.to_str().unwrap(),
            "--snapshot-repo",
            "r",
            "--snapshot-every",
            "1",
        ],
        &log,
    );
    let addr = serving_addr(&log);
    let mut client = tcp_client(addr).await;
    let mut stream = client
        .get_objects(blob_request(&fake))
        .await
        .expect("the call is served")
        .into_inner();
    let part = stream
        .message()
        .await
        .expect("the reply reads")
        .expect("a part");
    assert_eq!(part.id, fake.blob);

    let file = snaps.join("r.scnsnap");
    let started = std::time::Instant::now();
    while !file.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the snapshot file never appeared"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // The file is a snapshot of repo `r`: it starts as one and names it.
    let bytes = fs::read(&file).unwrap();
    assert!(
        bytes.starts_with(snapshot::MAGIC),
        "the file starts as a snapshot: {:?}",
        &bytes[..bytes.len().min(16)]
    );
    let len = u16::from_le_bytes([bytes[9], bytes[10]]) as usize;
    assert_eq!(&bytes[11..11 + len], b"r", "the snapshot names repo r");
    drop(bin);
}

// --- R2: the operator's limits ride to the running serve, the way
// tests/serve_limits.rs pins the server's.

/// The client preface, so a raw connection speaks HTTP/2 from byte one.
const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// The settings a client must send, empty, to get the server's.
fn empty_settings() -> [u8; 9] {
    [0, 0, 0, 0x4, 0, 0, 0, 0, 0]
}

/// Reads the server's SETTINGS frame and returns its
/// `MAX_CONCURRENT_STREAMS` value.
fn max_concurrent_streams(stream: &mut impl Read) -> u32 {
    let mut header = [0_u8; 9];
    let mut at = 0;
    while at < 9 {
        match stream.read(&mut header[at..]) {
            Ok(0) => panic!("the server closed before its settings were read"),
            Ok(n) => at += n,
            Err(err) => panic!("the settings read failed: {err}"),
        }
    }
    assert_eq!(header[3], 0x4, "the first frame is a SETTINGS frame");
    let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
    let mut payload = vec![0_u8; len];
    let mut at = 0;
    while at < len {
        match stream.read(&mut payload[at..]) {
            Ok(0) => panic!("the server closed in its settings"),
            Ok(n) => at += n,
            Err(err) => panic!("the settings read failed: {err}"),
        }
    }
    (0..len / 6)
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
        .map_or_else(
            || panic!("the settings name MAX_CONCURRENT_STREAMS: {payload:?}"),
            |(_, value)| value,
        )
}

#[tokio::test]
async fn the_cache_streams_limit_rides_the_settings_frame() {
    let fake = Fake::new(false);
    let upstream = serve_fake(&fake).await;
    let run = spawn_cache(
        upstream,
        &[
            "--ticket-public-key",
            &fake.public_key_hex(),
            "--max-streams",
            "1",
        ],
    );
    let log = run.log.clone();
    let addr = serving_addr(&log);
    let mut stream = TcpStream::connect(addr).expect("the client connects");
    stream.write_all(PREFACE).expect("the preface writes");
    stream
        .write_all(&empty_settings())
        .expect("the settings write");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    assert_eq!(
        max_concurrent_streams(&mut stream),
        1,
        "the operator's streams limit is announced"
    );
}

#[test]
fn the_daemon_streams_limit_rides_the_settings_frame() {
    let dir = private_dir();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::set_permissions(&data, fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("daemon.sock");
    let log = dir.path().join("daemon.log");
    let _bin = spawn_bin(
        env!("CARGO_BIN_EXE_scion-daemon"),
        &[
            "--data",
            data.to_str().unwrap(),
            "--socket",
            socket.to_str().unwrap(),
            "--upstream",
            "http://127.0.0.1:1",
            "--max-streams",
            "1",
        ],
        &log,
    );
    wait_for_log(&log, "serving");
    let mut stream = StdUnixStream::connect(&socket).expect("the client connects");
    stream.write_all(PREFACE).expect("the preface writes");
    stream
        .write_all(&empty_settings())
        .expect("the settings write");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    assert_eq!(
        max_concurrent_streams(&mut stream),
        1,
        "the operator's streams limit is announced"
    );
}

/// The body of a Prometheus scrape at `addr`, over plain HTTP.
fn scrape(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).expect("the metrics listener answers");
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

#[tokio::test]
async fn a_cache_connection_over_the_connections_limit_is_refused_and_counted() {
    use tokio::io::AsyncWriteExt as _;
    let fake = Fake::new(true);
    let upstream = serve_fake(&fake).await;
    let dir = private_dir();
    let log = dir.path().join("cache.log");
    let _bin = spawn_bin(
        env!("CARGO_BIN_EXE_scion-cache"),
        &[
            "--listen",
            "127.0.0.1:0",
            "--upstream",
            &format!("http://{upstream}"),
            "--ticket-public-key",
            &fake.public_key_hex(),
            "--max-connections",
            "1",
            "--metrics-listen",
            "127.0.0.1:0",
        ],
        &log,
    );
    let addr = serving_addr(&log);
    let metrics_line = wait_for_log(&log, "serving metrics");
    let metrics_addr: SocketAddr = metrics_line
        .split("listen=")
        .nth(1)
        .unwrap_or_else(|| panic!("the metrics line names its address: {metrics_line}"))
        .split_whitespace()
        .next()
        .expect("the metrics line names an address")
        .parse()
        .expect("the metrics line names a socket address");

    // The first connection is served and holds the one place: its call
    // never ends, because the upstream's reply never ends.
    let mut first = tcp_client(addr).await;
    let held = first
        .get_objects(blob_request(&fake))
        .await
        .expect("the first call is served")
        .into_inner();

    // The second connection waits for a place, and is closed when none
    // frees. A connection that is served instead answers with its own
    // SETTINGS bytes.
    let second = tokio::net::TcpStream::connect(addr)
        .await
        .expect("the second connects");
    let (mut read_half, mut write_half) = second.into_split();
    write_half
        .write_all(PREFACE)
        .await
        .expect("the preface writes");
    let mut byte = [0_u8; 1];
    let read = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::io::AsyncReadExt::read(&mut read_half, &mut byte),
    )
    .await;
    match read {
        Ok(Ok(0)) => {}
        Ok(Err(err)) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
        Ok(Ok(_)) => panic!("a connection over the limit was served"),
        Ok(Err(err)) => panic!("the second connection errored instead of closing: {err}"),
        Err(elapsed) => {
            panic!("the second connection was neither served nor closed in 60 s: {elapsed}")
        }
    }

    let body = scrape(metrics_addr);
    let count = body
        .lines()
        .find(|line| line.starts_with("scion_connections_refused_total"))
        .unwrap_or_else(|| panic!("the refusal count is served: {body}"))
        .split_whitespace()
        .last()
        .expect("the counter names its value")
        .to_owned();
    assert_eq!(count, "1", "the served-over connection is counted once");
    drop(held);
}

/// The daemon's client side over its socket, for the connections test.
async fn daemon_client(socket: PathBuf) -> ObjectsClient<tonic::transport::Channel> {
    let channel = Endpoint::try_from("http://daemon")
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(60))
        .connect_with_connector(service_fn(move |_: tonic::transport::Uri| {
            let socket = socket.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(socket).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .unwrap();
    ObjectsClient::new(channel)
}

#[tokio::test]
async fn a_daemon_connection_over_the_connections_limit_is_refused() {
    use tokio::io::AsyncWriteExt as _;
    // The upstream chain: the fake's reply never ends, so a call through
    // the cache holds the daemon's one place.
    let fake = Fake::new(true);
    let upstream = serve_fake(&fake).await;
    let cache_run = spawn_cache(upstream, &["--ticket-public-key", &fake.public_key_hex()]);
    let cache_log = cache_run.log.clone();
    let cache_addr = serving_addr(&cache_log);

    let dir = private_dir();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::set_permissions(&data, fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("daemon.sock");
    let log = dir.path().join("daemon.log");
    let _bin = spawn_bin(
        env!("CARGO_BIN_EXE_scion-daemon"),
        &[
            "--data",
            data.to_str().unwrap(),
            "--socket",
            socket.to_str().unwrap(),
            "--upstream",
            &format!("http://{cache_addr}"),
            "--max-connections",
            "1",
        ],
        &log,
    );
    wait_for_log(&log, "serving");

    let mut first = daemon_client(socket.clone()).await;
    let held = first
        .get_objects(blob_request(&fake))
        .await
        .expect("the first call is served")
        .into_inner();

    let second = tokio::net::UnixStream::connect(&socket)
        .await
        .expect("the second connects");
    let (mut read_half, mut write_half) = second.into_split();
    write_half
        .write_all(PREFACE)
        .await
        .expect("the preface writes");
    let mut byte = [0_u8; 1];
    let read = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::io::AsyncReadExt::read(&mut read_half, &mut byte),
    )
    .await;
    match read {
        Ok(Ok(0)) => {}
        Ok(Err(err)) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
        Ok(Ok(_)) => panic!("a connection over the limit was served"),
        Ok(Err(err)) => panic!("the second connection errored instead of closing: {err}"),
        Err(elapsed) => {
            panic!("the second connection was neither served nor closed in 60 s: {elapsed}")
        }
    }
    drop(held);
}

/// A reply whose parked bytes cross the operator's
/// `--max-stalled-bytes` loses its connection: the client that stopped
/// reading sees the connection end, on a limit no default would cross.
/// The raw h2 client keeps the flow window shut without releasing it,
/// and the test waits on the connection's end, never on a clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cache_reply_over_the_stalled_bytes_cap_ends_its_connection() {
    let fake = Fake::new(true);
    let upstream = serve_fake(&fake).await;
    let run = spawn_cache(
        upstream,
        &[
            "--ticket-public-key",
            &fake.public_key_hex(),
            "--max-stalled-bytes",
            "1",
        ],
    );
    let log = run.log.clone();
    let addr = serving_addr(&log);

    let tcp = tokio::net::TcpStream::connect(addr)
        .await
        .expect("the client connects");
    let (mut h2, connection) = h2::client::handshake(tcp)
        .await
        .expect("the handshake lands");
    let connection = tokio::spawn(async move {
        let _ = connection.await;
    });
    let request = GetObjectsRequest {
        refs: vec![ObjectRef {
            kind: ObjectKind::Blob.into(),
            id: fake.blob.clone(),
            path: b"f".to_vec(),
        }],
        accept: Vec::new(),
        ticket: fake.ticket(),
    };
    let message = request.encode_to_vec();
    let mut body = vec![0_u8; 5];
    let [a, b, c, d] = u32::try_from(message.len())
        .expect("the message fits u32")
        .to_be_bytes();
    body[1..5].copy_from_slice(&[a, b, c, d]);
    body.extend_from_slice(&message);
    let built = http::Request::builder()
        .method(http::Method::POST)
        .uri("http://x/scion.v1.Objects/GetObjects")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("authorization", "Bearer t")
        .body(())
        .unwrap();
    let (reply, mut send) = h2.send_request(built, false).expect("the call opens");
    send.send_data(body.into(), true).expect("the body sends");
    let response = tokio::time::timeout(Duration::from_secs(60), reply)
        .await
        .expect("the reply's headers arrive in 60 s")
        .expect("the reply's headers arrive");
    let mut reply = response.into_body();
    // The first part arrives; its flow window is never released, so the
    // rest of the 16 MiB object has nowhere to go. The reply parks at
    // the serve's own 120 s stall timeout, and its parked 4 MiB estimate
    // crosses the one-byte cap at once, so the serve closes the
    // connection. The 180 s bound is the serve's 120 s plus a margin,
    // never an assert on when; the assert is on the connection ending.
    let first = tokio::time::timeout(Duration::from_secs(60), reply.data())
        .await
        .expect("the first part arrives in 60 s")
        .expect("the first part reads")
        .expect("the first part carries bytes");
    assert!(!first.is_empty());
    let ended = tokio::time::timeout(Duration::from_secs(180), connection)
        .await
        .is_ok();
    assert!(
        ended,
        "the stalled connection never ended within 180 s; cache log: {}",
        fs::read_to_string(&run.log).unwrap_or_default()
    );
}

/// The daemon builds its own `Limits`, so its stalled-bytes cap needs
/// its own proof: a reply parked past the operator's
/// `--max-stalled-bytes` ends its connection, over the daemon's unix
/// socket, the way the cache's does over TCP.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_daemon_reply_over_the_stalled_bytes_cap_ends_its_connection() {
    // The upstream chain: the cache fetches the big object from the
    // fake; the daemon relays it; the client on the daemon's socket
    // stops reading after one part.
    let fake = Fake::new(true);
    let upstream = serve_fake(&fake).await;
    let cache_run = spawn_cache(upstream, &["--ticket-public-key", &fake.public_key_hex()]);
    let cache_addr = serving_addr(&cache_run.log);

    let dir = private_dir();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::set_permissions(&data, fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("daemon.sock");
    let log = dir.path().join("daemon.log");
    let _bin = spawn_bin(
        env!("CARGO_BIN_EXE_scion-daemon"),
        &[
            "--data",
            data.to_str().unwrap(),
            "--socket",
            socket.to_str().unwrap(),
            "--upstream",
            &format!("http://{cache_addr}"),
            "--max-stalled-bytes",
            "1",
        ],
        &log,
    );
    wait_for_log(&log, "serving");

    let unix = tokio::net::UnixStream::connect(&socket)
        .await
        .expect("the client connects");
    let (mut h2, connection) = h2::client::handshake(unix)
        .await
        .expect("the handshake lands");
    let connection = tokio::spawn(async move {
        let _ = connection.await;
    });
    let request = GetObjectsRequest {
        refs: vec![ObjectRef {
            kind: ObjectKind::Blob.into(),
            id: fake.blob.clone(),
            path: b"f".to_vec(),
        }],
        accept: Vec::new(),
        ticket: fake.ticket(),
    };
    let message = request.encode_to_vec();
    let mut body = vec![0_u8; 5];
    let [a, b, c, d] = u32::try_from(message.len())
        .expect("the message fits u32")
        .to_be_bytes();
    body[1..5].copy_from_slice(&[a, b, c, d]);
    body.extend_from_slice(&message);
    let built = http::Request::builder()
        .method(http::Method::POST)
        .uri("http://x/scion.v1.Objects/GetObjects")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("authorization", "Bearer t")
        .body(())
        .unwrap();
    let (reply, mut send) = h2.send_request(built, false).expect("the call opens");
    send.send_data(body.into(), true).expect("the body sends");
    let response = tokio::time::timeout(Duration::from_secs(60), reply)
        .await
        .expect("the reply's headers arrive in 60 s")
        .expect("the reply's headers arrive");
    let mut reply = response.into_body();
    // The window stays shut past the first part, the reply parks at the
    // serve's own 120 s stall timeout, and the parked 4 MiB estimate
    // crosses the one-byte cap at once, so the serve closes the
    // connection. The bound is the serve's 120 s plus a margin, never
    // an assert on when.
    let first = tokio::time::timeout(Duration::from_secs(60), reply.data())
        .await
        .expect("the first part arrives in 60 s")
        .expect("the first part reads")
        .expect("the first part carries bytes");
    assert!(!first.is_empty());
    let ended = tokio::time::timeout(Duration::from_secs(180), connection)
        .await
        .is_ok();
    assert!(
        ended,
        "the stalled connection never ended within 180 s; daemon log: {}",
        fs::read_to_string(&log).unwrap_or_default()
    );
}
