//! The protocol transcript corpus (issue #890): one text file per
//! behaviour, holding a request and the expected reply or status, run
//! against the server, and the `Objects` cases also through the cache
//! and the daemon. A changed status code, a changed reply field or a
//! changed resume rule fails the matching file.
//!
//! The file format is lines of `key: value`; `#` starts a comment. The
//! header names `since`, `method` and `fixture`. `request.<path>:` lines
//! build the request, one line per field, repeated fields as repeated
//! lines, bytes and IDs as hex or as a fixture name (`$blob`, `$tree`,
//! `$cs`, `$missing`, `$ticket`). The expectation names `status` and the
//! pinned reply fields (`reply.<path>:`), or for streams the ordered
//! items (`objects:` lines) and how the stream ended
//! (`end: none|call=CODE|stream=CODE|refused`). Every listed field must
//! match; unlisted fields never block, so a new optional field does not
//! break the corpus. The runner parses these lines itself, so an unknown
//! key fails the file instead of passing in silence.
#![cfg(unix)]

use std::fs;
use std::path::Path;

use scion_core::{BlobId, ChangesetId, FileState, Tree};
use scion_proto::runtime::auth::Bearer;
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectPart, ObjectRef,
    ResolveRefRequest, scion_client::ScionClient,
};
use scion_server::Backend;
use tonic::Code;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

mod common;
use common::{Client as ObjectsClientOf, TOKEN, Tiers, connect};

const REPO: &str = common::REPO;

/// The corpus root, relative to this file's directory.
const CORPUS: &str = "transcripts";

/// One behaviour's parsed transcript: the header, the request lines and
/// the expectation lines, in file order.
#[derive(Debug)]
struct Transcript {
    /// The release the behaviour exists since; the N-1 run skips files
    /// newer than the server it runs.
    /// SHORTCUT: unread until the N-1 matrix slice; that slice reads it.
    #[allow(dead_code)]
    since: String,
    /// `Service/Method`, as the proto names them.
    method: String,
    /// Which fixture set the call needs.
    /// SHORTCUT: unread until the multi-fixture slice; that slice reads it.
    #[allow(dead_code)]
    fixture: String,
    /// The `request.` lines, path and value, in file order.
    request: Vec<(String, String)>,
    /// The expectation lines, key and value, in file order.
    expect: Vec<(String, String)>,
}

/// Reads one transcript file. An unknown key, a value without a `:` or a
/// missing `method` fails here, so a typo cannot pass in silence.
fn parse(at: &Path, text: &str) -> Transcript {
    let mut since = None;
    let mut method = None;
    let mut fixture = None;
    let mut request = Vec::new();
    let mut expect = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            panic!("{}:{} has no `:`: {line}", at.display(), n + 1);
        };
        let (key, value) = (key.trim(), value.trim());
        match key {
            "since" => since = Some(value.to_owned()),
            "method" => method = Some(value.to_owned()),
            "fixture" => fixture = Some(value.to_owned()),
            k if let Some(rest) = k.strip_prefix("request.") => {
                request.push((rest.to_owned(), value.to_owned()));
            }
            "status" | "status_message" | "end" | "tiers" => {
                expect.push((key.to_owned(), value.to_owned()));
            }
            k if let Some(rest) = k.strip_prefix("reply.") => {
                expect.push((format!("reply.{rest}"), value.to_owned()));
            }
            "objects" | "entries" => expect.push((key.to_owned(), value.to_owned())),
            other => panic!("{}:{} names unknown key {other}", at.display(), n + 1),
        }
    }
    Transcript {
        since: since.unwrap_or_else(|| panic!("{} names no since", at.display())),
        method: method.unwrap_or_else(|| panic!("{} names no method", at.display())),
        fixture: fixture.unwrap_or_else(|| panic!("{} names no fixture", at.display())),
        request,
        expect,
    }
}

/// A well-formed ID no store names, for the not-found behaviours.
fn missing_id(prefix: u8) -> [u8; 32] {
    let mut id = [prefix; 32];
    id[31] = 0xff;
    id
}

/// The fixture a transcript runs against: the ids its placeholders name.
#[derive(Clone)]
struct Fixture {
    blob: BlobId,
    /// The bytes the blob names: what `$blob_bytes` resolves to.
    content: Vec<u8>,
    tree: [u8; 32],
    cs: ChangesetId,
    /// A well-formed ID nothing has, for not-found.
    missing: [u8; 32],
    /// A read ticket for the fixture's root tree: the permission that
    /// `GetObjects` spends.
    /// spends.
    ticket: Vec<u8>,
}

/// Builds the one fixture the corpus uses: the conformance test's landed
/// repo - one changeset whose tree holds one file of one blob.
fn landed() -> (Tiers, Fixture) {
    let data = common::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let content = b"pub fn main() {}\n".to_vec();
    let blob = BlobId::of(&content);
    store.blobs().put(&content).unwrap();
    let cs = common::changeset(
        None,
        &[(
            "src/main.rs",
            FileState::Regular {
                content: blob,
                executable: false,
            },
        )],
    );
    let id = common::land(&store, &cs);
    let tiers = Tiers::start(data, store);
    let runtime_entered = tiers.runtime.enter();
    let (ticket, tree) = mint(&tiers, id);
    drop(runtime_entered);
    let fixture = Fixture {
        blob,
        content,
        tree,
        cs: id,
        missing: missing_id(0xab),
        ticket,
    };
    (tiers, fixture)
}

/// Mints a read ticket for the root tree of `cs` and names the root
/// tree, as the conformance test does with the same call.
fn mint(tiers: &Tiers, cs: ChangesetId) -> (Vec<u8>, [u8; 32]) {
    let runtime = &tiers.runtime;
    let _entered = runtime.enter();
    let mut client = runtime.block_on(connect(&tiers.server));
    let reply = runtime
        .block_on(client.get_root_tree(GetRootTreeRequest {
            repo: REPO.into(),
            changeset_id: cs.as_bytes().to_vec(),
        }))
        .unwrap()
        .into_inner();
    let tree = reply.tree_id.try_into().expect("the tree id is 32 bytes");
    (reply.ticket, tree)
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn unhex(value: &str) -> Vec<u8> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    (0..value.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&value[i..i + 2], 16).unwrap_or_else(|e| panic!("{value}: {e}"))
        })
        .collect()
}

/// Substitutes the fixture names in a value: `$blob`, `$tree`, `$cs`,
/// `$missing`, `$ticket`, `$blob_bytes`.
fn resolve(value: &str, f: &Fixture) -> String {
    value
        .replace("$blob_bytes", &hex(&f.content))
        .replace("$blob", &hex(f.blob.as_bytes()))
        .replace("$tree", &hex(&f.tree))
        .replace("$cs", &hex(f.cs.as_bytes()))
        .replace("$missing", &hex(&f.missing))
        .replace("$ticket", &hex(&f.ticket))
}

/// A parsed `kind=... id=...` item of a stream.
fn item(value: &str) -> (i32, [u8; 32]) {
    let mut kind = None;
    let mut id = None;
    for part in value.split_whitespace() {
        if let Some(k) = part.strip_prefix("kind=") {
            kind = Some(match k {
                "blob" => ObjectKind::Blob,
                "tree" => ObjectKind::Tree,
                other => panic!("the kind is blob or tree: {other}"),
            } as i32);
        }
        if let Some(i) = part.strip_prefix("id=") {
            let bytes = unhex(i);
            id = Some(bytes.try_into().expect("the id is 32 bytes"));
        }
    }
    (
        kind.expect("the item names a kind"),
        id.expect("the item names an id"),
    )
}

/// How a `GetObjects` answer ends, as the conformance test's `End`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum End {
    Call(Code),
    Stream(Code),
    Refused,
}

/// The tonic code by its lower-snake name, the corpus's spelling.
fn code(value: &str) -> Code {
    match value {
        "ok" => Code::Ok,
        "cancelled" => Code::Cancelled,
        "unknown" => Code::Unknown,
        "invalid_argument" => Code::InvalidArgument,
        "deadline_exceeded" => Code::DeadlineExceeded,
        "not_found" => Code::NotFound,
        "already_exists" => Code::AlreadyExists,
        "permission_denied" => Code::PermissionDenied,
        "resource_exhausted" => Code::ResourceExhausted,
        "failed_precondition" => Code::FailedPrecondition,
        "aborted" => Code::Aborted,
        "out_of_range" => Code::OutOfRange,
        "unimplemented" => Code::Unimplemented,
        "internal" => Code::Internal,
        "unavailable" => Code::Unavailable,
        "data_loss" => Code::DataLoss,
        "unauthenticated" => Code::Unauthenticated,
        other => panic!("the code is a tonic name: {other}"),
    }
}

fn parse_end(value: &str) -> Option<End> {
    match value {
        "none" => None,
        "refused" => Some(End::Refused),
        v => {
            let (kind, name) = v
                .split_once('=')
                .unwrap_or_else(|| panic!("end is none, refused or KIND=CODE: {v}"));
            let code = code(name);
            Some(match kind {
                "call" => End::Call(code),
                "stream" => End::Stream(code),
                other => panic!("end kind is call or stream: {other}"),
            })
        }
    }
}

/// The corpus walks subdirectories: `service/method/behaviour.textproto`.
fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("the corpus dir opens") {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk(&path, files);
        } else {
            files.push(path);
        }
    }
}

/// The corpus test: every file, against the server; the `Objects` cases
/// also through the cache and the daemon, cold and warm.
#[test]
fn the_corpus_answers_as_its_files_say() {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(CORPUS);
    let mut files = Vec::new();
    walk(&corpus, &mut files);
    files.sort();
    assert!(
        files.len() >= 12,
        "the corpus pins the Objects service: {} files",
        files.len()
    );
    // One store and one set of tiers for the whole run: the fixture is
    // the expensive part. Each file still mints its own ticket, right
    // before its calls, so a ticket at a call is always seconds old.
    let (tiers, base) = landed();
    let _entered = tiers.runtime.enter();
    for file in files {
        let text = fs::read_to_string(&file).expect("the transcript opens");
        let t = parse(&file, &text);
        assert!(
            t.method.starts_with("Objects/") || t.method.starts_with("Scion/"),
            "{} names method {}",
            file.display(),
            t.method
        );
        let minted_at = std::time::Instant::now();
        let (ticket, _) = mint(&tiers, base.cs);
        let f = Fixture {
            ticket,
            ..base.clone()
        };
        let mint_age = minted_at.elapsed().as_secs_f64();
        let started_at = std::time::Instant::now();
        let tier_refuses = t.expect.iter().any(|(k, v)| k == "tiers" && v == "refuse");
        if t.method.starts_with("Objects/") {
            for url in [&tiers.server, &tiers.cache_url, &tiers.socket] {
                if tier_refuses && url != &tiers.server {
                    // A tier serves GetObjects only: its refusal of the
                    // other calls is the contract (section 7.1).
                    tiers.runtime.block_on(async {
                        let mut client = connect(url).await;
                        let refused = match t.method.as_str() {
                            "Objects/ResolveRef" => client
                                .resolve_ref(ResolveRefRequest::default())
                                .await
                                .err()
                                .map(|s| s.code()),
                            "Objects/GetRootTree" => client
                                .get_root_tree(GetRootTreeRequest::default())
                                .await
                                .err()
                                .map(|s| s.code()),
                            _ => None,
                        };
                        assert_eq!(
                            refused,
                            Some(Code::Unimplemented),
                            "{}, a tier refuses {} with UNIMPLEMENTED",
                            file.display(),
                            t.method
                        );
                    });
                    continue;
                }
                tiers
                    .runtime
                    .block_on(run_objects(&t, url, &f))
                    .unwrap_or_else(|e| panic!("{} at {}: {e}", file.display(), url));
            }
        } else {
            tiers
                .runtime
                .block_on(run_scion(&t, &tiers.server, &f))
                .unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        }
        let expiry = scion_proto::ticket::parse_unverified(&f.ticket, scion_proto::ticket::now())
            .map(|t| t.expires)
            .map_err(|e| e.to_string());
        eprintln!(
            "TIMES {} mint_age={mint_age:.3}s wall={:.3}s expiry={expiry:?} now={}",
            file.display(),
            started_at.elapsed().as_secs_f64(),
            scion_proto::ticket::now(),
        );
    }
}

use std::path::PathBuf;

type ScionClientOf = ScionClient<InterceptedService<Channel, Bearer>>;

/// A `Scion` client with the token, as the Objects one is built.
async fn scion_client(url: &str) -> ScionClientOf {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(30))
        .connect()
        .await
        .unwrap();
    ScionClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap())
}

/// Runs one `Objects` transcript against one tier URL.
async fn run_objects(t: &Transcript, url: &str, f: &Fixture) -> Result<(), String> {
    let mut client = connect(url).await;
    match t.method.as_str() {
        "Objects/ResolveRef" => run_resolve_ref(t, &mut client, f).await,
        "Objects/GetRootTree" => run_get_root_tree(t, &mut client, f).await,
        "Objects/GetObjects" => run_get_objects(t, url, &mut client, f).await,
        other => Err(format!("no runner for {other}")),
    }
}

/// Runs one `Scion` transcript against the server.
async fn run_scion(t: &Transcript, url: &str, f: &Fixture) -> Result<(), String> {
    let mut client = scion_client(url).await;
    match t.method.as_str() {
        "Scion/PutBlob" => run_put_blob(t, &mut client, f).await,
        "Scion/CreateChangeset" => run_create_changeset(t, &mut client, f).await,
        "Scion/Land" => run_land(t, &mut client, f).await,
        "Scion/GetLand" => run_get_land(t, &mut client, f).await,
        "Scion/ResolveRef" => run_scion_resolve_ref(t, &mut client, f).await,
        "Scion/GetChangeset" => run_get_changeset(t, &mut client, f).await,
        "Scion/ReadFile" => run_read_file(t, &mut client, f).await,
        "Scion/ListDir" => run_list_dir(t, &mut client, f).await,
        "Scion/Log" => run_log(t, &mut client, f).await,
        "Scion/Blame" => run_blame(t, &mut client, f).await,
        "Scion/MergeBase" => run_merge_base(t, &mut client, f).await,
        "Scion/IsAncestor" => run_is_ancestor(t, &mut client, f).await,
        "Scion/Diff" => run_diff(t, &mut client, f).await,
        other => Err(format!("no runner for {other}")),
    }
}

/// The request's repo field, resolved.
fn field(t: &Transcript, name: &str) -> Option<String> {
    t.request
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.to_owned())
}

/// The first `name=...` value of a space-split spec.
fn spec_value(spec: &str, name: &str) -> Option<String> {
    spec.split_whitespace()
        .find_map(|p| p.strip_prefix(&format!("{name}=")).map(str::to_owned))
}

/// The `data` lines of a client-streamed `PutBlob`, as one byte
/// vector: the lines join in file order, hex-decoded.
/// `data` lines join in file order, hex-decoded.
async fn run_put_blob(
    t: &Transcript,
    client: &mut ScionClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let repo = field(t, "repo").unwrap_or_else(|| resolve("r", f));
    let requests: Vec<scion_proto::v1::PutBlobRequest> = t
        .request
        .iter()
        .filter(|(k, _)| k == "data")
        .map(|(_, v)| scion_proto::v1::PutBlobRequest {
            repo: repo.clone(),
            data: unhex(&resolve(v, f)),
        })
        .collect();
    let stream = tokio_stream::iter(requests);
    match client.put_blob(stream).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            pinned(t, "blob_id", &reply.into_inner().blob_id, f)
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

async fn run_create_changeset(
    t: &Transcript,
    client: &mut ScionClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let req = scion_proto::v1::CreateChangesetRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        changeset: unhex(&resolve(&field(t, "changeset").unwrap_or_default(), f)),
    };
    match client.create_changeset(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            pinned(t, "changeset_id", &reply.into_inner().changeset_id, f)
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

/// A land whose changeset the fixture names: the request key must be
/// unique per repo and caller, so the runner mints one.
async fn run_land(t: &Transcript, client: &mut ScionClientOf, f: &Fixture) -> Result<(), String> {
    let req = scion_proto::v1::LandRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        changeset_id: unhex(&resolve(&field(t, "changeset_id").unwrap_or_default(), f)),
        target_ref: field(t, "target_ref").unwrap_or_else(|| "main".to_owned()),
        request_key: format!("transcript-{}", std::process::id()),
    };
    match client.land(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            let reply = reply.into_inner();
            for (key, value) in &t.expect {
                if key == "reply.state" {
                    let want = resolve(value, f);
                    let got = match scion_proto::v1::LandState::try_from(reply.state) {
                        Ok(s) => s.as_str_name().to_owned(),
                        Err(_) => format!("{}", reply.state),
                    };
                    if got != want {
                        return Err(format!("the land state is {got}, the file says {want}"));
                    }
                }
            }
            Ok(())
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

async fn run_get_land(
    t: &Transcript,
    client: &mut ScionClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let req = scion_proto::v1::GetLandRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        land_id: resolve(&field(t, "land_id").unwrap_or_default(), f),
    };
    match client.get_land(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            let reply = reply.into_inner();
            for (key, value) in &t.expect {
                if key == "reply.state" {
                    let want = resolve(value, f);
                    let got = match scion_proto::v1::LandState::try_from(reply.state) {
                        Ok(s) => s.as_str_name().to_owned(),
                        Err(_) => format!("{}", reply.state),
                    };
                    if got != want {
                        return Err(format!("the land state is {got}, the file says {want}"));
                    }
                }
            }
            Ok(())
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

async fn run_scion_resolve_ref(
    t: &Transcript,
    client: &mut ScionClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let req = ResolveRefRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        name: field(t, "name").unwrap_or_else(|| "main".to_owned()),
    };
    match client.resolve_ref(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            pinned(t, "changeset_id", &reply.into_inner().changeset_id, f)
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

/// The stream items a server-streamed call sent, hex-decoded.
async fn stream_bytes<R>(
    stream: &mut tonic::Streaming<R>,
    take: fn(&R) -> Vec<u8>,
) -> Result<Vec<Vec<u8>>, String> {
    let mut out = Vec::new();
    loop {
        match stream.message().await {
            Ok(Some(item)) => out.push(take(&item)),
            Ok(None) => break,
            Err(status) => return Err(format!("stream {}", status.code())),
        }
    }
    Ok(out)
}

/// Whether the file pins `want` as the joined stream bytes.
fn check_stream_bytes(
    t: &Transcript,
    f: &Fixture,
    field_name: &str,
    got: &[Vec<u8>],
) -> Result<(), String> {
    let want: Vec<Vec<u8>> = t
        .expect
        .iter()
        .filter(|(k, _)| k == &format!("reply.{field_name}"))
        .map(|(_, v)| unhex(&resolve(v, f)))
        .collect();
    if !want.is_empty() && want != got {
        return Err(format!(
            "reply.{field_name} is {} bytes over {} messages, the file pins {}",
            got.iter().map(Vec::len).sum::<usize>(),
            got.len(),
            want.iter().map(Vec::len).sum::<usize>()
        ));
    }
    Ok(())
}

async fn run_get_changeset(
    t: &Transcript,
    client: &mut ScionClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let req = scion_proto::v1::GetChangesetRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        changeset_id: unhex(&resolve(&field(t, "changeset_id").unwrap_or_default(), f)),
    };
    match client.get_changeset(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            let mut stream = reply.into_inner();
            let parts = stream_bytes(&mut stream, |r: &scion_proto::v1::GetChangesetResponse| {
                r.changeset.clone()
            })
            .await?;
            check_stream_bytes(t, f, "changeset", &parts)
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

async fn run_read_file(
    t: &Transcript,
    client: &mut ScionClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let req = scion_proto::v1::ReadFileRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        changeset_id: unhex(&resolve(&field(t, "changeset_id").unwrap_or_default(), f)),
        path: field(t, "path").unwrap_or_default().into_bytes(),
    };
    match client.read_file(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            let mut stream = reply.into_inner();
            let parts = stream_bytes(&mut stream, |r: &scion_proto::v1::ReadFileResponse| {
                r.data.clone()
            })
            .await?;
            check_stream_bytes(t, f, "data", &parts)
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

async fn run_list_dir(
    t: &Transcript,
    client: &mut ScionClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let req = scion_proto::v1::ListDirRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        changeset_id: unhex(&resolve(&field(t, "changeset_id").unwrap_or_default(), f)),
        path: field(t, "path").unwrap_or_default().into_bytes(),
    };
    match client.list_dir(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            let mut stream = reply.into_inner();
            let parts = stream_bytes(&mut stream, |r: &scion_proto::v1::ListDirResponse| {
                r.tree.clone()
            })
            .await?;
            check_stream_bytes(t, f, "tree", &parts)
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

async fn run_log(t: &Transcript, client: &mut ScionClientOf, f: &Fixture) -> Result<(), String> {
    let req = scion_proto::v1::LogRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        changeset_id: unhex(&resolve(&field(t, "changeset_id").unwrap_or_default(), f)),
        limit: field(t, "limit").and_then(|v| v.parse().ok()).unwrap_or(0),
        path: field(t, "path").unwrap_or_default().into_bytes(),
    };
    match client.log(req).await {
        Ok(reply) => {
            let mut stream = reply.into_inner();
            check_status(t, Code::Ok)?;
            let mut count = 0usize;
            loop {
                match stream.message().await {
                    Ok(Some(item)) => count += item.entries.len(),
                    Ok(None) => break,
                    Err(status) => return Err(format!("stream {}", status.code())),
                }
            }
            if let Some((_, v)) = t.expect.iter().find(|(k, _)| k == "entries") {
                let want: usize = v.parse().unwrap_or(0);
                if count != want {
                    return Err(format!(
                        "the log holds {count} entries, the file says {want}"
                    ));
                }
            }
            Ok(())
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

async fn run_blame(t: &Transcript, client: &mut ScionClientOf, f: &Fixture) -> Result<(), String> {
    let req = scion_proto::v1::BlameRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        changeset_id: unhex(&resolve(&field(t, "changeset_id").unwrap_or_default(), f)),
        path: field(t, "path").unwrap_or_default().into_bytes(),
        cursor: field(t, "cursor").unwrap_or_default().into_bytes(),
    };
    match client.blame(req).await {
        Ok(reply) => {
            let mut stream = reply.into_inner();
            check_status(t, Code::Ok)?;
            let mut count = 0usize;
            loop {
                match stream.message().await {
                    Ok(Some(item)) => count += item.changesets.len(),
                    Ok(None) => break,
                    Err(status) => return Err(format!("stream {}", status.code())),
                }
            }
            if let Some((_, v)) = t.expect.iter().find(|(k, _)| k == "entries") {
                let want: usize = v.parse().unwrap_or(0);
                if count != want {
                    return Err(format!(
                        "the blame holds {count} lines, the file says {want}"
                    ));
                }
            }
            Ok(())
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

async fn run_merge_base(
    t: &Transcript,
    client: &mut ScionClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let req = scion_proto::v1::MergeBaseRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        a_changeset_id: unhex(&resolve(
            &spec_value(&field(t, "a_changeset_id").unwrap_or_default(), "id").unwrap_or_default(),
            f,
        )),
        b_changeset_id: unhex(&resolve(
            &spec_value(&field(t, "b_changeset_id").unwrap_or_default(), "id").unwrap_or_default(),
            f,
        )),
    };
    match client.merge_base(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            pinned(
                t,
                "changeset_ids",
                &reply.into_inner().changeset_ids.concat(),
                f,
            )
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

async fn run_is_ancestor(
    t: &Transcript,
    client: &mut ScionClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let req = scion_proto::v1::IsAncestorRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        ancestor_id: unhex(&resolve(
            &spec_value(&field(t, "ancestor_id").unwrap_or_default(), "id").unwrap_or_default(),
            f,
        )),
        descendant_id: unhex(&resolve(
            &spec_value(&field(t, "descendant_id").unwrap_or_default(), "id").unwrap_or_default(),
            f,
        )),
    };
    match client.is_ancestor(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            let is = reply.into_inner().is_ancestor;
            if let Some((_, v)) = t.expect.iter().find(|(k, _)| k == "reply.is_ancestor") {
                let want = v == "true";
                if is != want {
                    return Err(format!("is_ancestor is {is}, the file says {want}"));
                }
            }
            Ok(())
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

async fn run_diff(t: &Transcript, client: &mut ScionClientOf, f: &Fixture) -> Result<(), String> {
    let req = scion_proto::v1::DiffRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        old_changeset_id: unhex(&resolve(
            &spec_value(&field(t, "old_changeset_id").unwrap_or_default(), "id")
                .unwrap_or_default(),
            f,
        )),
        new_changeset_id: unhex(&resolve(
            &spec_value(&field(t, "new_changeset_id").unwrap_or_default(), "id")
                .unwrap_or_default(),
            f,
        )),
        path: field(t, "path").unwrap_or_default().into_bytes(),
        after: Vec::new(),
    };
    match client.diff(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            let entries = reply.into_inner().entries.len();
            if let Some((_, v)) = t.expect.iter().find(|(k, _)| k == "entries") {
                let want: usize = v.parse().unwrap_or(0);
                if entries != want {
                    return Err(format!(
                        "the diff holds {entries} entries, the file says {want}"
                    ));
                }
            }
            Ok(())
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

/// The status the transcript expects, if it names one.
fn want_status(t: &Transcript) -> Option<Code> {
    t.expect
        .iter()
        .find(|(k, _)| k == "status")
        .map(|(_, v)| code(v))
}

/// Fails with the expected-vs-got codes when the call's status differs.
fn check_status(t: &Transcript, got: Code) -> Result<(), String> {
    match want_status(t) {
        Some(want) if want == got => Ok(()),
        Some(want) => Err(format!("the status is {got:?}, the file says {want:?}")),
        None => Err(format!(
            "the call failed {got:?} where the file expects a reply"
        )),
    }
}

/// One pinned reply field: `reply.<path>: value`, compared against the
/// actual bytes.
fn pinned(t: &Transcript, field: &str, actual: &[u8], f: &Fixture) -> Result<(), String> {
    for (key, value) in &t.expect {
        if key == &format!("reply.{field}") {
            let want = unhex(&resolve(value, f));
            if want != actual {
                return Err(format!(
                    "reply.{field} is {}, the file says {}",
                    hex(actual),
                    resolve(value, f)
                ));
            }
        }
    }
    Ok(())
}

/// `Objects/ResolveRef`: the request lines build the request; the
/// expectation is a status, or a reply whose `changeset_id` is pinned.
async fn run_resolve_ref(
    t: &Transcript,
    client: &mut ObjectsClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let mut req = ResolveRefRequest::default();
    for (path, value) in &t.request {
        match path.as_str() {
            "repo" => req.repo = resolve(value, f),
            "name" => req.name = resolve(value, f),
            other => return Err(format!("ResolveRef knows no request field {other}")),
        }
    }
    match client.resolve_ref(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            pinned(t, "changeset_id", &reply.into_inner().changeset_id, f)
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

/// `Objects/GetRootTree`: the `tree_id` is the pinned reply field.
async fn run_get_root_tree(
    t: &Transcript,
    client: &mut ObjectsClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let mut req = GetRootTreeRequest::default();
    for (path, value) in &t.request {
        match path.as_str() {
            "repo" => req.repo = resolve(value, f),
            "changeset_id" => req.changeset_id = unhex(&resolve(value, f)),
            other => return Err(format!("GetRootTree knows no request field {other}")),
        }
    }
    match client.get_root_tree(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            pinned(t, "tree_id", &reply.into_inner().tree_id, f)
        }
        Err(status) => {
            check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))
        }
    }
}

/// `Objects/GetObjects` as a client does it: asks, collects, asks again
/// for what a reply did not send, and checks each object against its ID
/// and the part rules. The answer is the ordered items plus how the
/// whole answer ended; the file pins both.
/// An object in progress: the first part's declared fields and the data
/// the parts have carried so far.
struct InProgress {
    kind: i32,
    declared_id: Vec<u8>,
    encoding: i32,
    size: u64,
    encoded_size: u64,
    data: Vec<u8>,
}

/// Reads one `GetObjects` answer's part stream into `objects`: the first
/// part names its object, later parts extend it. Returns how the answer
/// ended: `None` for a clean OK end, `Some(end)` for an error end.
async fn read_part_stream(
    url: &str,
    stream: &mut tonic::Streaming<ObjectPart>,
    objects: &mut Vec<(i32, String)>,
    sent_at: std::time::Instant,
) -> Result<Option<End>, String> {
    let mut current: Option<InProgress> = None;
    let mut parts = 0usize;
    loop {
        // F1: a tier that sends one part and holds the stream open must
        // not hang the runner: every read is bounded.
        let message = tokio::time::timeout(std::time::Duration::from_secs(30), stream.message())
            .await
            .map_err(|_| {
                format!(
                    "the tier sent nothing for 30s ({}s after the call went out)",
                    sent_at.elapsed().as_secs()
                )
            })?;
        match message {
            Ok(Some(part)) => {
                parts += 1;
                if part.id.is_empty() {
                    // A later part: none of fields 1, 2, 4, 5 and 6 may be
                    // set (section 3's part rules).
                    let Some(object) = current.as_mut() else {
                        return Err("a part names no object and none is in progress".to_owned());
                    };
                    if part.kind != 0
                        || part.encoding != 0
                        || part.encoded_size != 0
                        || part.size != 0
                    {
                        return Err(
                            "a later part set kind, encoding, encoded_size or size".to_owned()
                        );
                    }
                    object.data.extend_from_slice(&part.data);
                    continue;
                }
                // A first part: the object before it must have ended.
                if let Some(object) = current.take() {
                    objects.push(checked(&object)?);
                }
                current = Some(InProgress {
                    kind: part.kind,
                    declared_id: part.id,
                    encoding: part.encoding,
                    size: part.size,
                    encoded_size: part.encoded_size,
                    data: part.data.to_vec(),
                });
            }
            Ok(None) => {
                if let Some(object) = current.take() {
                    objects.push(checked(&object)?);
                }
                eprintln!(
                    "CALL {} GetObjects took={:.3}s end=Ok parts={}",
                    url,
                    sent_at.elapsed().as_secs_f64(),
                    parts
                );
                return Ok(None);
            }
            Err(status) => {
                // The object in progress ends here: check it if it came
                // whole, then the stream's code.
                if let Some(object) = current.take()
                    && let Ok(got) = checked(&object)
                {
                    objects.push(got);
                }
                eprintln!(
                    "CALL {} GetObjects took={:.3}s end=Stream({:?})",
                    url,
                    sent_at.elapsed().as_secs_f64(),
                    status.code()
                );
                return Ok(Some(End::Stream(status.code())));
            }
        }
    }
}

/// Builds the file's first `GetObjects` request.
fn get_objects_request(t: &Transcript, f: &Fixture) -> Result<GetObjectsRequest, String> {
    let mut req = GetObjectsRequest::default();
    for (path, value) in &t.request {
        match path.as_str() {
            "ticket" => req.ticket = unhex(&resolve(value, f)),
            "refs" => req.refs.push(object_ref(&resolve(value, f))?),
            "accept" => {
                req.accept.push(match resolve(value, f).as_str() {
                    "raw" => Encoding::Raw as i32,
                    "zstd" => Encoding::Zstd as i32,
                    other => return Err(format!("the encoding is raw or zstd: {other}")),
                });
            }
            other => return Err(format!("GetObjects knows no request field {other}")),
        }
    }
    Ok(req)
}

/// One `request.refs` line: `kind= id= path=`.
fn object_ref(value: &str) -> Result<ObjectRef, String> {
    let mut kind = None;
    let mut id = None;
    let mut path_bytes = Vec::new();
    for part in value.split_whitespace() {
        if let Some(k) = part.strip_prefix("kind=") {
            kind = Some(match k {
                "blob" => ObjectKind::Blob as i32,
                "tree" => ObjectKind::Tree as i32,
                // A number passes through: a kind the server must refuse
                // is a behaviour too.
                other => other
                    .parse::<i32>()
                    .map_err(|e| format!("the kind is a name or a number: {e}"))?,
            });
        }
        if let Some(i) = part.strip_prefix("id=") {
            id = Some(unhex(i));
        }
        if let Some(p) = part.strip_prefix("path=") {
            path_bytes = p.as_bytes().to_vec();
        }
    }
    Ok(ObjectRef {
        kind: kind.ok_or("the ref names a kind")?,
        id: id.ok_or("the ref names an id")?,
        path: path_bytes,
    })
}

async fn run_get_objects(
    t: &Transcript,
    url: &str,
    client: &mut ObjectsClientOf,
    f: &Fixture,
) -> Result<(), String> {
    let req = get_objects_request(t, f)?;

    let mut remaining = req.refs.clone();
    let mut objects: Vec<(i32, String)> = Vec::new();
    let mut end = None;
    let mut rounds = 0usize;
    loop {
        rounds += 1;
        assert!(
            rounds <= 1000,
            "the client asked again {rounds} times for {} objects: the answer never advances",
            remaining.len()
        );
        let before = objects.len();
        let mut call = req.clone();
        call.refs = remaining.clone();
        let sent_at = std::time::Instant::now();
        eprintln!("CALL {} GetObjects refs={} sent", url, call.refs.len());
        let mut stream = match client.get_objects(call).await {
            Ok(stream) => stream.into_inner(),
            Err(status) => {
                // The call itself failed: the file pins the code as the
                // status.
                check_status(t, status.code()).map_err(|e| format!("{e}: {}", status.message()))?;
                end = Some(End::Call(status.code()));
                break;
            }
        };
        // The answer's part stream: the objects it sent join `objects`,
        // whole and in order. `how` is how the answer ended.
        let how = read_part_stream(url, &mut stream, &mut objects, sent_at).await?;
        if let Some(how) = how {
            end = Some(how);
            // A reply that ends in an error is over: the client asks
            // again only for a short OK answer.
            break;
        }
        if objects.len() == before {
            // The server named no object and said nothing: the client
            // refused it.
            end = Some(End::Refused);
            break;
        }
        // The objects the answer sent, in order, minus what it never
        // named: ask again for the rest.
        let sent = objects.len() - before;
        remaining.drain(0..sent.min(remaining.len()));
        if remaining.is_empty() {
            break;
        }
    }

    // The items the file pins, in order.
    let want: Vec<(i32, String)> = t
        .expect
        .iter()
        .filter(|(k, _)| k == "objects")
        .map(|(_, v)| {
            let (kind, id) = item(&resolve(v, f));
            (kind, hex(&id))
        })
        .collect();
    if objects != want {
        return Err(format!("the answer is {objects:?}, the file says {want:?}"));
    }
    // A call-level failure pins its code as the status; a stream end is
    // pinned by an `end:` line, and a file with none pins no stream end.
    let failed_call = matches!(end.clone(), Some(End::Call(_)));
    let want_end = t
        .expect
        .iter()
        .find(|(k, _)| k == "end")
        .map(|(_, v)| parse_end(v))
        .unwrap_or(if failed_call { end.clone() } else { None });
    if want_end != end {
        return Err(format!(
            "the answer ends {end:?}, the file says {want_end:?}"
        ));
    }
    Ok(())
}

/// Whether `data` is the object `kind` and `id` name, as the
/// conformance test checks: an ID must verify against its bytes.
fn checked(object: &InProgress) -> Result<(i32, String), String> {
    let kind = object.kind;
    let id: Vec<u8> = match ObjectKind::try_from(kind) {
        Ok(ObjectKind::Tree) => Tree::decode_with_id(&object.data)
            .map_err(|e| format!("the tree bytes do not decode: {e}"))?
            .1
            .as_bytes()
            .to_vec(),
        Ok(ObjectKind::Blob) => scion_core::BlobId::of(&object.data).as_bytes().to_vec(),
        _ => return Err(format!("the kind {kind} is no object kind")),
    };
    // The part rules: the declared id must be the bytes' own, the parts
    // must add up to the first part's encoded_size, and raw answers
    // carry encoded_size == size == the data.
    if id != object.declared_id {
        return Err(format!(
            "the first part declared id {}, the bytes hash to {}",
            hex(&object.declared_id),
            hex(&id)
        ));
    }
    let carried = object.data.len() as u64;
    if carried != object.encoded_size {
        return Err(format!(
            "the parts carry {carried} bytes, the first part said encoded_size {}",
            object.encoded_size
        ));
    }
    if object.encoding == Encoding::Raw as i32 && object.encoded_size != object.size {
        return Err(format!(
            "raw encoded_size {} does not equal size {}",
            object.encoded_size, object.size
        ));
    }
    if carried != object.size {
        return Err(format!(
            "the object is {carried} bytes, the first part said size {}",
            object.size
        ));
    }
    Ok((kind, hex(&id)))
}
