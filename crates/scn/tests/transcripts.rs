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
//!
//! Tier keys, for the `Objects` files: `tiers: refuse` asserts a tier's
//! UNIMPLEMENTED refusal of the non-GetObjects calls (section 7.1);
//! `tiers: fresh` adds a second run of the file on a cache and daemon no
//! earlier file touched, over the same store; `daemon_warm: serves` warms
//! the daemon with the file's valid ticket as its setup, then pins the
//! warm daemon serving the file's unsigned ticket from its store - a
//! daemon checks no ticket by design (docs/protocol-v1.md section 7.1).
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
    /// The file's own path: the Land  is per file.
    path: String,
    /// The release the behaviour exists since; the N-1 run runs the
    /// files at or under the N-1 version and skips the newer ones.
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
        // `if let` guards need a newer Rust than the minimum (1.88), so the
        // prefixed keys are handled before the match.
        if let Some(rest) = key.strip_prefix("request.") {
            request.push((rest.to_owned(), value.to_owned()));
            continue;
        }
        if let Some(rest) = key.strip_prefix("reply.") {
            expect.push((format!("reply.{rest}"), value.to_owned()));
            continue;
        }
        match key {
            "since" => since = Some(value.to_owned()),
            "method" => method = Some(value.to_owned()),
            "fixture" => fixture = Some(value.to_owned()),
            "status" | "end" | "resume_status" => {
                expect.push((key.to_owned(), value.to_owned()));
            }
            "tiers" if value == "refuse" || value == "fresh" => {
                expect.push((key.to_owned(), value.to_owned()));
            }
            "tiers" => panic!(
                "{}:{}: the runner handles no tiers value but refuse or fresh: {value}",
                at.display(),
                n + 1
            ),
            "daemon_warm" if value == "serves" => {
                expect.push((key.to_owned(), value.to_owned()));
            }
            "daemon_warm" => panic!(
                "{}:{}: the runner handles no daemon_warm value but serves: {value}",
                at.display(),
                n + 1
            ),
            "objects" | "entries" => expect.push((key.to_owned(), value.to_owned())),
            other => panic!("{}:{} names unknown key {other}", at.display(), n + 1),
        }
    }
    Transcript {
        path: at.display().to_string(),
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
    ticket: Vec<u8>,
    /// The same ticket with one signature byte flipped: it parses, the
    /// server's keys do not sign it, and a daemon that checks no ticket
    /// still serves what its store holds.
    unsigned: Vec<u8>,
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
        unsigned: unsigned_of(&ticket),
        ticket,
    };
    (tiers, fixture)
}

/// Mints a read ticket for the root tree of `cs` and names the root
/// tree, as the conformance test does with the same call.
/// The ticket with one byte of its signature flipped: the format still
/// parses, the signature does not verify.
fn unsigned_of(ticket: &[u8]) -> Vec<u8> {
    let mut unsigned = ticket.to_vec();
    let n = unsigned.len();
    unsigned[n - 1] ^= 0xff;
    unsigned
}

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
        .replace("$unsigned", &hex(&f.unsigned))
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
fn walk(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in fs::read_dir(dir).expect("the corpus dir opens") {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk(&path, files)?;
        } else if path.extension().is_some_and(|e| e == "textproto") {
            files.push(path);
        } else {
            return Err(format!(
                "the corpus holds a non-transcript file: {}",
                path.display()
            ));
        }
    }
    Ok(())
}

/// The corpus test: every file, against the server; the `Objects` cases
/// also through the cache and the daemon. Files whose behaviour depends
/// on the ticket and the per-file `tiers: refuse` files get a fresh
/// cache and daemon, so a warm disk or a relay that does not check
/// signatures cannot answer for them; the rest share one warm pair,
/// cold only for the first file that touches it.
///
/// The ticket files also pin the warm daemon's serve (`daemon_warm:
/// serves`): a daemon checks no ticket by design, and the blob is in its
/// disk store from the success file on. Note the refusal is enforced in
/// depth: a cache that skipped only its signature check would still
/// refuse, because its walk re-fetches the trees from upstream with the
/// call's ticket, and the server refuses the unsigned one. A file that
/// pins a refusal therefore catches a cache or daemon that serves
/// without ANY proof, which is what the fresh pair and the warm runs are
/// for.
/// A tier serves `GetObjects` only: its refusal of the other calls is the
/// contract (section 7.1).
async fn assert_tier_refuses(url: &str, t: &Transcript, file: &Path) {
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
}

/// The warm-up a `daemon_warm: serves` file runs as its setup: the
/// file's refs through the daemon with its valid ticket, and the blob
/// checked as arrived, so the pin below tests a warm store wherever the
/// file sorts.
fn warm_daemon_for(
    runtime: &tokio::runtime::Runtime,
    socket: &str,
    t: &Transcript,
    f: &Fixture,
    file: &Path,
) {
    runtime.block_on(async {
        let mut client = connect(socket).await;
        let mut stream = client
            .get_objects(
                request_with_ticket(t, f, f.ticket.clone()).expect("the file's request builds"),
            )
            .await
            .expect("the warm-up call answers")
            .into_inner();
        let mut warmed = Vec::new();
        read_part_stream(socket, &mut stream, &mut warmed, std::time::Instant::now())
            .await
            .expect("the warm-up stream reads");
        let want = t
            .request
            .iter()
            .find(|(path, _)| path == "refs")
            .map(|(_, v)| item(&resolve(v, f)))
            .expect("the file names the object it reads");
        let want = (want.0, hex(&want.1));
        assert!(
            warmed.contains(&want),
            "{}, the warm-up did not deliver {want:?} (it sent {warmed:?})",
            file.display()
        );
    });
}

/// The per-tier run for one file on one pair: the tier refusal, the warm
/// daemon's serve pin, then the file's own expectations.
fn run_objects_across_tiers(
    runtime: &tokio::runtime::Runtime,
    urls: [&str; 3],
    t: &Transcript,
    f: &Fixture,
    file: &Path,
) {
    let tier_refuses = t.expect.iter().any(|(k, v)| k == "tiers" && v == "refuse");
    let daemon_warm_serves = t
        .expect
        .iter()
        .any(|(k, v)| k == "daemon_warm" && v == "serves");
    for url in urls {
        if tier_refuses && url != urls[0] {
            // A tier serves GetObjects only: its refusal of the
            // other calls is the contract (section 7.1).
            runtime.block_on(assert_tier_refuses(url, t, file));
            continue;
        }
        if url == urls[2] && daemon_warm_serves {
            // A daemon checks no ticket by design: it serves what its
            // store holds (docs/protocol-v1.md, section 7.1's daemon
            // paragraph). The setup warmed this daemon with the file's
            // valid ticket; pin the serve of the unsigned ticket by the
            // stream, not by the call's headers: a daemon that answers
            // OK and ends the stream with an error must not pass.
            runtime.block_on(async {
                let mut client = connect(url).await;
                let want = t
                    .request
                    .iter()
                    .find(|(path, _)| path == "refs")
                    .map(|(_, v)| item(&resolve(v, f)))
                    .expect("the file names the object it reads");
                let want = (want.0, hex(&want.1));

                let mut stream = client
                    .get_objects(
                        request_with_ticket(t, f, f.unsigned.clone())
                            .expect("the file's request builds"),
                    )
                    .await
                    .expect("the pinned call answers")
                    .into_inner();
                let mut served = Vec::new();
                let ended =
                    read_part_stream(url, &mut stream, &mut served, std::time::Instant::now())
                        .await
                        .expect("the pinned stream reads");
                assert!(
                    served.contains(&want),
                    "{}, a warm daemon serves what its store holds (it sent {served:?})",
                    file.display()
                );
                assert!(
                    ended.is_none(),
                    "{}, a warm daemon ends the unsigned serve clean (it ended {ended:?})",
                    file.display()
                );
            });
            continue;
        }
        runtime
            .block_on(run_objects(t, url, f))
            .unwrap_or_else(|e| panic!("{} at {}: {e}", file.display(), url));
    }
}

#[test]
fn the_corpus_answers_as_its_files_say() {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(CORPUS);
    let mut files = Vec::new();
    walk(&corpus, &mut files).expect("the corpus holds only transcripts");
    files.sort();
    assert_eq!(
        files.len(),
        25,
        "the corpus pins 25 behaviours; a file added or removed must update this count"
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
            unsigned: unsigned_of(&ticket),
            ticket,
            ..base.clone()
        };
        let mint_age = minted_at.elapsed().as_secs_f64();
        let started_at = std::time::Instant::now();
        let tier_value = |name: &str| {
            t.expect
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        };
        let tier_refuses = tier_value("tiers") == Some("refuse");
        let tiers_fresh = tier_value("tiers") == Some("fresh");
        if t.method.starts_with("Objects/") {
            if t.expect
                .iter()
                .any(|(k, v)| k == "daemon_warm" && v == "serves")
            {
                warm_daemon_for(&tiers.runtime, &tiers.socket, &t, &f, &file);
            }
            run_objects_across_tiers(
                &tiers.runtime,
                [&tiers.server, &tiers.cache_url, &tiers.socket],
                &t,
                &f,
                &file,
            );
            if tiers_fresh {
                // The same file again on a cache and daemon no earlier
                // file touched: the same server and keys, so the file's
                // ticket still verifies; the fresh daemon forwards, and
                // the upstream refuses, so the file's own expectations
                // hold everywhere.
                let fresh = tiers.fresh_relays();
                for url in [&fresh.cache_url, &fresh.socket] {
                    if tier_refuses {
                        tiers.runtime.block_on(assert_tier_refuses(url, &t, &file));
                        continue;
                    }
                    tiers
                        .runtime
                        .block_on(run_objects(&t, url, &f))
                        .unwrap_or_else(|e| panic!("{} at {} (fresh): {e}", file.display(), url));
                }
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

/// The files the N-1 legs run: every file whose `since` is at or under
/// the N-1 version. A Scion file's fixture names the changeset this
/// tree's store landed, and the N-1 side's changeset id is not plumbed
/// into the harness yet (the next slice pairs it through the ref), so
/// such a file stays out of the legs and the count reports it. Returns
/// the qualifying files, the Scion count, and the corpus's oldest
/// `since`.
fn pick_n1_files(
    files: &[PathBuf],
    prev: (u64, u64, u64),
) -> (Vec<(PathBuf, Transcript)>, usize, String) {
    let mut qualifying = Vec::new();
    let mut scion_files = 0usize;
    let mut oldest = None;
    for file in files {
        let text = fs::read_to_string(file).expect("the transcript opens");
        let t = parse(file, &text);
        oldest = Some(match oldest {
            Some(o) if o <= t.since => o,
            _ => t.since.clone(),
        });
        if version_of(&t.since) > prev {
            continue;
        }
        if t.method.starts_with("Scion/") {
            scion_files += 1;
            continue;
        }
        qualifying.push((file.clone(), t));
    }
    (
        qualifying,
        scion_files,
        oldest.expect("the corpus is not empty"),
    )
}

/// The N-1 legs of the corpus (Refs #890): every file whose `since` is
/// at or under the N-1 version replays against the N-1 server and its
/// tiers, and the N-1 tiers in front of this server - the real upgrade
/// order. This tree's runner is the harness; the N-1 side contributes
/// its binaries. A difference on the N-1 side is either a wrong `since`
/// in the file or a break of N-1 compatibility; the failure names the
/// file, the side and both answers. Skipped green when `PREV_BIN` is
/// unset: the legs belong to the version-matrix job, which builds N-1
/// and points `PREV_BIN` at it.
#[test]
fn the_n1_side_answers_as_its_files_say() {
    let Ok(prev_bin) = std::env::var("PREV_BIN") else {
        eprintln!("PREV_BIN unset: the N-1 legs run in the version-matrix job");
        return;
    };
    let prev_bin = PathBuf::from(prev_bin);
    let work = common::private_tempdir();
    let dir = work.path();

    // The N-1 version decides which files qualify: a file's `since`
    // must be at or under it. The binary says it itself, so no env
    // wiring can lose it.
    let out = Command::new(prev_bin.join("scion-server"))
        .arg("--version")
        .output()
        .expect("the N-1 scion-server runs");
    let printed = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    let prev = version_of(
        printed
            .rsplit(' ')
            .next()
            .expect("the version line has a number"),
    );
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(CORPUS);
    let mut files = Vec::new();
    walk(&corpus, &mut files).expect("the corpus holds only transcripts");
    files.sort();
    let (qualifying, scion_files, oldest) = pick_n1_files(&files, prev);
    if qualifying.is_empty() {
        eprintln!(
            "the N-1 legs ran nothing: the corpus's oldest since is {oldest:?}, the N-1 side is {prev:?} ({printed})",
        );
        return;
    }

    // Leg 1: the qualifying files against the N-1 server and its tiers.
    let stack = start_n1_stack(&prev_bin, dir);
    let runtime = tokio::runtime::Runtime::new().expect("the runtime starts");
    let _entered = runtime.enter();
    let fixture = n1_fixture(&runtime, &stack.server);
    let mut ran = 0usize;
    for (file, t) in &qualifying {
        run_objects_across_tiers(
            &runtime,
            [&stack.server, &stack.cache, &stack.socket],
            t,
            &fixture,
            file,
        );
        ran += 1;
    }

    // Leg 2: the N-1 tiers in front of this server - the upgrade order.
    // The corpus's landed store and tickets are the fixture; the N-1
    // cache verifies the tickets of this server's keys, the N-1 daemon
    // forwards to it.
    let (tiers, base) = landed();
    let _entered2 = tiers.runtime.enter();
    let leg2 = start_n1_tiers(&tiers, &prev_bin, dir);
    for (file, t) in &qualifying {
        let f = Fixture {
            ticket: mint(&tiers, base.cs).0,
            ..base.clone()
        };
        run_objects_across_tiers(
            &tiers.runtime,
            [&tiers.server, &leg2.cache, &leg2.socket],
            t,
            &f,
            file,
        );
        ran += 1;
    }
    drop(leg2);
    eprintln!(
        "the N-1 legs ran {ran} file-runs over the two legs; {scion_files} Scion files stayed out (their fixture needs the N-1 changeset id, the next slice)"
    );
}

/// Leg 2's tiers: the N-1 cache in front of this server, the N-1 daemon
/// in front of that cache. The cache verifies this server's ticket keys,
/// which `tiers` holds; the children die with the return value.
fn start_n1_tiers(tiers: &common::Tiers, prev_bin: &Path, dir: &Path) -> N1Leg2 {
    let keys: Vec<String> = tiers
        .ticket_public_keys()
        .into_iter()
        .map(|k| format!("--ticket-public-key={k}"))
        .collect();
    let cache_log = dir.join("tiers-cache.log");
    let cache = spawn_logged(
        prev_bin.join("scion-cache"),
        std::iter::empty()
            .chain(["--listen", "127.0.0.1:0", "--upstream", &tiers.server])
            .chain(keys.iter().map(String::as_str)),
        &[],
        &cache_log,
    );
    let cport = serving_port(&cache_log);
    let relay_dir = dir.join("tiers");
    fs::create_dir_all(&relay_dir).expect("the leg-2 tiers dir is made");
    make_private(&relay_dir);
    let relay_sock = dir.join("tier.sock");
    let daemon = spawn_logged(
        prev_bin.join("scion-daemon"),
        [
            "--data",
            &relay_dir.display().to_string(),
            "--socket",
            &relay_sock.display().to_string(),
            "--upstream",
            &format!("http://127.0.0.1:{cport}"),
        ],
        &[],
        &dir.join("tiers.log"),
    );
    wait_serving(&dir.join("tiers.log"));
    N1Leg2 {
        cache: format!("http://127.0.0.1:{cport}"),
        socket: format!("unix://{}", relay_sock.display()),
        _children: vec![cache, daemon],
    }
}

/// The version a `since:` or a binary names, as (major, minor, patch);
/// a missing part is zero, so `v0.7` is `(0, 7, 0)`.
fn version_of(v: &str) -> (u64, u64, u64) {
    let one = |p: Option<&str>| p.and_then(|p| p.parse().ok()).unwrap_or(0);
    let mut parts = v.trim().trim_start_matches('v').split('.');
    (one(parts.next()), one(parts.next()), one(parts.next()))
}

/// Leg 2's tiers: the cache url, the daemon's socket url, and the
/// children that keep them alive.
struct N1Leg2 {
    cache: String,
    socket: String,
    _children: Vec<Child>,
}

/// A child killed when the test ends, so a red run leaves no server.
struct Child(std::process::Child);

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts BIN with ARGS, its log at PATH. The caller reads readiness
/// with `serving_port` or `wait_serving`.
fn spawn_logged<I, S>(bin: PathBuf, args: I, env: &[(&str, &str)], log: &Path) -> Child
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    use std::process::Stdio;
    let file = fs::File::create(log).expect("the log opens");
    let err = file.try_clone().expect("the log clones");
    let mut cmd = Command::new(bin);
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let c = cmd
        .stdout(Stdio::from(file))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("the binary starts");
    Child(c)
}

/// Reads the `serving listen=127.0.0.1:PORT` line of LOG, waiting up to
/// 30 s, so a slow start is not a race.
fn serving_port(log: &Path) -> u16 {
    for _ in 0..300 {
        if let Ok(text) = fs::read_to_string(log) {
            for line in text.lines().rev() {
                let Some(rest) = line.split_once("serving listen=127.0.0.1:") else {
                    continue;
                };
                if let Some(port) = rest
                    .1
                    .split([' ', ':', ','])
                    .next()
                    .and_then(|p| p.parse().ok())
                {
                    return port;
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("{}, no serving listen line in 30 s", log.display());
}

/// Waits for LOG's first ` serving` line, up to 30 s: a daemon serves a
/// socket, not a port, and its serving line is the readiness.
fn wait_serving(log: &Path) {
    for _ in 0..300 {
        if let Ok(text) = fs::read_to_string(log)
            && text.contains(" serving")
        {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("{}, no serving line in 30 s", log.display());
}

/// CHMOD 0700 on DIR: the daemon refuses a data directory open to
/// others.
fn make_private(dir: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).expect("the mode sets");
}

/// Runs one git command, failing the test on a non-zero exit.
fn git(args: &[&str]) {
    let out = Command::new("git").args(args).output().expect("git runs");
    assert!(
        out.status.success(),
        "git {:?}: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Runs BIN with ARGS to completion, failing the test on a non-zero
/// exit, with ENV set for the call.
fn run_to_end(bin: &Path, args: &[&str], env: &[(&str, &str)]) {
    let mut cmd = Command::new(bin);
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("the binary runs");
    assert!(
        out.status.success(),
        "{} {:?}: {}",
        bin.display(),
        args,
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The N-1 stack: the repo imported into its own database, the ticket
/// keys made, the server serving, the cache in front of it, the daemon
/// in front of the cache. Returns the three urls; the children live in
/// the stack and die with it.
struct N1Stack {
    server: String,
    cache: String,
    socket: String,
    _children: Vec<Child>,
}

/// The N-1 server's repository: one file of the fixture's bytes, one
/// commit. The ids are content-addressed, so the fixture's names
/// resolve on the N-1 side too. Returns the tree's path.
fn n1_repo(dir: &Path) -> PathBuf {
    let src = dir.join("src");
    fs::create_dir_all(&src).expect("the src tree is made");
    git(&["init", "-q", "-b", "main", &src.display().to_string()]);
    git(&[
        "-C",
        &src.display().to_string(),
        "config",
        "user.name",
        "n1",
    ]);
    git(&[
        "-C",
        &src.display().to_string(),
        "config",
        "user.email",
        "n1@localhost",
    ]);
    fs::create_dir_all(src.join("src")).expect("the file's dir is made");
    fs::write(src.join("src/main.rs"), b"pub fn main() {}\n").expect("the file writes");
    git(&["-C", &src.display().to_string(), "add", "-A"]);
    git(&["-C", &src.display().to_string(), "commit", "-qam", "first"]);
    src
}

/// The N-1 server's database: one of this run's own, as version-matrix's
/// `pg_db_for` makes one - the two builds share the database server,
/// never its rows. Returns the address.
fn n1_database() -> String {
    let base = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL names the test database");
    let base = base.split('?').next().expect("the base url").to_owned();
    let db = format!("scion_n1_{}", std::process::id());
    let admin = format!(
        "{}/postgres",
        base.rsplit_once('/').expect("the url has a path").0
    );
    run_to_end(
        Path::new("psql"),
        &[
            &admin,
            "-X",
            "-q",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            &format!("DROP DATABASE IF EXISTS \"{db}\""),
            "-c",
            &format!("CREATE DATABASE \"{db}\""),
        ],
        &[],
    );
    format!("{base}/{db}")
}

fn start_n1_stack(prev_bin: &Path, dir: &Path) -> N1Stack {
    let pg = n1_database();
    let src = n1_repo(dir);

    let server_dir = dir.join("server");
    fs::create_dir_all(&server_dir).expect("the data dir is made");
    make_private(&server_dir);
    let scion = prev_bin.join("scion-server");
    run_to_end(
        &scion,
        &[
            "--data",
            &server_dir.display().to_string(),
            "import",
            &src.display().to_string(),
        ],
        &[("SCION_POSTGRES_URL", &pg)],
    );
    let key = dir.join("ticket.key");
    let pub_hex = String::from_utf8_lossy(
        &Command::new(&scion)
            .args([
                "--data",
                &server_dir.display().to_string(),
                "ticket-key",
                "--out",
                &key.display().to_string(),
            ])
            .env("SCION_POSTGRES_URL", &pg)
            .output()
            .expect("the ticket key prints")
            .stdout,
    )
    .trim()
    .to_owned();

    let server_log = dir.join("server.log");
    let server = spawn_logged(
        scion.clone(),
        [
            "--data",
            &server_dir.display().to_string(),
            "serve",
            "--listen",
            "127.0.0.1:0",
            "--ticket-key",
            &key.display().to_string(),
        ],
        &[("SCION_POSTGRES_URL", &pg)],
        &server_log,
    );
    let sport = serving_port(&server_log);
    let server_url = format!("http://127.0.0.1:{sport}");

    let cache_log = dir.join("cache.log");
    let cache = spawn_logged(
        prev_bin.join("scion-cache"),
        [
            "--listen",
            "127.0.0.1:0",
            "--upstream",
            &server_url,
            "--ticket-public-key",
            &pub_hex,
        ],
        &[],
        &cache_log,
    );
    let cport = serving_port(&cache_log);
    let cache_url = format!("http://127.0.0.1:{cport}");

    let relay_dir = dir.join("daemon");
    fs::create_dir_all(&relay_dir).expect("the daemon dir is made");
    make_private(&relay_dir);
    let relay_sock = dir.join("daemon.sock");
    let relay_log = dir.join("daemon.log");
    let daemon = spawn_logged(
        prev_bin.join("scion-daemon"),
        [
            "--data",
            &relay_dir.display().to_string(),
            "--socket",
            &relay_sock.display().to_string(),
            "--upstream",
            &cache_url,
        ],
        &[],
        &relay_log,
    );
    wait_serving(&relay_log);
    let socket_url = format!("unix://{}", relay_sock.display());

    N1Stack {
        server: server_url,
        cache: cache_url,
        socket: socket_url,
        _children: vec![server, cache, daemon],
    }
}

/// The N-1 side's fixture: the ref's changeset, then the root tree and
/// the ticket it names, resolved from the N-1 server like the client
/// does. The blob's bytes are the corpus's, so its names resolve.
fn n1_fixture(runtime: &tokio::runtime::Runtime, server: &str) -> Fixture {
    runtime.block_on(async {
        let mut client = connect(server).await;
        let ref_reply = client
            .resolve_ref(scion_proto::v1::ResolveRefRequest {
                repo: common::REPO.into(),
                name: "main".into(),
            })
            .await
            .expect("the ref resolves")
            .into_inner();
        let cs = scion_core::ChangesetId::from_bytes(
            ref_reply
                .changeset_id
                .try_into()
                .expect("the cs is 32 bytes"),
        );
        let tree_reply = client
            .get_root_tree(scion_proto::v1::GetRootTreeRequest {
                repo: common::REPO.into(),
                changeset_id: cs.as_bytes().to_vec(),
            })
            .await
            .expect("the root tree answers")
            .into_inner();
        let tree = tree_reply.tree_id.try_into().expect("the tree is 32 bytes");
        let content = b"pub fn main() {}\n".to_vec();
        Fixture {
            blob: BlobId::of(&content),
            content,
            tree,
            cs,
            missing: missing_id(0xab),
            unsigned: unsigned_of(&tree_reply.ticket),
            ticket: tree_reply.ticket,
        }
    })
}

use std::path::PathBuf;
use std::process::Command;

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
    for (k, _) in &t.request {
        if k != "repo" && k != "data" {
            return Err(unknown_field("PutBlob", k));
        }
    }
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
    for (k, _) in &t.request {
        if !matches!(k.as_str(), "repo" | "changeset") {
            return Err(unknown_field("CreateChangeset", k));
        }
    }
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
    for (k, _) in &t.request {
        if !matches!(k.as_str(), "repo" | "changeset_id" | "target_ref") {
            return Err(unknown_field("Land", k));
        }
    }
    let req = scion_proto::v1::LandRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        changeset_id: unhex(&resolve(&field(t, "changeset_id").unwrap_or_default(), f)),
        target_ref: field(t, "target_ref").unwrap_or_else(|| "main".to_owned()),
        request_key: format!("transcript-{}-{}", std::process::id(), t.path),
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
    for (k, _) in &t.request {
        if !matches!(k.as_str(), "repo" | "land_id") {
            return Err(unknown_field("GetLand", k));
        }
    }
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

/// One bounded stream read: a tier that sends one item and then holds
/// the stream open fails the file after 30 s instead of hanging the
/// runner (review F1).
async fn read_bounded<R>(stream: &mut tonic::Streaming<R>) -> Result<Option<R>, String> {
    tokio::time::timeout(std::time::Duration::from_secs(30), stream.message())
        .await
        .map_err(|_| "the stream sent nothing for 30s".to_owned())?
        .map_err(|status| format!("stream {}", status.code()))
}

/// The stream items a server-streamed call sent, hex-decoded.
async fn stream_bytes<R>(
    stream: &mut tonic::Streaming<R>,
    take: fn(&R) -> Vec<u8>,
) -> Result<Vec<Vec<u8>>, String> {
    let mut out = Vec::new();
    while let Some(item) = read_bounded(stream).await? {
        out.push(take(&item));
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
    for (k, _) in &t.request {
        if !matches!(k.as_str(), "repo" | "changeset_id") {
            return Err(unknown_field("GetChangeset", k));
        }
    }
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
    for (k, _) in &t.request {
        if !matches!(k.as_str(), "repo" | "changeset_id" | "path") {
            return Err(unknown_field("ReadFile", k));
        }
    }
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
    for (k, _) in &t.request {
        if !matches!(k.as_str(), "repo" | "changeset_id" | "path") {
            return Err(unknown_field("ListDir", k));
        }
    }
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
    for (k, _) in &t.request {
        if !matches!(k.as_str(), "repo" | "changeset_id" | "path" | "cursor") {
            return Err(unknown_field("Log", k));
        }
    }
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
            while let Some(item) = read_bounded(&mut stream).await? {
                count += item.entries.len();
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
    for (k, _) in &t.request {
        if !matches!(k.as_str(), "repo" | "changeset_id" | "path" | "cursor") {
            return Err(unknown_field("Blame", k));
        }
    }
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
            while let Some(item) = read_bounded(&mut stream).await? {
                count += item.changesets.len();
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
    for (k, _) in &t.request {
        if !matches!(k.as_str(), "repo" | "a_changeset_id" | "b_changeset_id") {
            return Err(unknown_field("MergeBase", k));
        }
    }
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
    for (k, _) in &t.request {
        if !matches!(k.as_str(), "repo" | "ancestor_id" | "descendant_id") {
            return Err(unknown_field("IsAncestor", k));
        }
    }
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
    for (k, _) in &t.request {
        if !matches!(
            k.as_str(),
            "repo" | "old_changeset_id" | "new_changeset_id" | "path"
        ) {
            return Err(unknown_field("Diff", k));
        }
    }
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

/// F6: an unknown `request.*` key is a typo that would otherwise send an
/// empty field and pass for the wrong reason.
fn unknown_field(what: &str, name: &str) -> String {
    format!("{what} knows no request field {name}")
}

/// Builds the file's first `GetObjects` request.
fn get_objects_request(t: &Transcript, f: &Fixture) -> Result<GetObjectsRequest, String> {
    let value = t
        .request
        .iter()
        .find(|(path, _)| path == "ticket")
        .map(|(_, v)| resolve(v, f))
        .ok_or_else(|| "the file names no ticket".to_owned())?;
    request_with_ticket(t, f, unhex(&value))
}

/// The file's `GetObjects` request with `ticket` in place of the file's
/// own ticket value, so a pin can send the ticket it names.
fn request_with_ticket(
    t: &Transcript,
    f: &Fixture,
    ticket: Vec<u8>,
) -> Result<GetObjectsRequest, String> {
    let mut req = GetObjectsRequest::default();
    let mut ticket = Some(ticket);
    for (path, value) in &t.request {
        match path.as_str() {
            "ticket" => req.ticket = ticket.take().expect("the file names one ticket"),
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
            Ok(stream) => {
                // The call's headers are OK: the file's `status:` pin
                // must be Ok, or the pin would never be compared on a
                // served reply and a file that pins a failure would pass
                // on one.
                check_status(t, Code::Ok)?;
                stream.into_inner()
            }
            Err(status) => {
                // The call itself failed: the file pins the code as the
                // status, or as `resume_status` when this is a resumed
                // round - section 8 allows a short OK reply and then the
                // upstream error on the next call.
                let want = if rounds > 1 {
                    t.expect
                        .iter()
                        .find(|(k, _)| k == "resume_status")
                        .map(|(_, v)| v.as_str())
                } else {
                    None
                };
                let got = status.code();
                let ok = match want {
                    Some(name) => got == code(name),
                    None => check_status(t, got).is_ok(),
                };
                if !ok {
                    return Err(format!(
                        "the resumed round's status is {got:?}, the file pins {:?}: {}",
                        want,
                        status.message()
                    ));
                }
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
    // The reply's encoding decides the bytes: a zstd answer is unpacked
    // with the first part's size as the bound, then hashed.
    let raw = match Encoding::try_from(object.encoding) {
        Ok(Encoding::Raw) => object.data.clone(),
        Ok(Encoding::Zstd) => zstd::bulk::decompress(
            &object.data,
            usize::try_from(object.size)
                .map_err(|e| format!("size {} is no usize: {e}", object.size))?,
        )
        .map_err(|e| format!("the zstd frame does not unpack: {e}"))?,
        _ => return Err(format!("the encoding {} is no encoding", object.encoding)),
    };
    let id: Vec<u8> = match ObjectKind::try_from(kind) {
        Ok(ObjectKind::Tree) => Tree::decode_with_id(&raw)
            .map_err(|e| format!("the tree bytes do not decode: {e}"))?
            .1
            .as_bytes()
            .to_vec(),
        Ok(ObjectKind::Blob) => scion_core::BlobId::of(&raw).as_bytes().to_vec(),
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
    if object.encoding == Encoding::Raw as i32 && object.encoded_size != object.size {
        return Err(format!(
            "raw encoded_size {} does not equal size {}",
            object.encoded_size, object.size
        ));
    }
    if raw.len() as u64 != object.size {
        return Err(format!(
            "the object is {} bytes, the first part said size {}",
            raw.len(),
            object.size
        ));
    }
    Ok((kind, hex(&id)))
}
