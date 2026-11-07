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
    Encoding, GetChangesetRequest, GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectPart,
    ObjectRef, ResolveRefRequest, scion_client::ScionClient,
};
use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
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
    /// The changeset the file's ticket is minted for, when the file
    /// reaches past the root changeset (`big_cs` and friends).
    ticket_for: Option<String>,
    /// The bearer token the file's calls carry, when the file pins an
    /// auth refusal; every other file carries the one valid token.
    token: Option<String>,
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
    let mut ticket_for = None;
    let mut token = None;
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
            "ticket_for" => ticket_for = Some(value.to_owned()),
            "token" => token = Some(value.to_owned()),
            "status" | "end" | "resume_status" | "objects" | "entries" | "next"
            | "next_nonempty" | "cursor_from" | "objects_parts" => {
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
            other => panic!("{}:{} names unknown key {other}", at.display(), n + 1),
        }
    }
    // The header's format is checked here, so a malformed since fails
    // the corpus run too, not only the N-1 legs.
    let _since_version = version_of(since.as_deref().expect("the file names its since"));
    Transcript {
        path: at.display().to_string(),
        since: since.unwrap_or_else(|| panic!("{} names no since", at.display())),
        method: method.unwrap_or_else(|| panic!("{} names no method", at.display())),
        fixture: fixture.unwrap_or_else(|| panic!("{} names no fixture", at.display())),
        ticket_for,
        token,
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
    /// The chain the read files replay against: the root changeset and
    /// the middle one, whose tree still holds the old src/main.rs.
    root_cs: ChangesetId,
    mid_cs: ChangesetId,
    /// The head of the blame chain: src/lines.txt at its 100th version.
    blame_head: ChangesetId,
    /// The big side branch's head: the seeded blob, the wide tree and
    /// the seeded message ride its one commit.
    big_cs: ChangesetId,
    /// The seeded content of big/seeded.txt, 1.5 MiB.
    big_content: Vec<u8>,
    /// The `ChangesetV1` bytes of the head's next changeset, and the id
    /// they hash to: the `CreateChangeset` files' bytes and pin.
    encoded_next: Vec<u8>,
    next_cs: ChangesetId,
    /// The `ChangesetV1` bytes of a changeset whose parent no store
    /// names: the unknown-parent file's bytes.
    encoded_orphan: Vec<u8>,
    /// The `ChangesetV1` bytes of the head's scratch changeset, created
    /// and landed through the server: the `GetLand` file's subject.
    encoded_scratch: Vec<u8>,
    /// The id of the land the fixture itself made, as a string: the
    /// `GetLand` file's request.
    land_id: String,
    /// The root changeset's own tree: a CAS object, which `GetObjects`
    /// serves. The head's root tree is derived and lives elsewhere.
    root_tree: [u8; 32],
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

/// The blame chain: src/lines.txt gains one line per changeset, 101
/// versions on a side branch off the middle, so the blame call stops
/// at its 100-version bound with one line left and answers a cursor.
/// Main keeps its three-commit shape, so the other files' pins hold.
/// Answers the branch head.
fn blame_chain(store: &Backend, mid_id: ChangesetId) -> ChangesetId {
    let mut lines: Vec<u8> = Vec::new();
    let mut blame_parent = mid_id;
    for k in 0..101 {
        lines.extend_from_slice(format!("line {k}\n").as_bytes());
        let line_blob = BlobId::of(&lines);
        store.blobs().put(&lines).unwrap();
        let version = common::changeset(
            Some(blame_parent),
            &[(
                "src/lines.txt",
                FileState::Regular {
                    content: line_blob,
                    executable: false,
                },
            )],
        );
        blame_parent = common::land_ref(store, &version, "blame");
    }
    blame_parent
}

/// The big commit's message: the seeded bytes plus the one trailing
/// newline git's cleanup leaves, so both sides encode to one length.
fn big_message() -> Vec<u8> {
    let mut msg = seeded(1_258_291);
    msg.push(b'\n');
    msg
}

/// The big side branch: one commit off `parent` carrying the seeded
/// 1.5 MiB blob, the seeded 1.2 MiB message, and 14,000 one-entry files
/// whose directory tree passes 1 MiB. Answers the branch head and the
/// seeded blob's bytes.
fn big_branch(store: &Backend, parent: ChangesetId) -> (ChangesetId, Vec<u8>) {
    let big_content = seeded(1_572_864);
    let big_blob = BlobId::of(&big_content);
    store.blobs().put(&big_content).unwrap();
    let mut big_files: Vec<(String, FileState)> = vec![(
        "big/seeded.txt".to_owned(),
        FileState::Regular {
            content: big_blob,
            executable: false,
        },
    )];
    let dot_blob = BlobId::of(b".\n");
    store.blobs().put(b".\n").unwrap();
    for k in 0..14_000 {
        big_files.push((
            format!("wide/file-{k:05}-of-the-wide-tree-directory-entry.txt"),
            FileState::Regular {
                content: dot_blob,
                executable: false,
            },
        ));
    }
    let big_changes: Vec<(&str, FileState)> =
        big_files.iter().map(|(p, st)| (p.as_str(), *st)).collect();
    let big_cs = common::changeset_msg(Some(parent), &big_changes, &big_message());
    let big_id = common::land_ref(store, &big_cs, "big");
    (big_id, big_content)
}

/// The scratch ref, the next and orphan repo blobs, and the three
/// encodings: the store-side preconditions the create files' calls
/// must pass before the server walks the changeset at all. Answers
/// the four pins' values.
fn encodings(
    store: &Backend,
    head: &scion_core::Changeset,
    id: &ChangesetId,
) -> (Vec<u8>, ChangesetId, Vec<u8>, Vec<u8>) {
    // The scratch ref exists before the land: a land to a ref that
    // does not exist is rejected. Its tip is the head, the scratch
    // changeset's parent, so the land is a fast-forward.
    common::land_ref(store, head, "scratch");
    // The next and orphan blobs join the repo first: a
    // CreateChangeset whose blobs the repo does not hold answers the
    // precondition, "upload it with PutBlob".
    let next_blob = BlobId::of(b"pub fn next() {}\n");
    let orphan_blob = BlobId::of(b"pub fn orphan() {}\n");
    store
        .blobs()
        .put(b"pub fn next() {}\n")
        .expect("the next blob stores");
    store
        .blobs()
        .put(b"pub fn orphan() {}\n")
        .expect("the orphan blob stores");
    store
        .write(|c| {
            let repo = c.repo_id(common::REPO)?;
            c.add_repo_blobs(repo, &[next_blob, orphan_blob])
        })
        .unwrap();
    let (encoded_next, next_id) = encoded_next(id);
    let encoded_orphan = encoded_orphan();
    let encoded_scratch = encoded_scratch(id);
    (encoded_next, next_id, encoded_orphan, encoded_scratch)
}

/// The head's next changeset: its `ChangesetV1` bytes and the id they
/// hash to. The `CreateChangeset` files' bytes and pin.
fn encoded_next(parent: &ChangesetId) -> (Vec<u8>, ChangesetId) {
    let next = common::changeset(
        Some(*parent),
        &[(
            "src/next.rs",
            FileState::Regular {
                content: BlobId::of(b"pub fn next() {}\n"),
                executable: false,
            },
        )],
    );
    next.encode_with_id()
}

/// A changeset whose parent no store names: the unknown-parent file's
/// `ChangesetV1` bytes.
fn encoded_orphan() -> Vec<u8> {
    common::changeset(
        Some(ChangesetId::from_bytes(missing_id(0xab))),
        &[(
            "src/orphan.rs",
            FileState::Regular {
                content: BlobId::of(b"pub fn orphan() {}\n"),
                executable: false,
            },
        )],
    )
    .encode()
}

/// The head's scratch changeset: its `ChangesetV1` bytes, created and
/// landed through the server for the `GetLand` file.
fn encoded_scratch(parent: &ChangesetId) -> Vec<u8> {
    common::changeset(
        Some(*parent),
        &[(
            "src/scratch.rs",
            FileState::Regular {
                content: BlobId::of(b"pub fn scratch() {}\n"),
                executable: false,
            },
        )],
    )
    .encode()
}

/// The fixture's own land: the scratch bytes created through the
/// server, then landed on the scratch ref, so main keeps its tip and
/// the `GetLand` file has an id to read. Answers the land reply's id.
fn fixture_land(tiers: &Tiers, encoded: &[u8]) -> String {
    let runtime = &tiers.runtime;
    let mut client = runtime.block_on(scion_client(&tiers.server));
    // The blob rides first: a CreateChangeset whose blobs the store
    // lacks answers not-found's precondition, "upload it with PutBlob".
    let _ = runtime
        .block_on(
            client.put_blob(tokio_stream::iter([scion_proto::v1::PutBlobRequest {
                repo: common::REPO.into(),
                data: b"pub fn scratch() {}\n".to_vec(),
            }])),
        )
        .expect("the scratch blob uploads");
    let created = runtime
        .block_on(
            client.create_changeset(scion_proto::v1::CreateChangesetRequest {
                repo: common::REPO.into(),
                changeset: encoded.to_vec(),
            }),
        )
        .expect("the scratch changeset is created")
        .into_inner();
    let reply = runtime
        .block_on(client.land(scion_proto::v1::LandRequest {
            repo: common::REPO.into(),
            changeset_id: created.changeset_id,
            target_ref: "scratch".into(),
            request_key: "fixture-land".into(),
        }))
        .expect("the fixture's land answers")
        .into_inner();
    assert_eq!(
        reply.state,
        scion_proto::v1::LandState::Landed as i32,
        "the fixture's land must land; the server says: {}",
        reply.detail
    );
    reply.land_id
}

/// Builds the one fixture the corpus uses: the conformance test's
/// landed repo grown into a first-parent chain - the root changeset
/// creates src/main.rs, the middle changeset changes it, the head adds
/// src/lib.rs - and a `feature` ref parked at the middle changeset,
/// which the Land files move. The head answers the read files as before;
/// the chain gives Log its three entries and Blame its two.
fn landed() -> (Tiers, Fixture) {
    let data = common::private_tempdir();
    let store = Backend::open_test(data.path()).unwrap();
    let content = b"pub fn main() {}\n".to_vec();
    let blob = BlobId::of(&content);
    store.blobs().put(&content).unwrap();
    let root = common::changeset(
        None,
        &[(
            "src/main.rs",
            FileState::Regular {
                content: blob,
                executable: false,
            },
        )],
    );
    let root_id = common::land(&store, &root);
    // One line survives from the root and one is new: Blame's two
    // changesets.
    let edited = b"pub fn main() {}\npub fn extra() {}\n".to_vec();
    let edited_blob = BlobId::of(&edited);
    store.blobs().put(&edited).unwrap();
    let mid = common::changeset(
        Some(root_id),
        &[(
            "src/main.rs",
            FileState::Regular {
                content: edited_blob,
                executable: false,
            },
        )],
    );
    let mid_id = common::land(&store, &mid);
    let lib = b"pub const X: u8 = 7;\n".to_vec();
    let lib_blob = BlobId::of(&lib);
    store.blobs().put(&lib).unwrap();
    let head = common::changeset(
        Some(mid_id),
        &[(
            "src/lib.rs",
            FileState::Regular {
                content: lib_blob,
                executable: false,
            },
        )],
    );
    let id = common::land(&store, &head);
    let blame_parent = blame_chain(&store, mid_id);
    // The big side branch: one commit off the head that carries the
    // seeded 1.5 MiB blob, the seeded 1.2 MiB message, and 14,000
    // one-entry files whose directory tree passes 1 MiB. The parts
    // files replay its streams; the bytes ride no transcript file.
    let (big_id, big_content) = big_branch(&store, id);
    store
        .write(|c| {
            let repo = c.repo_id(common::REPO)?;
            assert!(c.cas_ref(repo, "feature", None, mid_id)?);
            Ok(())
        })
        .unwrap();
    let (encoded_next, next_id, encoded_orphan, encoded_scratch) = encodings(&store, &head, &id);
    let tiers = Tiers::start(data, store);
    let runtime_entered = tiers.runtime.enter();
    let (ticket, tree) = mint(&tiers, id);
    let root_tree = {
        let (_, root_tree) = mint(&tiers, root_id);
        root_tree
    };
    // The fixture's own land: the head's child lands on the scratch
    // ref, so main keeps its tip and the GetLand file has an id to
    // read. The land id comes from the land reply, as a client sees it.
    let land_id = fixture_land(&tiers, &encoded_scratch);
    drop(runtime_entered);
    let fixture = Fixture {
        blob,
        content,
        tree,
        cs: id,
        root_cs: root_id,
        mid_cs: mid_id,
        root_tree,
        blame_head: blame_parent,
        big_cs: big_id,
        big_content,
        encoded_next,
        next_cs: next_id,
        encoded_orphan,
        encoded_scratch,
        land_id,

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
        .replace("$tree_root", &hex(&f.root_tree))
        .replace("$tree", &hex(&f.tree))
        .replace("$cs_root", &hex(f.root_cs.as_bytes()))
        .replace("$cs_mid", &hex(f.mid_cs.as_bytes()))
        .replace("$blame_head", &hex(f.blame_head.as_bytes()))
        .replace("$big_cs", &hex(f.big_cs.as_bytes()))
        .replace(
            "$big_blake3",
            &hex(blake3::hash(&f.big_content).as_bytes().as_slice()),
        )
        .replace("$big_blob", &hex(BlobId::of(&f.big_content).as_bytes()))
        .replace("$encoded_next", &hex(&f.encoded_next))
        .replace("$next_cs", &hex(f.next_cs.as_bytes()))
        .replace("$encoded_orphan", &hex(&f.encoded_orphan))
        .replace("$encoded_scratch", &hex(&f.encoded_scratch))
        .replace("$land_id", &f.land_id)
        .replace("$cs", &hex(f.cs.as_bytes()))
        .replace("$missing", &hex(&f.missing))
        .replace("$unsigned", &hex(&f.unsigned))
        .replace("$ticket", &hex(&f.ticket))
}

/// Builds `n` bytes of seeded text: a xorshift64 stream over the
/// letters. The same seed answers the same bytes on every side, so the
/// big fixtures ride no file and the transcripts pin their length and
/// BLAKE3 instead.
fn seeded(n: usize) -> Vec<u8> {
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push(b'a' + (x % 26) as u8);
    }
    out
}

/// The last cursor each streamed reply answered, by the file's
/// corpus-relative path: a continuation file names the full path of the
/// file whose cursor it sends back, and the runner feeds it here. The
/// full path, not the name: the corpus leg and the N-1 leg run in one
/// process when a whole pass runs locally, hold different stores, and
/// a name key would let one leg overwrite the other's cursor.
static CURSORS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// The key a file's cursor sits under: the path's part after
/// `tests/`, so a file's own path and a `cursor_from` value name the
/// same key whatever form the walker printed the path in.
fn cursor_key(path: &str) -> String {
    let marker = "tests/";
    match path.rfind(marker) {
        Some(i) => path[i + marker.len()..].to_owned(),
        None => path.to_owned(),
    }
}

fn remember_cursor(t: &Transcript, cursor: Vec<u8>) {
    let key = cursor_key(&t.path);
    CURSORS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key, cursor);
}

/// The cursor a `cursor_from` file sends: the reply of the file it
/// names. Missing when that file has not run - a wiring mistake, and
/// an error, never an empty cursor sent by luck.
fn cursor_of(source: &str) -> Result<Vec<u8>, String> {
    let key = cursor_key(source);
    CURSORS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&key)
        .cloned()
        .ok_or_else(|| format!("CURSOR_FROM names {source}, which has not run in this pass"))
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
        "Scion/GetChangeset" => {
            let mut scion = scion_client(url).await;
            scion
                .get_changeset(GetChangesetRequest::default())
                .await
                .err()
                .map(|s| s.code())
        }
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

/// A cache and daemon no other file touched: the pair a `tiers: fresh`
/// file reruns on. The in-process pair's directory guard and the N-1
/// pair's children keep their pair alive for the file's run.
struct FreshPair {
    cache_url: String,
    socket: String,
    _in_process: Option<common::FreshRelays>,
    _processes: Vec<Child>,
}

/// One file's full run on one environment: the warm-up a `daemon_warm:
/// serves` file pins, the file's run across the warm trio, and the
/// `tiers: fresh` rerun on a pair no earlier file touched. The corpus
/// test and the N-1 legs share this, so the legs run the same steps the
/// main test runs.
fn run_one_object_file(
    runtime: &tokio::runtime::Runtime,
    warm: [&str; 3],
    fresh: &dyn Fn() -> FreshPair,
    t: &Transcript,
    f: &Fixture,
    file: &Path,
) {
    let tier_value = |name: &str| {
        t.expect
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    };
    let tier_refuses = tier_value("tiers") == Some("refuse");
    if t.expect
        .iter()
        .any(|(k, v)| k == "daemon_warm" && v == "serves")
    {
        warm_daemon_for(runtime, warm[2], t, f, file);
    }
    run_objects_across_tiers(runtime, warm, t, f, file);
    if tier_value("tiers") == Some("fresh") {
        let fresh = fresh();
        for url in [&fresh.cache_url, &fresh.socket] {
            if tier_refuses {
                runtime.block_on(assert_tier_refuses(url, t, file));
                continue;
            }
            runtime
                .block_on(run_objects(t, url, f))
                .unwrap_or_else(|e| panic!("{} at {} (fresh): {e}", file.display(), url));
        }
    }
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
                        .expect("the pinned stream reads")
                        .0;
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

/// Every `cursor_from` names a file in the corpus whose since is the
/// same or older, so the source runs (or skips) beside its dependent in
/// every pass, never leaves it without a cursor.
fn check_cursor_sources(files: &[PathBuf]) -> Result<(), String> {
    let mut sinces: std::collections::HashMap<String, (u64, u64, u64)> =
        std::collections::HashMap::new();
    for file in files {
        let text = fs::read_to_string(file).expect("the transcript opens");
        let key = cursor_key(&file.display().to_string());
        sinces.insert(key, version_of(&parse(file, &text).since));
    }
    for file in files {
        let text = fs::read_to_string(file).expect("the transcript opens");
        let t = parse(file, &text);
        for (k, v) in &t.expect {
            if k != "cursor_from" {
                continue;
            }
            let name = cursor_key(v);
            let source = sinces
                .get(&name)
                .ok_or_else(|| format!("CURSOR_FROM names {v}, which is not in the corpus"))?;
            let mine = version_of(&t.since);
            if *source > mine {
                return Err(format!(
                    "CURSOR_FROM names {v}, whose since is newer than {}'s",
                    file.display()
                ));
            }
            // The pass runs the corpus in the sorted path order, so the
            // source must sort before its dependent: a source that
            // sorts after would leave it without a cursor.
            if name >= cursor_key(&file.display().to_string()) {
                return Err(format!(
                    "CURSOR_FROM names {v}, which sorts after {} in the corpus's run order",
                    file.display()
                ));
            }
        }
    }
    Ok(())
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
        56,
        "the corpus pins 56 behaviours; a file added or removed must update this count"
    );
    check_cursor_sources(&files).unwrap_or_else(|e| panic!("{e}"));
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
        let ticket_cs = ticket_changeset(&t, &base, base.root_cs);
        let (ticket, _) = mint(&tiers, ticket_cs);
        let f = Fixture {
            unsigned: unsigned_of(&ticket),
            ticket,
            ..base.clone()
        };
        let mint_age = minted_at.elapsed().as_secs_f64();
        let started_at = std::time::Instant::now();
        let across_tiers = t.method.starts_with("Objects/")
            || file.parent().is_some_and(|dir| dir.ends_with("objects"));
        if across_tiers {
            let fresh = || {
                // The same file again on a cache and daemon no earlier
                // file touched: the same server and keys, so the file's
                // ticket still verifies; the fresh daemon forwards, and
                // the upstream refuses, so the file's own expectations
                // hold everywhere.
                let fresh = tiers.fresh_relays();
                FreshPair {
                    cache_url: fresh.cache_url.clone(),
                    socket: fresh.socket.clone(),
                    _in_process: Some(fresh),
                    _processes: Vec::new(),
                }
            };
            run_one_object_file(
                &tiers.runtime,
                [&tiers.server, &tiers.cache_url, &tiers.socket],
                &fresh,
                &t,
                &f,
                &file,
            );
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

/// The release version a ref carries: `v0.7` and `v0.7.1` parse; a
/// commit hash or a branch name carries none. A `v`-prefixed ref whose
/// version does not parse is a wiring mistake and fails.
fn ref_version(r: &str) -> Option<(u64, u64, u64)> {
    let rest = r.strip_prefix('v')?;
    let bad = || panic!("PREV_REF names a release whose version does not parse: {r}");
    let mut parts = rest.split('.');
    let one =
        |p: Option<&str>| -> u64 { p.and_then(|p| p.parse::<u64>().ok()).unwrap_or_else(bad) };
    Some((
        one(parts.next()),
        one(parts.next()),
        one(parts.next().or(Some("0"))),
    ))
}

/// The files the N-1 legs run: every file whose `since` is at or under
/// the N-1 version. A Scion file's fixture names the changeset this
/// tree's store landed, and the N-1 side's changeset id is not plumbed
/// into the harness yet (the next slice pairs it through the ref), so
/// such a file stays out of the legs and the count reports it. Returns
/// the qualifying files, the Scion count, and the corpus's oldest
/// `since`.
/// A `since:` value's version and the text it came from.
type Since = ((u64, u64, u64), String);

fn pick_n1_files(files: &[PathBuf], prev: (u64, u64, u64)) -> (Vec<(PathBuf, Transcript)>, Since) {
    let mut qualifying = Vec::new();
    let mut oldest: Option<Since> = None;
    for file in files {
        let text = fs::read_to_string(file).expect("the transcript opens");
        let t = parse(file, &text);
        let v = version_of(&t.since);
        oldest = Some(match oldest {
            Some((ov, os)) if ov <= v => (ov, os),
            _ => (v, t.since.clone()),
        });
        if v > prev {
            continue;
        }
        qualifying.push((file.clone(), t));
    }
    (qualifying, oldest.expect("the corpus is not empty"))
}

/// The Ok-on-$cs control guards the Scion refusal files that name $cs:
/// a wrong changeset id passes them with the refusal they expect anyway,
/// because the same `NOT_FOUND` answers a missing path and a missing
/// changeset alike. Both sides derive from the files' content, so a new
/// refusal file joins the guard by itself. The control runs first, so a
/// wrong id fails early with the clearest line the corpus has; the leg
/// fails naming the file when the control does not qualify with it.
fn guard_cs_refusals(qualifying: &mut [(PathBuf, Transcript)]) {
    let names_cs = |t: &Transcript| t.request.iter().any(|(_, v)| v.contains("$cs"));
    let expects =
        |t: &Transcript, want: &str| t.expect.iter().any(|(k, v)| k == "status" && v == want);
    let needs_control =
        |t: &Transcript| t.method.starts_with("Scion/") && names_cs(t) && !expects(t, "ok");
    let answers_ok_on_cs =
        |t: &Transcript| t.method.starts_with("Scion/") && names_cs(t) && expects(t, "ok");
    qualifying.sort_by_key(|(_, t)| !answers_ok_on_cs(t));
    let has_control = qualifying.iter().any(|(_, t)| answers_ok_on_cs(t));
    for (file, t) in qualifying.iter() {
        assert!(
            !needs_control(t) || has_control,
            "{}, a $cs refusal file qualifies without an Ok-on-$cs control: the wrong-changeset pass is unguarded",
            file.display()
        );
    }
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
/// The files the N-1 side runs: a file's `since` must be at or under
/// the version `PREV_REF` names. `PREV_REF` names the ref the
/// version-matrix job tested; a `v`-prefixed release ref carries the
/// version itself, a stand-in commit carries none and so every file
/// qualifies against it.
fn n1_qualifying() -> Vec<(PathBuf, Transcript)> {
    let ref_name = std::env::var("PREV_REF").unwrap_or_else(|_| "unset".to_owned());
    let (prev, printed) = match ref_version(&ref_name) {
        Some(v) => (v, format!("N-1 {ref_name}")),
        None => (
            (u64::MAX, u64::MAX, u64::MAX),
            format!("{ref_name}, a stand-in with no release version, so every file qualifies"),
        ),
    };
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(CORPUS);
    let mut files = Vec::new();
    walk(&corpus, &mut files).expect("the corpus holds only transcripts");
    files.sort();
    let (mut qualifying, (oldest_v, oldest_s)) = pick_n1_files(&files, prev);
    let oldest_printed = if oldest_v.0 == u64::MAX {
        oldest_s.clone()
    } else {
        format!("{oldest_s} at version {oldest_v:?}")
    };
    assert!(
        !qualifying.is_empty(),
        "the N-1 legs have nothing to prove: the corpus's oldest since is {oldest_printed}, the N-1 side is {prev:?} ({printed})"
    );
    guard_cs_refusals(&mut qualifying);
    qualifying
}

/// The N-1 database's rows for `id`: the tree body's byte length (none
/// when the table holds no row) and whether a derived value names it.
fn n1_store_rows(pg: &str, id: &str, cs: &str) -> String {
    let sql = format!(
        "SELECT (SELECT length(body) FROM trees WHERE id = decode('{id}','hex')) \
         AS tree_body, \
         (SELECT count(*) FROM derived WHERE value = decode('{id}','hex')) \
         AS derived_rows, \
         (SELECT count(*) FROM derived WHERE cs = decode('{cs}','hex')) \
         AS derived_cs",
    );
    let out = std::process::Command::new("psql")
        .args(["-t", "-A", "-F", " ", "-c", &sql, pg])
        .output()
        .expect("psql runs");
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// Prints the N-1 server's ids for the chain's ends: the head's and the
/// root changeset's, with the trees `GetRootTree` names for each.
fn n1_print_ids(
    runtime: &tokio::runtime::Runtime,
    server: &str,
    base: &Fixture,
) -> (String, String) {
    let root_tree_of = |cs: &scion_core::ChangesetId| -> String {
        runtime.block_on(async {
            let mut client = connect(server).await;
            client
                .get_root_tree(scion_proto::v1::GetRootTreeRequest {
                    repo: common::REPO.into(),
                    changeset_id: cs.as_bytes().to_vec(),
                })
                .await
                .expect("the N-1 server mints")
                .into_inner()
                .tree_id
                .try_into()
                .map(|t: [u8; 32]| hex(t.as_slice()))
                .expect("the tree id is 32 bytes")
        })
    };
    let head_tree = root_tree_of(&base.cs);
    let root_tree = root_tree_of(&base.root_cs);
    eprintln!(
        "N1 IDS head={} head_tree={} root_cs={} root_tree={}",
        hex(base.cs.as_bytes()),
        head_tree,
        hex(base.root_cs.as_bytes()),
        root_tree,
    );
    (head_tree, root_tree)
}

/// The store's rows for the two trees, on the N-1 database.
fn n1_print_store(pg: &str, base: &Fixture, head_tree: &str, root_tree: &str) {
    eprintln!(
        "N1 STORE head_tree={head_tree} [{}] root_tree={root_tree} [{}]",
        n1_store_rows(pg, head_tree, &hex(base.cs.as_bytes())),
        n1_store_rows(pg, root_tree, &hex(base.root_cs.as_bytes())),
    );
}

/// Prints one `GetObjects` file's inputs: the ticket's own root tree and
/// the refs after substitution, so a red run names the id that disagrees.
fn n1_print_file(file: &std::path::Path, t: &Transcript, base: &Fixture, ticket: &[u8]) {
    let scope = scion_proto::ticket::parse_unverified(ticket, scion_proto::ticket::now())
        .map(|t| hex(t.root_tree.as_slice()))
        .map_err(|e| e.to_string());
    let refs: Vec<String> = t
        .request
        .iter()
        .filter(|(k, _)| k == "refs")
        .map(|(_, v)| resolve(v, base))
        .collect();
    eprintln!(
        "N1 {} ticket_root={scope:?} refs={refs:?} root_cs={} blob={}",
        file.display(),
        hex(base.root_cs.as_bytes()),
        hex(base.blob.as_bytes()),
    );
}

/// The blame diagnostics, before the leg's files run: the store's
/// counts, and the N-1's own answer for the blame head (the parts'
/// ranges and changesets, and the cursor's length).
fn n1_blame_probe(runtime: &tokio::runtime::Runtime, pg: &str, server: &str, base: &Fixture) {
    let blame = hex(base.blame_head.as_bytes());
    let sql = "SELECT (SELECT count(*) FROM changesets) AS cs, (SELECT count(*) FROM derived WHERE kind = 'path_history') AS ph, (SELECT count(*) FROM repo_blobs) AS blobs";
    let out = std::process::Command::new("psql")
        .args(["-t", "-A", "-c", sql, pg])
        .output()
        .expect("psql runs");
    eprintln!(
        "N1 BLAME head={blame} store={}",
        String::from_utf8_lossy(&out.stdout).trim()
    );
    runtime
        .block_on(async {
            let mut client = scion_client(server).await;
            let reply = client
                .blame(scion_proto::v1::BlameRequest {
                    repo: common::REPO.into(),
                    changeset_id: base.blame_head.as_bytes().to_vec(),
                    path: b"src/lines.txt".to_vec(),
                    cursor: Vec::new(),
                })
                .await;
            match reply {
                Ok(reply) => {
                    let mut stream = reply.into_inner();
                    let mut n = 0usize;
                    while let Some(item) = read_bounded(&mut stream).await? {
                        n += 1;
                        eprintln!(
                            "N1 BLAME part {n}: ranges={} changesets={} cursor_len={}",
                            item.ranges.len(),
                            item.changesets.len(),
                            item.next_cursor.len()
                        );
                    }
                }
                Err(status) => eprintln!("N1 BLAME probe status={status:?}"),
            }
            Ok::<(), String>(())
        })
        .unwrap_or_else(|e| eprintln!("N1 BLAME probe error: {e}"));
}

/// The objects diagnostics, before the leg's files run: the N-1's own
/// answer for the big blob through a ticket minted for its changeset.
fn n1_objects_probe(runtime: &tokio::runtime::Runtime, server: &str, base: &Fixture) {
    runtime
        .block_on(async {
            let mut client = connect(server).await;
            let reply = client
                .get_root_tree(GetRootTreeRequest {
                    repo: common::REPO.into(),
                    changeset_id: base.big_cs.as_bytes().to_vec(),
                })
                .await
                .expect("the big root tree answers")
                .into_inner();
            eprintln!("N1 OBJECTS big_root_tree={}", hex(reply.tree_id.as_slice()));
            let mut client = connect(server).await;
            let req = GetObjectsRequest {
                ticket: reply.ticket,
                refs: vec![ObjectRef {
                    kind: ObjectKind::Blob as i32,
                    id: BlobId::of(&base.big_content).as_bytes().to_vec(),
                    path: b"big/seeded.txt".to_vec(),
                }],
                accept: vec![Encoding::Raw as i32],
            };
            let mut stream = match client.get_objects(req).await {
                Ok(stream) => stream.into_inner(),
                Err(status) => {
                    eprintln!("N1 OBJECTS probe status={status:?}");
                    return Ok::<(), String>(());
                }
            };
            let mut n = 0usize;
            loop {
                let item = match read_bounded(&mut stream).await {
                    Ok(Some(item)) => item,
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("N1 OBJECTS probe read: {e}");
                        break;
                    }
                };
                n += 1;
                eprintln!(
                    "N1 OBJECTS part {n}: id={} kind={} size={} data_len={}",
                    hex(item.id.as_slice()),
                    item.kind,
                    item.size,
                    item.data.len()
                );
            }
            eprintln!("N1 OBJECTS parts={n}");
            Ok::<(), String>(())
        })
        .unwrap_or_else(|e| eprintln!("N1 OBJECTS probe error: {e}"));
}

#[test]
fn the_n1_side_answers_as_its_files_say() {
    let Ok(prev_bin) = std::env::var("PREV_BIN") else {
        eprintln!("PREV_BIN unset: the N-1 legs run in the version-matrix job");
        return;
    };
    let prev_bin = PathBuf::from(prev_bin);

    // The logs must outlive a red run: the version-matrix job uploads
    // /tmp/scion-matrix*/, so the work dir sits there, named for this
    // run, and only a green run removes it.
    let dir = std::env::temp_dir().join(format!("scion-matrix-n1-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("the work dir is made");
    make_private(&dir);
    eprintln!("the N-1 work dir: {}", dir.display());

    let qualifying = n1_qualifying();

    // Leg 1: the qualifying files against the N-1 server and its tiers.
    let (db_guard, pg) = n1_database();
    let stack = start_n1_stack(&prev_bin, &dir, &pg);
    let runtime = tokio::runtime::Runtime::new().expect("the runtime starts");
    let _entered = runtime.enter();
    let base = n1_fixture(&runtime, &stack.server);
    let mut ran = 0usize;
    // What the N-1 server answers for the chain's ends: the ids a
    // GetObjects file's refs name must be these.
    let (head_tree, root_tree) = n1_print_ids(&runtime, &stack.server, &base);
    n1_print_store(&pg, &base, &head_tree, &root_tree);
    // The blame chain's shape on this side, and the N-1's own answer
    // for the blame head, before the files run.
    n1_blame_probe(&runtime, &pg, &stack.server, &base);
    n1_objects_probe(&runtime, &stack.server, &base);
    for (file, t) in &qualifying {
        // The GetObjects file's refs name the root changeset's tree and
        // blob, unless the file names another changeset: the ticket
        // must be that tree's, as the corpus's is.
        let ticket_cs = ticket_changeset(t, &base, base.root_cs);
        let ticket = n1_mint(&runtime, &stack.server, &ticket_cs);
        if t.method == "Objects/GetObjects" {
            n1_print_file(file, t, &base, &ticket);
        }
        let f = Fixture {
            unsigned: unsigned_of(&ticket),
            ticket,
            ..base.clone()
        };
        if t.method.starts_with("Scion/") {
            // A tier serves GetObjects only: the Scion service is the
            // server's, so the file runs against the N-1 server alone.
            runtime
                .block_on(run_scion(t, &stack.server, &f))
                .unwrap_or_else(|e| panic!("{} at {} (leg 1): {e}", file.display(), stack.server));
            ran += 1;
            continue;
        }
        let index = ran;
        let fresh = || n1_fresh_pair(&prev_bin, &dir, index, &stack.server, &stack.pub_hex);
        run_one_object_file(
            &runtime,
            [&stack.server, &stack.cache, &stack.socket],
            &fresh,
            t,
            &f,
            file,
        );
        ran += 1;
    }

    // Leg 2: the N-1 tiers in front of this server - the upgrade order.
    // The corpus's landed store and tickets are the fixture; the N-1
    // cache verifies the tickets of this server's keys, the N-1 daemon
    // forwards to it.
    let (tiers, base2) = landed();
    let _entered2 = tiers.runtime.enter();
    let leg2 = start_n1_tiers(&tiers, &prev_bin, &dir);
    for (file, t) in &qualifying {
        if t.method.starts_with("Scion/") {
            // A tier serves GetObjects only: a Scion call never
            // traverses the tiers, and this server's Scion answers are
            // the corpus test's every PR. Leg 2 is Objects only.
            continue;
        }
        // The GetObjects file's refs name the root changeset's tree and
        // blob unless the file names another changeset: the ticket must
        // be that tree's, as leg 1's is.
        let ticket_cs = ticket_changeset(t, &base2, base2.root_cs);
        let (ticket, _) = mint(&tiers, ticket_cs);
        let f = Fixture {
            unsigned: unsigned_of(&ticket),
            ticket,
            ..base2.clone()
        };
        let index = ran;
        let fresh = || {
            let keys: Vec<String> = tiers.ticket_public_keys();
            n1_fresh_pair_keys(&prev_bin, &dir, index, &tiers.server, &keys)
        };
        run_one_object_file(
            &tiers.runtime,
            [&tiers.server, &leg2.cache, &leg2.socket],
            &fresh,
            t,
            &f,
            file,
        );
        ran += 1;
    }
    drop(leg2);
    eprintln!("the N-1 legs ran {ran} file-runs over the two legs");
    // A green run leaves nothing: the children die with the guards, the
    // database drops with `db_guard`, and the work dir goes last.
    drop(stack);
    drop(db_guard);
    let _ = fs::remove_dir_all(&dir);
}

/// A fresh N-1 cache and daemon in front of `server`, verifying the key
/// whose hex is `pub_hex`: the pair a `tiers: fresh` file reruns on.
fn n1_fresh_pair(
    prev_bin: &Path,
    dir: &Path,
    index: usize,
    server: &str,
    pub_hex: &str,
) -> FreshPair {
    n1_fresh_pair_keys(prev_bin, dir, index, server, &[pub_hex.to_owned()])
}

/// The same, for a caller that holds several public keys.
fn n1_fresh_pair_keys(
    prev_bin: &Path,
    dir: &Path,
    index: usize,
    server: &str,
    keys: &[String],
) -> FreshPair {
    let key_flags: Vec<String> = keys
        .iter()
        .map(|k| format!("--ticket-public-key={k}"))
        .collect();
    let cache_log = dir.join(format!("fresh-{index}-cache.log"));
    let mut cache = spawn_logged(
        prev_bin.join("scion-cache"),
        std::iter::empty()
            .chain(["--listen", "127.0.0.1:0", "--upstream", server])
            .chain(key_flags.iter().map(String::as_str)),
        &tier_tokens(),
        &cache_log,
    );
    let port = serving_port(&mut cache, &cache_log);
    let cache_url = format!("http://127.0.0.1:{port}");
    let relay_dir = dir.join(format!("fresh-{index}"));
    fs::create_dir_all(&relay_dir).expect("the fresh pair's dir is made");
    make_private(&relay_dir);
    let sock = dir.join(format!("fresh-{index}.sock"));
    let mut daemon = spawn_logged(
        prev_bin.join("scion-daemon"),
        [
            "--data",
            &relay_dir.display().to_string(),
            "--socket",
            &sock.display().to_string(),
            "--upstream",
            &cache_url,
        ],
        &tier_tokens(),
        &dir.join(format!("fresh-{index}.log")),
    );
    wait_serving(&mut daemon, &dir.join(format!("fresh-{index}.log")));
    FreshPair {
        cache_url,
        socket: format!("unix://{}", sock.display()),
        _in_process: None,
        _processes: vec![cache, daemon],
    }
}

/// The token pair every tier process speaks: the corpus client's token
/// as the process's own, and the same upstream, so a relay forwards.
fn tier_tokens() -> [(&'static str, &'static str); 2] {
    [
        ("SCION_TOKEN", common::TOKEN),
        ("SCION_UPSTREAM_TOKEN", common::TOKEN),
    ]
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
    let mut cache = spawn_logged(
        prev_bin.join("scion-cache"),
        std::iter::empty()
            .chain(["--listen", "127.0.0.1:0", "--upstream", &tiers.server])
            .chain(keys.iter().map(String::as_str)),
        &tier_tokens(),
        &cache_log,
    );
    let cport = serving_port(&mut cache, &cache_log);
    let relay_dir = dir.join("tiers");
    fs::create_dir_all(&relay_dir).expect("the leg-2 tiers dir is made");
    make_private(&relay_dir);
    let relay_sock = dir.join("tier.sock");
    let mut daemon = spawn_logged(
        prev_bin.join("scion-daemon"),
        [
            "--data",
            &relay_dir.display().to_string(),
            "--socket",
            &relay_sock.display().to_string(),
            "--upstream",
            &format!("http://127.0.0.1:{cport}"),
        ],
        &tier_tokens(),
        &dir.join("tiers.log"),
    );
    wait_serving(&mut daemon, &dir.join("tiers.log"));
    N1Leg2 {
        cache: format!("http://127.0.0.1:{cport}"),
        socket: format!("unix://{}", relay_sock.display()),
        _children: vec![cache, daemon],
    }
}

/// The version a `since:` or a binary names, as (major, minor, patch);
/// a missing part is zero, so `v0.7` is `(0, 7, 0)`.
fn version_of(v: &str) -> (u64, u64, u64) {
    if v.trim() == "unreleased" {
        // Newer than every release named so far: it qualifies on no
        // N-1 side until a release names it (the release steps bump it).
        return (u64::MAX, 0, 0);
    }
    let rest = v.trim().strip_prefix('v').unwrap_or_else(|| {
        panic!("a since value that is not vMAJOR.MINOR[.PATCH] or unreleased: {v}")
    });
    let mut parts = rest.split('.');
    let one = |p: Option<&str>| -> u64 {
        p.and_then(|p| p.parse::<u64>().ok()).unwrap_or_else(|| {
            panic!("a since value that is not vMAJOR.MINOR[.PATCH] or unreleased: {v}")
        })
    };
    (
        one(parts.next()),
        one(parts.next()),
        one(parts.next().or(Some("0"))),
    )
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

impl Child {
    /// Whether the child has exited: the waiters fail at once on a
    /// child that died before serving.
    fn try_wait(&mut self) -> Option<std::process::ExitStatus> {
        self.0.try_wait().expect("the child's state is readable")
    }
}

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
fn serving_port(child: &mut Child, log: &Path) -> u16 {
    for _ in 0..300 {
        if let Some(status) = child.try_wait() {
            panic!(
                "{}, the child exited with {status} before serving: {}",
                log.display(),
                log_tail(log)
            );
        }
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
    panic!(
        "{}, no serving listen line in 30 s: {}",
        log.display(),
        log_tail(log)
    );
}

/// The log's last lines, for a panic that must say why the child died.
fn log_tail(log: &Path) -> String {
    let lines: Vec<String> = fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .rev()
        .take(5)
        .map(str::to_owned)
        .collect();
    lines.into_iter().rev().collect::<Vec<_>>().join(" | ")
}

/// Waits for LOG's first ` serving` line, up to 30 s: a daemon serves a
/// socket, not a port, and its serving line is the readiness.
fn wait_serving(child: &mut Child, log: &Path) {
    for _ in 0..300 {
        if let Some(status) = child.try_wait() {
            panic!(
                "{}, the child exited with {status} before serving: {}",
                log.display(),
                log_tail(log)
            );
        }
        if let Ok(text) = fs::read_to_string(log)
            && text.contains(" serving")
        {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!(
        "{}, no serving line in 30 s: {}",
        log.display(),
        log_tail(log)
    );
}

/// CHMOD 0700 on DIR: the daemon refuses a data directory open to
/// others.
fn make_private(dir: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).expect("the mode sets");
}

/// Runs one git command, failing the test on a non-zero exit.
fn git(args: &[&str]) {
    let out = Command::new("git")
        .args(args)
        .env("GIT_AUTHOR_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_NAME", "n1")
        .env("GIT_COMMITTER_EMAIL", "n1@localhost")
        .output()
        .expect("git runs");
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
    /// The server's ticket public key, hex: a fresh pair's cache
    /// verifies the tickets with it.
    pub_hex: String,
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
    // The chain the Log, Blame and Land files replay: one line added to
    // src/main.rs, then src/lib.rs, with the feature branch parked at
    // the middle commit - the Land files' target.
    fs::write(
        src.join("src/main.rs"),
        b"pub fn main() {}\npub fn extra() {}\n",
    )
    .expect("the file writes");
    git(&["-C", &src.display().to_string(), "add", "-A"]);
    git(&["-C", &src.display().to_string(), "commit", "-qam", "second"]);
    git(&["-C", &src.display().to_string(), "branch", "feature"]);
    fs::write(src.join("src/lib.rs"), b"pub const X: u8 = 7;\n").expect("the file writes");
    git(&["-C", &src.display().to_string(), "add", "-A"]);
    git(&["-C", &src.display().to_string(), "commit", "-qam", "third"]);
    // The blame branch: src/lines.txt gains one line per commit, 101
    // commits off the middle, so the blame call stops at its bound.
    git(&[
        "-C",
        &src.display().to_string(),
        "checkout",
        "-q",
        "feature",
    ]);
    // The file grows by one line per commit, as the corpus's chain
    // does: a rewrite per commit names one changeset for ever.
    let mut n1_lines: Vec<u8> = b"pub fn main() {}\npub fn extra() {}\n".to_vec();
    for k in 0..101 {
        n1_lines.extend_from_slice(format!("line {k}\n").as_bytes());
        fs::write(src.join("src/lines.txt"), &n1_lines).expect("the file writes");
        git(&["-C", &src.display().to_string(), "add", "-A"]);
        git(&[
            "-C",
            &src.display().to_string(),
            "commit",
            "-qam",
            &format!("line {k}"),
        ]);
    }
    git(&["-C", &src.display().to_string(), "branch", "blame"]);
    // The big branch: the seeded 1.5 MiB file, the seeded 1.2 MiB
    // message, and 14,000 one-entry files - the parts files' fixture.
    // The commit rides its own branch, so main keeps its three commits.
    git(&[
        "-C",
        &src.display().to_string(),
        "checkout",
        "-q",
        "-b",
        "bigtmp",
        "main",
    ]);
    let big = seeded(1_572_864);
    fs::create_dir_all(src.join("big")).expect("the big dir is made");
    fs::write(src.join("big/seeded.txt"), &big).expect("the big file writes");
    fs::create_dir_all(src.join("wide")).expect("the wide dir is made");
    for k in 0..14_000 {
        fs::write(
            src.join(format!(
                "wide/file-{k:05}-of-the-wide-tree-directory-entry.txt"
            )),
            b".\n",
        )
        .expect("the entry writes");
    }
    git(&["-C", &src.display().to_string(), "add", "-A"]);
    let msg = big_message();
    let msg_path = dir.join("big-message.txt");
    fs::write(&msg_path, &msg).expect("the message writes");
    // The commit's author, committer and times are fixed, so the
    // imported changeset encodes to the length the corpus's own
    // changeset encodes to; only the parent id differs per store.
    git(&[
        "-C",
        &src.display().to_string(),
        "-c",
        "user.name=Ada",
        "-c",
        "user.email=ada@example.com",
        "commit",
        "-q",
        "-F",
        &msg_path.display().to_string(),
    ]);
    git(&["-C", &src.display().to_string(), "branch", "big"]);
    git(&["-C", &src.display().to_string(), "checkout", "-q", "main"]);
    src
}

/// The N-1 server's database: one of this run's own, as version-matrix's
/// `pg_db_for` makes one - the two builds share the database server,
/// never its rows. The guard drops the database when the run ends,
/// green or by unwind. Returns the guard and the address.
fn n1_database() -> (N1Db, String) {
    let base = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL names the test database");
    let base = base.split('?').next().expect("the base url").to_owned();
    // The base names a database in its path; the N-1 database replaces
    // that last segment, as the script's pg_db_for does.
    let stem = base
        .rsplit_once('/')
        .expect("the url has a path")
        .0
        .to_owned();
    let db = format!("scion_n1_{}", std::process::id());
    let admin = format!("{stem}/postgres");
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
    (
        N1Db {
            admin,
            db: db.clone(),
        },
        format!("{stem}/{db}"),
    )
}

/// The N-1 run's database, dropped on the way out.
struct N1Db {
    admin: String,
    db: String,
}

impl Drop for N1Db {
    fn drop(&mut self) {
        let out = Command::new("psql")
            .args([
                &self.admin,
                "-X",
                "-q",
                "-c",
                &format!("DROP DATABASE IF EXISTS \"{}\"", self.db),
            ])
            .output();
        match out {
            Ok(o) if o.status.success() => {}
            Ok(o) => eprintln!(
                "the N-1 database {} may be left behind: {}",
                self.db,
                String::from_utf8_lossy(&o.stderr)
            ),
            Err(e) => eprintln!("the N-1 database {} may be left behind: {e}", self.db),
        }
    }
}

/// Starts the imported N-1 server on a free port and answers its URL.
fn start_n1_server(
    scion: &Path,
    server_dir: &Path,
    key: &Path,
    pg: &str,
    dir: &Path,
) -> (Child, String) {
    let log = dir.join("server.log");
    let mut server = spawn_logged(
        scion.to_path_buf(),
        [
            "--data",
            &server_dir.display().to_string(),
            "serve",
            "--listen",
            "127.0.0.1:0",
            "--ticket-key",
            &key.display().to_string(),
        ],
        &[("SCION_POSTGRES_URL", pg), ("SCION_TOKEN", common::TOKEN)],
        &log,
    );
    let port = serving_port(&mut server, &log);
    (server, format!("http://127.0.0.1:{port}"))
}

fn start_n1_stack(prev_bin: &Path, dir: &Path, pg: &str) -> N1Stack {
    let src = n1_repo(dir);

    let server_dir = dir.join("server");
    fs::create_dir_all(&server_dir).expect("the data dir is made");
    make_private(&server_dir);
    let scion = prev_bin.join("scion-server");
    let data = server_dir.display().to_string();
    for (rev, target) in [
        ("HEAD", "main"),
        ("feature", "feature"),
        ("blame", "blame"),
        ("big", "big"),
        ("HEAD", "scratch"),
    ] {
        run_to_end(
            &scion,
            &[
                "--data",
                &data,
                "import",
                "--repo",
                common::REPO,
                "--rev",
                rev,
                "--ref",
                target,
                &src.display().to_string(),
            ],
            &[("SCION_POSTGRES_URL", pg)],
        );
    }
    let key = dir.join("ticket.key");
    let key_out = Command::new(&scion)
        .args([
            "--data",
            &server_dir.display().to_string(),
            "ticket-key",
            "--out",
            &key.display().to_string(),
        ])
        .env("SCION_POSTGRES_URL", pg)
        .output()
        .expect("the ticket key runs");
    assert!(
        key_out.status.success(),
        "the ticket key failed: {}",
        String::from_utf8_lossy(&key_out.stderr)
    );
    let pub_hex = String::from_utf8_lossy(&key_out.stdout).trim().to_owned();

    let (server, server_url) = start_n1_server(&scion, &server_dir, &key, pg, dir);

    let cache_log = dir.join("cache.log");
    let mut cache = spawn_logged(
        prev_bin.join("scion-cache"),
        [
            "--listen",
            "127.0.0.1:0",
            "--upstream",
            &server_url,
            "--ticket-public-key",
            &pub_hex,
        ],
        &tier_tokens(),
        &cache_log,
    );
    let cport = serving_port(&mut cache, &cache_log);
    let cache_url = format!("http://127.0.0.1:{cport}");

    let relay_dir = dir.join("daemon");
    fs::create_dir_all(&relay_dir).expect("the daemon dir is made");
    make_private(&relay_dir);
    let relay_sock = dir.join("daemon.sock");
    let relay_log = dir.join("daemon.log");
    let mut daemon = spawn_logged(
        prev_bin.join("scion-daemon"),
        [
            "--data",
            &relay_dir.display().to_string(),
            "--socket",
            &relay_sock.display().to_string(),
            "--upstream",
            &cache_url,
        ],
        &tier_tokens(),
        &relay_log,
    );
    wait_serving(&mut daemon, &relay_log);
    let socket_url = format!("unix://{}", relay_sock.display());

    N1Stack {
        server: server_url,
        cache: cache_url,
        socket: socket_url,
        pub_hex,
        _children: vec![server, cache, daemon],
    }
}

/// A fresh ticket from the N-1 server, seconds old at the call, as the
/// corpus test mints one per file.
fn n1_mint(
    runtime: &tokio::runtime::Runtime,
    server: &str,
    cs: &scion_core::ChangesetId,
) -> Vec<u8> {
    runtime.block_on(async {
        let mut client = connect(server).await;
        client
            .get_root_tree(scion_proto::v1::GetRootTreeRequest {
                repo: common::REPO.into(),
                changeset_id: cs.as_bytes().to_vec(),
            })
            .await
            .expect("the N-1 server mints")
            .into_inner()
            .ticket
    })
}

/// The N-1 side's fixture: the ref's changeset, then the root tree and
/// the ticket it names, resolved from the N-1 server like the client
/// does. The blob's bytes are the corpus's, so its names resolve.
/// The first parent of the changeset `id` names, streamed by
/// `GetChangeset` and decoded: the chain walk steps on it.
async fn parent_changeset(client: &mut ScionClientOf, id: ChangesetId) -> ChangesetId {
    let req = GetChangesetRequest {
        repo: common::REPO.into(),
        changeset_id: id.as_bytes().to_vec(),
    };
    let mut stream = client
        .get_changeset(req)
        .await
        .expect("the changeset answers")
        .into_inner();
    let mut parts = Vec::new();
    while let Some(part) = stream.message().await.expect("the part answers") {
        parts.extend_from_slice(&part.changeset);
    }
    *scion_core::Changeset::decode(&parts)
        .expect("the changeset decodes")
        .parents()
        .first()
        .expect("the changeset names its parent")
}

/// The N-1 chain's ends and the fixture's own land, walked on the N-1
/// server: the middle and root changesets by `GetChangeset`, the blame
/// head by its ref, and the scratch changeset created and landed for
/// the `GetLand` file.
async fn n1_chain_ends(
    scion: &mut ScionClientOf,
    client: &mut ObjectsClientOf,
    server: &str,
    cs: ChangesetId,
) -> (ChangesetId, ChangesetId, ChangesetId, String) {
    let mid_cs = parent_changeset(scion, cs).await;
    let root_cs = parent_changeset(scion, mid_cs).await;
    let blame_reply = client
        .resolve_ref(scion_proto::v1::ResolveRefRequest {
            repo: common::REPO.into(),
            name: "blame".into(),
        })
        .await
        .expect("the blame ref resolves")
        .into_inner();
    let blame_head = scion_core::ChangesetId::from_bytes(
        blame_reply
            .changeset_id
            .try_into()
            .expect("the cs is 32 bytes"),
    );
    // The fixture's own land: the same scratch bytes created through
    // the N-1 server, then landed; the land id comes from the reply.
    // The scratch, next and orphan blobs ride the N-1 store first: a
    // CreateChangeset whose blobs the repo does not hold answers the
    // precondition, "upload it with PutBlob".
    let mut land_client = scion_client(server).await;
    let blobs: [&[u8]; 3] = [
        b"pub fn scratch() {}\n",
        b"pub fn next() {}\n",
        b"pub fn orphan() {}\n",
    ];
    for data in blobs {
        let _ = land_client
            .put_blob(tokio_stream::iter([scion_proto::v1::PutBlobRequest {
                repo: common::REPO.into(),
                data: (*data).to_vec(),
            }]))
            .await
            .expect("the N-1 blob uploads");
    }
    let scratch = common::changeset(
        Some(cs),
        &[(
            "src/scratch.rs",
            scion_core::FileState::Regular {
                content: scion_core::BlobId::of(b"pub fn scratch() {}\n"),
                executable: false,
            },
        )],
    );
    let created = land_client
        .create_changeset(scion_proto::v1::CreateChangesetRequest {
            repo: common::REPO.into(),
            changeset: scratch.encode(),
        })
        .await
        .expect("the N-1 scratch changeset is created")
        .into_inner();
    let land_reply = land_client
        .land(scion_proto::v1::LandRequest {
            repo: common::REPO.into(),
            changeset_id: created.changeset_id,
            target_ref: "scratch".into(),
            request_key: "n1-fixture-land".into(),
        })
        .await
        .expect("the N-1 fixture's land answers")
        .into_inner();
    (mid_cs, root_cs, blame_head, land_reply.land_id)
}

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
        // The chain's ends, the scratch land, and the encodings: the
        // helper below walks the N-1 server for each.
        let mut scion = scion_client(server).await;
        let big_reply = client
            .resolve_ref(scion_proto::v1::ResolveRefRequest {
                repo: common::REPO.into(),
                name: "big".into(),
            })
            .await
            .expect("the big ref resolves")
            .into_inner();
        let big_cs = scion_core::ChangesetId::from_bytes(
            big_reply
                .changeset_id
                .try_into()
                .expect("the cs is 32 bytes"),
        );
        let (mid_cs, root_cs, blame_head, land_id) =
            n1_chain_ends(&mut scion, &mut client, server, cs).await;
        let root_reply = client
            .get_root_tree(GetRootTreeRequest {
                repo: common::REPO.into(),
                changeset_id: root_cs.as_bytes().to_vec(),
            })
            .await
            .expect("the root tree answers")
            .into_inner();
        let root_ticket = root_reply.ticket;
        let root_tree = root_reply.tree_id.try_into().expect("the tree is 32 bytes");
        let content = b"pub fn main() {}\n".to_vec();
        Fixture {
            blob: BlobId::of(&content),
            content,
            tree,
            cs,
            root_cs,
            mid_cs,
            root_tree,
            blame_head,
            big_cs,
            big_content: seeded(1_572_864),
            encoded_next: encoded_next(&cs).0,
            next_cs: encoded_next(&cs).1,
            encoded_orphan: encoded_orphan(),
            encoded_scratch: encoded_scratch(&cs),
            land_id,
            missing: missing_id(0xab),
            unsigned: unsigned_of(&root_ticket),
            ticket: root_ticket,
        }
    })
}

use std::path::PathBuf;
use std::process::Command;

type ScionClientOf = ScionClient<InterceptedService<Channel, Bearer>>;

/// A `Scion` client with the token, as the Objects one is built.
async fn scion_client(url: &str) -> ScionClientOf {
    scion_client_as(url, TOKEN).await
}

/// A `Scion` client whose bearer is `token`, so a file can pin what a
/// wrong token answers.
async fn scion_client_as(url: &str, token: &str) -> ScionClientOf {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(30))
        .connect()
        .await
        .unwrap();
    ScionClient::with_interceptor(channel, Bearer::new(token).unwrap())
}

/// Runs one `Objects` transcript against one tier URL.
async fn run_objects(t: &Transcript, url: &str, f: &Fixture) -> Result<(), String> {
    // A Scion-method file in the objects dir runs its call at the
    // server (the first url) and rides the tier refusals past it; its
    // expectations name the server's answer.
    if t.method.starts_with("Scion/") {
        return run_scion(t, url, f).await;
    }
    let mut client = connect(url).await;
    match t.method.as_str() {
        "Objects/ResolveRef" => run_resolve_ref(t, &mut client, f).await,
        "Objects/GetRootTree" => run_get_root_tree(t, &mut client, f).await,
        "Objects/GetObjects" => run_get_objects(t, url, &mut client, f).await,
        other => Err(format!("no runner for {other}")),
    }
}

/// Runs one `Scion` transcript against the server, as the file's own
/// token: the one valid token, unless the file pins an auth refusal.
async fn run_scion(t: &Transcript, url: &str, f: &Fixture) -> Result<(), String> {
    let mut client = scion_client_as(url, t.token.as_deref().unwrap_or(TOKEN)).await;
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

/// The changeset a file's ticket names: its `ticket_for` resolved
/// against the fixture, or the default when the file names none.
fn ticket_changeset(t: &Transcript, f: &Fixture, default: ChangesetId) -> ChangesetId {
    match &t.ticket_for {
        Some(name) => {
            let bytes = unhex(&resolve(name, f));
            ChangesetId::from_bytes(bytes.try_into().expect("the cs is 32 bytes"))
        }
        None => default,
    }
}

/// Whether the file pins `want` as the joined stream bytes, and the
/// joined bytes' length, BLAKE3 and part count as it says. The big
/// fixtures ride no file: the harness builds them from a seed, and the
/// transcript pins `reply.<field>_len`, `_blake3` and `_parts`.
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
    let joined: Vec<u8> = got.concat();
    let pins = |suffix: &str| {
        t.expect
            .iter()
            .find(|(k, _)| k == &format!("reply.{field_name}{suffix}"))
            .map(|(_, v)| v.clone())
    };
    if let Some(v) = pins("_len") {
        let want_len: usize = v.parse().unwrap_or(0);
        if joined.len() != want_len {
            return Err(format!(
                "reply.{field_name} joins to {} bytes, the file says {want_len}",
                joined.len()
            ));
        }
    }
    if let Some(v) = pins("_blake3") {
        let want_hash = resolve(&v, f);
        let got_hash = hex(blake3::hash(&joined).as_bytes().as_slice());
        if got_hash != want_hash {
            return Err(format!(
                "reply.{field_name} hashes to {got_hash}, the file says {want_hash}"
            ));
        }
    }
    if let Some(v) = pins("_parts") {
        let want_parts: usize = v.parse().unwrap_or(0);
        if got.len() != want_parts {
            return Err(format!(
                "reply.{field_name} came in {} parts, the file says {want_parts}",
                got.len()
            ));
        }
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

/// The `next` pin: the cursor the reply ended with, resolved against
/// the fixture (an empty value pins the last page's empty cursor).
/// `next_nonempty: true` pins that a cursor came at all.
fn check_next(t: &Transcript, f: &Fixture) -> Result<(), String> {
    let got = CURSORS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&cursor_key(&t.path))
        .cloned()
        .unwrap_or_default();
    if let Some((_, v)) = t.expect.iter().find(|(k, _)| k == "next") {
        let want = resolve(v, f);
        if hex(&got) != want {
            return Err(format!(
                "the reply's cursor is {}, the file says {want}",
                hex(&got)
            ));
        }
    }
    if let Some((_, v)) = t.expect.iter().find(|(k, _)| k == "next_nonempty") {
        let want = v == "true";
        if got.is_empty() == want {
            return Err(format!(
                "the reply's cursor is {} bytes, the file says nonempty={want}",
                got.len()
            ));
        }
    }
    Ok(())
}

async fn run_log(t: &Transcript, client: &mut ScionClientOf, f: &Fixture) -> Result<(), String> {
    for (k, _) in &t.request {
        if !matches!(
            k.as_str(),
            "repo" | "changeset_id" | "path" | "cursor" | "limit"
        ) {
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
            let mut last = Vec::new();
            while let Some(item) = read_bounded(&mut stream).await? {
                count += item.entries.len();
                last = item.next_changeset_id;
            }
            remember_cursor(t, last);
            if let Some((_, v)) = t.expect.iter().find(|(k, _)| k == "entries") {
                let want: usize = v.parse().unwrap_or(0);
                if count != want {
                    return Err(format!(
                        "the log holds {count} entries, the file says {want}"
                    ));
                }
            }
            check_next(t, f)?;
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
    let cursor = match t.expect.iter().find(|(k, _)| k == "cursor_from") {
        Some((_, source)) => cursor_of(source)?,
        None => field(t, "cursor").unwrap_or_default().into_bytes(),
    };
    let req = scion_proto::v1::BlameRequest {
        repo: resolve(&field(t, "repo").unwrap_or_else(|| "r".to_owned()), f),
        changeset_id: unhex(&resolve(&field(t, "changeset_id").unwrap_or_default(), f)),
        path: field(t, "path").unwrap_or_default().into_bytes(),
        cursor,
    };
    match client.blame(req).await {
        Ok(reply) => {
            let mut stream = reply.into_inner();
            check_status(t, Code::Ok)?;
            let mut count = 0usize;
            let mut last = Vec::new();
            while let Some(item) = read_bounded(&mut stream).await? {
                count += item.changesets.len();
                last = item.next_cursor;
            }
            remember_cursor(t, last);
            if let Some((_, v)) = t.expect.iter().find(|(k, _)| k == "entries") {
                let want: usize = v.parse().unwrap_or(0);
                if count != want {
                    return Err(format!(
                        "the blame holds {count} lines, the file says {want}"
                    ));
                }
            }
            check_next(t, f)?;
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
            "repo" | "old_changeset_id" | "new_changeset_id" | "path" | "after_from"
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
        after: match t.expect.iter().find(|(k, _)| k == "after_from") {
            Some((_, source)) => {
                String::from_utf8(cursor_of(source)?).expect("the diff's cursor is a path")
            }
            None => field(t, "after").unwrap_or_default(),
        }
        .into_bytes(),
    };
    match client.diff(req).await {
        Ok(reply) => {
            check_status(t, Code::Ok)?;
            let reply = reply.into_inner();
            let entries = reply.entries.len();
            remember_cursor(t, reply.next_after.clone());
            if let Some((_, v)) = t.expect.iter().find(|(k, _)| k == "entries") {
                let want: usize = v.parse().unwrap_or(0);
                if entries != want {
                    return Err(format!(
                        "the diff holds {entries} entries, the file says {want}"
                    ));
                }
            }
            check_next(t, f)?;
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
) -> Result<(Option<End>, usize), String> {
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
                return Ok((None, parts));
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
                return Ok((Some(End::Stream(status.code())), parts));
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
    let mut total_parts = 0usize;
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
        let (how, came_parts) = read_part_stream(url, &mut stream, &mut objects, sent_at).await?;
        total_parts += came_parts;
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
    if let Some((_, v)) = t.expect.iter().find(|(k, _)| k == "objects_parts") {
        let want: usize = v.parse().unwrap_or(0);
        if total_parts != want {
            return Err(format!(
                "the reply came in {total_parts} parts, the file says {want}"
            ));
        }
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
