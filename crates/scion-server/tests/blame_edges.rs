//! Blame's edges, through the public API: the shapes of a file and of a
//! reply that the happy-path tests do not cover, and the cursors that a
//! confused or hostile client sends back.

use std::sync::Arc;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{BlameChangeset, BlameRange, BlameRequest};
use scion_server::Access;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, test_support::service};
use tokio::net::TcpListener;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const REPO: &str = "main";
const TOKEN: &str = "alice";
const PATH: &str = "file.txt";

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

type Stop = tokio::sync::oneshot::Sender<()>;

/// Adds `authorization: Bearer <token>` to every request.
#[derive(Clone)]
struct Bearer(Option<String>);

impl tonic::service::Interceptor for Bearer {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = &self.0 {
            let value: tonic::metadata::AsciiMetadataValue =
                format!("Bearer {token}").parse().unwrap();
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

fn hex(token: &str) -> String {
    blake3::Hash::from(token_digest(token)).to_hex().to_string()
}

fn regular(content: BlobId) -> FileState {
    FileState::Regular {
        content,
        executable: false,
    }
}

/// A changeset with `parents`, one change at `path`, and an author whose
/// name is `name_len` bytes long.
fn commit(
    parents: Vec<ChangesetId>,
    path: &str,
    state: Option<FileState>,
    name_len: usize,
) -> Changeset {
    Changeset::new(
        parents,
        IdentityTime::new(
            vec![b'a'; name_len],
            b"a@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"change\n".to_vec(),
        [(RepoPath::new(path).unwrap(), state)].into(),
    )
    .expect("changeset fits the format")
}

/// A store whose repo has one root changeset that puts `file.txt` with
/// `root_body`, written by a 4-byte author.
fn store(root_body: &[u8]) -> (tempfile::TempDir, Backend, ChangesetId) {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let blob = store.blobs().put(root_body).unwrap();
    let root = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = c.insert_changeset(repo, &commit(vec![], PATH, Some(regular(blob)), 4))?;
            c.cas_ref(repo, "main", None, root)?;
            c.add_repo_blob(repo, blob)?;
            Ok(root)
        })
        .unwrap();
    (dir, store, root)
}

/// Stores `content` as a blob, as an upload would.
fn put(store: &Backend, content: &[u8]) -> BlobId {
    let blob = store.blobs().put(content).unwrap();
    store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            c.add_repo_blob(repo, blob)?;
            Ok(())
        })
        .unwrap();
    blob
}

/// A whole-repo reader that may write the repo.
fn whole_access() -> Access {
    Access::parse(&format!(
        "token\t{TOKEN}\t{}\nread\t{TOKEN}\t{REPO}\nwrite\t{TOKEN}\t{REPO}\n",
        hex(TOKEN)
    ))
    .unwrap()
}

/// Serves `store` with `access`, and returns the stop signal and a client
/// that sends `token`.
async fn serve(
    store: &Backend,
    dir: &std::path::Path,
    access: Access,
    token: &str,
) -> (Stop, Scion) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(service::serve(
        store.clone(),
        listener,
        access,
        service::ServeOptions {
            data: dir.to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::new(scion_server::Metrics::default()),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(Arc::new(scion_server::Keys::generate())),
        async {
            stopped.await.ok();
        },
    ));
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    let client = ScionClient::with_interceptor(channel, Bearer(Some(token.to_string())));
    (stop, client)
}

/// One blame call, with its parts joined: the ranges in order, the
/// changesets the parts carried, the cursor of the last part, and how many
/// parts the reply took.
async fn blame(
    client: &mut Scion,
    changeset_id: &ChangesetId,
    path: &str,
    cursor: &[u8],
) -> (Vec<BlameRange>, Vec<BlameChangeset>, Vec<u8>, usize) {
    let mut pages = client
        .blame(BlameRequest {
            repo: REPO.to_string(),
            changeset_id: changeset_id.as_bytes().to_vec(),
            path: path.as_bytes().to_vec(),
            cursor: cursor.to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut ranges = Vec::new();
    let mut changesets = Vec::new();
    let mut next = Vec::new();
    let mut parts = 0usize;
    while let Some(page) = pages.message().await.unwrap() {
        ranges.extend(page.ranges);
        changesets.extend(page.changesets);
        next = page.next_cursor;
        parts += 1;
    }
    (ranges, changesets, next, parts)
}

/// The lines the ranges cover, in order, each once.
fn covered(ranges: &[BlameRange]) -> Vec<u32> {
    let mut lines = Vec::new();
    for range in ranges {
        for n in 0..range.line_count {
            lines.push(range.start_line + n);
        }
    }
    lines
}

/// An empty file has no lines, so its blame names nothing: no ranges, no
/// changesets, and no cursor to go on with. A blame that synthesized a
/// range for it would have a client print a line that does not exist.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_file_is_blamed_with_no_ranges() {
    let (dir, store, root) = store(b"start\n");
    let empty = put(&store, b"");
    let tip = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            c.insert_changeset(
                repo,
                &commit(vec![root], "empty.txt", Some(regular(empty)), 4),
            )
        })
        .unwrap();
    let (_stop, mut client) = serve(&store, dir.path(), whole_access(), TOKEN).await;
    let (ranges, changesets, next, _parts) = blame(&mut client, &tip, "empty.txt", b"").await;
    assert!(ranges.is_empty(), "{ranges:?}");
    assert!(changesets.is_empty(), "{changesets:?}");
    assert!(next.is_empty(), "an empty file has nothing to continue");
}

/// A file whose last line has no `\n` still has that line: the blame
/// covers it. A walk that counted lines only where a `\n` ends one would
/// drop it.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_with_no_final_newline_still_names_its_last_line() {
    let (dir, store, root) = store(b"one\n");
    let body = put(&store, b"one\ntwo");
    let tip = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            c.insert_changeset(repo, &commit(vec![root], PATH, Some(regular(body)), 8))
        })
        .unwrap();
    let (_stop, mut client) = serve(&store, dir.path(), whole_access(), TOKEN).await;
    let (ranges, changesets, next, _parts) = blame(&mut client, &tip, PATH, b"").await;
    assert_eq!(covered(&ranges), vec![1, 2], "{ranges:?}");
    assert!(next.is_empty(), "both lines are explained at once");
    assert!(
        changesets
            .iter()
            .any(|c| c.author_name == vec![b'a'; 4] && c.author_name.len() == 4),
        "the child names its author: {changesets:?}"
    );
}

/// A line longer than a stream part is still one line: the reply streams
/// around it, and its range is whole. A walk that cut content at part
/// boundaries would blame the pieces of the line apart.
#[tokio::test(flavor = "multi_thread")]
async fn a_line_longer_than_a_stream_part_is_blamed_whole() {
    let (dir, store, root) = store(b"start\n");
    let mut body = vec![b'x'; 2 * scion_proto::CHUNK_BYTES];
    body.extend_from_slice(b"\nend\n");
    let blob = put(&store, &body);
    let tip = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            c.insert_changeset(repo, &commit(vec![root], PATH, Some(regular(blob)), 4))
        })
        .unwrap();
    let (_stop, mut client) = serve(&store, dir.path(), whole_access(), TOKEN).await;
    let (ranges, _changesets, next, _parts) = blame(&mut client, &tip, PATH, b"").await;
    assert_eq!(covered(&ranges), vec![1, 2], "{ranges:?}");
    assert!(next.is_empty(), "both lines are explained at once");
    // The long line is one line: its bytes are not lines. A walk that cut
    // content at part boundaries would cover a third line.
    assert_eq!(ranges.len(), 1, "{ranges:?}");
    assert_eq!(ranges[0].line_count, 2, "{ranges:?}");
}

/// The store of the seam tests: a root that puts a file of `count` lines,
/// and `count` children, the `i`-th rewriting line `i + 1` with
/// `gen {i}`, with `name_len`-byte author names. The blame of the tip
/// names every child, one per line.
fn seam_store(name_len: usize, count: usize) -> (tempfile::TempDir, Backend, Vec<ChangesetId>) {
    let seed: Vec<u8> = (1..=count)
        .map(|line| format!("line {line}\n"))
        .collect::<Vec<_>>()
        .concat()
        .into_bytes();
    let (dir, store, root) = store(&seed);
    let ids = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let mut ids = vec![root];
            for i in 0..count {
                let body: Vec<u8> = (1..=count)
                    .map(|line| {
                        if line == i + 1 {
                            format!("gen {i}\n")
                        } else {
                            format!("line {line}\n")
                        }
                    })
                    .collect::<Vec<_>>()
                    .concat()
                    .into_bytes();
                let blob = store.blobs().put(&body).unwrap();
                c.add_repo_blob(repo, blob)?;
                let cs = commit(
                    vec![ids[ids.len() - 1]],
                    PATH,
                    Some(regular(blob)),
                    name_len,
                );
                ids.push(c.insert_changeset(repo, &cs)?);
            }
            Ok(ids)
        })
        .unwrap();
    (dir, store, ids)
}

/// A reply whose parts straddle the one-part seam keeps every range: the
/// joined ranges cover the file once each, and no changeset is lost or
/// doubled at the seam. The same holds when the whole reply fits one part.
#[tokio::test(flavor = "multi_thread")]
async fn the_part_seam_loses_no_range_and_no_changeset() {
    for (name_len, parts_expected) in [(1_000, 1usize), (14_000, 2usize)] {
        let (dir, store, ids) = seam_store(name_len, 90);
        let tip = ids[ids.len() - 1];
        let (_stop, mut client) = serve(&store, dir.path(), whole_access(), TOKEN).await;
        let (ranges, changesets, next, parts) = blame(&mut client, &tip, PATH, b"").await;
        assert_eq!(parts, parts_expected, "name_len {name_len}");
        // Every line is covered, once each.
        let lines: Vec<u32> = (1..=90).collect();
        assert_eq!(covered(&ranges), lines, "name_len {name_len}");
        // The changesets carried are unique, and every range names one of
        // them, or an older changeset the reply does not carry (empty).
        let mut ids: Vec<Vec<u8>> = changesets.iter().map(|c| c.changeset_id.clone()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), changesets.len(), "a changeset was doubled");
        for range in &ranges {
            assert!(
                range.changeset_id.is_empty() || ids.contains(&range.changeset_id),
                "a range names a changeset no part carried: {range:?}"
            );
        }
        assert!(next.is_empty(), "the whole chain was explained");
    }
}

/// A resumed part names each line its own changeset. The cursor's
/// window carries the named runs across the part seam, and adjacent
/// lines of the seam store name different changesets: a run packed
/// across two of them would name the second line's lines with the
/// first line's changeset on the resume. The reply of every part must
/// match the chain line for line: line n's last change is the child
/// at `ids[n + 1]`.
#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_part_names_each_line_its_own_changeset() {
    // 151 versions over the walk's 100-version bound: the first call
    // stops mid-file and hands a cursor whose window carries the named
    // lines - adjacent lines of different changesets.
    let (dir, store, ids) = seam_store(1_000, 150);
    let tip = ids[ids.len() - 1];
    let (_stop, mut client) = serve(&store, dir.path(), whole_access(), TOKEN).await;
    let mut cursor: Vec<u8> = Vec::new();
    let mut calls = 0usize;
    loop {
        calls += 1;
        let (ranges, _changesets, next, _parts) = blame(&mut client, &tip, PATH, &cursor).await;
        let mut named: Vec<usize> = Vec::new();
        for range in &ranges {
            if range.changeset_id.is_empty() {
                continue;
            }
            // Child i rewrites line i+1 and reverts line i, so line n's
            // last change is the child at ids[n + 1] - the tip for the
            // last line. A range names one changeset, so it must not
            // stretch over a line the next child rewrote.
            let start = range.start_line as usize;
            for n in start..start + range.line_count as usize {
                named.push(n);
                let last = ids[(n + 1).min(ids.len() - 1)].as_bytes();
                assert_eq!(
                    range.changeset_id.as_slice(),
                    last,
                    "line {n} names the wrong changeset: {range:?}"
                );
            }
        }
        if next.is_empty() {
            // A resumed call answers the same window again (scion.proto;
            // #1070 asks for that in the proto doc), so it re-names the
            // lines an earlier call named: the once-each check is on
            // this, the last call's reply.
            named.sort_unstable();
            assert_eq!(named, (1..=150).collect::<Vec<usize>>());
            break;
        }
        cursor = next;
    }
    // The walk stopped at its 100-version bound: the reply needed a
    // resume. If the bound grows past the fixture, this test stops
    // guarding the run merge and must grow with it.
    assert!(calls >= 2, "the walk finished in one call; no resume");
}

/// A cursor made for one file, or one changeset, is refused when it is
/// sent for another: a confused or hostile client gets `INVALID_ARGUMENT`,
/// never the ranges of a blame it did not ask for.
#[tokio::test(flavor = "multi_thread")]
async fn a_cursor_of_another_file_or_changeset_is_refused() {
    let (dir, store, root) = store(b"a\n");
    let other = put(&store, b"other\n");
    // 120 children that all put the same blob: the walk keeps the line and
    // runs out of versions at 100, so the first blame returns a cursor.
    let tip = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let blob = store.blobs().put(b"a\n").unwrap();
            c.add_repo_blob(repo, blob)?;
            let mut id = root;
            for _ in 0..120 {
                id = c.insert_changeset(repo, &commit(vec![id], PATH, Some(regular(blob)), 4))?;
            }
            // A second file, whose blame has its own cursors.
            c.insert_changeset(
                repo,
                &commit(vec![id], "other.txt", Some(regular(other)), 4),
            )
        })
        .unwrap();
    let (_stop, mut client) = serve(&store, dir.path(), whole_access(), TOKEN).await;
    let (_ranges, _changesets, cursor, _parts) = blame(&mut client, &tip, PATH, b"").await;
    assert!(!cursor.is_empty(), "the walk stopped at its bound");

    // The cursor sent for another path.
    let err = client
        .blame(BlameRequest {
            repo: REPO.to_string(),
            changeset_id: tip.as_bytes().to_vec(),
            path: b"other.txt".to_vec(),
            cursor: cursor.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    // The cursor sent for another changeset of the same file's history:
    // the root, whose version of the file is the seed body.
    let err = client
        .blame(BlameRequest {
            repo: REPO.to_string(),
            changeset_id: root.as_bytes().to_vec(),
            path: PATH.as_bytes().to_vec(),
            cursor,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
}

/// A cursor sent twice gives the same answer twice: the resume re-explains
/// the window it did the first time, instead of skipping past it.
#[tokio::test(flavor = "multi_thread")]
async fn a_cursor_sent_twice_gives_the_same_answer() {
    let (dir, store, root) = store(b"a\n");
    // 120 children that all put the same blob: the walk keeps the line and
    // runs out of versions at 100, so the first blame returns a cursor.
    let tip = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let blob = store.blobs().put(b"a\n").unwrap();
            c.add_repo_blob(repo, blob)?;
            let mut id = root;
            for _ in 0..120 {
                id = c.insert_changeset(repo, &commit(vec![id], PATH, Some(regular(blob)), 4))?;
            }
            Ok(id)
        })
        .unwrap();
    let (_stop, mut client) = serve(&store, dir.path(), whole_access(), TOKEN).await;
    let (first, _, cursor, _parts) = blame(&mut client, &tip, PATH, b"").await;
    assert!(!cursor.is_empty(), "the walk stopped at its bound");
    let (again, _, cursor_again, _parts) = blame(&mut client, &tip, PATH, &cursor).await;
    assert_eq!(
        covered(&first),
        covered(&again),
        "the same cursor covers the same window"
    );
    // The resumed call goes on from where the cursor waits, and finishes
    // the window: it does not send the client round the same step again.
    assert!(cursor_again.is_empty(), "the window is explained at last");
}

/// A file that history deletes and adds again: the tip's lines come from
/// the changeset that added them the second time, not from the one that
/// added them before the deletion.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_deleted_and_added_again_blames_the_new_add() {
    let (dir, store, root) = store(b"gone\n");
    // Added again, with new content and a new author. `put` takes the
    // store's write lock itself, so it runs before the changeset write.
    let body = put(&store, b"back\n");
    let ids = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            // Deleted.
            let deleted = c.insert_changeset(repo, &commit(vec![root], PATH, None, 6))?;
            let readded =
                c.insert_changeset(repo, &commit(vec![deleted], PATH, Some(regular(body)), 9))?;
            Ok(vec![deleted, readded])
        })
        .unwrap();
    let tip = ids[1];
    let (_stop, mut client) = serve(&store, dir.path(), whole_access(), TOKEN).await;
    let (ranges, changesets, next, _parts) = blame(&mut client, &tip, PATH, b"").await;
    assert_eq!(covered(&ranges), vec![1], "{ranges:?}");
    // Every named changeset is the one that added the line the second
    // time; the author of the version before the deletion is not there.
    assert!(
        changesets.iter().all(|c| c.author_name == vec![b'a'; 9]),
        "{changesets:?}"
    );
    assert!(next.is_empty(), "the line is explained at the re-add");
}

/// A partial reader's blame answers the same for a path its rules hide and
/// for one they allow: the v0.1 of the folder rules is off for it, with no
/// per-object difference, so a hidden path reveals nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_reader_gets_one_answer_for_any_path() {
    let (dir, store, root) = store(b"docs body\n");
    let (docs_blob, secret_blob) = (put(&store, b"docs a\n"), put(&store, b"secret b\n"));
    let tip = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let cs = commit(vec![root], "docs/a", Some(regular(docs_blob)), 4);
            let mut changes = cs.changes().clone();
            changes.insert(
                RepoPath::new("secret/b").unwrap(),
                Some(regular(secret_blob)),
            );
            let cs = Changeset::new(
                cs.parents().to_vec(),
                cs.author().clone(),
                cs.committer().cloned(),
                cs.message().to_vec(),
                changes,
            )
            .expect("changeset fits the format");
            c.insert_changeset(repo, &cs)
        })
        .unwrap();
    // pat may read `docs` of the repo, by folder rules: a partial reader.
    let access = Access::parse(&format!(
        "token\tpat\t{}\nread\tpat\t{REPO}\tdocs\n",
        hex("pat")
    ))
    .unwrap();
    let (_stop, mut client) = serve(&store, dir.path(), access, "pat").await;
    for (path, note) in [(&b"docs/a"[..], "allowed"), (&b"secret/b"[..], "hidden")] {
        let err = client
            .blame(BlameRequest {
                repo: REPO.to_string(),
                changeset_id: tip.as_bytes().to_vec(),
                path: path.to_vec(),
                cursor: Vec::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unimplemented, "{note}: {err}");
        assert!(
            err.message().contains("folder read rules"),
            "{note}: {}",
            err.message()
        );
    }
}

/// A hand-made cursor that claims an impossible line count is refused,
/// not answered: the count is the client's word, past the line cap, so
/// the walk refuses it as too big to serve. The cursor is one any client
/// can build - the digest is over the request's own repo, changeset and
/// path - so the check cannot trust the count.
#[tokio::test(flavor = "multi_thread")]
async fn a_cursor_that_names_an_impossible_line_count_is_refused() {
    let (dir, store, root) = store(b"a\n");
    let (_stop, mut client) = serve(&store, dir.path(), whole_access(), TOKEN).await;

    // The cursor: version 1, the digest of this repo, changeset and path,
    // a walk that goes on at the root (so the count is the cursor's), a
    // line count of 2^57, and a window of one open line at line 0.
    let counted: u64 = 1 << 57;
    let digest = blake3::Hasher::new()
        .update(REPO.as_bytes())
        .update(&[0])
        .update(root.as_bytes())
        .update(PATH.as_bytes())
        .finalize();
    let mut cursor = Vec::new();
    cursor.push(1);
    cursor.extend_from_slice(&digest.as_bytes()[..16]);
    cursor.push(1);
    cursor.extend_from_slice(root.as_bytes());
    cursor.extend_from_slice(&u32::try_from(PATH.len()).unwrap().to_be_bytes());
    cursor.extend_from_slice(PATH.as_bytes());
    cursor.extend_from_slice(&0u64.to_be_bytes());
    cursor.push(0);
    cursor.extend_from_slice(&(counted - 1).to_be_bytes());
    cursor.extend_from_slice(&counted.to_be_bytes());
    cursor.extend_from_slice(&0u32.to_be_bytes());
    cursor.extend_from_slice(&1u32.to_be_bytes());
    cursor.push(1);
    cursor.push(0);
    cursor.push(0);

    let err = client
        .blame(BlameRequest {
            repo: REPO.to_string(),
            changeset_id: root.as_bytes().to_vec(),
            path: PATH.as_bytes().to_vec(),
            cursor,
        })
        .await
        .unwrap_err();
    // The count is past the line cap, so the refusal is the one code for
    // too big to serve: RESOURCE_EXHAUSTED, with the cap in the message.
    assert_eq!(err.code(), Code::ResourceExhausted, "{err}");
    assert!(err.message().contains("lines"), "{err}");
}

/// A file of more lines than one diff may read is refused with
/// `RESOURCE_EXHAUSTED`, the same code as a file whose memory share is
/// more than the whole budget: both are too big to serve, and the client
/// sees one code for it. The file here is 1 MiB of one-byte lines - the
/// cap is line-based, so no big file is needed to cross it.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_with_too_many_lines_is_refused() {
    let (dir, store, root) = store(b"");
    let over = scion_server::test_support::blame::MAX_BLAME_DIFF_LINES + 1;
    let blob = put(&store, vec![b'\n'; over].as_slice());
    let tip = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let cs = commit(vec![root], PATH, Some(regular(blob)), 4);
            c.insert_changeset(repo, &cs)
        })
        .unwrap();
    let (_stop, mut client) = serve(&store, dir.path(), whole_access(), TOKEN).await;
    let err = client
        .blame(BlameRequest {
            repo: REPO.to_string(),
            changeset_id: tip.as_bytes().to_vec(),
            path: PATH.as_bytes().to_vec(),
            cursor: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted, "{err}");
    assert!(err.message().contains("lines"), "{err}");
}

/// A version past the line cap in the file's history counts as absent:
/// the walk goes on, and the lines it held name the change that
/// introduced the text being explained. The reply completes - no cursor,
/// no diff of the over-cap version.
#[tokio::test(flavor = "multi_thread")]
async fn an_older_version_with_too_many_lines_counts_as_absent() {
    let (dir, store, root) = store(b"");
    let over = scion_server::test_support::blame::MAX_BLAME_DIFF_LINES + 1;
    // The parent: `over` empty lines. The child: the first `over - 1` of
    // them, under the cap, so the request itself is servable.
    let parent_blob = put(&store, vec![b'\n'; over].as_slice());
    let under = scion_server::test_support::blame::MAX_BLAME_DIFF_LINES;
    // `under - 1` newlines and a last line without one: exactly `under`
    // lines, at the cap, so the request itself is servable.
    let mut body = vec![b'\n'; under - 1];
    body.push(b'x');
    let child_blob = put(&store, &body);
    let (_parent, child) = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let parent = c.insert_changeset(
                repo,
                &commit(vec![root], PATH, Some(regular(parent_blob)), 4),
            )?;
            let child = c.insert_changeset(
                repo,
                &commit(vec![parent], PATH, Some(regular(child_blob)), 4),
            )?;
            Ok((parent, child))
        })
        .unwrap();
    let (_stop, mut client) = serve(&store, dir.path(), whole_access(), TOKEN).await;
    let (ranges, changesets, next, _parts) = blame(&mut client, &child, PATH, b"").await;
    let under = u32::try_from(under).unwrap();
    assert_eq!(covered(&ranges), (1..=under).collect::<Vec<u32>>());
    assert_eq!(changesets.len(), 1, "{changesets:?}");
    assert_eq!(changesets[0].changeset_id, child.as_bytes().to_vec());
    assert!(next.is_empty(), "the walk explained the whole file");
}

#[path = "common/mod.rs"]
mod common;

/// A rename search that one call's budget cannot finish goes on in the
/// next call, across the walk's steps: the walk's cursor and its search
/// state carry, and the answer names the file the search finds. A step
/// that ended mid-version once left the cursor at the version's parent
/// while the path still named the version, and the next call looked for
/// the blamed path there.
#[test]
fn a_rename_search_goes_on_across_calls() {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    // `new` replaces 24 deleted files of 65,536 lines, one chunk each, so
    // one call's budget reads about 15 of them, and the search goes on in
    // a second call. All are 99% like `new`, so the first by path wins.
    let empty = "\n".repeat((1 << 16) - 1);
    let put = |text: String| Some(regular(store.blobs().put(text.as_bytes()).unwrap()));
    let names: Vec<String> = (0..24).map(|k| format!("f/{k:02}")).collect();
    let root_changes: std::collections::BTreeMap<RepoPath, Option<FileState>> = names
        .iter()
        .enumerate()
        .map(|(k, p)| {
            (
                RepoPath::new(p.as_str()).unwrap(),
                put(format!("{empty}{k}\n")),
            )
        })
        .collect();
    let mut rename_changes: Vec<(RepoPath, Option<FileState>)> = names
        .iter()
        .map(|p| (RepoPath::new(p.as_str()).unwrap(), None))
        .collect();
    rename_changes.push((RepoPath::new("new").unwrap(), put(format!("x\n{empty}"))));
    let author = IdentityTime::new(vec![b'a'; 4], b"a@example.com".to_vec(), 1_700_000_000, 0)
        .expect("identity fits the format");
    let message = b"change\n".to_vec();
    let mut head: Option<ChangesetId> = None;
    let mut ids = Vec::new();
    for changes in [
        root_changes,
        rename_changes
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>(),
    ] {
        let cs = Changeset::new(
            head.into_iter().collect::<Vec<_>>(),
            author.clone(),
            None,
            message.clone(),
            changes,
        )
        .expect("changeset fits the format");
        let id = store
            .write(|c| {
                let repo = c.create_repo(REPO)?;
                c.insert_changeset(repo, &cs)
            })
            .unwrap();
        ids.push(id);
        head = Some(id);
    }

    // Blame the tip's `new`: the first call reads about 15 candidates and
    // stops; the second finishes the search and the walk. The lines of
    // the reply name the tip for its own line, and the file the search
    // found for the rest.
    let mut resume = scion_server::test_support::blame::Resume::default();
    let mut lines: Vec<(ChangesetId, u64)> = Vec::new();
    let mut paths: std::collections::HashMap<ChangesetId, RepoPath> =
        std::collections::HashMap::new();
    loop {
        let out = scion_server::test_support::blame::blame(
            &store,
            REPO,
            ids[1],
            &RepoPath::new("new").unwrap(),
            &resume,
            &scion_server::test_support::memory::Bytes::new(scion_server::REPLY_MEMORY_BUDGET),
            Some(std::time::Instant::now() + std::time::Duration::from_secs(600)),
        )
        .unwrap();
        for (id, _, path) in &out.changesets {
            paths.insert(*id, path.clone());
        }
        for range in &out.ranges {
            let Some(id) = range.changeset else { continue };
            for n in 0..u64::from(range.line_count) {
                lines.push((id, u64::from(range.origin_line) + n));
            }
        }
        match out.resume.clone() {
            None => break,
            Some(next) => resume = next,
        }
    }
    assert_eq!(lines.len(), 1 << 16, "every line of the file named");
    assert_eq!(lines[0].0, ids[1], "the line the tip adds is the tip's");
    assert!(
        lines[1..].iter().all(|(id, _)| *id == ids[0]),
        "the rest come from one older changeset"
    );
    assert_eq!(paths.get(&ids[0]), Some(&RepoPath::new("f/00").unwrap()));
}

/// A cursor that carries a rename search at a root changeset is refused:
/// a root has no parent, so the search has nothing to search in, and
/// dropping it silently would blame every line on the root.
#[test]
fn a_rename_search_at_a_root_changeset_is_refused() {
    let (dir, store, root) = store(b"content\n");
    let resume = scion_server::test_support::blame::Resume {
        next: None,
        search: scion_server::test_support::blame::RenameSearch {
            searched: 1,
            best: None,
        },
        ..Default::default()
    };
    let err = scion_server::test_support::blame::blame(
        &store,
        REPO,
        root,
        &RepoPath::new(PATH).unwrap(),
        &resume,
        &scion_server::test_support::memory::Bytes::new(scion_server::REPLY_MEMORY_BUDGET),
        Some(std::time::Instant::now() + std::time::Duration::from_secs(600)),
    )
    .unwrap_err();
    assert!(
        matches!(err, scion_server::Error::InvalidArgument(_)),
        "{err:?}"
    );
    drop(dir);
}
