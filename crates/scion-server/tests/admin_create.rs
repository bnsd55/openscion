//! The admin commands that make a repo and a ref without a Git import:
//! `scion-server create-repo` (one root changeset, the empty tree, the
//! trunk ref at it) and `scion-server create-ref` (a new ref at a
//! changeset the repo holds). The made repo and ref are visible to a
//! server that runs beside the commands: the metadata is the database's,
//! and nothing in the made state waits for a restart.

mod common;

use scion_core::{Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::v1::{
    CreateChangesetRequest, GetLandRequest, LandRequest, LandState, PutBlobRequest,
    ResolveRefRequest,
};
use scion_server::Backend;
use std::fs;
use std::process::{Command, Output};

/// The URL a store on `dir` opens with: the test database, under the
/// directory's own schema, created first. The command reads it from the
/// env, as every command that opens a store does.
fn url_of(dir: &std::path::Path) -> String {
    Backend::test_postgres_url(dir).unwrap()
}

/// Runs `scion-server` with `args` after the global `--data`, against the
/// store of `dir`, as its own process.
fn run(data: &std::path::Path, args: &[&str]) -> Output {
    scion_proto::runtime::auth::create_private_dir_all(data).unwrap();
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data)
        .args(args)
        .env("NO_COLOR", "1")
        .env("SCION_POSTGRES_URL", url_of(data))
        .output()
        .unwrap()
}

/// The audit lines of the data directory's audit.log.
fn audit_lines(data: &std::path::Path) -> Vec<serde_json::Value> {
    let text = fs::read_to_string(data.join("audit.log")).unwrap();
    text.lines()
        .map(|line| serde_json::from_str(line).expect("each audit line is one JSON object"))
        .collect()
}

/// The changeset ID of the bytes `reply.changeset_id` carried.
fn changeset_id(bytes: &[u8]) -> ChangesetId {
    ChangesetId::try_from(bytes).expect("the reply carries a changeset ID")
}

/// A single-parent change over `parent`: one file `docs/x`.
fn change_over(parent: ChangesetId, blob: scion_core::BlobId) -> (Vec<u8>, ChangesetId) {
    Changeset::new(
        vec![parent],
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"change\n".to_vec(),
        [(
            RepoPath::new("docs/x").unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format")
    .encode_with_id()
}

/// The made repo answers a live server, takes a client's first change,
/// and the land moves the made ref: everything the issue asks for, with
/// the server and the command as two processes that never share state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_made_repo_answers_a_live_server_and_takes_a_land() {
    let dir = scion_server::test_support::private_tempdir();
    let (data, log) = (dir.path().join("data"), dir.path().join("log"));
    let server = common::Server::start(&data, &log);
    let out = run(&data, &["create-repo", "--repo", "fresh"]);
    assert!(
        out.status.success(),
        "create-repo ran: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The made repo answers ResolveRef on the live server: the trunk ref
    // points at the root changeset the command stored.
    let (mut scion, mut objects) = common::connect(&server.url).await.unwrap();
    let root = objects
        .resolve_ref(ResolveRefRequest {
            repo: "fresh".into(),
            name: "main".into(),
        })
        .await
        .expect("the made repo answers")
        .into_inner();
    let root = changeset_id(&root.changeset_id);

    // A client puts a blob and creates a single-parent changeset on the
    // root the command made, then lands it on the made ref.
    let blob = scion
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: "fresh".into(),
            data: b"a file of the first change\n".to_vec(),
        }))
        .await
        .expect("the upload is allowed")
        .into_inner()
        .blob_id;
    let blob = scion_core::BlobId::try_from(blob.as_slice()).unwrap();
    let (bytes, id) = change_over(root, blob);
    scion
        .create_changeset(CreateChangesetRequest {
            repo: "fresh".into(),
            changeset: bytes,
        })
        .await
        .expect("the change is allowed");
    let land = scion
        .land(LandRequest {
            repo: "fresh".into(),
            changeset_id: id.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "first-1".into(),
        })
        .await
        .expect("the land is allowed")
        .into_inner();
    assert_eq!(land.state(), LandState::Landed, "{land:?}");

    // ResolveRef names the landed changeset now, on the same live server.
    let tip = objects
        .resolve_ref(ResolveRefRequest {
            repo: "fresh".into(),
            name: "main".into(),
        })
        .await
        .expect("the made ref answers")
        .into_inner();
    assert_eq!(changeset_id(&tip.changeset_id), id, "the ref moved");

    // The land's own answer, asked again: it remembers the outcome.
    let got = scion
        .get_land(GetLandRequest {
            repo: "fresh".into(),
            land_id: land.land_id,
        })
        .await
        .expect("the land is remembered")
        .into_inner();
    assert_eq!(got.state(), LandState::Landed, "{got:?}");
}

/// A refused second create-repo changes nothing and answers the audit's
/// intent with one failed line; the repo the first run made keeps its
/// one root.
#[test]
fn a_second_create_refuses_and_the_audit_says_so() {
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    let first = run(&data, &["create-repo", "--repo", "fresh"]);
    assert!(first.status.success(), "the first create ran");

    let second = run(&data, &["create-repo", "--repo", "fresh"]);
    assert!(
        !second.status.success(),
        "a second root is refused: stdout {} stderr {}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    let lines = audit_lines(&data);
    let whats: Vec<_> = lines
        .iter()
        .map(|l| {
            (
                l["outcome"].as_str().unwrap_or(""),
                l["what"].as_str().unwrap_or(""),
            )
        })
        .collect();
    assert!(
        matches!(
            whats.as_slice(),
            [
                ("intent", "create-repo"),
                ("ok", "create-repo"),
                ("intent", "create-repo"),
                ("failed", "create-repo")
            ]
        ),
        "{whats:?}"
    );
}

/// `create-ref` makes a second ref at a changeset the repo holds, refuses
/// a ref that exists, and refuses a changeset another repo holds.
#[test]
fn create_ref_makes_one_ref_and_refuses_the_rest() {
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    let out = run(&data, &["create-repo", "--repo", "fresh"]);
    assert!(out.status.success(), "create-repo ran");
    let lines = audit_lines(&data);
    let root = lines
        .iter()
        .find(|l| l["outcome"] == "ok" && l["what"] == "create-repo")
        .expect("the ok line carries the root")["changeset"]
        .as_str()
        .expect("the ok line names the changeset")
        .to_owned();

    // A second ref at the same root: fine, its own name.
    let out = run(
        &data,
        &[
            "create-ref",
            "--repo",
            "fresh",
            "--ref",
            "dev",
            "--at",
            &root,
        ],
    );
    assert!(out.status.success(), "the new ref ran: {out:?}");

    // The same name again: refused.
    let again = run(
        &data,
        &[
            "create-ref",
            "--repo",
            "fresh",
            "--ref",
            "dev",
            "--at",
            &root,
        ],
    );
    assert!(!again.status.success(), "a made ref is refused");
    assert!(
        String::from_utf8_lossy(&again.stderr).contains("already exists"),
        "the refusal names the conflict"
    );

    // A changeset another repo holds: refused, even though the ID parses.
    let other = run(&data, &["create-repo", "--repo", "other"]);
    assert!(other.status.success(), "the second repo ran");
    let wrong = run(
        &data,
        &[
            "create-ref",
            "--repo",
            "other",
            "--ref",
            "dev",
            "--at",
            &root,
        ],
    );
    assert!(
        !wrong.status.success(),
        "a foreign changeset is refused: {}",
        String::from_utf8_lossy(&wrong.stderr)
    );

    // The audit holds every intent's answer, in order.
    let lines = audit_lines(&data);
    let whats: Vec<_> = lines
        .iter()
        .map(|l| {
            (
                l["outcome"].as_str().unwrap_or(""),
                l["what"].as_str().unwrap_or(""),
            )
        })
        .collect();
    assert!(
        matches!(
            whats.as_slice(),
            [
                ("intent", "create-repo"),
                ("ok", "create-repo"),
                ("intent", "create-ref"),
                ("ok", "create-ref"),
                ("intent", "create-ref"),
                ("failed", "create-ref"),
                ("intent", "create-repo"),
                ("ok", "create-repo"),
                ("intent", "create-ref"),
                ("failed", "create-ref")
            ]
        ),
        "{whats:?}"
    );
}

/// A name that breaks the rule is refused, and the audit holds the
/// attempt's intent and its one failed line.
#[test]
fn a_bad_name_is_refused_and_the_audit_says_so() {
    let dir = scion_server::test_support::private_tempdir();
    let data = dir.path().join("data");
    // A control character is what the name rule refuses.
    let out = run(&data, &["create-repo", "--repo", "bad	name"]);
    assert!(!out.status.success(), "a bad repo name is refused");
    let lines = audit_lines(&data);
    let whats: Vec<_> = lines
        .iter()
        .map(|l| {
            (
                l["outcome"].as_str().unwrap_or(""),
                l["what"].as_str().unwrap_or(""),
            )
        })
        .collect();
    assert!(
        matches!(
            whats.as_slice(),
            [("intent", "create-repo"), ("failed", "create-repo")]
        ),
        "{whats:?}"
    );
}
