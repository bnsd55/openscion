//! The Diff wire form over gRPC: the rename pair's sides, the modified
//! file's both states, and the paging that follows `next_after` to the
//! end. The engine (`read::diff`) is tested in `query.rs`; nothing here
//! before checked the wire, so a mutant that flips the rename's sides,
//! drops a state, or breaks the page cursor passed every suite.

mod common;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::auth::Bearer;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{CreateChangesetRequest, DiffEntry, DiffRequest, FileKind, PutBlobRequest};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::tree;
use scion_server::{Backend, Metrics, test_support::service};
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

const REPO: &str = "main";
const TOKEN: &str = "diff-wire-token";

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

fn author() -> IdentityTime {
    IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 1_700_000_000, 0)
        .expect("identity fits the format")
}

/// Serves a repo whose root puts `old-name` and `kept`, both at blob A.
#[allow(clippy::type_complexity)] // the harness's four handles
async fn served() -> (tempfile::TempDir, Scion, ChangesetId, BlobId) {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let blob_a = store.blobs().put(b"a\n").unwrap();
    let root = Changeset::new(
        vec![],
        author(),
        None,
        b"root\n".to_vec(),
        [
            (
                RepoPath::new(b"old-name").unwrap(),
                Some(FileState::Regular {
                    content: blob_a,
                    executable: false,
                }),
            ),
            (
                RepoPath::new(b"kept").unwrap(),
                Some(FileState::Regular {
                    content: blob_a,
                    executable: false,
                }),
            ),
        ]
        .into(),
    )
    .expect("changeset fits the format");
    let root = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            Ok(tree::store_changeset(c, repo, &root, &mut |_| Ok(0))?.0)
        })
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(service::serve(
        store.clone(),
        listener,
        TOKEN,
        service::ServeOptions {
            data: dir.path().to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            limits: scion_proto::runtime::listen::Limits::default(),
            metrics: Arc::new(Metrics::default()),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        None,
        std::future::pending(),
    ));
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    let client = ScionClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap());
    (dir, client, root, blob_a)
}

/// The diff of `root` to `second`, paged to the end.
async fn whole_diff(
    client: &mut Scion,
    root: &ChangesetId,
    second: &ChangesetId,
) -> Vec<DiffEntry> {
    let mut after: Vec<u8> = Vec::new();
    let mut entries = Vec::new();
    // A 1004-entry diff pages twice; more pages than that is a cursor
    // that loops, so the walk is bounded and the bound is the answer.
    for _ in 0..5 {
        let page = client
            .diff(DiffRequest {
                repo: REPO.into(),
                old_changeset_id: root.as_bytes().to_vec(),
                new_changeset_id: second.as_bytes().to_vec(),
                path: Vec::new(),
                after: after.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        let came = page.entries.len();
        entries.extend(page.entries);
        if page.next_after.is_empty() {
            assert!(came > 0 || entries.is_empty(), "the last page has entries");
            return entries;
        }
        assert!(came > 0, "a page before the end has entries");
        after = page.next_after;
    }
    panic!("the diff pages never end: the cursor loops");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_diff_names_its_rename_sides_states_and_pages() {
    let (_dir, mut client, root, blob_a) = served().await;

    // The second changeset: a rename, a modify, and 1001 adds, so the
    // diff pages past MAX_DIFF_ENTRIES.
    let new_blob = client
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.into(),
            data: b"b\n".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .blob_id;
    let new_blob = BlobId::try_from(new_blob.as_slice()).unwrap();
    let mut change_list = vec![
        (
            RepoPath::new(b"new-name").unwrap(),
            Some(FileState::Regular {
                content: blob_a,
                executable: false,
            }),
        ),
        (RepoPath::new(b"old-name").unwrap(), None),
        (
            RepoPath::new(b"kept").unwrap(),
            Some(FileState::Regular {
                content: new_blob,
                executable: false,
            }),
        ),
    ];
    change_list.extend((0..1_001).map(|i| {
        (
            RepoPath::new(format!("add-{i:04}").as_bytes()).unwrap(),
            Some(FileState::Regular {
                content: new_blob,
                executable: false,
            }),
        )
    }));
    let changes: std::collections::BTreeMap<RepoPath, Option<FileState>> =
        change_list.into_iter().collect();
    let second = Changeset::new(vec![root], author(), None, b"diff\n".to_vec(), changes)
        .expect("changeset fits the format");
    let second_id = ChangesetId::try_from(
        client
            .create_changeset(CreateChangesetRequest {
                repo: REPO.into(),
                changeset: second.encode(),
            })
            .await
            .unwrap()
            .into_inner()
            .changeset_id
            .as_slice(),
    )
    .unwrap();

    // The diff, paged to the end: 1004 entries, none repeated.
    let entries = whole_diff(&mut client, &root, &second_id).await;
    assert_eq!(entries.len(), 1_004, "every entry came");
    let paths: std::collections::BTreeSet<Vec<u8>> =
        entries.iter().map(|e| e.path.clone()).collect();
    assert_eq!(paths.len(), entries.len(), "no entry repeats");

    // The rename pair: the other path on the right side of the pair.
    let added = entries
        .iter()
        .find(|e| e.path == b"new-name".to_vec())
        .expect("the added side of the rename");
    assert_eq!(
        added.rename_from,
        b"old-name".to_vec(),
        "added: rename_from"
    );
    assert!(added.rename_to.is_empty(), "added: no rename_to");
    assert!(added.similarity > 0, "the rename's similarity");
    let deleted = entries
        .iter()
        .find(|e| e.path == b"old-name".to_vec())
        .expect("the deleted side of the rename");
    assert_eq!(
        deleted.rename_to,
        b"new-name".to_vec(),
        "deleted: rename_to"
    );
    assert!(deleted.rename_from.is_empty(), "deleted: no rename_from");

    // The modified file: both states' kind and id on the wire.
    let kept = entries
        .iter()
        .find(|e| e.path == b"kept".to_vec())
        .expect("the modified file");
    let old_state = kept.old.as_ref().expect("the old state");
    let new_state = kept.new.as_ref().expect("the new state");
    assert_eq!(old_state.kind(), FileKind::Regular, "the old kind");
    assert_eq!(old_state.id, blob_a.as_bytes().to_vec(), "the old id");
    assert_eq!(new_state.kind(), FileKind::Regular, "the new kind");
    assert_eq!(new_state.id, new_blob.as_bytes().to_vec(), "the new id");
}
