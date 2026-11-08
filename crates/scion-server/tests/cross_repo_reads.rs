//! Cross-repo reads, end to end: a changeset of one repo read through
//! another repo's name answers `NotFound`, on a stored derived value
//! too. The stored-hit path is scoped to the repo by one checked choke
//! point (`derived::require`), so the served readers must keep refusing
//! whatever that choke point does not leak: a reply here that carried
//! the other repo's history or graph would be a live cross-repo read
//! (#1028).

use std::sync::Arc;

use scion_core::{Changeset, FileState, RepoPath};
use scion_proto::runtime::auth::Bearer;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{IsAncestorRequest, LogRequest};
mod common;

use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::{service, tree};
use scion_server::{Backend, Metrics};
use tonic::Code;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

const TOKEN: &str = "secret";

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

/// A store with repos `a` and `b`, and a two-changeset chain in `b`:
/// the parent changes `d/f.txt`, the child changes `other.txt`.
fn two_repos() -> (tempfile::TempDir, Backend, Vec<scion_core::ChangesetId>) {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test_shared(dir.path()).unwrap();
    let ids = store
        .write(|c| {
            let a = c.create_repo("a")?;
            let _ = a;
            let b = c.create_repo("b")?;
            let mut ids = Vec::new();
            for (i, path) in [b"d/f.txt".as_slice(), b"other.txt".as_slice()]
                .into_iter()
                .enumerate()
            {
                let content = vec![b'a' + u8::try_from(i).unwrap(); i + 1];
                let regular = FileState::Regular {
                    content: store.blobs().put(&content).unwrap(),
                    executable: false,
                };
                let cs = Changeset::new(
                    ids.last().copied().into_iter().collect(),
                    scion_core::IdentityTime::new(b"a".to_vec(), b"t@x".to_vec(), 1_700_000_000, 0)
                        .expect("identity fits the format"),
                    None,
                    format!("change {i}\n").into_bytes(),
                    [(RepoPath::new(path).unwrap(), Some(regular))].into(),
                )
                .expect("changeset fits the format");
                ids.push(tree::store_changeset(c, b, &cs, &mut |_| Ok(0))?.0);
            }
            Ok(ids)
        })
        .unwrap();
    (dir, store, ids)
}

async fn serve(store: Backend, dir: &std::path::Path) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(service::serve(
        store,
        listener,
        TOKEN,
        service::ServeOptions {
            data: dir.to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::new(Metrics::default()),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        None,
        std::future::pending(),
    ));
    format!("http://{addr}")
}

async fn client(url: &str) -> Scion {
    ScionClient::with_interceptor(
        common::channel(url).await.unwrap(),
        Bearer::new(TOKEN).unwrap(),
    )
}

/// One `Log` call's status: the reply's error code, or `None` when the
/// call answered.
async fn log_status(scion: &mut Scion, repo: &str, from: &scion_core::ChangesetId) -> Option<Code> {
    let reply = scion
        .log(LogRequest {
            repo: repo.to_string(),
            changeset_id: from.as_bytes().to_vec(),
            limit: 10,
            path: Vec::new(),
        })
        .await;
    match reply {
        Err(status) => Some(status.code()),
        Ok(streamed) => match streamed.into_inner().message().await {
            Err(status) => Some(status.code()),
            Ok(_) => None,
        },
    }
}

/// The history of repo `b`'s changeset read through repo `a`: warm the
/// stored value through `b` first, so the served read rides the stored
/// hit, then ask `a`. The answer is `NotFound`, never `b`'s page.
#[tokio::test(flavor = "multi_thread")]
async fn a_log_of_another_repos_changeset_answers_not_found() {
    let (dir, store, ids) = two_repos();
    let url = serve(store.clone(), dir.path()).await;
    let mut scion = client(&url).await;

    // Warm: the same read through `b` derives and stores the value.
    assert_eq!(log_status(&mut scion, "b", &ids[1]).await, None);
    // And once more, so the warm read itself is a stored hit.
    assert_eq!(log_status(&mut scion, "b", &ids[1]).await, None);

    assert_eq!(
        log_status(&mut scion, "a", &ids[1]).await,
        Some(Code::NotFound),
        "repo a must not read repo b's history, stored value or not"
    );
    assert_eq!(
        log_status(&mut scion, "a", &ids[0]).await,
        Some(Code::NotFound),
        "the parent is not readable through `a` either"
    );
}

/// `IsAncestor` of repo `b`'s changesets asked through repo `a`: the
/// commit graph is warm (one true read through `b` derived it), and the
/// answer through `a` is `NotFound`, never a verdict about `b`'s chain.
#[tokio::test(flavor = "multi_thread")]
async fn an_is_ancestor_of_another_repos_changesets_answers_not_found() {
    let (dir, store, ids) = two_repos();
    let url = serve(store.clone(), dir.path()).await;
    let mut scion = client(&url).await;

    // Warm: the same question through `b` derives and stores the graph.
    let warm = scion
        .is_ancestor(IsAncestorRequest {
            repo: "b".into(),
            ancestor_id: ids[0].as_bytes().to_vec(),
            descendant_id: ids[1].as_bytes().to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(warm.is_ancestor, "the chain's parent is its ancestor");

    let why = scion
        .is_ancestor(IsAncestorRequest {
            repo: "a".into(),
            ancestor_id: ids[0].as_bytes().to_vec(),
            descendant_id: ids[1].as_bytes().to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(why.code(), Code::NotFound, "{why:?}");
}
