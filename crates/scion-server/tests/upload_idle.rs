//! The idle-upload bound (#1385): an upload that sends no message for
//! 120 s ends and frees its permit, and a principal that may not write
//! any repo is refused before it takes a permit at all. Every test has
//! its own wait, shorter than the server's 600 s call timeout, so a
//! server without the fix fails fast instead of hanging.

use scion_core::{Changeset, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::PutBlobRequest;
use scion_proto::v1::scion_client::ScionClient;
use std::sync::Arc;
use std::time::{Duration, Instant};

use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{
    Access, Backend, Keys, Metrics, Quotas, REPLY_MEMORY_BUDGET, ServeOptions, serve,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::{InterceptedService, Interceptor};
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

mod common;

const REPO: &str = "main";
/// The upload slots the server serves at once (`MAX_UPLOADS` in
/// service.rs). A public-RPC test names the number, not the private const.
const MAX_UPLOADS: usize = 16;

/// Adds `authorization: Bearer <token>` to every request.
#[derive(Clone)]
struct Bearer(Option<AsciiMetadataValue>);

impl Interceptor for Bearer {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = self.0.clone() {
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

fn hex(token: &str) -> String {
    blake3::Hash::from(token_digest(token)).to_hex().to_string()
}

fn regular(content: scion_core::BlobId) -> FileState {
    FileState::Regular {
        content,
        executable: false,
    }
}

/// A store whose repo `main` has one root changeset that puts `README`.
fn store() -> (tempfile::TempDir, Backend) {
    let dir = scion_server::test_support::private_tempdir();
    let store = Backend::open_test(dir.path()).unwrap();
    let blob = store.blobs().put(b"# hello\n").unwrap();
    let root = Changeset::new(
        vec![],
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"root\n".to_vec(),
        [(RepoPath::new("README").unwrap(), Some(regular(blob)))].into(),
    )
    .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = c.insert_changeset(repo, &root)?;
            c.cas_ref(repo, "main", None, root)?;
            c.add_repo_blob(repo, blob)?;
            Ok(root)
        })
        .unwrap();
    (dir, store)
}

/// A server of `store` with `access`. Hold the stop signal, or the server
/// shuts down when it drops.
async fn serve_store(
    store: &Backend,
    dir: &std::path::Path,
    access: Access,
) -> (tokio::sync::oneshot::Sender<()>, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(
        store.clone(),
        listener,
        access,
        ServeOptions {
            data: dir.to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: Quotas::off(),
            walk_share: 0,
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::new(Metrics::default()),
            reply_memory_budget: REPLY_MEMORY_BUDGET,
        },
        Some(Arc::new(Keys::generate())),
        async {
            stopped.await.ok();
        },
    ));
    (stop, format!("http://{addr}"))
}

/// A client whose every call carries `timeout` as its own ceiling.
async fn client_within(url: &str, token: &str, timeout: Duration) -> Scion {
    let channel = common::channel_within(url, timeout).await.unwrap();
    let bearer = Bearer(Some(format!("Bearer {token}").parse().unwrap()));
    ScionClient::with_interceptor(channel, bearer)
}

/// An upload stream that says nothing: the client holds the call open and
/// sends no message. The sender stays with the test, so the stream never
/// ends on its own.
fn idle_stream() -> (
    tokio::sync::mpsc::Sender<PutBlobRequest>,
    ReceiverStream<PutBlobRequest>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel::<PutBlobRequest>(1);
    (tx, ReceiverStream::new(rx))
}

/// Sixteen idle streams of a read-only principal cannot block a writer's
/// upload: the fixed server refuses each before it takes a permit, so the
/// writer's upload goes through inside 30 s. On main the idle streams pin
/// all 16 slots for 600 s each and the upload never goes through.
#[tokio::test(flavor = "multi_thread")]
async fn sixteen_idle_read_only_streams_cannot_block_a_writers_upload() {
    let (dir, store) = store();
    let access = Access::parse(&format!(
        "token\tbob\t{}\ntoken\tci\t{}\nread\tbob\t{REPO}\nwrite\tci\t{REPO}\nread\tci\t{REPO}\n",
        hex("bob"),
        hex("ci"),
    ))
    .unwrap();
    let (_stop, url) = serve_store(&store, dir.path(), access).await;
    // The idle streams' ceiling is far above the writer's 30 s window:
    // on main they hold their slots across it, and the writer's wait is
    // what fails the test. On the fixed server they are refused at once.
    let bob = client_within(&url, "bob", Duration::from_secs(180)).await;
    let mut ci = client_within(&url, "ci", Duration::from_secs(35)).await;

    // The streams outlive the loop: each call is spawned with its own
    // client clone, so the idle streams answer even after `bob` moves.
    let mut idle = Vec::new();
    // The senders stay with the test: a dropped one would close its
    // stream, and the server would see an empty upload, not an idle one.
    let mut senders = Vec::new();
    for _ in 0..MAX_UPLOADS {
        let (tx, rx) = idle_stream();
        let mut client = bob.clone();
        idle.push(tokio::spawn(async move { client.put_blob(rx).await }));
        senders.push(tx);
    }

    // The idle streams reach the server before the writer does, so on
    // main all 16 slots are pinned when the writer arrives. On the fixed
    // server they are refused in that window and hold nothing.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let reply = tokio::time::timeout(Duration::from_secs(30), async {
        ci.put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.into(),
            data: b"up".to_vec(),
        }))
        .await
    })
    .await
    .expect("the writer's upload completes within 30 s");
    assert!(
        !reply
            .expect("the upload is accepted")
            .into_inner()
            .blob_id
            .is_empty(),
        "the upload answers with a blob id"
    );

    // Every idle stream was refused, with no permit ever held.
    for handle in idle {
        let status = handle.await.unwrap().expect_err("an idle stream ends");
        assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    }
}

/// The idle bound itself: a writer's upload that sends no message is
/// ended by the server's own 120 s bound with `DEADLINE_EXCEEDED`,
/// naming the bound. The test's own wait (150 s) sits under the
/// channel's ceiling (180 s) and the server's call timeout (600 s), so
/// on a server without the bound the test fails at 150 s.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_writer_stream_loses_the_call_within_its_own_wait() {
    let (dir, store) = store();
    let access = Access::parse(&format!(
        "token\tci\t{}\nread\tci\t{REPO}\nwrite\tci\t{REPO}\n",
        hex("ci"),
    ))
    .unwrap();
    let (_stop, url) = serve_store(&store, dir.path(), access).await;
    let mut ci = client_within(&url, "ci", Duration::from_secs(180)).await;
    let (_tx, rx) = idle_stream();
    let started = Instant::now();
    let status = tokio::time::timeout(Duration::from_secs(150), ci.put_blob(rx))
        .await
        .expect("the idle upload ends within 150 s")
        .expect_err("an idle upload ends in an error");
    assert_eq!(status.code(), Code::DeadlineExceeded, "{status:?}");
    assert!(
        status.message().contains("no PutBlob message"),
        "{status:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(150),
        "the server's bound, not the test's ceiling, ended the call"
    );
}

/// A read-only principal is refused with the write check's own status and
/// message, before any message is sent - so it never holds an upload
/// permit. On main the stream pins a permit and answers nothing: the test
/// fails at its own 10 s wait.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_principal_is_refused_before_any_message() {
    let (dir, store) = store();
    let access =
        Access::parse(&format!("token\tbob\t{}\nread\tbob\t{REPO}\n", hex("bob"))).unwrap();
    let (_stop, url) = serve_store(&store, dir.path(), access).await;

    let mut client = client_within(&url, "bob", Duration::from_secs(30)).await;
    let (_tx, rx) = idle_stream();
    let status = tokio::time::timeout(Duration::from_secs(10), client.put_blob(rx))
        .await
        .expect("the refusal arrives within 10 s, with no message sent")
        .expect_err("a read-only principal is refused");
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    assert!(
        status.message().contains("may not write in any repo"),
        "{}",
        status.message()
    );
}
