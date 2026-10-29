//! Every RPC of both services answers `UNAUTHENTICATED` to a call that
//! carries no token or a wrong token, and none of the calls has a side
//! effect. The list of RPCs comes from `proto/scion.proto` itself: a new
//! RPC without a row here fails this test, so a new method cannot land
//! without auth coverage.

use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::objects_server::SERVICE_NAME as OBJECTS_SERVICE;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::scion_server::SERVICE_NAME as SCION_SERVICE;
use scion_proto::v1::{
    BlameRequest, CreateChangesetRequest, DiffRequest, GetChangesetRequest, GetLandRequest,
    GetObjectsRequest, GetRootTreeRequest, IsAncestorRequest, LandRequest, ListDirRequest,
    LogRequest, MergeBaseRequest, PutBlobRequest, ReadFileRequest, ResolveRefRequest,
};

use scion_server::Access;
use scion_server::Keys;
use scion_server::Metrics;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, test_support::service};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::service::interceptor::{InterceptedService, Interceptor};
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const REPO: &str = "main";

/// Adds `authorization: Bearer <token>` to every request. `None` adds
/// nothing: the client of a caller that sends no token.
#[derive(Clone)]
struct Bearer(Option<tonic::metadata::AsciiMetadataValue>);

impl Interceptor for Bearer {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = self.0.clone() {
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;
type Objects = ObjectsClient<InterceptedService<Channel, Bearer>>;

fn access() -> Access {
    let digest = blake3::Hash::from(token_digest("ci")).to_hex().to_string();
    let text = format!("token\tci\t{digest}\nread\tci\t{REPO}\nwrite\tci\t{REPO}\n");
    Access::parse(&text).expect("the access file parses")
}

/// A store whose repo `main` has one root changeset that puts `README`,
/// and its blob.
async fn store() -> (tempfile::TempDir, Backend, scion_core::ChangesetId) {
    let dir = scion_server::test_support::private_tempdir();
    let store = {
        let path = dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || Backend::open_test(&path).unwrap())
            .await
            .unwrap()
    };
    let blob = store.blobs().put(b"# hello\n").unwrap();
    let cs = scion_core::Changeset::new(
        vec![],
        scion_core::IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"root\n".to_vec(),
        [(
            scion_core::RepoPath::new("README").unwrap(),
            Some(scion_core::FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    let root = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = c.insert_changeset(repo, &cs)?;
            c.cas_ref(repo, "main", None, root)?;
            c.add_repo_blob(repo, blob)?;
            Ok(root)
        })
        .unwrap();
    (dir, store, root)
}

/// The server of `store` behind `access`, as a `http://` URL. Hold the
/// stop signal, or the server shuts down when it drops.
async fn serve(
    store: &Backend,
    dir: &std::path::Path,
    access: Access,
) -> (tokio::sync::oneshot::Sender<()>, String) {
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
            metrics: Arc::new(Metrics::default()),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(Arc::new(Keys::generate())),
        async {
            stopped.await.ok();
        },
    ));
    (stop, format!("http://{addr}"))
}

/// A client pair of `url` as `token`; `None` sends no token at all.
struct Pair {
    scion: Scion,
    objects: Objects,
}

fn client_with(channel: Channel, token: Option<&str>) -> Pair {
    let bearer = match token {
        None => Bearer(None),
        Some(token) => Bearer::from_token(token),
    };
    Pair {
        scion: ScionClient::with_interceptor(channel.clone(), bearer.clone()),
        objects: ObjectsClient::with_interceptor(channel, bearer),
    }
}

impl Bearer {
    fn from_token(token: &str) -> Self {
        Self(Some(
            format!("Bearer {token}")
                .parse()
                .expect("the header value is visible ASCII"),
        ))
    }
}

/// The requests that must be refused only for their token: real bytes, a
/// real changeset on `main`, and a real land. If the server refused one
/// of these for another reason, the side-effect check would say nothing.
struct Payload {
    put_blob: PutBlobRequest,
    create_changeset: CreateChangesetRequest,
    land: LandRequest,
}

/// One call of one RPC, drained to its end.
type Call = fn(&Pair, &Payload) -> Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>;

/// The RPCs of one service, and the call that reaches each. The row's
/// name is the method name as the proto spells it; the test fails when a
/// row has no `rpc` line, or an `rpc` line has no row.
struct Row {
    service: &'static str,
    rpc: &'static str,
    call: Call,
}

/// The same method on the client that has it; the other client's call of
/// the same name (the two services share `ResolveRef`) is covered by its
/// own row.
macro_rules! scion_only {
    ($method:ident, $request:ident) => {
        |pair: &Pair, _payload: &Payload| {
            let mut scion = pair.scion.clone();
            Box::pin(async move {
                scion.$method(<$request as Default>::default()).await?;
                Ok(())
            }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
        }
    };
}

macro_rules! objects_only {
    ($method:ident, $request:ty) => {
        |pair: &Pair, _payload: &Payload| {
            let mut objects = pair.objects.clone();
            Box::pin(async move {
                objects.$method(<$request as Default>::default()).await?;
                Ok(())
            }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
        }
    };
}

/// The rows: one per RPC of `proto/scion.proto`.
#[allow(clippy::too_many_lines)]
fn rows() -> Vec<Row> {
    vec![
        Row {
            service: "scion.v1.Scion",
            rpc: "PutBlob",
            call: |pair: &Pair, payload: &Payload| {
                let mut scion = pair.scion.clone();
                let request = payload.put_blob.clone();
                Box::pin(async move {
                    scion.put_blob(tokio_stream::once(request)).await?;
                    Ok(())
                }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
            },
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "CreateChangeset",
            call: |pair: &Pair, payload: &Payload| {
                let mut scion = pair.scion.clone();
                let request = payload.create_changeset.clone();
                Box::pin(async move {
                    scion.create_changeset(request).await?;
                    Ok(())
                }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
            },
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "Land",
            call: |pair: &Pair, payload: &Payload| {
                let mut scion = pair.scion.clone();
                let request = payload.land.clone();
                Box::pin(async move {
                    scion.land(request).await?;
                    Ok(())
                }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
            },
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "GetLand",
            call: scion_only!(get_land, GetLandRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "ResolveRef",
            call: scion_only!(resolve_ref, ResolveRefRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "GetChangeset",
            call: scion_only!(get_changeset, GetChangesetRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "ReadFile",
            call: scion_only!(read_file, ReadFileRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "ListDir",
            call: scion_only!(list_dir, ListDirRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "Log",
            call: scion_only!(log, LogRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "Blame",
            call: scion_only!(blame, BlameRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "MergeBase",
            call: scion_only!(merge_base, MergeBaseRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "IsAncestor",
            call: scion_only!(is_ancestor, IsAncestorRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "Diff",
            call: scion_only!(diff, DiffRequest),
        },
        Row {
            service: "scion.v1.Objects",
            rpc: "ResolveRef",
            call: objects_only!(resolve_ref, ResolveRefRequest),
        },
        Row {
            service: "scion.v1.Objects",
            rpc: "GetRootTree",
            call: objects_only!(get_root_tree, GetRootTreeRequest),
        },
        Row {
            service: "scion.v1.Objects",
            rpc: "GetObjects",
            // The one RPC with no token check: its permission is the
            // read ticket (docs/architecture.md, section 8). A call with
            // no ticket at all is refused with PERMISSION_DENIED.
            call: |pair: &Pair, _payload: &Payload| {
                let mut objects = pair.objects.clone();
                Box::pin(async move {
                    objects
                        .get_objects(GetObjectsRequest {
                            refs: vec![scion_proto::v1::ObjectRef {
                                kind: scion_proto::v1::ObjectKind::Blob as i32,
                                id: vec![0; 32],
                                path: Vec::new(),
                            }],
                            accept: Vec::new(),
                            ticket: Vec::new(),
                        })
                        .await?;
                    Ok(())
                }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
            },
        },
    ]
}

/// The `(service, rpc)` pairs that `proto/scion.proto` declares, and the
/// service names the generated code claims. A new RPC without a row
/// fails the test; so does a row whose RPC is gone.
fn declared() -> (Vec<(String, String)>, Vec<String>) {
    // The proto of the repository: the same file the build compiles.
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto/scion.proto");
    let text = std::fs::read_to_string(&path).expect("proto/scion.proto is readable");
    let package = text
        .lines()
        .map(str::trim)
        .find_map(|line| {
            line.strip_prefix("package ")
                .and_then(|rest| rest.strip_suffix(';'))
        })
        .expect("the proto declares a package")
        .to_owned();
    let mut service = String::new();
    let mut declared = Vec::new();
    for line in text.lines().map(str::trim) {
        if let Some(name) = line
            .strip_prefix("service ")
            .and_then(|r| r.strip_suffix(" {"))
        {
            // The file's package makes the full name the generated code
            // publishes as SERVICE_NAME.
            service = format!("{package}.{}", name.trim());
        } else if let Some(rest) = line.strip_prefix("rpc ") {
            let name = rest.split('(').next().expect("rpc NAME(").trim().to_owned();
            declared.push((service.clone(), name));
        }
    }
    let generated = vec![SCION_SERVICE.to_owned(), OBJECTS_SERVICE.to_owned()];
    (declared, generated)
}

/// The repo's state that the calls must not change: the version of
/// `main`, and the row count of every metadata table. The tables come
/// from the store's own catalog, so a new table joins the check on its
/// own. On Postgres the rows are counted through a client of the test
/// database, in the schema of this data directory. The reads block, so
/// they run on a thread outside the runtime.
async fn state_of(store: &Backend, dir: &Path) -> (i64, Vec<i64>) {
    let store = store.clone();
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || state_of_blocking(&store, &dir))
        .await
        .unwrap()
}

fn state_of_blocking(store: &Backend, dir: &Path) -> (i64, Vec<i64>) {
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let schema = Backend::test_schema(dir);
    let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    client
        .batch_execute(&format!("SET search_path TO {schema}"))
        .unwrap();
    let tables = client
        .query(
            "SELECT tablename FROM pg_tables
             WHERE schemaname = current_schema()
             ORDER BY tablename",
            &[],
        )
        .unwrap();
    let counts = tables
        .iter()
        .map(|row| {
            let table: String = row.get(0);
            client
                .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
                .unwrap()
                .get(0)
        })
        .collect();
    let version = client
        .query_one("SELECT version FROM refs WHERE name = 'main'", &[])
        .unwrap()
        .get(0);
    (version, counts)
}
