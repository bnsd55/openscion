#![forbid(unsafe_code)]
//! Cache tiers for the `Objects` service: `scion-cache` and `scion-daemon`.
//!
//! A tier sits between clients and the server: a `scion-cache` holds the
//! hot objects of many users near them, and a `scion-daemon` is a
//! single-user cache on the client machine, serving `scn` over a private
//! Unix socket. Both are built on this crate; the server itself is the
//! [`open-scion-server`](https://docs.rs/open-scion-server) crate, and
//! the calls they forward and answer are defined in
//! [`scion-proto`](https://docs.rs/open-scion-proto).
//!
//! A [`Tier`] answers the same `Objects` calls as the server. It serves trees
//! and blobs from its own [`Store`] and sends each miss to its one upstream:
//! the next tier, or the server. It never trusts that upstream:
//!
//! - Every tree and blob from upstream is unpacked within the object limit
//!   and checked against its ID before the tier stores it or sends it on.
//! - `GetRootTree` answers are kept, but only answers that upstream gave. A
//!   changeset never changes, so its root tree never does.
//! - `ResolveRef` is always forwarded, never kept. A ref moves.
//!
//! A tier keeps a tree as its `TreeV1` bytes and a blob as one zstd frame,
//! the form in which the server stores it. It sends a blob on as that frame
//! to a client that accepts zstd, so a blob is packed once, on the server,
//! and unpacked once, by the client that writes it.
//!
//! A `GetObjects` reply follows the server's rules ([`scion_proto::runtime::reply`]):
//! 1 to 1,000 objects per call, a reply stops after 64 MiB, each object takes
//! its share of a byte budget before its bytes are read, and a reply ends when
//! its client stops reading. Misses are fetched in batches, and concurrent
//! misses for the same object share one fetch.

pub mod snapshot;
mod store;

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::Read as _;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use std::{fmt, io, iter};

use anyhow::Context as _;
use scion_core::{BlobId, Tree, TreeId};
use scion_proto::runtime::auth::{Auth, Bearer};
use scion_proto::runtime::check;
use scion_proto::runtime::deadline::Until;
use scion_proto::runtime::listen::{self, Limits};
use scion_proto::runtime::object::{
    self, Assembler, Header, MAX_ENCODED_BYTES, UNPACK_BYTES, zstd_bound,
};
use scion_proto::runtime::reply::{
    self, BUSY_TIMEOUT, Body, IN_FLIGHT_BYTES, ObjectStream, Outgoing, READS_IN_FLIGHT,
    STALL_TIMEOUT,
};
use scion_proto::runtime::tls::{ClientTls, ServerTls};
use scion_proto::ticket::Walk;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind, ObjectPart,
    ObjectRef, ResolveRefRequest, ResolveRefResponse,
};
use scion_proto::{Bytes, MAX_MESSAGE_BYTES, MAX_OBJECT_BYTES, MAX_PART_MESSAGE_BYTES};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio_stream::Stream;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::Connected;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::Instrument as _;

use store::lock;
pub use store::{DiskStore, MemoryStore, Store};

/// Most objects that one fetch asks upstream for. A reply sends at most
/// 64 MiB, so fetching far ahead of it only fills the store.
const MAX_BATCH: usize = 128;

// A reply's reads run at most [`READS_IN_FLIGHT`] at a time, in order, so a
// claim is never more than a batch behind the reply's tiling boundary and
// the batches tile the ask without a gap. If READS_IN_FLIGHT ever passed
// MAX_BATCH, a claim in the else branch would start a batch ahead of the
// boundary and leave a gap behind it.
const _: () = assert!(READS_IN_FLIGHT < MAX_BATCH);

/// Most fetches that read from upstream at once. Each holds at most one
/// message that the fetch budget has not counted yet: the first part of an
/// object, before its header is read.
const FETCH_SLOTS: usize = 64;

/// The fetch budget: bytes of objects from upstream, from the header of each
/// object until it is stored. With one message per fetch slot, all fetches
/// together hold at most [`IN_FLIGHT_BYTES`].
const FETCH_BYTES: usize = IN_FLIGHT_BYTES - FETCH_SLOTS * MAX_PART_MESSAGE_BYTES;

// The largest object fits in the fetch budget in both its forms, with a
// codec at work, so a fetch that waits for room gets it once others finish.
const _: () = assert!(MAX_ENCODED_BYTES + MAX_OBJECT_BYTES + UNPACK_BYTES <= FETCH_BYTES);

/// How long `scion-cache` waits for each step of the server: an answer, or
/// the next part of an object.
pub const CACHE_UPSTREAM_TIMEOUT: Duration = Duration::from_secs(60);

/// How long `scion-daemon` waits for each step of its cache. Before the cache
/// sends the first part of an object, it may wait [`BUSY_TIMEOUT`] for a
/// fetch slot, one step of the server, [`BUSY_TIMEOUT`] for room in its
/// fetch budget, and [`BUSY_TIMEOUT`] for room in its reply budget.
pub const DAEMON_UPSTREAM_TIMEOUT: Duration = Duration::from_secs(180);

// The limits nest: a busy server answers the cache before the cache gives
// up, and a cache that waits on the server answers the daemon before the
// daemon gives up. A caller's deadline also ends each wait, and each tier
// passes what is left of it upstream, less its margin (`Until`), so the
// deadlines nest as well.
const _: () = {
    // Seconds for the work around the waits: reading, checking and moving one
    // object.
    let margin = 30;
    let (busy, cache) = (BUSY_TIMEOUT.as_secs(), CACHE_UPSTREAM_TIMEOUT.as_secs());
    assert!(busy + margin <= cache);
    assert!(cache + 3 * busy + margin <= DAEMON_UPSTREAM_TIMEOUT.as_secs());
};

/// Most memory that decoding a tree holds, per byte of its `TreeV1` bytes.
/// An entry takes at least 38 bytes encoded (47 for a file, which also
/// carries its byte length); decoded, it is a name and an entry in a list
/// and again in a map, plus the name's own buffer (see the test
/// `decoding_a_tree_fits_its_factor`).
const TREE_DECODE_FACTOR: usize = 7;

/// One object, by kind and ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Key {
    /// A tree, stored as its `TreeV1` bytes.
    Tree(TreeId),
    /// A blob, stored as one zstd frame of its content.
    Blob(BlobId),
}

impl Key {
    fn parse(object: &ObjectRef) -> Result<Self, Status> {
        match ObjectKind::try_from(object.kind) {
            Ok(ObjectKind::Tree) => Ok(Self::Tree(check::id(&object.id, "tree id")?)),
            Ok(ObjectKind::Blob) => Ok(Self::Blob(check::id(&object.id, "blob id")?)),
            Ok(ObjectKind::Unspecified) | Err(_) => {
                Err(Status::invalid_argument("object kind must be TREE or BLOB"))
            }
        }
    }

    fn kind(self) -> ObjectKind {
        match self {
            Self::Tree(_) => ObjectKind::Tree,
            Self::Blob(_) => ObjectKind::Blob,
        }
    }

    fn id(self) -> [u8; 32] {
        match self {
            Self::Tree(id) => *id.as_bytes(),
            Self::Blob(id) => *id.as_bytes(),
        }
    }

    fn to_ref(self, path: &[u8]) -> ObjectRef {
        ObjectRef {
            kind: self.kind().into(),
            id: self.id().to_vec(),
            path: path.to_vec(),
        }
    }

    /// The encoding of the form in which a tier stores this object.
    fn stored_encoding(self) -> Encoding {
        match self {
            Self::Tree(_) => Encoding::Raw,
            Self::Blob(_) => Encoding::Zstd,
        }
    }

    /// Whether the first part of an object names this object. Says nothing
    /// about its bytes.
    fn names(self, first: &ObjectPart) -> bool {
        first.kind == i32::from(self.kind()) && first.id == self.id()
    }

    /// The header of this object in its stored form `stored`, which was
    /// checked when it was stored.
    fn stored_header(self, stored: &[u8]) -> Result<Header, String> {
        let encoding = self.stored_encoding();
        let size = match encoding {
            Encoding::Raw => stored.len(),
            Encoding::Zstd => object::frame_size(stored)?,
        };
        Ok(Header {
            kind: self.kind(),
            id: self.id(),
            encoding,
            encoded_size: stored.len(),
            size,
        })
    }

    /// Checks that `stored` is this object in the form a tier stores it: a
    /// tree's `TreeV1` bytes that decode and hash to its ID, or one zstd frame
    /// of a blob that unpacks within the object limit and hashes to its ID.
    /// Unpacks as a stream, so it holds at most [`UNPACK_BYTES`], not the
    /// content.
    ///
    /// # Errors
    ///
    /// Why `stored` is not this object.
    pub fn check_stored(self, stored: &[u8]) -> Result<(), String> {
        let matches = match self {
            Self::Tree(id) => Tree::decode_with_id(stored).is_ok_and(|(_, actual)| actual == id),
            Self::Blob(id) => {
                let size = object::frame_size(stored)?;
                let unpacked =
                    object::unpacker(stored).map_err(|e| format!("cannot unpack: {e}"))?;
                // One byte over the size is enough to see that it is larger.
                let mut unpacked = unpacked.take(size as u64 + 1);
                let actual =
                    BlobId::of_reader(&mut unpacked).map_err(|e| format!("cannot unpack: {e}"))?;
                if unpacked.limit() != 1 {
                    return Err("unpacks to another size than its header gives".into());
                }
                actual == id
            }
        };
        if !matches {
            return Err(format!("the bytes are not {self}"));
        }
        Ok(())
    }

    /// The memory that reading this object from a store holds besides its
    /// stored form of `size` bytes: the disk store decodes a tree, and
    /// unpacks a blob, to check it.
    fn read_bytes(self, size: usize) -> usize {
        match self {
            Self::Tree(_) => TREE_DECODE_FACTOR * size,
            Self::Blob(_) => UNPACK_BYTES,
        }
    }

    /// Checks an object from upstream, whose header and encoded bytes are
    /// `header` and `data`, against this key. Returns it in the form a tier
    /// stores: a tree unpacked, a blob packed (as it came, or packed here).
    fn to_stored(self, header: &Header, data: Vec<u8>) -> Result<Vec<u8>, String> {
        let stored = match (self, header.encoding) {
            (Self::Tree(_), _) => object::decode(header, data)?,
            (Self::Blob(_), Encoding::Zstd) => data,
            (Self::Blob(id), Encoding::Raw) => {
                if BlobId::of(&data) != id {
                    return Err(format!("the bytes are not {self}"));
                }
                let packed = object::pack(&data).map_err(|e| format!("cannot pack: {e}"))?;
                // Only the packed form is held from here.
                drop(data);
                packed
            }
        };
        self.check_stored(&stored)?;
        if self.stored_header(&stored)?.size != header.size {
            return Err(format!("{self} is not {} bytes", header.size));
        }
        Ok(stored)
    }

    /// The most memory that [`Key::to_stored`] holds for an object with
    /// `header`: its encoded bytes, its other form, and a codec at work.
    fn check_bytes(self, header: &Header) -> usize {
        let other = match (self, header.encoding) {
            // Decoded, to check it.
            (Self::Tree(_), Encoding::Raw) => TREE_DECODE_FACTOR * header.size,
            // Unpacked whole, then decoded.
            (Self::Tree(_), Encoding::Zstd) => (1 + TREE_DECODE_FACTOR) * header.size,
            // Unpacked as a stream to check it.
            (Self::Blob(_), Encoding::Zstd) => UNPACK_BYTES,
            // Packed here, then checked as a stream.
            (Self::Blob(_), Encoding::Raw) => zstd_bound(header.size) + UNPACK_BYTES,
        };
        header.encoded_size + other
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tree(id) => write!(f, "tree {id}"),
            Self::Blob(id) => write!(f, "blob {id}"),
        }
    }
}

/// The client for the next tier up.
type Upstream = ObjectsClient<InterceptedService<Channel, UpstreamAuth>>;

/// What the replies that wait for one object get from its fetch: nothing yet,
/// then the object in its stored form, or why the fetch failed.
type Outcome = Option<Result<Bytes, Failed>>;

/// Why a tier could not get an object.
#[derive(Clone, Debug)]
struct Failed {
    status: Status,
    /// The whole upstream call failed (its own status, a transport error, a
    /// timeout), not this object alone.
    whole_call: bool,
    /// The deadline of the fetch that failed. A caller with a later one asks
    /// again: the failure may have come from that deadline.
    until: Until,
}

impl From<Status> for Failed {
    fn from(status: Status) -> Self {
        Self {
            status,
            whole_call: false,
            until: Until::NONE,
        }
    }
}

/// An object for a reply, or why the tier could not get it, with its share
/// of the reply budget.
type Found = (Result<Outgoing, Failed>, OwnedSemaphorePermit);

/// Splits what a reply found: `Err` with the call's status if the whole
/// upstream call failed, else the object or why this one object failed.
fn split(found: Found) -> Result<reply::Found, Status> {
    match found {
        (Err(failed), _) if failed.whole_call => Err(failed.status),
        (found, permit) => Ok((found.map_err(|failed| failed.status), permit)),
    }
}

/// The fetches in progress, by object. Objects are content-addressed, so
/// one ID is one object, whatever repo it came from.
type Flights = HashMap<Key, watch::Receiver<Outcome>>;

/// The 32 bytes of 64 hex characters, for a key flag. `None` if the text is
/// not that.
#[must_use]
pub fn key_from_hex(hex: &str) -> Option<[u8; 32]> {
    let hex = hex.trim();
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        let hi = hex.as_bytes()[2 * i] as char;
        let lo = hex.as_bytes()[2 * i + 1] as char;
        let hi = u8::from_str_radix(&hi.to_string(), 16).ok()?;
        let lo = u8::from_str_radix(&lo.to_string(), 16).ok()?;
        *byte = hi << 4 | lo;
    }
    Some(out)
}

/// Adds the tier's own token to every upstream call, and counts the calls.
#[derive(Clone)]
struct UpstreamAuth {
    bearer: Option<Bearer>,
    calls: Arc<AtomicU64>,
}

impl Interceptor for UpstreamAuth {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        match &mut self.bearer {
            // An object read's permission is its ticket: a tier sends no
            // token unless it was given one.
            Some(bearer) => bearer.call(request),
            None => Ok(request),
        }
    }
}

/// One cache tier. Clones share the same store and upstream.
pub struct Tier<S> {
    inner: Arc<Inner<S>>,
}

impl<S> Clone for Tier<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct Inner<S> {
    store: S,
    upstream: Upstream,
    /// How long to wait for upstream.
    timeout: Duration,
    /// How long to wait for room in a budget: [`BUSY_TIMEOUT`].
    busy: Duration,
    /// Calls sent upstream, of every kind.
    upstream_calls: Arc<AtomicU64>,
    /// The registry that counts objects served from the store and fetched
    /// from upstream, and, when the binary exports, the `Objects` calls.
    metrics: Arc<scion_proto::runtime::metrics::Metrics>,
    /// The reply budget, as on the server: object bytes read or copied for a
    /// reply and not yet taken by its client.
    in_flight: Arc<Semaphore>,
    /// [`FETCH_SLOTS`]: the fetches that read from upstream.
    fetch_slots: Arc<Semaphore>,
    /// [`FETCH_BYTES`]: object bytes received from upstream and not yet
    /// stored.
    fetching: Arc<Semaphore>,
    flights: Mutex<Flights>,
    /// Fires when a flight gives up an entry, so a waiter for the table to
    /// empty wakes once per settle instead of polling.
    settled: tokio::sync::Notify,
    /// The keys that a ticket of a call must verify against: the keys of
    /// The tickets this tier checks, and how.
    tickets: Tickets,
}

/// How a tier treats the ticket of a call. The mode is the constructor's
/// explicit choice and lives here, so no `is_empty()` ever decides a
/// security path.
enum Tickets {
    /// A cache: every call's ticket must verify against one of these
    /// keys, and the walk from its root tree proves every object. The
    /// list is never empty: `Tier::new` refuses it.
    Verify(Vec<ed25519_dalek::VerifyingKey>),
    /// A daemon: the ticket is relayed as it came, unverified, and the
    /// upstream does the checking. The walk is skipped with it (a relay
    /// has no root tree of its own to walk from).
    Relay,
}

/// What one reply has done: hits and misses for the log, and how far into
/// its keys the fetches it started reach.
#[derive(Default)]
struct Progress {
    hits: AtomicUsize,
    misses: AtomicUsize,
    /// Keys below this index were in a fetch that this reply started.
    fetched_to: AtomicUsize,
}

/// The bytes of the call's request-ID header, if it sent one.
fn request_id_bytes<T>(request: &Request<T>) -> Option<&[u8]> {
    request
        .metadata()
        .get(scion_proto::runtime::request_id::REQUEST_ID)
        .map(tonic::metadata::MetadataValue::as_bytes)
}

/// What one reply asks for.
struct Ask {
    /// The repo of the ticket, for the store's bookkeeping and the logs.
    repo: String,
    /// The ticket of the call: the only permission of the objects it
    /// names, and the repo and root tree that its walk starts from.
    ticket: scion_proto::ticket::Ticket,
    /// The walk of the ticket's root tree, shared by the reply's objects,
    /// so a tree on the way is read once.
    walk: Arc<Mutex<scion_proto::ticket::Walker>>,
    /// Each object with the path that its ref names, from the root tree.
    keys: Vec<(Key, Vec<u8>)>,
    /// The client accepts zstd.
    zstd: bool,
    /// When the reply ends its waits: the caller's deadline less a margin.
    until: Until,
    /// The request ID of the call: the caller's if it sent one that fits,
    /// else one this process made. It rides on the fetch to upstream, so
    /// one operation shows the same ID in the logs of every hop.
    request_id: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    /// The handle of the caller's connection, which counts what a stalled
    /// reply leaves in HTTP/2.
    stalled: Option<scion_proto::runtime::listen::Stalled>,
}

impl<S: Store> Tier<S> {
    /// A tier that keeps objects in `store` and sends misses to `upstream`
    /// (`http://HOST:PORT` or `unix:///PATH`). A non-empty `upstream_token`
    /// rides on every call; an empty one sends none, which is enough for
    /// `GetObjects` of a server: its permission is the ticket. It waits
    /// at most `timeout` for each step of upstream, and connects on the first
    /// call. With `client_tls`, the upstream is trusted with that CA, and a
    /// client certificate is presented for mTLS.
    ///
    /// # Errors
    ///
    /// A bad upstream address, a token that is not ASCII, or an https
    /// upstream without a CA to trust.
    pub fn new(
        store: S,
        upstream: &str,
        upstream_token: &str,
        timeout: Duration,
        client_tls: Option<&ClientTls>,
        ticket_keys: Vec<ed25519_dalek::VerifyingKey>,
    ) -> anyhow::Result<Self> {
        // A tier that verifies tickets verifies them on every call: an
        // empty key list would silently turn the tier into a relay that
        // serves whatever its store holds, to any caller that names an
        // object ID. The relay mode is the daemon's explicit choice
        // (`new_relay`), never inferred from an empty list.
        anyhow::ensure!(
            !ticket_keys.is_empty(),
            "a ticket-verifying tier needs at least one public key (the hex \
             that `scion-server ticket-key` printed)"
        );
        Self::build(
            store,
            upstream,
            upstream_token,
            timeout,
            client_tls,
            Tickets::Verify(ticket_keys),
        )
    }

    /// The daemon's tier: it relays the ticket its caller presents,
    /// unverified, and its upstream does the checking (the daemon's
    /// socket is 0600 in a private directory, so the caller is the one
    /// OS user that owns the socket).
    ///
    /// # Errors
    ///
    /// A bad upstream address, or a client TLS that cannot load.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new_relay(
        store: S,
        upstream: &str,
        upstream_token: &str,
        timeout: Duration,
        client_tls: Option<&ClientTls>,
    ) -> anyhow::Result<Self> {
        Self::build(
            store,
            upstream,
            upstream_token,
            timeout,
            client_tls,
            Tickets::Relay,
        )
    }

    fn build(
        store: S,
        upstream: &str,
        upstream_token: &str,
        timeout: Duration,
        client_tls: Option<&ClientTls>,
        tickets: Tickets,
    ) -> anyhow::Result<Self> {
        let endpoint = Endpoint::from_shared(upstream.to_owned())
            .with_context(|| format!("bad upstream address {upstream:?}"))?;
        let endpoint = if let Some(client_tls) = client_tls {
            client_tls.apply_to(upstream, endpoint)?
        } else {
            anyhow::ensure!(
                !scion_proto::runtime::tls::is_https(upstream),
                "the upstream {upstream} is https, but no CA is given to trust its \
                 certificate; pass --tls-ca"
            );
            endpoint
        };
        // The mirror of the server's own keepalive (listen.rs): a peer
        // that died without a FIN or RST is noticed in about
        // `CLIENT_KEEPALIVE_INTERVAL` plus its timeout, not at the tier's
        // upstream timeout of a whole call.
        let (interval, answer) = scion_proto::runtime::listen::client_keepalive();
        let channel = endpoint
            .http2_keep_alive_interval(interval)
            .keep_alive_timeout(answer)
            // An idle channel must notice a dead peer too: the next call
            // answers at once, not after the upstream window.
            .keep_alive_while_idle(true)
            .tcp_keepalive(Some(scion_proto::runtime::listen::client_tcp_keepalive()))
            .connect_lazy();
        let bearer = if upstream_token.is_empty() {
            None
        } else {
            Some(Bearer::new(upstream_token).context("upstream token is not valid ASCII")?)
        };
        let upstream_calls = Arc::default();
        let auth = UpstreamAuth {
            bearer,
            calls: Arc::clone(&upstream_calls),
        };
        let upstream = ObjectsClient::with_interceptor(channel, auth)
            .max_decoding_message_size(MAX_PART_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                upstream,
                timeout,
                busy: BUSY_TIMEOUT,
                upstream_calls,
                metrics: Arc::new(scion_proto::runtime::metrics::Metrics::default()),
                in_flight: Arc::new(Semaphore::new(IN_FLIGHT_BYTES)),
                fetch_slots: Arc::new(Semaphore::new(FETCH_SLOTS)),
                fetching: Arc::new(Semaphore::new(FETCH_BYTES)),
                flights: Mutex::default(),
                settled: tokio::sync::Notify::new(),
                tickets,
            }),
        })
    }

    /// The store that this tier serves from.
    #[must_use]
    pub fn store(&self) -> &S {
        &self.inner.store
    }

    /// How many calls this tier has sent upstream.
    #[must_use]
    pub fn upstream_calls(&self) -> u64 {
        self.inner.upstream_calls.load(Ordering::Relaxed)
    }

    /// How many objects this tier has served from its store, and how many
    /// it has fetched from upstream, over the life of the process.
    #[must_use]
    pub fn cache_hits_and_misses(&self) -> (u64, u64) {
        self.inner.metrics.cache_counts()
    }

    /// The registry this tier counts in: `scion-cache` exports it.
    #[must_use]
    pub fn metrics(&self) -> Arc<scion_proto::runtime::metrics::Metrics> {
        Arc::clone(&self.inner.metrics)
    }

    /// Serves the `Objects` calls on `incoming` connections, to callers that
    /// send `token`, within `limits`, until `shutdown` resolves. With `tls`,
    /// every connection is a TLS connection, whose handshake `listen` ends
    /// within its timeout.
    ///
    /// # Errors
    ///
    /// Transport errors.
    pub async fn serve<I, IO>(
        self,
        token: &str,
        incoming: I,
        limits: Limits,
        tls: Option<ServerTls>,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), tonic::transport::Error>
    where
        I: Stream<Item = io::Result<IO>> + Send + 'static,
        IO: scion_proto::runtime::listen::PeerOf
            + AsyncRead
            + AsyncWrite
            + Connected
            + Unpin
            + Send
            + 'static,
    {
        // Outside the token check, so a refused call counts too.
        let metrics = Arc::clone(&self.inner.metrics);
        let objects = scion_proto::runtime::metrics::MetricsLayer::new(metrics.clone()).layer(
            InterceptedService::new(
                ObjectsServer::new(self)
                    .max_decoding_message_size(MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_PART_MESSAGE_BYTES),
                Auth::new(token),
            ),
        );
        // The health service answers `grpc.health.v1.Check` and `Watch` on
        // the same port, without a token: a probe carries none. The store
        // and the upstream channel opened before this call, so the tier
        // starts serving. The tier has no audit log: its readiness
        // watch never flips on its own.
        let (_ready, ready) = tokio::sync::watch::channel(true);
        let (health, shutdown) = listen::health(&["scion.v1.Objects"], shutdown, ready).await;
        let router = |mut server: Server| server.add_service(health).add_service(objects);
        limits
            .serve(
                router,
                incoming,
                tls.map(|tls| tls.config()),
                shutdown,
                metrics,
            )
            .await
    }

    /// Runs store work on a blocking thread. `repo` names the call's repo
    /// in the one line that a failure of this work makes.
    async fn blocking<T: Send + 'static>(
        &self,
        repo: &str,
        f: impl FnOnce(&S) -> io::Result<T> + Send + 'static,
    ) -> Result<T, Status> {
        let repo = repo.to_owned();
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || f(&inner.store))
            .await
            .unwrap_or_else(|e| Err(io::Error::other(e)))
            .map_err(|e| {
                // The detail stays in the log, as on the server.
                tracing::error!(repo = %repo, error = %e, "store failed");
                Status::internal("internal error")
            })
    }

    /// Waits for an upstream call at most the tier's timeout, and not past
    /// `until`.
    async fn within<T>(
        &self,
        until: Until,
        call: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        let wait = until.wait(self.inner.timeout);
        tokio::time::timeout(wait, call).await.unwrap_or_else(|_| {
            Err(Status::unavailable(format!(
                "upstream did not answer within {wait:?}"
            )))
        })
    }

    /// Streams the objects of `ask` into `tx`, starting with `first`, which
    /// [`Objects::get_objects`] found before the reply began. If the whole
    /// upstream call fails for a later object, the reply ends there without
    /// an error: the client asks again for the rest, and gets the failure as
    /// that call's status. So an error in the stream is always about one
    /// object.
    async fn reply(
        self,
        ask: Ask,
        progress: Progress,
        first: reply::Found,
        tx: mpsc::Sender<reply::Sent>,
        slot: reply::StallSlot,
    ) {
        let (tier, ask, progress) = (&self, &ask, &progress);
        let stalled = ask.stalled.clone();
        let produce = |i| async move { split(tier.produce(ask, i, progress).await?).ok() };
        reply::send_objects(
            first,
            1..ask.keys.len(),
            tx,
            reply::ReplyStall::new(STALL_TIMEOUT, stalled, slot),
            reply::READS_IN_FLIGHT,
            produce,
        )
        .await;
        // Per call, so it is DEBUG: the daemon and cache serve a tier's
        // whole traffic through here, and a line per call at INFO was
        // 7.7 GB in 66 h of the soak (#399 Run A). The hit and miss
        // counts are already the metrics' cache_counts, so the log
        // carries nothing the metrics do not.
        tracing::debug!(
            repo = %ask.repo,
            objects = ask.keys.len(),
            hits = progress.hits.load(Ordering::Relaxed),
            misses = progress.misses.load(Ordering::Relaxed),
            "GetObjects"
        );
    }

    /// Finds `ask.keys[i]` for a reply: in the store, or else from upstream.
    /// Takes the object's share of the reply budget before its bytes are read
    /// from the store or copied into the reply.
    // SHORTCUT: each hit is still two blocking hops (size, then read), but
    // the reply keeps READS_IN_FLIGHT of them running ahead of its sends,
    // as on the server, so the hops of many small objects overlap. What
    // stays serialized is the pair of hops within one hit, and a miss's
    // fetch (bounded by FETCH_SLOTS) waits on the upstream call. Upgrade:
    // one hop that sizes and reads together (a store API that returns the
    // object in one call; the section 9 row that names this file).
    async fn produce(&self, ask: &Ask, i: usize, progress: &Progress) -> Option<Found> {
        let (key, _path) = ask.keys[i].clone();
        // A share taken before the object was in hand.
        let mut held = None;
        let mut missed = false;
        // The ticket's walk proves the object before it goes out: over its
        // path from the ticket's root tree, fetching a tree that the store
        // lacks with the same ticket. Anything the walk does not reach
        // never goes out.
        //
        // SHORTCUT: a daemon skips the walk (it checks no ticket, so it has
        // no root tree to walk from). That is safe only while a daemon
        // serves the one OS user that owns its private 0600 socket in its
        // private directory: that user can read the daemon's store files
        // anyway, so serving unproved objects adds nothing. Trigger to
        // upgrade: a daemon that serves several OS users must walk (and
        // then check tickets), like a cache.
        if matches!(self.inner.tickets, Tickets::Verify(_))
            && let Err(failed) = self.prove(i, ask).await
        {
            return Some((Err(failed), self.share(held, 1, ask).await?));
        }
        loop {
            let size = match self.blocking(&ask.repo, move |store| store.size(key)).await {
                Ok(size) => size,
                Err(status) => return Some((Err(status.into()), self.share(held, 1, ask).await?)),
            };
            if let Some(size) = size {
                let mut permit = self
                    .share(held.take(), size + key.read_bytes(size), ask)
                    .await?;
                let stored = self.stored(&ask.repo, key, size).await;
                reply::trim(&mut permit, size);
                match stored {
                    Ok(Some(stored)) => {
                        if !missed {
                            progress.hits.fetch_add(1, Ordering::Relaxed);
                            self.inner.metrics.cache_hit();
                            tracing::debug!(%key, "hit");
                        }
                        // The walk proved it, so the snapshot of the
                        // ticket's repo gains it.
                        self.inner.store.served(&ask.ticket.repo, key);
                        return self.outgoing(key, stored, permit, ask).await;
                    }
                    // The stored copy went (dropped, or bad) after its size
                    // was read: fetch it.
                    Ok(None) => held = Some(permit),
                    Err(status) => return Some((Err(status.into()), permit)),
                }
            }
            if !missed {
                missed = true;
                progress.misses.fetch_add(1, Ordering::Relaxed);
                self.inner.metrics.cache_miss();
                tracing::debug!(%key, "miss");
            }
            let data = match self.fetch(ask, i, progress).await {
                Ok(Some(data)) => data,
                // A fetch stored it a moment ago: read it from the store,
                // with its share, as a hit is.
                Ok(None) => continue,
                Err(failed) => return Some((Err(failed), self.share(held, 1, ask).await?)),
            };
            // The bytes were counted in the fetch budget until stored. Take
            // them into the reply now if the reply budget has room. Else let
            // them go, wait for room, and read them again.
            let permit = match held.take() {
                Some(permit) => self.share(Some(permit), data.len(), ask).await,
                None => reply::try_take(&self.inner.in_flight, data.len()),
            };
            if let Some(permit) = permit {
                // The walk proved it, so the snapshot of the ticket's repo
                // gains it.
                self.inner.store.served(&ask.ticket.repo, key);
                return self.outgoing(key, data, permit, ask).await;
            }
            let size = data.len();
            drop(data);
            held = Some(self.share(None, size + key.read_bytes(size), ask).await?);
        }
    }

    /// Proves `ask.keys[i]` for the reply: the walk from the ticket's root
    /// tree must reach it, over its path, through the trees of the store
    /// checked against their IDs. A tree that the store lacks is fetched
    /// with the ticket first, and the walk runs again. A path that the walk
    /// does not reach fails the object: nothing goes out for it.
    async fn prove(&self, i: usize, ask: &Ask) -> Result<(), Failed> {
        let (key, path) = &ask.keys[i];
        let (kind, id) = (key.kind(), key.id());
        // The trees fetched for this walk, so a store that keeps nothing
        // does not send the walk into an endless fetch of the same tree.
        let mut learned: HashMap<[u8; 32], Vec<u8>> = HashMap::new();
        loop {
            let walk = Arc::clone(&ask.walk);
            let path = path.clone();
            let learned_now = learned.clone();
            let verdict = self
                .blocking(&ask.repo, move |store: &S| {
                    let mut walk = walk.lock().unwrap_or_else(PoisonError::into_inner);
                    let mut reader = |tree_id: [u8; 32]| {
                        if let Some(bytes) = learned_now.get(&tree_id) {
                            return Ok(Some(bytes.clone()));
                        }
                        let key = Key::Tree(TreeId::from_bytes(tree_id));
                        match store.size(key) {
                            Ok(Some(size)) => Ok(store.get(key, size)?.map(|bytes| bytes.to_vec())),
                            Ok(None) => Ok(None),
                            Err(why) => Err(why),
                        }
                    };
                    walk.reach(&mut reader, kind, &id, &path)
                })
                .await;
            match verdict {
                Ok(Walk::Reached) => return Ok(()),
                Ok(Walk::Missing { id, path }) => {
                    // The tree on the way is not in the store: fetch it
                    // with the ticket, then walk again. Its bytes are kept
                    // here whether the store keeps them or not.
                    let tree = Key::Tree(TreeId::from_bytes(id));
                    if let Some(bytes) = self.fetch_for(tree, &path, Vec::new(), ask).await? {
                        learned.insert(id, bytes.to_vec());
                    }
                }
                Ok(Walk::Absent) => {
                    // Expected when a client asks outside its checkout,
                    // so it stays at debug.
                    tracing::debug!(
                        repo = %ask.ticket.repo,
                        principal = %ask.ticket.principal,
                        object = ?key,
                        "the walk did not reach the object from the ticket's root tree"
                    );
                    return Err(Failed {
                        status: Status::not_found(format!("{key:?} is not in the ticket")),
                        whole_call: false,
                        until: ask.until,
                    });
                }
                Err(why) => {
                    return Err(Failed {
                        status: Status::internal(format!("cannot read a tree of the walk: {why}")),
                        whole_call: false,
                        until: ask.until,
                    });
                }
            }
        }
    }

    /// `held` grown to `bytes` of the reply budget, or a new share of
    /// `bytes`. Waits at most [`BUSY_TIMEOUT`], and not past `ask.until`;
    /// `None` if the room does not come.
    async fn share(
        &self,
        held: Option<OwnedSemaphorePermit>,
        bytes: usize,
        ask: &Ask,
    ) -> Option<OwnedSemaphorePermit> {
        match held {
            Some(mut permit) => {
                self.cover(&mut permit, bytes, ask.until).await?;
                Some(permit)
            }
            None => {
                reply::take(
                    &self.inner.in_flight,
                    bytes,
                    ask.until.wait(self.inner.busy),
                )
                .await
            }
        }
    }

    /// `key` for a reply, from its stored form, in an encoding that the
    /// client accepts. Grows `permit`, the object's share of the reply
    /// budget, to cover what the reply holds. For a client that does not
    /// accept `zstd`, a blob is unpacked as a stream into chunks of its own,
    /// and then only the chunks are held.
    async fn outgoing(
        &self,
        key: Key,
        stored: Bytes,
        mut permit: OwnedSemaphorePermit,
        ask: &Ask,
    ) -> Option<Found> {
        let header = match key.stored_header(&stored) {
            Ok(header) => header,
            Err(why) => {
                tracing::error!(%key, %why, "a stored object has a bad header");
                return Some((Err(Status::internal("internal error").into()), permit));
            }
        };
        if header.encoding == Encoding::Raw || ask.zstd {
            self.cover(&mut permit, stored.len(), ask.until).await?;
            reply::trim(&mut permit, stored.len());
            let body = Body::Whole(stored);
            return Some((Ok(Outgoing { header, body }), permit));
        }
        let size = header.size;
        self.cover(&mut permit, stored.len() + size + UNPACK_BYTES, ask.until)
            .await?;
        let chunks = self
            .blocking(&ask.repo, move |_| {
                object::read_chunks(object::unpacker(&stored)?, size)
            })
            .await;
        reply::trim(&mut permit, size);
        let header = Header {
            encoding: Encoding::Raw,
            encoded_size: size,
            ..header
        };
        let object = chunks.map(|chunks| Outgoing {
            header,
            body: Body::Chunks(chunks),
        });
        Some((object.map_err(Failed::from), permit))
    }

    /// Grows `permit` to `bytes` of the reply budget, if it holds less.
    /// Waits at most [`BUSY_TIMEOUT`], and not past `until`; `None` if the
    /// room does not come.
    async fn cover(
        &self,
        permit: &mut OwnedSemaphorePermit,
        bytes: usize,
        until: Until,
    ) -> Option<()> {
        let held = permit.num_permits();
        if bytes > held {
            let wait = until.wait(self.inner.busy);
            let more = reply::take(&self.inner.in_flight, bytes - held, wait).await?;
            permit.merge(more);
        }
        Some(())
    }

    /// Reads `key` in its stored form from the store, at most `size` bytes.
    /// `None` if it is not there.
    async fn stored(&self, repo: &str, key: Key, size: usize) -> Result<Option<Bytes>, Status> {
        self.blocking(repo, move |store| store.get(key, size)).await
    }

    /// Gets `ask.keys[i]` in its stored form from upstream. Joins the fetch
    /// in progress for it, or starts one. A fetch that a reply starts also
    /// takes the later keys, up to [`MAX_BATCH`] in all, that the store
    /// lacks. A key that an earlier fetch of the same reply covered, but the
    /// store did not keep, is fetched alone, so such a store costs one fetch
    /// per object, not one per remaining call. A fetch has the deadline of
    /// the caller that started it.
    ///
    /// The wait is bounded: each step of a fetch has the tier's timeout and
    /// the fetch's deadline, and a fetch always settles every object it
    /// holds. A caller waits no longer than its own deadline. Then it stops,
    /// and the fetch goes on for the others. If the fetch failed as a whole
    /// and its deadline came first, the caller asks again with its own: the
    /// failure may have come from the shorter deadline.
    ///
    /// `None` if a fetch that ended after the caller found no stored copy has
    /// stored the object: the caller reads it from the store, with its
    /// share, and does not fetch it again.
    async fn fetch(
        &self,
        ask: &Ask,
        i: usize,
        progress: &Progress,
    ) -> Result<Option<Bytes>, Failed> {
        let keys = &ask.keys;
        let (key, path) = keys[i].clone();
        let later = if i < progress.fetched_to.load(Ordering::Relaxed) {
            Vec::new()
        } else {
            #[cfg(test)]
            tests::claim_gate(i).await;
            self.claim(ask, i, progress)
        };
        self.fetch_for(key, &path, later, ask).await
    }

    /// Claims the keys after `i` for the fetch that this reply starts, and
    /// registers them in the flights table, under the one lock: a read
    /// ahead that misses cannot start its own fetch before this batch has
    /// claimed the keys after it. The batches tile the ask from
    /// `fetched_to`, the end of this reply's last batch, which the claim
    /// writes under this lock. The table alone cannot say where the last
    /// batch began: a read that reaches the lock first claims from its
    /// own index, which can leave the key of a read just before it
    /// outside every flight, and a finished flight has taken its keys out
    /// of the table. A read within one batch of that end starts its batch
    /// there, so the read holding a key that a racing claim or a finished
    /// flight left out joins the batch that covers it instead of starting
    /// a flight of one. The count of upstream calls is then the same in
    /// every schedule: one claim per batch, and the reads ahead join it.
    ///
    /// The store's checks for the window the claim may cover run before
    /// the lock: they are sync file I/O, and under a shared std Mutex
    /// they would block every other claim on an async worker, and a slow
    /// disk or a network mount would stall the worker (the same class as
    /// #795). A key that lands in the store between a check and the lock
    /// gets fetched once more, which is harmless: the put is idempotent.
    ///
    /// Returns the keys along, which the caller passes to [`Self::
    /// fetch_for`] only to join the flight `claim` already started.
    fn claim(&self, ask: &Ask, i: usize, progress: &Progress) -> Vec<(Key, Vec<u8>)> {
        let keys = &ask.keys;
        let (key, _) = &keys[i];
        let stored = |k: &Key| matches!(self.inner.store.size(*k), Ok(Some(_)));
        if stored(key) {
            return Vec::new();
        }
        // The window the claim may cover: from one batch behind `i` - a
        // batch tiles from `fetched_to`, which is at least `i` - 127 here -
        // to one batch ahead. One store call for the whole window: a sync
        // size check per key would be up to 255 round trips to the store
        // on this worker.
        let from = i.saturating_sub(MAX_BATCH - 1);
        let to = keys.len().min(i + MAX_BATCH);
        let window: Vec<Key> = keys[from..to].iter().map(|(key, _)| *key).collect();
        let has: Vec<bool> = self
            .inner
            .store
            .sizes(&window)
            .into_iter()
            .map(|stored| matches!(stored, Ok(Some(_))))
            .collect();
        let mut flights = lock(&self.inner.flights);
        if flights.contains_key(key) {
            // Another reply's fetch got this key: join, and fetch nothing
            // along.
            return Vec::new();
        }
        let fetched = progress.fetched_to.load(Ordering::Relaxed);
        // The else branch: a claim a batch ahead of the boundary. It cannot
        // happen today - a reply reads at most [`READS_IN_FLIGHT`] keys at
        // once, in order, so no claim is ever a batch behind the boundary
        // (the const assertion above makes that loud) - but if it did,
        // starting at `i` is the safe choice, and `fetch_max` below keeps
        // the boundary from following it down.
        let start = if fetched <= i && i - fetched < MAX_BATCH {
            fetched
        } else {
            i
        };
        let end = keys.len().min(start + MAX_BATCH);
        // Another claim can have moved the boundary past this read between
        // its caller's check and this lock; the boundary only grows.
        progress.fetched_to.fetch_max(end, Ordering::Relaxed);
        let later: Vec<_> = keys[i + 1..end].to_vec();
        let mut waiting = VecDeque::new();
        for j in start..end {
            if has[j - from] {
                continue;
            }
            let (k, p) = keys[j].clone();
            if let Entry::Vacant(slot) = flights.entry(k) {
                let (tx, rx) = watch::channel(None);
                slot.insert(rx);
                waiting.push_back((k, p, tx));
            }
        }
        drop(flights);
        let flight = Flight {
            tier: self.clone(),
            repo: ask.ticket.repo.clone(),
            ticket: ask.ticket.clone(),
            waiting,
            until: ask.until,
            request_id: ask.request_id.clone(),
        };
        tokio::spawn(flight.run());
        later
    }

    /// Gets `key` in its stored form from upstream, with the path that its
    /// ref names and the ticket of the call, taking `later` along. The walk
    /// of the ticket's root tree names a tree that the store lacks, so the
    /// fetch runs before the walk can finish.
    async fn fetch_for(
        &self,
        key: Key,
        path: &[u8],
        later: Vec<(Key, Vec<u8>)>,
        ask: &Ask,
    ) -> Result<Option<Bytes>, Failed> {
        let until = ask.until;
        loop {
            let joined = lock(&self.inner.flights).get(&key).cloned();
            let flight = if let Some(flight) = joined {
                Some(flight)
            } else {
                self.start(
                    key,
                    path,
                    later.clone(),
                    until,
                    &ask.ticket,
                    ask.request_id.clone(),
                )
            };
            let Some(mut flight) = flight else {
                return Ok(None);
            };
            let settled = flight.wait_for(Option::is_some);
            let Ok(settled) = tokio::time::timeout(until.wait(Duration::MAX), settled).await else {
                return Err(Failed {
                    status: Status::deadline_exceeded(
                        "the deadline passed while the call waited for a fetch from upstream",
                    ),
                    whole_call: true,
                    until,
                });
            };
            match settled.ok().and_then(|outcome| outcome.clone()) {
                None => {
                    return Err(
                        Status::internal("a fetch from upstream ended without a result").into(),
                    );
                }
                Some(Err(failed))
                    if failed.whole_call && failed.until.before(until) && !until.passed() =>
                {
                    tracing::debug!(%key, status = %failed.status, "a fetch with an earlier deadline failed; asking again");
                }
                Some(outcome) => return outcome.map(Some),
            }
        }
    }

    /// Starts one fetch for `key` and `later`, minus the objects that another
    /// fetch already has or the store holds. Returns the receiver for `key`,
    /// or `None` if the store holds it.
    ///
    /// The store is asked under the fetch table's lock. A fetch stores each
    /// object before it takes the object out of the table, so an object in
    /// neither has not been fetched yet, and only one fetch starts for it.
    fn start(
        &self,
        key: Key,
        path: &[u8],
        later: Vec<(Key, Vec<u8>)>,
        until: Until,
        ticket: &scion_proto::ticket::Ticket,
        request_id: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    ) -> Option<watch::Receiver<Outcome>> {
        let stored = |k| matches!(self.inner.store.size(k), Ok(Some(_)));
        let mut flights = lock(&self.inner.flights);
        if let Some(rx) = flights.get(&key) {
            return Some(rx.clone());
        }
        if stored(key) {
            return None;
        }
        let mut waiting = VecDeque::new();
        for (k, p) in iter::once((key, path.to_vec())).chain(later) {
            if let Entry::Vacant(slot) = flights.entry(k)
                && (k == key || !stored(k))
            {
                let (tx, rx) = watch::channel(None);
                slot.insert(rx);
                waiting.push_back((k, p, tx));
            }
        }
        let rx = flights[&key].clone();
        drop(flights);
        let flight = Flight {
            tier: self.clone(),
            repo: ticket.repo.clone(),
            ticket: ticket.clone(),
            waiting,
            until,
            request_id,
        };
        tokio::spawn(flight.run());
        Some(rx)
    }
}

/// One fetch from upstream: a few objects, which other replies can wait
/// for. It runs on its own task, so it never waits for a reply, and it ends
/// when every object is fetched or failed. Objects still waiting when it
/// ends leave the fetch table, so a later call starts anew.
struct Flight<S: Store> {
    tier: Tier<S>,
    repo: String,
    /// The ticket that names every object of this fetch to upstream.
    ticket: scion_proto::ticket::Ticket,
    /// The objects not fetched yet, in request order, each with the path
    /// that its ref names and the sender that its waiters listen to.
    waiting: VecDeque<(Key, Vec<u8>, watch::Sender<Outcome>)>,
    /// When the fetch ends its waits: the deadline of the caller that
    /// started it, less the margin.
    until: Until,
    /// The request ID of the call that started the fetch. It rides on the
    /// upstream request, so one operation names itself in the logs of
    /// every hop.
    request_id: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}

/// The next object of an upstream reply.
enum Next {
    /// The reply ended between objects.
    End,
    /// The object at the front of the fetch, still encoded, with its share
    /// of the fetch budget.
    Object(Key, Header, Vec<u8>, OwnedSemaphorePermit),
    /// The object at the front of the fetch failed: an error about it, or
    /// parts that cannot be it. The rest of the reply is not read.
    Failed(Status),
}

impl<S: Store> Flight<S> {
    async fn run(mut self) {
        tracing::debug!(objects = self.waiting.len(), "fetching from upstream");
        if let Err(status) = self.fetch_all().await {
            tracing::warn!(
                repo = %self.repo,
                %status,
                objects = self.waiting.len(),
                "fetch from upstream failed"
            );
            let failed = Failed {
                status,
                whole_call: true,
                until: self.until,
            };
            for (_, _, tx) in &self.waiting {
                tx.send_replace(Some(Err(failed.clone())));
            }
        }
    }

    /// Hands the first waiting object, or why upstream could not give that
    /// one object, to its waiters. The other objects keep waiting.
    fn settle_front(&mut self, outcome: Result<Bytes, Status>) {
        if let Some((key, _, tx)) = self.waiting.pop_front() {
            if let Err(status) = &outcome {
                tracing::warn!(repo = %self.repo, %key, %status, "upstream could not give an object");
            }
            tx.send_replace(Some(outcome.map_err(Failed::from)));
            lock(&self.tier.inner.flights).remove(&key);
            self.tier.inner.settled.notify_waiters();
        }
    }

    /// Fetches every waiting object. Checks each against its ID, stores it,
    /// and hands it to its waiters. An error inside upstream's reply about
    /// one object, or bytes that cannot be that object, fail only that
    /// object. An upstream reply may stop early; the rest are asked for
    /// again. An error status of the call itself, a transport error, a
    /// timeout or an object out of order fails every object still waiting.
    async fn fetch_all(&mut self) -> Result<(), Status> {
        // Bounded as a reply's wait for memory is. Then every object of the
        // fetch fails with `UNAVAILABLE`, and the callers ask again.
        let Inner {
            fetch_slots, busy, ..
        } = &*self.tier.inner;
        let slot = Arc::clone(fetch_slots).acquire_owned();
        let wait = self.until.wait(*busy);
        let _slot = match tokio::time::timeout(wait, slot).await {
            Ok(Ok(slot)) => slot,
            Ok(Err(_)) => return Err(Status::unavailable("the tier is shutting down")),
            Err(_) => {
                return Err(Status::unavailable(format!(
                    "no fetch slot within {wait:?}; ask again"
                )));
            }
        };
        while !self.waiting.is_empty() {
            if self.until.passed() {
                return Err(Status::deadline_exceeded(
                    "the deadline passed before the fetch could ask upstream",
                ));
            }
            let refs = self
                .waiting
                .iter()
                .map(|(key, path, _)| key.to_ref(path))
                .collect();
            // Upstream gets what is left of the deadline, and the call's
            // request ID, so its logs name the same operation.
            let mut request = self.until.request(GetObjectsRequest {
                refs,
                accept: vec![Encoding::Zstd.into()],
                ticket: self.ticket.bytes.clone(),
            });
            if let Some(id) = &self.request_id {
                request
                    .metadata_mut()
                    .insert(scion_proto::runtime::request_id::REQUEST_ID, id.clone());
            }
            let mut upstream = self.tier.inner.upstream.clone();
            let mut stream = self
                .tier
                .within(self.until, upstream.get_objects(request))
                .await?
                .into_inner();
            let before = self.waiting.len();
            loop {
                let (key, header, data, permit) = match self.next(&mut stream).await? {
                    Next::End => break,
                    Next::Failed(status) => {
                        self.settle_front(Err(status));
                        break;
                    }
                    Next::Object(key, header, data, permit) => (key, header, data, permit),
                };
                // Unpacking and hashing up to 100 MiB takes a while: check
                // and store on a blocking thread.
                let repo = self.repo.clone();
                let checked = self
                    .tier
                    .blocking(&self.repo, move |store| {
                        let stored = match key.to_stored(&header, data) {
                            Ok(stored) => Bytes::from(stored),
                            Err(why) => return Ok(Err(why)),
                        };
                        // A failed put is logged; the object still goes to
                        // its waiters.
                        if let Err(e) = store.put(key, &stored) {
                            tracing::warn!(%repo, %key, error = %e, "cannot store an object");
                        }
                        Ok(Ok(stored))
                    })
                    .await?;
                self.settle_front(checked.map_err(|why| {
                    Status::data_loss(format!("upstream sent bytes that are not {key}: {why}"))
                }));
                drop(permit);
            }
            if self.waiting.len() == before {
                return Err(Status::unavailable(
                    "upstream sent none of the requested objects",
                ));
            }
        }
        Ok(())
    }

    /// Reads the next object of an upstream reply, part by part. It must be
    /// the object at the front of the fetch. Takes its share of the fetch
    /// budget from its header, before the rest of its parts arrive.
    async fn next(&self, stream: &mut Streaming<ObjectPart>) -> Result<Next, Status> {
        let first = match self.tier.within(self.until, stream.message()).await {
            Ok(Some(part)) => part,
            Ok(None) => return Ok(Next::End),
            // Upstream ends its reply after an error about one object.
            Err(status) if about_one_object(&status) => return Ok(Next::Failed(status)),
            Err(status) => return Err(status),
        };
        let Some(&(key, _, _)) = self.waiting.front().filter(|(key, ..)| key.names(&first)) else {
            return Err(Status::internal(
                "upstream sent an object that was not asked for",
            ));
        };
        let bad = |why: String| {
            Ok(Next::Failed(Status::data_loss(format!(
                "upstream sent bytes that are not {key}: {why}"
            ))))
        };
        let header = match Header::of(&first, true) {
            Ok(header) => header,
            Err(why) => return bad(why),
        };
        // The upstream reply waits meanwhile, so the wait is bounded as a
        // reply's is. Then the fetch ends, and every object it holds fails
        // with `UNAVAILABLE`: the callers ask again.
        let Inner { fetching, busy, .. } = &*self.tier.inner;
        let wait = self.until.wait(*busy);
        let permit = reply::take(fetching, key.check_bytes(&header), wait)
            .await
            .ok_or_else(|| {
                Status::unavailable(format!(
                    "no memory within {wait:?} for an object from upstream; ask again"
                ))
            })?;
        let mut object = Assembler::new(header, first);
        while !object.is_done() {
            let part = match self.tier.within(self.until, stream.message()).await {
                Ok(Some(part)) => part,
                Ok(None) => return bad("the reply ended in the middle of the object".into()),
                Err(status) if about_one_object(&status) => return Ok(Next::Failed(status)),
                Err(status) => return Err(status),
            };
            if let Err(why) = object.push(&part) {
                return bad(why);
            }
        }
        let (header, data) = object.finish();
        Ok(Next::Object(key, header, data, permit))
    }
}

impl<S: Store> Drop for Flight<S> {
    fn drop(&mut self) {
        let mut flights = lock(&self.tier.inner.flights);
        for (key, ..) in &self.waiting {
            flights.remove(key);
        }
        self.tier.inner.settled.notify_waiters();
    }
}

/// Whether an upstream error in the stream is about one object, not the
/// whole call. A transport error also ends a stream, with other codes, so
/// only these codes count as one object.
fn about_one_object(status: &Status) -> bool {
    matches!(status.code(), Code::NotFound | Code::DataLoss)
}

/// The `Objects` service of a tier: `GetObjects` of a ticket, and nothing
/// else.
#[tonic::async_trait]
impl<S: Store> Objects for Tier<S> {
    /// No refs here either: a caller resolves on the server, then brings
    /// the ticket of its root tree to `GetObjects`.
    async fn resolve_ref(
        &self,
        _: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        Err(Status::unimplemented(
            "ask the server for the ref and its ticket",
        ))
    }

    /// No root trees here: a caller gets its ticket from the server, and
    /// only `GetObjects` of a ticket comes to a tier. A tier that fetched
    /// root trees for callers would be a way to mint a ticket for any repo.
    async fn get_root_tree(
        &self,
        _: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
        Err(Status::unimplemented(
            "ask the server for the root tree and its ticket",
        ))
    }

    type GetObjectsStream = ObjectStream;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<ObjectStream>, Status> {
        let until = Until::of(&request);
        let stalled = scion_proto::runtime::listen::Stalled::of(&request);
        // The caller's request ID, if it sent one that fits; else one this
        // process makes. The fetch to upstream carries it.
        let request_id = scion_proto::runtime::request_id::sanitize(request_id_bytes(&request))
            .and_then(|id| id.parse().ok());
        let GetObjectsRequest {
            refs,
            accept,
            ticket,
        } = request.into_inner();
        check::object_count(refs.len())?;
        // The ticket is the only permission of an object read here too:
        // on a cache it is verified against the keys of the upstream
        // server, and the walk from its root tree proves every object
        // before it leaves. On a daemon, which only forwards tickets, the
        // ticket is read without its signature, and the proof is skipped:
        // a daemon with no ticket keys serves what its store holds, to the
        // one OS user that owns its private socket (see the SHORTCUT at
        // `produce`).
        let ticket = match &self.inner.tickets {
            Tickets::Relay => {
                scion_proto::ticket::parse_unverified(&ticket, scion_proto::ticket::now())
            }
            Tickets::Verify(keys) => {
                scion_proto::ticket::parse(&ticket, keys, scion_proto::ticket::now())
            }
        }
        .map_err(|why| {
            // The reason only: no ticket bytes, and no signature.
            tracing::warn!(reason = %why, "a GetObjects ticket was refused");
            Status::permission_denied(format!(
                "bad ticket: {why} (the tier's --ticket-public-key must hold a \
                 key the server signs with; operations.md section 2's rotation \
                 keeps the old key alive during a change)"
            ))
        })?;
        let keys = refs
            .into_iter()
            .map(|r| Ok((Key::parse(&r)?, r.path)))
            .collect::<Result<Vec<_>, Status>>()?;
        let ask = Ask {
            repo: ticket.repo.clone(),
            walk: Arc::new(Mutex::new(scion_proto::ticket::Walker::new(
                ticket.root_tree,
            ))),
            ticket,
            keys,
            zstd: accept.contains(&Encoding::Zstd.into()),
            until,
            request_id,
            stalled,
        };
        // The first object is found before the reply starts, as on the
        // server. So when the whole upstream call fails (for example,
        // upstream has no such repo), that is this call's status too, and a
        // tier below fails its fetch once instead of asking again per object.
        let progress = Progress::default();
        let first = self
            .produce(&ask, 0, &progress)
            .await
            .ok_or_else(reply::busy)?;
        let first = split(first)?;
        let (tx, stream, slot) = reply::channel();
        // The reply runs outside the call, but stays in its span: the
        // GetObjects line carries the call's rpc, peer and request_id, the
        // way the server's call lines do.
        let span = tracing::Span::current();
        tokio::spawn(
            self.clone()
                .reply(ask, progress, first, tx, slot)
                .instrument(span),
        );
        Ok(Response::new(stream))
    }
}

/// A temp directory the ancestor check accepts whatever the umask of the
/// run: this run's own, mode 0700. A store's or a snapshot's parent that
/// group or others can write is refused, as it must be.
#[cfg(test)]
fn private_tempdir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

#[cfg(test)]
mod tests {

    /// The claim gate a test arms to hold one read's claim until another
    /// has claimed, forcing a schedule the scheduler picks rarely. The
    /// armed index waits until the permit is set; no other read notices.
    static CLAIM_GATE_ARMED: AtomicUsize = AtomicUsize::new(usize::MAX);
    static CLAIM_GATE_PERMIT: AtomicU64 = AtomicU64::new(0);

    /// Called by [`Tier::fetch`] on the claim path in tests only.
    pub(super) async fn claim_gate(i: usize) {
        if CLAIM_GATE_ARMED.load(Ordering::SeqCst) == i {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            while CLAIM_GATE_PERMIT.load(Ordering::SeqCst) == 0 {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the gated claim was never let through"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    /// Waits until no fetch is in flight, so a test asserts on state, not on
    /// timing. The tier notifies when a flight settles; the ceiling bounds a
    /// missed wake or a stalled fetch at 10 s.
    async fn settled<S: Store>(tier: &Tier<S>) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let notified = tier.inner.settled.notified();
            if lock(&tier.inner.flights).is_empty() {
                return;
            }
            tokio::select! {
                () = notified => {}
                () = tokio::time::sleep_until(deadline) => {
                    panic!("the flights never settled: {:?}", lock(&tier.inner.flights).keys().collect::<Vec<_>>())
                }
            }
        }
    }

    use std::pin::Pin;

    use tokio::net::TcpListener;
    use tokio_stream::StreamExt as _;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;
    use scion_core::{Entry, FileEntry, FileState, PathElem};

    /// What the fake upstream keeps for one object: its kind with its raw
    /// bytes.
    type Raw = (ObjectKind, Vec<u8>);

    /// An upstream that serves fixed blobs RAW, and counts the objects asked
    /// for and the most in one call.
    #[derive(Clone)]
    struct Fake {
        objects: Arc<HashMap<Vec<u8>, Raw>>,
        key: ed25519_dalek::SigningKey,
        root: TreeId,
        asked: Arc<AtomicUsize>,
        largest: Arc<AtomicUsize>,
        calls: Arc<std::sync::Mutex<Vec<Vec<Vec<u8>>>>>,
    }

    type Replies = Pin<Box<dyn Stream<Item = Result<ObjectPart, Status>> + Send>>;

    #[tonic::async_trait]
    impl Objects for Fake {
        async fn resolve_ref(
            &self,
            _: Request<ResolveRefRequest>,
        ) -> Result<Response<ResolveRefResponse>, Status> {
            Err(Status::unimplemented("no refs"))
        }

        async fn get_root_tree(
            &self,
            request: Request<GetRootTreeRequest>,
        ) -> Result<Response<GetRootTreeResponse>, Status> {
            // A ticket for the repo of the request, for the one root tree.
            let repo = request.into_inner().repo;
            let ticket = scion_proto::ticket::sign(
                &self.key,
                &repo,
                "test",
                *self.root.as_bytes(),
                scion_proto::ticket::now(),
            )
            .unwrap();
            Ok(Response::new(GetRootTreeResponse {
                tree_id: self.root.as_bytes().to_vec(),
                ticket,
            }))
        }

        type GetObjectsStream = Replies;

        async fn get_objects(
            &self,
            request: Request<GetObjectsRequest>,
        ) -> Result<Response<Replies>, Status> {
            let refs = request.into_inner().refs;
            self.asked.fetch_add(refs.len(), Ordering::SeqCst);
            self.largest.fetch_max(refs.len(), Ordering::SeqCst);
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(refs.iter().map(|r| r.id.clone()).collect());
            let replies: Vec<_> = refs
                .into_iter()
                .flat_map(|r| {
                    let (kind, data) = self.objects[&r.id].clone();
                    let header = Header {
                        kind,
                        id: r.id.try_into().unwrap(),
                        encoding: Encoding::Raw,
                        encoded_size: data.len(),
                        size: data.len(),
                    };
                    header.parts(object::read_chunks(&data[..], data.len()).unwrap())
                })
                .map(Ok)
                .collect();
            Ok(Response::new(Box::pin(tokio_stream::iter(replies))))
        }
    }

    /// `count` small blobs, their keys, an upstream that serves them, and a
    /// tier in front of it with a store of `limit` bytes.
    async fn setup(count: u32, limit: usize) -> Setup {
        let blobs: Vec<Vec<u8>> = (0..count).map(|i| i.to_le_bytes().to_vec()).collect();
        let keys: Vec<Key> = blobs.iter().map(|b| Key::Blob(BlobId::of(b))).collect();
        // A real root tree, so the walk of a ticket reaches every blob
        // at its index.
        let paths: Vec<Vec<u8>> = (0..count).map(|i| i.to_string().into_bytes()).collect();
        let tree = Tree::new(
            keys.iter()
                .zip(&paths)
                .map(|(key, path)| {
                    (
                        PathElem::new(path.clone()).unwrap(),
                        Entry::File(FileEntry::new(
                            FileState::Regular {
                                content: BlobId::try_from(key.id().as_slice()).unwrap(),
                                executable: false,
                            },
                            0,
                        )),
                    )
                })
                .collect(),
        )
        .expect("tree fits the format");
        let (tree_bytes, root) = tree.encode_with_id();
        let key = ed25519_dalek::SigningKey::from_bytes(&[9; 32]);
        let mut objects: HashMap<_, _> = keys
            .iter()
            .zip(&blobs)
            .map(|(k, blob)| (k.id().to_vec(), (ObjectKind::Blob, blob.clone())))
            .collect();
        objects.insert(
            root.as_bytes().to_vec(),
            (ObjectKind::Tree, tree_bytes.clone()),
        );
        let fake = Fake {
            objects: Arc::new(objects),
            key: key.clone(),
            root,
            asked: Arc::default(),
            largest: Arc::default(),
            calls: Arc::default(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(fake.clone()))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let store = MemoryStore::new(limit).recording(&["r".into(), "q".into()]);
        let tier = Tier::new(
            store,
            &url,
            "token",
            CACHE_UPSTREAM_TIMEOUT,
            None,
            vec![key.verifying_key()],
        )
        .unwrap();
        let ticket = scion_proto::ticket::sign(
            &key,
            "r",
            "test",
            *root.as_bytes(),
            scion_proto::ticket::now(),
        )
        .unwrap();
        Setup {
            blobs,
            keys,
            paths,
            fake,
            tier,
            key,
            root,
            ticket,
        }
    }

    /// The fixture of the tests: blobs, their keys and paths, an upstream
    /// that serves them, a tier that trusts its key, and the root tree.
    struct Setup {
        blobs: Vec<Vec<u8>>,
        keys: Vec<Key>,
        paths: Vec<Vec<u8>>,
        fake: Fake,
        tier: Tier<MemoryStore>,
        key: ed25519_dalek::SigningKey,
        root: TreeId,
        ticket: Vec<u8>,
    }

    /// What one reply asks for: `keys` with the paths of the fixture, and
    /// a ticket of `repo` for the root tree.
    fn ask(setup: &Setup, keys: &[Key], repo: &str) -> Ask {
        let ticket = scion_proto::ticket::sign(
            &setup.key,
            repo,
            "test",
            *setup.root.as_bytes(),
            scion_proto::ticket::now(),
        )
        .unwrap();
        let ticket = scion_proto::ticket::parse(
            &ticket,
            &[setup.key.verifying_key()],
            scion_proto::ticket::now(),
        )
        .unwrap();
        let keys = keys
            .iter()
            .map(|key| {
                let i = setup.keys.iter().position(|k| k == key).unwrap();
                (*key, setup.paths[i].clone())
            })
            .collect();
        Ask {
            repo: repo.to_owned(),
            walk: Arc::new(Mutex::new(scion_proto::ticket::Walker::new(
                *setup.root.as_bytes(),
            ))),
            ticket,
            keys,
            zstd: false,
            until: Until::NONE,
            request_id: None,
            stalled: None,
        }
    }

    /// The two RPCs a tier must never answer: a ref and a root tree.
    /// A tier that fetched root trees would mint a ticket for any repo,
    /// and a ref would move the server's resolution onto the tier. The
    /// whole-body stub mutants answer Ok with an empty response; both
    /// must stay Unimplemented with the pointer to the server.
    /// A key checks the bytes that name it: another object's bytes are
    /// refused, and a blob round-trips through its stored form.
    #[test]
    fn a_key_refuses_another_objects_bytes_and_round_trips_its_own() {
        use scion_core::{BlobId, TreeId};

        let mine = b"the object that is mine".to_vec();
        let key = Key::Blob(BlobId::of(&mine));
        let packed = object::pack(&mine).unwrap();
        // Its own stored form checks, and to_stored keeps the packed bytes.
        key.check_stored(&packed).expect("its own bytes check");
        let header = Header {
            kind: ObjectKind::Blob,
            id: *BlobId::of(&mine).as_bytes(),
            encoding: Encoding::Zstd,
            encoded_size: packed.len(),
            size: mine.len(),
        };
        let stored = key.to_stored(&header, packed.clone()).unwrap();
        assert_eq!(stored, packed, "a zstd blob is stored as it came");

        // Another object's bytes are refused, in both directions.
        let other = b"the object that is not mine".to_vec();
        let other_packed = object::pack(&other).unwrap();
        assert!(
            key.check_stored(&other_packed).is_err(),
            "another object's bytes do not check"
        );
        let other_header = Header {
            id: *BlobId::of(&other).as_bytes(),
            encoded_size: other_packed.len(),
            size: other.len(),
            ..header
        };
        assert!(
            key.to_stored(&other_header, other_packed).is_err(),
            "a key refuses bytes that hash elsewhere"
        );

        // A raw blob is packed here, and the bytes that are not its ID are
        // refused before the pack.
        let raw_header = Header {
            encoding: Encoding::Raw,
            encoded_size: mine.len(),
            size: mine.len(),
            ..header
        };
        assert_eq!(
            key.to_stored(&raw_header, mine.clone()).unwrap(),
            packed,
            "a raw blob is packed for the store"
        );

        // A tree key against blob bytes: the decode refuses them.
        let tree = Key::Tree(TreeId::from_bytes([7; 32]));
        assert!(
            tree.check_stored(&packed).is_err(),
            "a tree key refuses blob bytes"
        );
    }

    #[tokio::test]
    async fn a_tier_answers_neither_refs_nor_root_trees() {
        use scion_proto::v1::{GetRootTreeRequest, ResolveRefRequest};
        let tier = Tier::new_relay(
            MemoryStore::new(1 << 20),
            "http://127.0.0.1:1",
            "token",
            CACHE_UPSTREAM_TIMEOUT,
            None,
        )
        .unwrap();
        let refused = Objects::resolve_ref(&tier, Request::new(ResolveRefRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::Unimplemented, "{refused:?}");
        assert!(refused.message().contains("ask the server"), "{refused:?}");
        let refused = Objects::get_root_tree(&tier, Request::new(GetRootTreeRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::Unimplemented, "{refused:?}");
        assert!(refused.message().contains("ask the server"), "{refused:?}");
    }

    /// An object whose path the ticket's walk does not reach fails alone:
    /// nothing goes out for it, and the caller is told the object is not
    /// in the ticket. The walk check's stub would serve it.
    #[tokio::test]
    async fn an_object_off_the_walk_is_refused_and_its_neighbour_still_serves() {
        let f = setup(3, 1 << 20).await;
        // A wrong path for a real key: the walk from the ticket's root
        // tree does not reach it.
        let mut off_walk = ask(&f, &f.keys, "r");
        off_walk.keys[0].1 = b"not/its/path".to_vec();
        let (Err(failed), _permit) = f
            .tier
            .produce(&off_walk, 0, &Progress::default())
            .await
            .unwrap()
        else {
            panic!("the walk must not reach the object")
        };
        assert_eq!(failed.status.code(), Code::NotFound, "{failed:?}");
        assert!(
            failed.status.message().contains("is not in the ticket"),
            "{failed:?}"
        );
        assert!(!failed.whole_call, "the object fails alone");
        settled(&f.tier).await;
        // Its neighbours are still reachable on a correct ask.
        let found = f
            .tier
            .produce(&ask(&f, &f.keys, "r"), 1, &Progress::default())
            .await;
        assert!(found.is_some(), "the neighbours still serve");
    }

    #[tokio::test]
    async fn a_tier_refuses_a_ticket_of_another_key() {
        use scion_proto::v1::GetObjectsRequest;
        let setup = setup(1, 1 << 20).await;
        // A ticket the tier's key did not sign: refused with the key named.
        let forged = scion_proto::ticket::sign(
            &ed25519_dalek::SigningKey::from_bytes(&[7; 32]),
            "r",
            "test",
            *setup.root.as_bytes(),
            scion_proto::ticket::now(),
        )
        .unwrap();
        let request = GetObjectsRequest {
            ticket: forged,
            refs: vec![scion_proto::v1::ObjectRef {
                kind: setup.keys[0].kind() as i32,
                id: setup.keys[0].id().as_slice().to_vec(),
                path: setup.paths[0].clone(),
            }],
            accept: Vec::new(),
        };
        let error = Objects::get_objects(&setup.tier, Request::new(request))
            .await
            .unwrap_err();
        assert!(
            error.message().contains("unknown key or bad signature"),
            "the refusal names the signature: {}",
            error.message()
        );
    }

    #[tokio::test]
    async fn the_cache_counts_its_hits_and_misses() {
        let setup = setup(2, 1 << 20).await;
        let tier = &setup.tier;
        assert_eq!(tier.cache_hits_and_misses(), (0, 0));
        // A miss: the object is not in the store yet.
        let (object, _permit) = tier
            .produce(&ask(&setup, &setup.keys, "r"), 0, &Progress::default())
            .await
            .unwrap();
        let Body::Chunks(chunks) = object.unwrap().body else {
            panic!("unpacked into chunks")
        };
        assert_eq!(chunks.len(), 1);
        assert_eq!(tier.cache_hits_and_misses(), (0, 1));
        // The same object again: a hit, once its fetch has settled.
        settled(tier).await;
        let (object, _permit) = tier
            .produce(&ask(&setup, &setup.keys, "r"), 0, &Progress::default())
            .await
            .unwrap();
        let Body::Chunks(chunks) = object.unwrap().body else {
            panic!("unpacked into chunks")
        };
        assert_eq!(chunks.len(), 1);
        assert_eq!(tier.cache_hits_and_misses(), (1, 1));
        // The metrics getter hands back the tier's own counters, not a
        // fresh default: the daemon's /metrics endpoint reads through
        // it, so a swapped Arc would serve all-zero counts while the
        // tier kept counting into a dropped one.
        assert_eq!(
            tier.metrics().cache_counts(),
            (1, 1),
            "the getter reads the counters the tier just wrote"
        );
    }

    #[tokio::test]
    async fn a_store_that_keeps_nothing_costs_one_fetch_per_later_miss() {
        // A limit of 0 bytes: the store keeps nothing.
        let f = setup(4, 0).await;

        // The objects of one call, each reached after the fetch before it
        // has ended, so none can join a fetch in progress.
        let progress = Progress::default();
        for (i, blob) in f.blobs.iter().enumerate() {
            let stored = f
                .tier
                .fetch(&ask(&f, &f.keys, "r"), i, &progress)
                .await
                .unwrap()
                .unwrap();
            // A RAW blob from upstream is kept packed.
            assert_eq!(object::unpack(&stored, blob.len()).unwrap(), *blob);
            settled(&f.tier).await;
        }
        // The first fetch asked for all four; each later one only for itself.
        assert_eq!(f.fake.asked.load(Ordering::SeqCst), 4 + 3);
    }

    #[tokio::test]
    async fn an_object_that_a_fetch_has_stored_is_never_fetched_again() {
        let f = setup(3, 1 << 20).await;
        let asked = || f.fake.asked.load(Ordering::SeqCst);
        let call_indexes = || {
            f.fake
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .map(|call| {
                    call.iter()
                        .map(|id| {
                            f.keys
                                .iter()
                                .position(|k| k.id().as_slice() == id.as_slice())
                                .unwrap_or(9999)
                        })
                        .collect::<Vec<usize>>()
                })
                .collect::<Vec<Vec<usize>>>()
        };
        let settled = || async {
            settled(&f.tier).await;
        };

        // A fetch of the last two objects takes the first along - the batch
        // tiles from fetched_to, which starts at zero - and stores all three.
        f.tier
            .fetch(&ask(&f, &f.keys, "r"), 1, &Progress::default())
            .await
            .unwrap();
        settled().await;
        assert_eq!(asked(), 3);
        // A read whose key the tiled batch already stored fetches nothing.
        f.tier
            .fetch(&ask(&f, &f.keys, "r"), 0, &Progress::default())
            .await
            .unwrap();
        settled().await;
        assert_eq!(asked(), 3);
        // A reply that missed an object just before its fetch stored it, and
        // asks after the fetch has ended, is sent to the store. This is the
        // window between a reply's store check and its fetch.
        for i in 0..f.blobs.len() {
            let fetched = f
                .tier
                .fetch(&ask(&f, &f.keys, "r"), i, &Progress::default())
                .await;
            assert!(fetched.unwrap().is_none(), "read it from the store");
        }
        assert_eq!(
            asked(),
            3,
            "nothing was fetched twice; upstream calls by ask index: {:?}",
            call_indexes()
        );
        // Through a reply, the object comes from the store, with its share.
        // The first prove fetches the root tree of the ticket: one more ask.
        for (i, blob) in f.blobs.iter().enumerate() {
            let (object, _permit) = f
                .tier
                .produce(&ask(&f, &f.keys, "r"), i, &Progress::default())
                .await
                .unwrap();
            // Unpacked for a client that takes only RAW.
            let Body::Chunks(chunks) = object.unwrap().body else {
                panic!("unpacked into chunks")
            };
            assert_eq!(chunks, std::slice::from_ref(blob));
        }
        // 3 blobs from earlier fetches, plus the root tree of the walk.
        assert_eq!(asked(), 3 + 1);
    }

    #[tokio::test]
    async fn a_reply_that_waits_for_budget_reads_the_object_again() {
        // A store that keeps the object, then one that keeps nothing.
        for (limit, asked) in [(1 << 20, 1), (0, 2)] {
            let f = setup(1, limit).await;
            let fake = &f.fake;
            let tier = &f.tier;
            let full = reply::try_take(&tier.inner.in_flight, IN_FLIGHT_BYTES).unwrap();
            let waiting = tier.clone();
            let ask = ask(&f, &f.keys, "r");
            let producing = tokio::spawn(async move {
                let found = waiting.produce(&ask, 0, &Progress::default()).await;
                found.map(|(parts, _permit)| parts)
            });
            // The fetch ends; the reply finds no room, lets the bytes go and
            // waits. The prove of the walk fetches the root tree first, so
            // the ask count starts at 1. Both waits are on state, bounded.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while fake.asked.load(Ordering::SeqCst) < 2 {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the walk never fetched"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            settled(tier).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!producing.is_finished());
            drop(full);

            let object = producing.await.unwrap().unwrap().unwrap();
            // A client that does not accept zstd gets the content.
            assert_eq!(object.header.encoding, Encoding::Raw);
            let Body::Chunks(chunks) = object.body else {
                panic!("unpacked into chunks")
            };
            assert_eq!(chunks, [f.blobs[0].clone()]);
            // Read again from the store, or fetched again if it was not kept.
            assert_eq!(
                fake.asked.load(Ordering::SeqCst),
                asked + 1,
                "limit {limit}"
            );
            // Every share went back.
            assert_eq!(tier.inner.in_flight.available_permits(), IN_FLIGHT_BYTES);
            assert_eq!(tier.inner.fetching.available_permits(), FETCH_BYTES);
        }
    }

    #[tokio::test]
    async fn a_cold_call_fetches_in_batches_of_at_most_max_batch() {
        let f = setup(300, 1 << 20).await;
        let keys: Vec<Key> = f.keys.clone();
        let request = GetObjectsRequest {
            refs: keys
                .iter()
                .zip(&f.paths)
                .map(|(key, path)| key.to_ref(path))
                .collect(),
            accept: vec![Encoding::Zstd.into()],
            ticket: f.ticket.clone(),
        };
        let reply = f.tier.get_objects(Request::new(request)).await.unwrap();
        let mut stream = reply.into_inner();
        let mut got = 0;
        while let Some(part) = stream.next().await {
            // Each blob packed, in one part.
            let part = part.unwrap();
            assert_eq!(part.encoding, i32::from(Encoding::Zstd));
            got += 1;
        }
        assert_eq!(got, 300);
        // 300 blobs and the root tree of the ticket.
        assert_eq!(f.fake.asked.load(Ordering::SeqCst), 300 + 1);
        assert_eq!(f.fake.largest.load(Ordering::SeqCst), MAX_BATCH);
        let call_indexes = || {
            f.fake
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .map(|call| {
                    call.iter()
                        .map(|id| {
                            f.keys
                                .iter()
                                .position(|k| k.id().as_slice() == id.as_slice())
                                .unwrap_or(9999)
                        })
                        .collect::<Vec<usize>>()
                })
                .collect::<Vec<Vec<usize>>>()
        };
        assert_eq!(
            f.tier.upstream_calls(),
            4,
            "upstream calls by ask index: {:?}",
            call_indexes()
        );
    }

    #[tokio::test]
    async fn the_batches_tile_from_the_replys_last_claim_after_a_flight_finished() {
        let f = setup(300, 1 << 20).await;
        let keys: Vec<Key> = f.keys.clone();
        // The first batch's flight finishes before reads 128 and 129 claim,
        // and read 129 takes the flights lock first. Read 128 must join the
        // batch that tiles next from fetched_to, not start a flight of one.
        CLAIM_GATE_ARMED.store(128, Ordering::SeqCst);
        let tier = f.tier.clone();
        let releaser = tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            while tier.upstream_calls() < 3 {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the claim after the finished batch never ran"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            CLAIM_GATE_PERMIT.store(1, Ordering::SeqCst);
        });
        let request = GetObjectsRequest {
            refs: keys
                .iter()
                .zip(&f.paths)
                .map(|(key, path)| key.to_ref(path))
                .collect(),
            accept: vec![Encoding::Zstd.into()],
            ticket: f.ticket.clone(),
        };
        let reply = f.tier.get_objects(Request::new(request)).await.unwrap();
        let mut stream = reply.into_inner();
        let mut got = 0;
        while let Some(part) = stream.next().await {
            part.unwrap();
            got += 1;
        }
        releaser.await.unwrap();
        CLAIM_GATE_ARMED.store(usize::MAX, Ordering::SeqCst);
        CLAIM_GATE_PERMIT.store(0, Ordering::SeqCst);
        assert_eq!(got, 300);
        // 300 blobs and the root tree of the ticket, in the design's
        // batches: 1 call for the root tree, then 128, 128, 44.
        assert_eq!(f.fake.asked.load(Ordering::SeqCst), 300 + 1);
        let call_indexes = || {
            f.fake
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .map(|call| {
                    call.iter()
                        .map(|id| {
                            f.keys
                                .iter()
                                .position(|k| k.id().as_slice() == id.as_slice())
                                .unwrap_or(9999)
                        })
                        .collect::<Vec<usize>>()
                })
                .collect::<Vec<Vec<usize>>>()
        };
        assert_eq!(
            f.tier.upstream_calls(),
            4,
            "upstream calls by ask index: {:?}",
            call_indexes()
        );
    }

    #[tokio::test]
    async fn each_object_sent_for_a_repo_is_noted_for_its_snapshot() {
        let f = setup(3, 1 << 20).await;
        let keys = &f.keys;
        // The first is a hit, the second comes from upstream.
        let frame = Bytes::from(object::pack(&f.blobs[0]).unwrap());
        f.tier.store().put(keys[0], &frame).unwrap();
        for (repo, keys) in [("r", &keys[..2]), ("q", &keys[2..])] {
            // A ticket of the repo, for the root tree.
            let ticket = scion_proto::ticket::sign(
                &f.key,
                repo,
                "test",
                *f.root.as_bytes(),
                scion_proto::ticket::now(),
            )
            .unwrap();
            let ticket = scion_proto::ticket::parse(
                &ticket,
                &[f.key.verifying_key()],
                scion_proto::ticket::now(),
            )
            .unwrap();
            let paths: Vec<Vec<u8>> = keys
                .iter()
                .map(|key| {
                    let i = f.keys.iter().position(|k| k == key).unwrap();
                    f.paths[i].clone()
                })
                .collect();
            let request = GetObjectsRequest {
                refs: keys
                    .iter()
                    .zip(&paths)
                    .map(|(key, path)| key.to_ref(path))
                    .collect(),
                accept: vec![Encoding::Zstd.into()],
                ticket: ticket.bytes,
            };
            let reply = f.tier.get_objects(Request::new(request)).await.unwrap();
            let parts: Vec<_> = reply.into_inner().collect().await;
            assert_eq!(parts.len(), keys.len(), "{parts:?}");
        }
        // keys[1] and keys[2] from upstream, and the root tree once.
        assert_eq!(f.fake.asked.load(Ordering::SeqCst), 2 + 1);
        // Only what each ticket's repo saw is recorded, and the walk of the
        // ticket reached it.
        let noted: Vec<Key> = f
            .tier
            .store()
            .snapshot("r", 1 << 20)
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(noted, keys[..2]);
        let noted_q: Vec<Key> = f
            .tier
            .store()
            .snapshot("q", 1 << 20)
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(noted_q, keys[2..]);
    }

    #[test]
    fn a_stored_form_is_checked_against_its_key() {
        let content = b"some content\n";
        let key = Key::Blob(BlobId::of(content));
        let frame = object::pack(content).unwrap();
        key.check_stored(&frame).unwrap();
        // The content itself is not the stored form of a blob.
        assert!(key.check_stored(content).is_err());
        // A frame of other content.
        let other = object::pack(b"other content\n").unwrap();
        assert!(key.check_stored(&other).is_err());
        // A RAW blob from upstream is packed; a ZSTD one is kept as it came.
        let raw = Header {
            kind: ObjectKind::Blob,
            id: key.id(),
            encoding: Encoding::Raw,
            encoded_size: content.len(),
            size: content.len(),
        };
        assert_eq!(key.to_stored(&raw, content.to_vec()).unwrap(), frame);
        let packed = Header {
            encoding: Encoding::Zstd,
            encoded_size: frame.len(),
            ..raw.clone()
        };
        assert_eq!(key.to_stored(&packed, frame.clone()).unwrap(), frame);
        // A frame whose header and the object's header disagree on the size.
        let lying = Header {
            size: content.len() + 1,
            ..packed
        };
        assert!(key.to_stored(&lying, frame).is_err());
    }

    /// An upstream that sends the first part of one large RAW blob, then
    /// nothing more.
    struct OneZstd(Vec<u8>);
    #[tonic::async_trait]
    impl Objects for OneZstd {
        async fn resolve_ref(
            &self,
            _: Request<ResolveRefRequest>,
        ) -> Result<Response<ResolveRefResponse>, Status> {
            Err(Status::unimplemented("no refs"))
        }
        async fn get_root_tree(
            &self,
            _: Request<GetRootTreeRequest>,
        ) -> Result<Response<GetRootTreeResponse>, Status> {
            Err(Status::unimplemented("no changesets"))
        }
        type GetObjectsStream = Replies;
        async fn get_objects(
            &self,
            request: Request<GetObjectsRequest>,
        ) -> Result<Response<Replies>, Status> {
            let data = self.0.clone();
            let packed = object::pack(&data).unwrap();
            let header = Header {
                kind: ObjectKind::Blob,
                id: *BlobId::of(&data).as_bytes(),
                encoding: Encoding::Zstd,
                encoded_size: packed.len(),
                size: data.len(),
            };
            let first = header
                .parts(object::read_chunks(&packed[..], packed.len()).unwrap())
                .remove(0);
            let names_it = request
                .get_ref()
                .refs
                .first()
                .is_some_and(|r| r.id == header.id && r.kind == i32::from(ObjectKind::Blob));
            let parts: Replies = if names_it {
                Box::pin(tokio_stream::iter([Ok(first)]))
            } else {
                Box::pin(tokio_stream::iter([Err(Status::not_found("not asked"))]))
            };
            Ok(Response::new(parts))
        }
    }

    struct Stalls(Vec<u8>);

    #[tonic::async_trait]
    impl Objects for Stalls {
        async fn resolve_ref(
            &self,
            _: Request<ResolveRefRequest>,
        ) -> Result<Response<ResolveRefResponse>, Status> {
            Err(Status::unimplemented("no refs"))
        }

        async fn get_root_tree(
            &self,
            _: Request<GetRootTreeRequest>,
        ) -> Result<Response<GetRootTreeResponse>, Status> {
            Err(Status::unimplemented("no changesets"))
        }

        type GetObjectsStream = Replies;

        async fn get_objects(
            &self,
            _: Request<GetObjectsRequest>,
        ) -> Result<Response<Replies>, Status> {
            let data = self.0.clone();
            let header = Header {
                kind: ObjectKind::Blob,
                id: *BlobId::of(&data).as_bytes(),
                encoding: Encoding::Raw,
                encoded_size: data.len(),
                size: data.len(),
            };
            let first = header
                .parts(object::read_chunks(&data[..], data.len()).unwrap())
                .remove(0);
            let parts = tokio_stream::iter([Ok(first)]).chain(tokio_stream::pending());
            Ok(Response::new(Box::pin(parts)))
        }
    }

    /// An upstream that refuses every `GetObjects` with a whole-call status
    /// (Unauthenticated), before any object part.
    struct Refuses;

    #[tonic::async_trait]
    impl Objects for Refuses {
        async fn resolve_ref(
            &self,
            _: Request<ResolveRefRequest>,
        ) -> Result<Response<ResolveRefResponse>, Status> {
            Err(Status::unimplemented("no refs"))
        }

        async fn get_root_tree(
            &self,
            _: Request<GetRootTreeRequest>,
        ) -> Result<Response<GetRootTreeResponse>, Status> {
            Err(Status::unimplemented("no changesets"))
        }

        type GetObjectsStream = Replies;

        async fn get_objects(
            &self,
            _: Request<GetObjectsRequest>,
        ) -> Result<Response<Replies>, Status> {
            Ok(Response::new(Box::pin(tokio_stream::iter([Err(
                Status::unauthenticated("the upstream token was revoked"),
            )]))))
        }
    }

    /// An upstream that answers the first object of the ask and then ends
    /// the stream with a per-object status (NotFound): the rest of the
    /// objects fail alone, not the whole call.
    struct OneThenNotFound(Vec<u8>);

    #[tonic::async_trait]
    impl Objects for OneThenNotFound {
        async fn resolve_ref(
            &self,
            _: Request<ResolveRefRequest>,
        ) -> Result<Response<ResolveRefResponse>, Status> {
            Err(Status::unimplemented("no refs"))
        }

        async fn get_root_tree(
            &self,
            _: Request<GetRootTreeRequest>,
        ) -> Result<Response<GetRootTreeResponse>, Status> {
            Err(Status::unimplemented("no changesets"))
        }

        type GetObjectsStream = Replies;

        async fn get_objects(
            &self,
            request: Request<GetObjectsRequest>,
        ) -> Result<Response<Replies>, Status> {
            let data = self.0.clone();
            let refs = request.get_ref().refs.clone();
            let header = Header {
                kind: ObjectKind::Blob,
                id: *BlobId::of(&data).as_bytes(),
                encoding: Encoding::Raw,
                encoded_size: data.len(),
                size: data.len(),
            };
            let first = header
                .parts(object::read_chunks(&data[..], data.len()).unwrap())
                .remove(0);
            let first_names_the_ask = refs
                .first()
                .is_some_and(|r| r.id == header.id && r.kind == i32::from(ObjectKind::Blob));
            let parts: Replies = if first_names_the_ask {
                Box::pin(
                    tokio_stream::iter([Ok(first)]).chain(tokio_stream::iter([Err(
                        Status::not_found("the second object is gone"),
                    )])),
                )
            } else {
                Box::pin(tokio_stream::iter([Err(Status::not_found(
                    "not the asked object",
                ))]))
            };
            Ok(Response::new(parts))
        }
    }

    /// An upstream whose first reply item is the ask's own first part, and
    /// whose second item is `mid`: the error lands inside the part loop,
    /// not at the RPC boundary. For a stranger key it refuses per object.
    struct PartThenDies(Vec<u8>, Code);

    #[tonic::async_trait]
    impl Objects for PartThenDies {
        async fn resolve_ref(
            &self,
            _: Request<ResolveRefRequest>,
        ) -> Result<Response<ResolveRefResponse>, Status> {
            Err(Status::unimplemented("no refs"))
        }

        async fn get_root_tree(
            &self,
            _: Request<GetRootTreeRequest>,
        ) -> Result<Response<GetRootTreeResponse>, Status> {
            Err(Status::unimplemented("no changesets"))
        }

        type GetObjectsStream = Replies;

        async fn get_objects(
            &self,
            request: Request<GetObjectsRequest>,
        ) -> Result<Response<Replies>, Status> {
            let data = self.0.clone();
            let refs = request.get_ref().refs.clone();
            let header = Header {
                kind: ObjectKind::Blob,
                id: *BlobId::of(&data).as_bytes(),
                encoding: Encoding::Raw,
                encoded_size: data.len(),
                size: data.len(),
            };
            // Two parts of the announced bytes; the stream carries only
            // the first, then the whole-call error.
            let first = header
                .parts(vec![
                    Bytes::copy_from_slice(&data[..data.len() / 2]),
                    Bytes::copy_from_slice(&data[data.len() / 2..]),
                ])
                .remove(0);
            let first_names_the_ask = refs
                .first()
                .is_some_and(|r| r.id == header.id && r.kind == i32::from(ObjectKind::Blob));
            let parts: Replies = if first_names_the_ask {
                Box::pin(
                    tokio_stream::iter([Ok(first)]).chain(tokio_stream::iter([Err(Status::new(
                        self.1,
                        "the stream died here",
                    ))])),
                )
            } else {
                Box::pin(tokio_stream::iter([Err(Status::not_found(
                    "not the asked object",
                ))]))
            };
            Ok(Response::new(parts))
        }
    }

    /// A tier in front of [`PartThenDies`], whose second stream item is a
    /// whole-call status.
    async fn part_then_dies_upstream(data: &[u8]) -> Tier<MemoryStore> {
        part_then_code_upstream(data, Code::Unauthenticated).await
    }

    /// A tier in front of [`PartThenDies`] with `mid` as the second item.
    async fn part_then_code_upstream(data: &[u8], mid: Code) -> Tier<MemoryStore> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(PartThenDies(data.to_vec(), mid)))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        Tier::new_relay(
            MemoryStore::new(1 << 20),
            &url,
            "token",
            CACHE_UPSTREAM_TIMEOUT,
            None,
        )
        .unwrap()
    }

    /// A tier in front of [`Refuses`].
    async fn refusing_upstream() -> Tier<MemoryStore> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(Refuses))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        Tier::new_relay(
            MemoryStore::new(0),
            &url,
            "token",
            CACHE_UPSTREAM_TIMEOUT,
            None,
        )
        .unwrap()
    }

    /// A tier in front of [`OneThenNotFound`].
    async fn one_then_not_found_upstream(data: &[u8]) -> Tier<MemoryStore> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(OneThenNotFound(data.to_vec())))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        Tier::new_relay(
            MemoryStore::new(1 << 20),
            &url,
            "token",
            CACHE_UPSTREAM_TIMEOUT,
            None,
        )
        .unwrap()
    }

    /// A tier in front of [`Stalls`] that sends the first part of `data`.
    async fn stalled_upstream(data: &[u8]) -> Tier<MemoryStore> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(Stalls(data.to_vec())))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        Tier::new_relay(
            MemoryStore::new(0),
            &url,
            "token",
            CACHE_UPSTREAM_TIMEOUT,
            None,
        )
        .unwrap()
    }

    /// An ask for a test that only fetches, so no ticket of this tier ever
    /// gets checked: any verified ticket names a walk that `fetch` alone
    /// never runs.
    fn only_fetch_ask(key: Key) -> Ask {
        let key_ed = ed25519_dalek::SigningKey::from_bytes(&[8; 32]);
        let ticket =
            scion_proto::ticket::sign(&key_ed, "r", "test", [7; 32], scion_proto::ticket::now())
                .unwrap();
        let ticket = scion_proto::ticket::parse(
            &ticket,
            &[key_ed.verifying_key()],
            scion_proto::ticket::now(),
        )
        .unwrap();
        Ask {
            repo: "r".to_owned(),
            walk: Arc::new(Mutex::new(scion_proto::ticket::Walker::new([7; 32]))),
            ticket,
            keys: vec![(key, b"f".to_vec())],
            zstd: false,
            until: Until::NONE,
            request_id: None,
            stalled: None,
        }
    }

    /// A store that counts how the claim path asks it: `size` per key, and
    /// `sizes` per window.
    struct Probe {
        memory: MemoryStore,
        size_calls: Arc<AtomicUsize>,
        sizes_calls: Arc<AtomicUsize>,
    }

    impl Store for Probe {
        fn size(&self, key: Key) -> io::Result<Option<usize>> {
            self.size_calls.fetch_add(1, Ordering::SeqCst);
            self.memory.size(key)
        }

        fn sizes(&self, keys: &[Key]) -> Vec<io::Result<Option<usize>>> {
            self.sizes_calls.fetch_add(1, Ordering::SeqCst);
            self.memory.sizes(keys)
        }

        fn get(&self, key: Key, size: usize) -> io::Result<Option<Bytes>> {
            self.memory.get(key, size)
        }

        fn put(&self, key: Key, data: &Bytes) -> io::Result<()> {
            self.memory.put(key, data)
        }
    }

    /// One claim's window scan is one store call, not one per key: the
    /// scan of a full window is up to [`MAX_BATCH`] x 2 - 1 keys, and it
    /// runs on the async worker.
    #[tokio::test]
    async fn a_claims_window_scan_asks_the_store_once() {
        // 300 keys the store lacks: the claim at 130 scans the window
        // 3..258, 255 keys, under the early single-key check.
        let keys: Vec<Key> = (0u32..300)
            .map(|i| Key::Blob(BlobId::of(i.to_le_bytes().as_slice())))
            .collect();
        let key_ed = ed25519_dalek::SigningKey::from_bytes(&[9; 32]);
        let ticket =
            scion_proto::ticket::sign(&key_ed, "r", "test", [7; 32], scion_proto::ticket::now())
                .unwrap();
        let ticket = scion_proto::ticket::parse(
            &ticket,
            &[key_ed.verifying_key()],
            scion_proto::ticket::now(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let fake = Fake {
            objects: Arc::new(HashMap::new()),
            key: key_ed.clone(),
            root: TreeId::from_bytes([7; 32]),
            asked: Arc::default(),
            largest: Arc::default(),
            calls: Arc::default(),
        };
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(fake))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let probe = Probe {
            memory: MemoryStore::new(1 << 20),
            size_calls: Arc::new(AtomicUsize::new(0)),
            sizes_calls: Arc::new(AtomicUsize::new(0)),
        };
        let size_calls = Arc::clone(&probe.size_calls);
        let batch_calls = Arc::clone(&probe.sizes_calls);
        let tier = Tier::new(
            probe,
            &url,
            "token",
            CACHE_UPSTREAM_TIMEOUT,
            None,
            vec![key_ed.verifying_key()],
        )
        .unwrap();
        let ask = Ask {
            repo: "r".to_owned(),
            walk: Arc::new(Mutex::new(scion_proto::ticket::Walker::new([7; 32]))),
            ticket,
            keys: keys.iter().map(|key| (*key, b"f".to_vec())).collect(),
            zstd: false,
            until: Until::NONE,
            request_id: None,
            stalled: None,
        };
        // A key the store holds, inside the batch the claim would fetch
        // (130..258): the claim's window must be aligned with the loop's
        // range, so the stored key takes no flight while every other key
        // of the batch takes one.
        tier.store()
            .put(keys[140], &Bytes::from_static(b"stored"))
            .unwrap();
        let later = tier.claim(&ask, 130, &Progress::default());
        // The claim starts the batch at its own index (the boundary is at
        // 0, more than a batch behind), so it holds 258 - 131 = 127 keys;
        // the window it scanned is wider: 255 keys, from one batch behind
        // to one ahead.
        assert_eq!(later.len(), 127, "the claim holds its batch");
        assert_eq!(batch_calls.load(Ordering::SeqCst), 1, "one window call");
        assert_eq!(size_calls.load(Ordering::SeqCst), 1, "one key check");
        {
            let flights = lock(&tier.inner.flights);
            assert!(
                !flights.contains_key(&keys[140]),
                "the stored key takes no flight"
            );
            assert_eq!(
                flights.len(),
                127,
                "every other key of the batch takes one flight"
            );
        }
        settled(&tier).await;
    }

    /// A claim within one batch of the boundary starts the batch at the
    /// boundary, not at its own index, and ends at one batch from there:
    /// the boundary tiles the fetches.
    #[tokio::test]
    async fn a_claim_within_one_batch_of_the_boundary_starts_there() {
        // 300 keys the store lacks.
        let keys: Vec<Key> = (0u32..300)
            .map(|i| Key::Blob(BlobId::of(i.to_le_bytes().as_slice())))
            .collect();
        let key_ed = ed25519_dalek::SigningKey::from_bytes(&[9; 32]);
        let ticket =
            scion_proto::ticket::sign(&key_ed, "r", "test", [7; 32], scion_proto::ticket::now())
                .unwrap();
        let ticket = scion_proto::ticket::parse(
            &ticket,
            &[key_ed.verifying_key()],
            scion_proto::ticket::now(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let fake = Fake {
            objects: Arc::new(HashMap::new()),
            key: key_ed.clone(),
            root: TreeId::from_bytes([7; 32]),
            asked: Arc::default(),
            largest: Arc::default(),
            calls: Arc::default(),
        };
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(fake))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let probe = Probe {
            memory: MemoryStore::new(1 << 20),
            size_calls: Arc::new(AtomicUsize::new(0)),
            sizes_calls: Arc::new(AtomicUsize::new(0)),
        };
        let tier = Tier::new(
            probe,
            &url,
            "token",
            CACHE_UPSTREAM_TIMEOUT,
            None,
            vec![key_ed.verifying_key()],
        )
        .unwrap();
        let ask = Ask {
            repo: "r".to_owned(),
            walk: Arc::new(Mutex::new(scion_proto::ticket::Walker::new([7; 32]))),
            ticket,
            keys: keys.iter().map(|key| (*key, b"f".to_vec())).collect(),
            zstd: false,
            until: Until::NONE,
            request_id: None,
            stalled: None,
        };
        // The boundary sits at 5: the claim at 130 is 125 keys past it,
        // within one batch, so the batch starts at the boundary and ends
        // one batch from there (5 + 128 = 133).
        let progress = Progress::default();
        progress.fetched_to.store(5, Ordering::Relaxed);
        let later = tier.claim(&ask, 130, &progress);
        assert_eq!(
            later.len(),
            2,
            "the batch ends at the boundary plus one batch"
        );
        {
            let flights = lock(&tier.inner.flights);
            assert_eq!(flights.len(), 128, "the batch covers 5..133");
            assert!(
                flights.contains_key(&keys[5]),
                "the batch starts at the boundary, not at the claim"
            );
        }
        settled(&tier).await;
        // The exact edge: 128 keys past the boundary is no longer within
        // one batch, so the batch starts at the claim itself.
        let progress = Progress::default();
        progress.fetched_to.store(2, Ordering::Relaxed);
        let later = tier.claim(&ask, 130, &progress);
        assert_eq!(
            later.len(),
            127,
            "i - fetched == MAX_BATCH is a claim a batch ahead"
        );
        settled(&tier).await;
    }

    /// A claim a batch ahead of the boundary starts at its own index:
    /// the boundary never follows it down.
    #[tokio::test]
    async fn a_claim_a_batch_ahead_of_the_boundary_starts_at_itself() {
        let keys: Vec<Key> = (0u32..300)
            .map(|i| Key::Blob(BlobId::of(i.to_le_bytes().as_slice())))
            .collect();
        let key_ed = ed25519_dalek::SigningKey::from_bytes(&[9; 32]);
        let ticket =
            scion_proto::ticket::sign(&key_ed, "r", "test", [7; 32], scion_proto::ticket::now())
                .unwrap();
        let ticket = scion_proto::ticket::parse(
            &ticket,
            &[key_ed.verifying_key()],
            scion_proto::ticket::now(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let fake = Fake {
            objects: Arc::new(HashMap::new()),
            key: key_ed.clone(),
            root: TreeId::from_bytes([7; 32]),
            asked: Arc::default(),
            largest: Arc::default(),
            calls: Arc::default(),
        };
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(fake))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let probe = Probe {
            memory: MemoryStore::new(1 << 20),
            size_calls: Arc::new(AtomicUsize::new(0)),
            sizes_calls: Arc::new(AtomicUsize::new(0)),
        };
        let tier = Tier::new(
            probe,
            &url,
            "token",
            CACHE_UPSTREAM_TIMEOUT,
            None,
            vec![key_ed.verifying_key()],
        )
        .unwrap();
        let ask = Ask {
            repo: "r".to_owned(),
            walk: Arc::new(Mutex::new(scion_proto::ticket::Walker::new([7; 32]))),
            ticket,
            keys: keys.iter().map(|key| (*key, b"f".to_vec())).collect(),
            zstd: false,
            until: Until::NONE,
            request_id: None,
            stalled: None,
        };
        // The boundary sits at 140, one batch past the claim at 130: the
        // guard `fetched <= i` is false, so the batch starts at the claim
        // itself and runs to 258. (The `i - fetched` side is never
        // evaluated: the && short-circuits.)
        let progress = Progress::default();
        progress.fetched_to.store(140, Ordering::Relaxed);
        let later = tier.claim(&ask, 130, &progress);
        assert_eq!(later.len(), 127, "the batch ends one batch from 130");
        {
            let flights = lock(&tier.inner.flights);
            assert_eq!(flights.len(), 128, "the batch covers 130..258");
        }
        settled(&tier).await;
    }

    /// An upstream that refuses the whole call before any object: the
    /// caller's failure is the call's own (`whole_call`), so a caller with
    /// a later deadline asks again. A per-object reading of a
    /// whole-call status would leave the other waiters hanging on their
    /// deadlines while the upstream is broken.
    #[tokio::test]
    async fn a_whole_call_error_inside_the_part_loop_fails_the_whole_call() {
        // The first part answers, then the stream dies with a whole-call
        // status: the error rides the call, not one object - the single
        // waiter must see whole_call true and the error's own code.
        let data = b"an object of eighteen".to_vec();
        let tier = part_then_dies_upstream(&data).await;
        let key = Key::Blob(BlobId::of(&data));
        let failed = tier
            .fetch(&only_fetch_ask(key), 0, &Progress::default())
            .await
            .expect_err("the stream died mid-object");
        assert_eq!(failed.status.code(), Code::Unauthenticated, "{failed:?}");
        assert!(
            failed.whole_call,
            "a mid-object whole-call error is the call's own"
        );
        settled(&tier).await;
    }

    #[tokio::test]
    async fn a_per_object_refusal_of_the_first_item_fails_one_object() {
        // The stream's first item is a per-object status: the object fails
        // alone (whole_call false) and the call itself does not error.
        let tier = part_then_dies_upstream(b"an object").await;
        let stranger = Key::Blob(BlobId::of(b"a stranger's object"));
        let failed = tier
            .fetch(&only_fetch_ask(stranger), 0, &Progress::default())
            .await
            .expect_err("the upstream refused the object");
        assert_eq!(failed.status.code(), Code::NotFound, "{failed:?}");
        assert!(!failed.whole_call, "the object fails alone");
        settled(&tier).await;
    }

    #[tokio::test]
    async fn an_upstream_that_refuses_the_call_fails_the_whole_call() {
        let tier = refusing_upstream().await;
        let data = b"an object".to_vec();
        let key = Key::Blob(BlobId::of(&data));
        let ask = only_fetch_ask(key);
        let failed = tier
            .fetch(&ask, 0, &Progress::default())
            .await
            .expect_err("the upstream refused the call");
        assert_eq!(failed.status.code(), Code::Unauthenticated, "{failed:?}");
        assert!(
            failed.whole_call,
            "the refusal is the call's own, not one object's"
        );
        settled(&tier).await;
    }

    /// An upstream that gives the first object and ends its reply with a
    /// per-object status: the given object reaches its caller, and the
    /// gone one fails alone (`whole_call` false), so the rest of the call
    /// stands.
    #[tokio::test]
    async fn a_per_object_error_inside_the_part_loop_fails_one_object() {
        // A second key rides the same ask; the stream gives the first
        // object's opening part, then a per-object status: the object in
        // flight fails alone (whole_call false), the call does not error.
        let data = b"an object of eighteen".to_vec();
        let tier = part_then_code_upstream(&data, Code::NotFound).await;
        let gone = Key::Blob(BlobId::of(&data));
        let other = Key::Blob(BlobId::of(b"a second object, never served"));
        let mut ask = only_fetch_ask(gone);
        ask.keys.push((other, b"s".to_vec()));
        let failed = tier
            .fetch(&ask, 0, &Progress::default())
            .await
            .expect_err("the stream died mid-object");
        assert_eq!(failed.status.code(), Code::NotFound, "{failed:?}");
        assert!(!failed.whole_call, "the object fails alone");
        settled(&tier).await;
    }

    #[tokio::test]
    async fn a_gone_object_fails_alone_after_a_given_one() {
        let data = b"the object upstream has".to_vec();
        let tier = one_then_not_found_upstream(&data).await;
        let given = Key::Blob(BlobId::of(&data));
        let gone = Key::Blob(BlobId::of(b"the object upstream lacks"));
        let ask = only_fetch_ask(given);
        let mut keys = ask.keys.clone();
        keys.push((gone, b"g".to_vec()));
        let ask = Ask { keys, ..ask };
        let given_out = tier.fetch(&ask, 0, &Progress::default()).await.unwrap();
        // The tier serves the object in its stored form; unpack to the
        // bytes the fake holds.
        assert_eq!(
            object::unpack(&given_out.unwrap(), data.len()).unwrap(),
            data
        );
        // The gone key rode the same flight: its waiter settles with the
        // per-object refusal, not the call's own.
        let failed = tier
            .fetch(&ask, 1, &Progress::default())
            .await
            .expect_err("the gone object is refused");
        assert_eq!(failed.status.code(), Code::NotFound, "{failed:?}");
        assert!(
            !failed.whole_call,
            "the gone object fails alone; the call stood"
        );
        settled(&tier).await;
    }

    /// A reply that must grow its share past what the budget still holds
    /// is refused, not served: cover waits and gives up, and the caller
    /// asks again. If cover always said yes, a tier under load would
    /// hold memory past its budget.
    #[tokio::test]
    async fn a_reply_that_grows_past_the_free_budget_is_refused() {
        let data = vec![7u8; 4096];
        let key = Key::Blob(BlobId::of(&data));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(OneZstd(data.clone())))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let mut tier = Tier::new_relay(
            MemoryStore::new(1 << 20),
            &url,
            "token",
            CACHE_UPSTREAM_TIMEOUT,
            None,
        )
        .unwrap();
        let ask = only_fetch_ask(key);
        // A short wait: the refusal comes in milliseconds.
        {
            let inner = Arc::get_mut(&mut tier.inner).unwrap();
            inner.busy = Duration::from_millis(300);
        }
        // The reply budget holds all but the reply's first take (the
        // stored form's length) plus 8 bytes: the first take fits, the
        // growth for a non-zstd client (the stored frame and the
        // unpacked bytes on top) does not.
        let packed = object::pack(&data).unwrap();
        // The reply's share must grow by the stored frame and the
        // unpacked bytes on top of its first take.
        let grow = packed.len() + data.len() + UNPACK_BYTES;
        let keep_free = packed.len() + 8;
        assert!(
            grow > keep_free,
            "the test needs a growth past what the budget leaves"
        );
        let in_flight = Arc::clone(&tier.inner.in_flight);
        let permit = in_flight
            .try_acquire_many_owned(u32::try_from(IN_FLIGHT_BYTES - keep_free).unwrap())
            .unwrap();
        let found = tier.produce(&ask, 0, &Progress::default()).await;
        drop(permit); // the budget is full around the call
        // The refusal: the reply cannot grow its share, so produce hands
        // back None - the RPC layer answers UNAVAILABLE and the caller
        // asks again.
        assert!(
            found.is_none(),
            "the growth must be refused while the budget is held"
        );
        settled(&tier).await;
    }

    #[tokio::test]
    async fn a_fetch_that_finds_no_slot_ends_with_unavailable() {
        let data = b"an object".to_vec();
        let key = Key::Blob(BlobId::of(&data));
        let mut tier = stalled_upstream(&data).await;
        Arc::get_mut(&mut tier.inner).unwrap().busy = Duration::from_millis(50);
        // Other fetches hold every slot.
        let slots = Arc::clone(&tier.inner.fetch_slots);
        let others = slots
            .acquire_many_owned(u32::try_from(FETCH_SLOTS).unwrap())
            .await
            .unwrap();

        let (ask, progress) = (only_fetch_ask(key), Progress::default());
        let fetch = tier.fetch(&ask, 0, &progress);
        let failed = tokio::time::timeout(Duration::from_secs(10), fetch)
            .await
            .expect("the wait for a slot is bounded")
            .unwrap_err();
        assert_eq!(failed.status.code(), Code::Unavailable, "{failed:?}");
        assert!(failed.status.message().contains("slot"), "{failed:?}");
        assert!(failed.whole_call);
        drop(others);
    }

    #[test]
    fn a_blob_packed_here_keeps_no_room_for_the_worst_case() {
        // Content that packs small.
        let data = vec![0; 1 << 20];
        let key = Key::Blob(BlobId::of(&data));
        let header = Header {
            kind: ObjectKind::Blob,
            id: key.id(),
            encoding: Encoding::Raw,
            encoded_size: data.len(),
            size: data.len(),
        };
        let stored = key.to_stored(&header, data).unwrap();
        assert!(stored.len() < 1024, "{}", stored.len());
        assert!(
            stored.capacity() <= stored.len() + 64,
            "{}",
            stored.capacity()
        );
    }

    #[test]
    fn decoding_a_tree_fits_its_factor() {
        use scion_core::{BlobId, Entry, FileEntry, FileState, PathElem};
        // The smallest entry: a one-byte name of a directory.
        let tree = |names: &[&str]| {
            Tree::new(
                names
                    .iter()
                    .map(|name| {
                        (
                            PathElem::new(*name).unwrap(),
                            Entry::Dir(Tree::default().id()),
                        )
                    })
                    .collect(),
            )
            .expect("tree fits the format")
        };
        let entry = tree(&["a", "b"]).encode().len() - tree(&["a"]).encode().len();
        assert_eq!(entry, 38);
        // A file entry carries its byte length, so it encodes wider; its
        // decoded pair is wider too. It sets the factor.
        let file = |names: &[&str]| {
            Tree::new(
                names
                    .iter()
                    .map(|name| {
                        (
                            PathElem::new(*name).unwrap(),
                            Entry::File(FileEntry::new(
                                FileState::Regular {
                                    content: BlobId::of(b"x"),
                                    executable: false,
                                },
                                1,
                            )),
                        )
                    })
                    .collect(),
            )
            .expect("tree fits the format")
        };
        let file_entry = file(&["a", "b"]).encode().len() - file(&["a"]).encode().len();
        assert_eq!(file_entry, 47);
        // Decoded: the pair in a list and in a map node, and the name's
        // buffer (a small allocation takes 16 bytes or more).
        let pair = std::mem::size_of::<(PathElem, Entry)>();
        let node = 2 * pair;
        let decoded = pair + node + 32;
        assert!(
            decoded <= TREE_DECODE_FACTOR * entry,
            "{decoded} > {}",
            TREE_DECODE_FACTOR * entry
        );
        assert!(
            decoded <= TREE_DECODE_FACTOR * file_entry,
            "{decoded} > {}",
            TREE_DECODE_FACTOR * file_entry
        );
    }

    #[tokio::test]
    async fn a_fetch_that_finds_no_memory_ends_with_unavailable() {
        let data = vec![7; 3 * scion_proto::CHUNK_BYTES];
        let key = Key::Blob(BlobId::of(&data));
        let mut tier = stalled_upstream(&data).await;
        Arc::get_mut(&mut tier.inner).unwrap().busy = Duration::from_millis(50);
        // Other fetches hold the whole fetch budget.
        let others = reply::try_take(&tier.inner.fetching, FETCH_BYTES).unwrap();

        // The upstream reply stays open meanwhile, so the wait is bounded:
        // the fetch fails, and so does every object it holds.
        let (ask, progress) = (only_fetch_ask(key), Progress::default());
        let fetch = tier.fetch(&ask, 0, &progress);
        let failed = tokio::time::timeout(Duration::from_secs(10), fetch)
            .await
            .expect("the wait for memory is bounded")
            .unwrap_err();
        assert_eq!(failed.status.code(), Code::Unavailable, "{failed:?}");
        assert!(failed.whole_call);
        assert!(lock(&tier.inner.flights).is_empty());
        drop(others);
    }

    #[tokio::test]
    async fn a_fetch_takes_what_the_first_part_announces_before_the_rest_arrives() {
        let data = vec![7; 3 * scion_proto::CHUNK_BYTES];
        let key = Key::Blob(BlobId::of(&data));
        let tier = stalled_upstream(&data).await;
        let fetching = tier.clone();
        let fetch = tokio::spawn(async move {
            fetching
                .fetch(&only_fetch_ask(key), 0, &Progress::default())
                .await
        });

        // The content, the frame packed from it and the codec, no more: a
        // small object takes no room for the largest one.
        let held = || FETCH_BYTES - tier.inner.fetching.available_permits();
        let expected = data.len() + zstd_bound(data.len()) + UNPACK_BYTES;
        let started = tokio::time::Instant::now();
        while held() != expected {
            assert!(held() <= expected, "{} > {expected}", held());
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "held {}",
                held()
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(tier.inner.fetch_slots.available_permits(), FETCH_SLOTS - 1);
        fetch.abort();
    }
}
