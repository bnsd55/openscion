//! How the server and the cache tiers serve connections. They share this
//! code, so every process that answers calls keeps the same bounds.
//!
//! A reply whose client stops reading keeps memory in HTTP/2 that no budget
//! counts, until its stream ends or its connection closes. A `GetObjects` or
//! `ReadFile` reply keeps three parts: the part in its channel, the part
//! that hyper has read ahead, and the part in the h2 send buffer, which also
//! keeps up to 400 KiB more. That is under 4 MiB. A unary reply keeps its
//! whole message, up to 64 MiB. hyper cannot reset such a stream from the
//! server side: while the stream waits for flow-control window, hyper does
//! not poll the reply body, so it never sees a body error.
//!
//! So [`Limits::serve`] bounds what stalled replies hold: at most
//! [`Limits::connections`] connections at once, each with at most
//! [`Limits::streams`] open streams, and each with at most
//! [`Limits::stalled_bytes`] bytes held by its stalled replies — when a
//! reply stalls, it adds what it leaves in HTTP/2 to its connection's
//! count, a connection over the limit is closed, which frees its
//! stalled streams' buffers at once ([`Stalled`]), and a stalled reply
//! gives its count back when its stream ends, so a connection that has
//! served its stalls counts nothing. The process bounds their sum too:
//! [`Limits::stalled_total`] over every connection, and a connection
//! whose count crosses it is closed the same way. And it closes every
//! connection in time, so a connection that waits for a place gets one:
//!
//! - a client that does not send the HTTP/2 preface within
//!   `HANDSHAKE_TIMEOUT` is closed. hyper has no timeout of its own
//!   there, and its keepalive starts only after the preface.
//! - after the preface, HTTP/2 keepalive closes a connection whose client
//!   does not answer a ping.
//! - every connection is asked to close (`GOAWAY`) after
//!   `MAX_CONNECTION_AGE`, and is closed [`CONNECTION_GRACE`] later, with
//!   any call still in it. A client then connects again.
//! - a connection over [`Limits::connections`] waits at most
//!   [`CONNECTION_WAIT`] for a place, and is closed when none frees in
//!   time, so a client under a connection storm learns at once instead of
//!   waiting unread for up to `MAX_CONNECTION_AGE`.
//!
//! With TLS, a connection takes its place first, and one deadline from its
//! accept bounds the TLS handshake and the HTTP/2 preface together: the
//! handshake gets what is left of the deadline, and the preface timer gets
//! the rest. A peer that never starts the handshake, never ends it, or
//! stops right after it gives its place up within
//! `HANDSHAKE_TIMEOUT` of its accept, the same bound as a silent
//! plain peer.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _};
use std::path::Path;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::runtime::metrics::Metrics;
use crate::runtime::request_id;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::time::Sleep;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::server::TlsStream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt as _};
use tonic::transport::Server;
use tonic::transport::server::{Connected, Router, TcpIncoming};

/// The default of [`Limits::connections`]. Room for the 50 checkouts at once
/// of `scripts/bench-checkout.sh`, each on its own connection to one daemon.
pub const DEFAULT_CONNECTIONS: NonZeroUsize = NonZeroUsize::new(128).unwrap();

/// The default of [`Limits::streams`]. One connection can still fill all
/// 256 job slots of the server, and carry the 200 agents of `scn bench`.
pub const DEFAULT_STREAMS: NonZeroU32 = NonZeroU32::new(256).unwrap();

/// A client must send the HTTP/2 preface within this long after it gets its
/// place, or its connection is closed. The same bound covers the TLS
/// handshake on a TLS listener.
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);

/// After this long without a frame from a client, the process sends it an
/// HTTP/2 ping.
pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// A connection whose client does not answer a ping within this long is
/// closed. So after the preface, a dead peer frees its connection, and all
/// that its replies hold, within [`KEEPALIVE_INTERVAL`] plus this of its
/// last frame.
pub(crate) const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// After a failed accept, the accept task waits this long before it
/// accepts again, doubling to [`MAX_ACCEPT_BACKOFF`], and reset by a
/// success. A failure that persists - most often the process out of file
/// descriptors - cannot spin a core on accepts that all fail at once.
pub(crate) const FIRST_ACCEPT_BACKOFF: Duration = Duration::from_millis(10);
/// The longest the accept task waits between accepts after failures.
pub(crate) const MAX_ACCEPT_BACKOFF: Duration = Duration::from_secs(1);
/// How often the accept task writes its rate-limited warning, with how
/// many accepts failed since the last line.
pub(crate) const ACCEPT_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// The wait between accepts after a failure, as its own state: the first
/// wait ([`FIRST_ACCEPT_BACKOFF`]) doubling to [`MAX_ACCEPT_BACKOFF`], and
/// reset by a success, so a failure that persists cannot spin the task.
#[derive(Debug)]
struct Backoff(Duration);

impl Backoff {
    fn new() -> Self {
        Self(FIRST_ACCEPT_BACKOFF)
    }

    /// Waits this backoff out, and doubles it for the next failure, to the
    /// cap.
    async fn wait(&mut self) {
        tokio::time::sleep(self.0).await;
        self.0 = (self.0 * 2).min(MAX_ACCEPT_BACKOFF);
    }

    /// A success: the next failure waits the first wait again.
    fn reset(&mut self) {
        self.0 = FIRST_ACCEPT_BACKOFF;
    }
}

/// After this long, a connection is asked to close, so one that a client
/// keeps for life (a daemon keeps one to its cache) gives its place up.
pub(crate) const MAX_CONNECTION_AGE: Duration = Duration::from_secs(30 * 60);

/// The HTTP/2 keepalive a client of a scion server runs: it pings its
/// peer at this interval, busy or idle, and the peer's answer must come
/// within [`CLIENT_KEEPALIVE_TIMEOUT`]. The server pings its side at
/// `KEEPALIVE_INTERVAL` (60 s, private to this module); a client that
/// asks sooner sees a dead server first, and a kill without a FIN or
/// RST (a frozen process, a host gone) ends its calls in about the
/// interval plus the timeout, not at a call deadline.
pub const CLIENT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// How long a client's connection waits for a keepalive answer before
/// the connection is dead: two thirds of [`CLIENT_KEEPALIVE_INTERVAL`],
/// so a dead peer costs about 50 s, inside the tier's 60 s upstream
/// window, and a healthy server that stalls for a moment (an fsync
/// storm, a swap burst) is not cut by every client at once.
pub const CLIENT_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// The TCP keepalive probe interval of a client connection, under the
/// HTTP/2 interval, so a dead connection is visible below HTTP/2 too.
pub const CLIENT_TCP_KEEPALIVE: Duration = Duration::from_secs(15);

const _: () = assert!(CLIENT_KEEPALIVE_INTERVAL.as_secs() == 30);
const _: () = assert!(CLIENT_KEEPALIVE_TIMEOUT.as_secs() == 20);
const _: () = assert!(CLIENT_TCP_KEEPALIVE.as_secs() == 15);
const _: () = assert!(
    CLIENT_KEEPALIVE_INTERVAL.as_secs() + CLIENT_KEEPALIVE_TIMEOUT.as_secs() <= 60,
    "a dead peer must fail a client inside the tier's 60 s upstream window"
);

thread_local! {
    /// A test's shorter keepalive, as `(interval, timeout)`: the same
    /// dead-peer failure is proved in seconds, and the product
    /// constants above stay the documented ones. `None` is the product.
    /// Thread-local, so a `multi_thread` test that sets it on one worker
    /// can build the channel on another and lose it; the test's bound on
    /// the failure time catches that.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub static CLIENT_KEEPALIVE_OVERRIDE: std::cell::Cell<Option<(Duration, Duration)>> =
        const { std::cell::Cell::new(None) };
}

/// The keepalive a client of a scion server runs: the interval it pings
/// at, and the time an answer gets, as `(interval, timeout)`.
#[must_use]
pub fn client_keepalive() -> (Duration, Duration) {
    #[cfg(any(test, feature = "test-hooks"))]
    if let Some(keepalive) = CLIENT_KEEPALIVE_OVERRIDE.with(std::cell::Cell::get) {
        return keepalive;
    }
    (CLIENT_KEEPALIVE_INTERVAL, CLIENT_KEEPALIVE_TIMEOUT)
}

/// The TCP keepalive probe interval of a client connection.
#[must_use]
pub fn client_tcp_keepalive() -> Duration {
    CLIENT_TCP_KEEPALIVE
}

/// A connection that was asked to close is closed this long later, with any
/// call still in it. At least the server's longest call (600 s).
pub const CONNECTION_GRACE: Duration = Duration::from_secs(600);

/// How long a connection over [`Limits::connections`] waits for a place
/// before the server closes it. Long enough that a place freed by a
/// connection that was ending anyway still serves the waiter; short enough
/// that a client under a connection storm learns at once, instead of
/// waiting unread for up to `MAX_CONNECTION_AGE`. The waiters are capped
/// at the same size as the places, so a storm holds at most twice
/// [`Limits::connections`] sockets, not one per connect.
pub const CONNECTION_WAIT: Duration = Duration::from_secs(5);

/// The default of `--drain-timeout-secs`: how long the in-flight calls
/// have to finish once the first signal starts the drain, before the
/// process cuts them and exits non-zero. At least the server's longest
/// call (`CONNECTION_GRACE`), so a clean drain is not cut.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = CONNECTION_GRACE;

/// The bytes of the HTTP/2 client preface.
const PREFACE_BYTES: usize = 24;

/// A connection whose stalled replies hold more than this is closed, with
/// all that they hold. 16 stalled replies of under 4 MiB each. The flag is
/// `--max-stalled-bytes`.
pub const DEFAULT_STALLED_BYTES: NonZeroU64 = NonZeroU64::new(64 << 20).unwrap();

/// The bytes that the stalled replies of the whole process may hold, over
/// every connection: the per-connection cap bounds one client, and this
/// bounds their sum, so a host sized for the process's fixed shares can
/// hold the worst case. A connection whose `hold` crosses it is closed,
/// like a connection over its own cap.
pub const STALLED_TOTAL_BYTES: u64 = 1 << 30;

/// How many connections a process serves, and how many streams each may
/// have open.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Most connections served at once. A connection beyond that is
    /// accepted, and waits at most [`CONNECTION_WAIT`] for another
    /// connection to close; if none frees in time, the server closes it and
    /// counts it in the refused-connections metric
    /// (`scion_connections_refused_total`). At most this many connections
    /// wait at once: one over that is closed at once, and counts too.
    pub connections: NonZeroUsize,
    /// Most streams (calls) that one connection may have open at once, as
    /// HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS`. A client that has this many
    /// waits for one to end before it opens another.
    pub streams: NonZeroU32,
    /// Most bytes that the stalled replies of one connection may hold. A
    /// connection over it is closed, and what its stalled replies hold goes.
    pub stalled_bytes: NonZeroU64,
    /// Most bytes that the stalled replies of the whole process may hold:
    /// the per-connection cap bounds one client, this bounds their sum. A
    /// connection whose count crosses it is closed, like one over its own
    /// cap.
    pub stalled_total: NonZeroU64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            connections: DEFAULT_CONNECTIONS,
            streams: DEFAULT_STREAMS,
            stalled_bytes: DEFAULT_STALLED_BYTES,
            stalled_total: NonZeroU64::new(STALLED_TOTAL_BYTES).unwrap(),
        }
    }
}

/// What the stalled replies of one connection hold, counted when a reply
/// stalls, and given back when the reply's stream ends, so a connection
/// whose stalled replies are all served again counts nothing ([`Stall`]).
/// A call gets the handle in its extensions (as the connection's connect
/// info), and passes it to its reply. When the count passes the limit, the
/// connection's read side fails, so hyper closes the connection and frees
/// what every stalled stream of it holds — even though the client sends
/// nothing, because the count wakes the stored read waker.
#[derive(Clone)]
pub struct Stalled(Arc<Counted>);

/// The process-wide count every connection's `Counted` shares: the sum of
/// what stalled replies hold over the process, bounded by
/// [`STALLED_TOTAL_BYTES`] (a `Limits::stalled_total` in the tests).
pub(crate) struct TotalBytes {
    limit: u64,
    bytes: AtomicU64,
}

impl TotalBytes {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            limit,
            bytes: AtomicU64::new(0),
        }
    }
}

struct Counted {
    /// Most bytes that the stalled replies of this connection may hold.
    limit: u64,
    /// Bytes that stalled replies are estimated to hold now.
    bytes: AtomicU64,
    /// The process-wide count the connection shares.
    total: Arc<TotalBytes>,
    /// The process's registry, whose `scion_stalled_reply_bytes` gauge
    /// sums what the stalled replies of every live connection hold.
    metrics: Arc<Metrics>,
    /// The waker of the connection's last read poll, woken once the count
    /// passes the limit, so the close is seen without a frame from the
    /// client.
    waker: Mutex<Option<Waker>>,
    /// Whether the crossing has been logged.
    logged: AtomicBool,
    /// The peer of the connection, when it is a socket: the audit's
    /// refusal lines name it, and the call span's `peer` field carries
    /// it. None on a connection that is not a socket.
    peer: Option<SocketAddr>,
}

impl Stalled {
    pub(crate) fn new(
        peer: Option<SocketAddr>,
        limit: u64,
        metrics: Arc<Metrics>,
        total: Arc<TotalBytes>,
    ) -> Self {
        Self(Arc::new(Counted {
            peer,
            limit,
            bytes: AtomicU64::new(0),
            total,
            metrics,
            waker: Mutex::new(None),
            logged: AtomicBool::new(false),
        }))
    }

    /// The peer of `request`'s connection, when it is a socket. Below
    /// the router the connect info is the only peer a call still has:
    /// the audit's refusal lines name it.
    #[must_use]
    pub fn peer(&self) -> Option<SocketAddr> {
        self.0.peer
    }

    /// The handle of `request`'s connection, if it is one of ours. A reply
    /// counts its stalled bytes on it.
    #[must_use]
    pub fn of<T>(request: &tonic::Request<T>) -> Option<Self> {
        request.extensions().get::<Self>().cloned()
    }

    /// Counts `bytes` more that stalled replies leave in HTTP/2, onto the
    /// connection and onto the process's `scion_stalled_reply_bytes`
    /// gauge, and hands the count back as a [`Stall`]: the reply gives it
    /// up when its body drops. Past the limit, logs once, and wakes the
    /// read side, which then fails the connection.
    ///
    /// # Panics
    ///
    /// Only if a lock is poisoned, which cannot happen: no holder panics.
    pub fn hold(&self, bytes: u64) -> Stall {
        let held = self.0.bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        let total = self.0.total.bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.0.metrics.stalled_reply_held(bytes);
        if (held >= self.0.limit || total >= self.0.total.limit)
            && !self.0.logged.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                held,
                limit = self.0.limit,
                total,
                total_limit = self.0.total.limit,
                peer = ?self.0.peer,
                "closing a connection: its stalled replies hold too much"
            );
            if let Some(waker) = self
                .0
                .waker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                waker.wake();
            }
        }
        Stall {
            counted: Arc::clone(&self.0),
            bytes,
        }
    }

    /// Whether the connection must close now: its own count, or the
    /// process total it shares, is over its limit.
    fn over(&self) -> bool {
        self.0.bytes.load(Ordering::Relaxed) >= self.0.limit
            || self.0.total.bytes.load(Ordering::Relaxed) >= self.0.total.limit
    }

    /// Registers the read waker, so a crossing of the limit is seen at once.
    fn register(&self, read: &Waker) {
        let mut waker = self.0.waker.lock().unwrap_or_else(PoisonError::into_inner);
        *waker = Some(read.clone());
    }

    /// The I/O error that closes a connection over a limit.
    fn error(&self) -> io::Error {
        io::Error::new(
            io::ErrorKind::ConnectionAborted,
            format!(
                "its stalled replies hold {} bytes, over the {} limit (the process holds {} of {})",
                self.0.bytes.load(Ordering::Relaxed),
                self.0.limit,
                self.0.total.bytes.load(Ordering::Relaxed),
                self.0.total.limit
            ),
        )
    }
}

/// The count of one stalled reply: the estimate of what its parts hold in
/// HTTP/2. Dropping it gives the bytes back to the connection's count and
/// to the process's gauge, which is when the reply's body drops: its
/// stream ended — it finished, its client reset it, or the connection
/// closed.
#[must_use = "the bytes stay counted until the Stall drops"]
pub struct Stall {
    counted: Arc<Counted>,
    bytes: u64,
}

impl Drop for Stall {
    fn drop(&mut self) {
        self.counted.bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        self.counted
            .total
            .bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
        self.counted.metrics.stalled_reply_freed(self.bytes);
    }
}

/// The timers of a connection.
struct Timers {
    handshake: Duration,
    keepalive: Duration,
    keepalive_timeout: Duration,
    age: Duration,
    grace: Duration,
    wait: Duration,
}

const TIMERS: Timers = Timers {
    handshake: HANDSHAKE_TIMEOUT,
    keepalive: KEEPALIVE_INTERVAL,
    keepalive_timeout: KEEPALIVE_TIMEOUT,
    age: MAX_CONNECTION_AGE,
    grace: CONNECTION_GRACE,
    wait: CONNECTION_WAIT,
};

/// The gRPC health service (`grpc.health.v1`), on the same port as the
/// other services and without a token, so a probe needs none. It is
/// `SERVING` for the whole server (the empty name) and for every name in
/// `services`. The readiness probe reads a service's name: when shutdown
/// starts, these names flip to `NOT_SERVING`, before the drain, while the
/// server still answers. The liveness probe reads the empty name: it
/// stays `SERVING` while the process answers, so a stop does not tell the
/// supervisor to kill a draining server.
///
/// Returns the service to add to the router, and the wrapped shutdown.
pub async fn health(
    services: &[&'static str],
    shutdown: impl Future<Output = ()>,
    mut ready: watch::Receiver<bool>,
) -> (
    tonic_health::pb::health_server::HealthServer<impl tonic_health::pb::health_server::Health>,
    impl Future<Output = ()>,
) {
    let (reporter, service) = tonic_health::server::health_reporter();
    for name in std::iter::once("").chain(services.iter().copied()) {
        reporter
            .set_service_status(name, tonic_health::ServingStatus::Serving)
            .await;
    }
    let shutdown = async move {
        tokio::select! {
            () = shutdown => {},
            // The audit lost a line: the log has a hole, so the names
            // flip now and stay flipped until a restart. The audit has
            // logged the reason on its own path.
            _ = ready.wait_for(|ready| !*ready) => {
                tracing::error!("readiness lost: an audit line was not made durable");
            },
        }
        // Only the service names flip: the empty name is liveness, and a
        // stop must not tell the supervisor to kill a draining server.
        for name in services.iter().copied() {
            reporter
                .set_service_status(name, tonic_health::ServingStatus::NotServing)
                .await;
        }
    };
    (service, shutdown)
}

impl Limits {
    /// Serves the services that `router` adds to a server, on `incoming`
    /// connections, within these limits and with the timers above, until
    /// `shutdown` resolves. A connection that waits for its place logs a
    /// warning. The stalled replies of its connections count in `metrics`,
    /// on the `scion_stalled_reply_bytes` gauge.
    ///
    /// # Errors
    ///
    /// Transport errors.
    pub async fn serve<I, IO>(
        self,
        router: impl FnOnce(Server) -> Router,
        incoming: I,
        tls: Option<Arc<ServerConfig>>,
        shutdown: impl Future<Output = ()> + Send + 'static,
        metrics: Arc<Metrics>,
    ) -> Result<(), tonic::transport::Error>
    where
        I: Stream<Item = io::Result<IO>> + Send + 'static,
        IO: AsyncRead + AsyncWrite + Connected + PeerOf + Unpin + Send + 'static,
    {
        let accepting = Accepting::of(&self, &TIMERS);
        self.serve_with(&accepting, router, incoming, tls, shutdown, metrics)
            .await
    }

    async fn serve_with<I, IO>(
        self,
        accepting: &Accepting,
        router: impl FnOnce(Server) -> Router,
        incoming: I,
        tls: Option<Arc<ServerConfig>>,
        shutdown: impl Future<Output = ()> + Send + 'static,
        metrics: Arc<Metrics>,
    ) -> Result<(), tonic::transport::Error>
    where
        I: Stream<Item = io::Result<IO>> + Send + 'static,
        IO: AsyncRead + AsyncWrite + Connected + PeerOf + Unpin + Send + 'static,
    {
        // One span per call: the RPC and the peer at the start, and the
        // principal and the repo once the call knows them. `error_span`, so
        // the span is open under any filter, and a `WARN` line under a
        // `warn` filter still shows its fields.
        let server = Server::builder()
            .trace_fn(|request| {
                // The connection's handle carries the peer: the connect
                // info of a request below the router is ours, and the
                // transport's `TcpConnectInfo` is not on the request.
                let peer = request
                    .extensions()
                    .get::<Stalled>()
                    .and_then(Stalled::peer)
                    .map(|addr| addr.to_string())
                    .unwrap_or_default();
                // The caller's request ID, if it sent one that fits. A
                // header that does not fit is dropped whole, not cleaned
                // up: nothing of it is logged.
                let request_id = request_id::sanitize(
                    request
                        .headers()
                        .get(request_id::REQUEST_ID)
                        .map(http::HeaderValue::as_bytes),
                )
                .unwrap_or_default();
                tracing::error_span!(
                    "call",
                    rpc = %request.uri().path(),
                    peer,
                    request_id = %request_id,
                    principal = tracing::field::Empty,
                    repo = tracing::field::Empty,
                )
            })
            .max_concurrent_streams(self.streams.get())
            .http2_keepalive_interval(Some(accepting.keepalive))
            .http2_keepalive_timeout(Some(accepting.keepalive_timeout))
            .max_connection_age(accepting.age)
            .max_connection_age_grace(accepting.grace);
        // The accept path owns the shutdown future and sets the watch when
        // it resolves (or when the listener ends); the serve below reads
        // the same watch, so the drain starts at the signal itself.
        let (stop, stopped) = watch::channel(false);
        if let Some(config) = tls {
            let incoming = accept_tls(
                incoming,
                accepting,
                stop,
                shutdown,
                config,
                Arc::clone(&metrics),
            );
            let outcome = router(server)
                .serve_with_incoming_shutdown(incoming, watch_signal(stopped))
                .await;
            Self::warn_the_last(accepting);
            return outcome;
        }
        let incoming = accept(incoming, accepting, stop, shutdown, metrics);
        let outcome = router(server)
            .serve_with_incoming_shutdown(incoming, watch_signal(stopped))
            .await;
        Self::warn_the_last(accepting);
        outcome
    }

    /// Writes whatever the last interval gathered, before the serve
    /// returns: the binaries exit soon after, and the flusher's detached
    /// task may not run again. Not the whole story: waiter tasks spawned
    /// before the signal can still time out up to [`CONNECTION_WAIT`]
    /// after this ran, and their refusals are counted then - in the
    /// binaries the exit takes the log with them first; an embedder whose
    /// runtime keeps running may miss those in its log.
    fn warn_the_last(accepting: &Accepting) {
        if let Some(waited) = accepting.waits.flush_all() {
            warn_waited(accepting.max, waited);
        }
        if let Some(waited) = accepting.refusals.flush_all() {
            warn_refusals(accepting.max, waited, accepting.wait);
        }
    }
}

/// A shutdown for the serve that completes when the accept side stopped:
/// the accept task sets the watch when the shutdown resolves, or when the
/// listener ends, and this reads it.
async fn watch_signal(mut stopped: watch::Receiver<bool>) {
    while !*stopped.borrow_and_update() {
        if stopped.changed().await.is_err() {
            return;
        }
    }
}

/// The TCP connections of `listener`, with Nagle's algorithm off, so a small
/// reply does not wait for the client's ACK. (tonic's own `tcp_nodelay`
/// applies only to a listener that tonic opens.)
#[must_use]
pub fn tcp(listener: TcpListener) -> TcpIncoming {
    TcpIncoming::from(listener).with_nodelay(Some(true))
}

/// How often the accept paths write a line about connections that wait
/// for a place, or that a full cap refused. A storm must not write a line
/// per connection.
const WAIT_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// Milliseconds since the process started, as the rate limiter's clock: a
/// wall-clock step cannot silence the warnings, and the clock never runs
/// backwards. From one, so zero always means "never wrote": the first
/// call's elapsed time can still read zero.
fn now_millis() -> u64 {
    static STARTED: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let started = STARTED.get_or_init(std::time::Instant::now);
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX - 1) + 1
}

/// Counts the connections that wait for a place, or that a full cap
/// refused, and lets one line through per interval, carrying the count
/// since the last line. `Clone` shares the counts. A flusher task
/// ([`Self::spawn_flusher`]) writes the last interval's count even when
/// no new connection comes, so a count never sits stale.
#[derive(Clone)]
struct WaitWarn {
    interval_millis: u64,
    last: Arc<AtomicU64>,
    since: Arc<AtomicU64>,
}

impl WaitWarn {
    fn new(interval: Duration) -> Self {
        Self {
            interval_millis: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
            last: Arc::new(AtomicU64::new(0)),
            since: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Counts one more connection, and says when the caller writes the
    /// line: at most once per interval, carrying how many connections the
    /// line covers. `last` starts at zero, so the first wait writes at
    /// once.
    fn tick(&self) -> Option<u64> {
        self.since.fetch_add(1, Ordering::Relaxed);
        self.write()
    }

    /// Writes the accumulated count when the interval has passed, without
    /// counting a new event: the flusher calls this, so a count never
    /// sits past its interval.
    fn flush(&self) -> Option<u64> {
        if self.since.load(Ordering::Relaxed) == 0 {
            return None;
        }
        self.write()
    }

    /// Writes whatever is left, without the interval check: the serve's
    /// last line, called before the serve returns, when the accept side
    /// has stopped and no new count can come.
    fn flush_all(&self) -> Option<u64> {
        let waited = self.since.swap(0, Ordering::Relaxed);
        (waited != 0).then_some(waited)
    }

    fn write(&self) -> Option<u64> {
        let now = now_millis();
        let last = self.last.load(Ordering::Relaxed);
        // `last == 0` is "never wrote": the process clock may still read
        // zero in the first millisecond, and the first wait writes then.
        if last != 0 && now.saturating_sub(last) < self.interval_millis {
            return None;
        }
        // Only the winner writes, so a burst writes one line, not one per
        // racing connection.
        if self
            .last
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // Swap, not store-and-forget: a count another task added while
            // this one wrote goes out with this line, not into the next.
            Some(self.since.swap(0, Ordering::Relaxed))
        } else {
            None
        }
    }

    /// Spawns the flusher: every interval it writes the count that
    /// interval gathered, even when no new connection came, and when
    /// `over` says the serve has ended it writes what is left and stops.
    /// `line` writes the message with the count.
    fn spawn_flusher(
        &self,
        over: impl Fn() -> bool + Send + 'static,
        line: impl Fn(u64) + Send + 'static,
    ) {
        if self.interval_millis == 0 {
            return; // the flusher's timer needs an interval
        }
        let warn = self.clone();
        let interval = Duration::from_millis(self.interval_millis);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            timer.tick().await; // the first tick completes at once
            loop {
                timer.tick().await;
                if let Some(waited) = warn.flush() {
                    line(waited);
                }
                if over() {
                    break;
                }
            }
        });
    }
}

/// Takes a place, waiting at most `wait` past a full cap: `None` when the
/// wait ran out, and the caller closes the connection and counts the
/// refusal. The waiters are capped too (`waiters`, the same size as the
/// places): a connection over that cap is closed at once, so a storm
/// holds at most `max` sockets waiting, not one per connect. `max` is for
/// the warning line; a closed semaphore reads as no place, and the accept
/// paths never close theirs.
async fn place_within(
    places: &Arc<Semaphore>,
    waiters: &Arc<Semaphore>,
    max: usize,
    wait: Duration,
    waits: &WaitWarn,
) -> Option<OwnedSemaphorePermit> {
    if let Ok(place) = places.clone().try_acquire_owned() {
        return Some(place);
    }
    // A waiter's slot, taken at once: a connection over this cap is
    // closed here, before it can hold a socket for the whole wait.
    let slot = waiters.clone().try_acquire_owned().ok()?;
    if let Some(waited) = waits.tick() {
        tracing::warn!(
            max,
            waited,
            "a connection waits: the most connections are open"
        );
    }
    let place = match tokio::time::timeout(wait, places.clone().acquire_owned()).await {
        Ok(Ok(place)) => Some(place),
        // The accept paths never close their semaphore; a close reads as
        // no place.
        _ => None,
    };
    drop(slot);
    place
}

/// What the serve and the accept paths share, fixed when the serve starts.
#[derive(Clone)]
struct Accepting {
    /// Most connections served at once, and most waiting at once.
    max: usize,
    /// The handshake and preface deadline of a connection with its place.
    handshake: Duration,
    /// How long a connection over the cap waits for a place.
    wait: Duration,
    /// After how long without a frame the process pings a connection, and
    /// how long it waits for the answer.
    keepalive: Duration,
    keepalive_timeout: Duration,
    /// When a connection is asked to close, and when it is closed.
    age: Duration,
    grace: Duration,
    /// The stalled-replies cap of one connection.
    stalled_bytes: u64,
    /// The stalled-replies cap of the whole process.
    total: Arc<TotalBytes>,
    /// The rate-limited line about connections that wait for a place.
    waits: WaitWarn,
    /// The rate-limited line about connections a full cap closed.
    refusals: WaitWarn,
}

impl Accepting {
    /// What a serve with `timers` shares, at the serve's start.
    fn of(self_: &Limits, timers: &Timers) -> Self {
        Self {
            max: self_.connections.get(),
            handshake: timers.handshake,
            wait: timers.wait,
            keepalive: timers.keepalive,
            keepalive_timeout: timers.keepalive_timeout,
            age: timers.age,
            grace: timers.grace,
            stalled_bytes: self_.stalled_bytes.get(),
            total: Arc::new(TotalBytes::new(self_.stalled_total.get())),
            waits: WaitWarn::new(WAIT_WARN_INTERVAL),
            refusals: WaitWarn::new(WAIT_WARN_INTERVAL),
        }
    }
}

/// The waits line, with the count it covers.
fn warn_waited(max: usize, waited: u64) {
    tracing::warn!(
        max,
        waited,
        "a connection waits: the most connections are open"
    );
}

/// The refusals line, with the count it covers.
fn warn_refusals(max: usize, waited: u64, wait: Duration) {
    tracing::warn!(
        max,
        waited,
        wait = ?wait,
        "the most connections are open: a connection that did not \
         get a place within its wait is closed"
    );
}

/// Spawns the two rate-limited warnings' flushers of one accept path: the
/// waits line and the refusals line, each at most one per interval, and
/// each writing its last count when the serve ends (`over` says so).
fn spawn_cap_flushers(
    over: impl Fn() -> bool + Send + Clone + 'static,
    max: usize,
    wait: Duration,
    waits: &WaitWarn,
    refusals: &WaitWarn,
) {
    waits.spawn_flusher(over.clone(), move |waited| warn_waited(max, waited));
    refusals.spawn_flusher(over, move |waited| warn_refusals(max, waited, wait));
}

/// Warns the rate-limited refusal line, with the count it covers.
fn warn_refused(refusals: &WaitWarn, max: usize, wait: Duration) {
    if let Some(waited) = refusals.tick() {
        warn_refusals(max, waited, wait);
    }
}

/// What the accept task does after a failed accept: the debug line, the
/// rate-limited warn with the failures since its last line, the count in
/// the metrics, the error handed on to the serve, and the wait that paces
/// the next attempt - 10 ms doubling to 1 s, reset by a success, so a
/// failure that persists cannot spin the task. `false` says the serve went
/// and the task stops.
async fn pace_after_failed_accept<IO>(
    error: io::Error,
    accept_warn: &WaitWarn,
    metrics: &Metrics,
    backoff: &mut Backoff,
    done: &mpsc::Sender<io::Result<Connection<IO>>>,
) -> bool {
    tracing::debug!(
        error = ?error,
        thread = ?std::thread::current().id(),
        "the listener failed to yield a socket"
    );
    if let Some(failures) = accept_warn.tick() {
        tracing::warn!(
            failures,
            backoff = ?backoff,
            error = ?error,
            "the listener failed to accept; waiting before it accepts again"
        );
    }
    metrics.accept_error();
    backoff.wait().await;
    done.send(Err(error)).await.is_ok()
}

/// One accepted TLS connection's own task: its place first, bounded as
/// [`accept`] describes, then its TLS handshake on this task under
/// `handshake`, and the decrypted connection handed to the serve with the
/// preface deadline that its place started, so a silent peer holds its
/// place for at most `handshake` in all. A handshake that failed or timed
/// out is warned, and the io drops, so the client sees the connection
/// close.
fn spawn_tls_accepted<IO>(
    io: IO,
    acceptor: tokio_rustls::TlsAcceptor,
    accepting: Accepting,
    places: &Arc<Semaphore>,
    waiters: &Arc<Semaphore>,
    done: &mpsc::Sender<io::Result<Connection<TlsStream<IO>>>>,
    metrics: &Arc<Metrics>,
) where
    IO: AsyncRead + AsyncWrite + PeerOf + Unpin + Send + 'static,
{
    let Accepting {
        max,
        handshake,
        wait,
        stalled_bytes,
        total,
        waits,
        refusals,
        ..
    } = accepting;
    let places = Arc::clone(places);
    let waiters = Arc::clone(waiters);
    let waits = waits.clone();
    let refusals = refusals.clone();
    let sender = done.clone();
    let metrics = Arc::clone(metrics);
    let total = Arc::clone(&total);
    tokio::spawn(async move {
        // The place first, bounded: a connection over the cap waits at
        // most `wait`, then the server closes it, so a storm fails fast
        // instead of queueing behind `MAX_CONNECTION_AGE`. The wait runs
        // in the connection's own task, so one waiter never holds the
        // accept loop, and the waiters are capped, so a storm holds at
        // most `max` waiting sockets.
        let Some(place) = place_within(&places, &waiters, max, wait, &waits).await else {
            metrics.connection_refused();
            warn_refused(&refusals, max, wait);
            // The io drops here: the client sees the connection close
            // before any reply.
            return;
        };
        // One deadline, from the place: the TLS handshake and the HTTP/2
        // preface that follows share it.
        let deadline = tokio::time::Instant::now() + handshake;
        let wait = tokio::time::timeout_at(deadline, acceptor.accept(io));
        let preface = Some((
            PREFACE_BYTES,
            Box::pin(tokio::time::sleep_until(deadline)) as Pin<Box<Sleep>>,
        ));
        match wait.await {
            Ok(Ok(stream)) => {
                let peer = stream.get_ref().0.peer_of();
                // The place goes with the connection, and frees when it is
                // dropped, which the gone receiver also does.
                let _ = sender
                    .send(Ok(Connection {
                        io: stream,
                        _place: place,
                        preface,
                        stalled: Stalled::new(peer, stalled_bytes, metrics, total),
                        born: std::time::Instant::now(),
                        first_poll: true,
                    }))
                    .await;
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "a TLS handshake failed");
            }
            Err(_) => {
                tracing::warn!(
                    timeout = ?handshake,
                    "no TLS handshake within the timeout"
                );
            }
        }
    });
}

/// The TLS connections of `incoming`, at most `max` at once, as
/// [`accept`] yields plain ones: a connection takes its place first (and
/// waits at the cap, at most [`CONNECTION_WAIT`], then the server closes
/// it), and then its TLS handshake runs on its own task, under
/// `handshake`, while it holds that place. So the handshakes of different
/// connections run at the same time, and a peer that never starts or never
/// ends its handshake holds at most one of at most `max` places, for at most
/// `handshake` long. A handshake that failed or timed out drops its place,
/// with a warning. The decrypted connection goes on with the preface timer
/// of [`accept`], so a peer that stops right after the handshake is closed
/// by that timer too.
///
/// The error of `incoming` must be [`io::Error`], which is what a TCP or a
/// unix listener yields.
fn accept_tls<I, IO>(
    incoming: I,
    accepting: &Accepting,
    stop: watch::Sender<bool>,
    shutdown: impl Future<Output = ()> + Send + 'static,
    config: Arc<ServerConfig>,
    metrics: Arc<Metrics>,
) -> ReceiverStream<io::Result<Connection<TlsStream<IO>>>>
where
    I: Stream<Item = io::Result<IO>> + Send + 'static,
    IO: AsyncRead + AsyncWrite + Connected + PeerOf + Unpin + Send + 'static,
{
    // The task below owns its copy: the caller's ref must not outlive it.
    let accepting = accepting.clone();
    let Accepting {
        max,
        wait,
        waits,
        refusals,
        ..
    } = accepting.clone();
    let places = Arc::new(Semaphore::new(max));
    let waiters = Arc::new(Semaphore::new(max));
    let (done, served) = mpsc::channel(max);
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    spawn_cap_flushers(
        {
            let done = done.clone();
            move || done.is_closed()
        },
        max,
        wait,
        &waits,
        &refusals,
    );
    tokio::spawn(async move {
        let mut incoming = pin!(incoming);
        let mut shutdown = pin!(shutdown);
        let accept_warn = WaitWarn::new(ACCEPT_WARN_INTERVAL);
        let mut backoff = Backoff::new();
        loop {
            // The shutdown signal itself stops the accept: a client that
            // reconnects after the GOAWAY is refused by its connect, not
            // counted through a drain. The listener drops with this task.
            let accepted = tokio::select! {
                accepted = incoming.next() => if let Some(accepted) = accepted {
                    accepted
                } else {
                    let _ = stop.send(true);
                    break;
                },
                () = &mut shutdown => {
                    let _ = stop.send(true);
                    break;
                }
                // The serve was dropped or aborted without its signal: no
                // answer would ever be taken, so stop, and drop the
                // listener with this task.
                () = done.closed() => break,
            };
            let io = match accepted {
                Ok(io) => {
                    backoff.reset();
                    io
                }
                Err(error) => {
                    if !pace_after_failed_accept(error, &accept_warn, &metrics, &mut backoff, &done)
                        .await
                    {
                        break;
                    }
                    continue;
                }
            };
            spawn_tls_accepted(
                io,
                acceptor.clone(),
                accepting.clone(),
                &places,
                &waiters,
                &done,
                &metrics,
            );
        }
    });
    ReceiverStream::new(served)
}

/// The connections from `incoming`, at most `max` at once. Each keeps its
/// place until it is dropped, when it closes. A connection over the cap
/// waits for a place on its own task, for at most [`CONNECTION_WAIT`]: a
/// place freed in time still serves it, and one that is not is closed,
/// counted in the refused-connections metric. Each must send the preface
/// within `handshake` once it has its place.
fn accept<I, IO>(
    incoming: I,
    accepting: &Accepting,
    stop: watch::Sender<bool>,
    shutdown: impl Future<Output = ()> + Send + 'static,
    metrics: Arc<Metrics>,
) -> ReceiverStream<io::Result<Connection<IO>>>
where
    I: Stream<Item = io::Result<IO>> + Send + 'static,
    IO: PeerOf + Send + 'static,
{
    let Accepting {
        max,
        handshake,
        wait,
        stalled_bytes,
        total,
        waits,
        refusals,
        ..
    } = accepting.clone();
    let places = Arc::new(Semaphore::new(max));
    let waiters = Arc::new(Semaphore::new(max));
    let (done, served) = mpsc::channel(max);
    spawn_cap_flushers(
        {
            let done = done.clone();
            move || done.is_closed()
        },
        max,
        wait,
        &waits,
        &refusals,
    );
    tokio::spawn(async move {
        let mut incoming = pin!(incoming);
        let mut shutdown = pin!(shutdown);
        let accept_warn = WaitWarn::new(ACCEPT_WARN_INTERVAL);
        let mut backoff = Backoff::new();
        loop {
            // The shutdown signal itself stops the accept: a client that
            // reconnects after the GOAWAY is refused by its connect, not
            // counted through a drain. The listener drops with this task.
            let accepted = tokio::select! {
                accepted = incoming.next() => if let Some(accepted) = accepted {
                    accepted
                } else {
                    let _ = stop.send(true);
                    break;
                },
                () = &mut shutdown => {
                    let _ = stop.send(true);
                    break;
                }
                // The serve was dropped or aborted without its signal: no
                // answer would ever be taken, so stop, and drop the
                // listener with this task.
                () = done.closed() => break,
            };
            // The accept's marks (issue #795): the moment the listener's
            // socket reaches this task, on the thread that saw it, and -
            // on the connection's first read - the wait it had. The gap
            // between a client's connect and the first line is the
            // accept's delay; the gap to the second is the connection
            // task's start delay. An accept error is logged at debug and
            // warned rate-limited by the pacer; tonic logs it at trace
            // only, below every default filter.
            let born = std::time::Instant::now();
            let io = match accepted {
                Ok(io) => {
                    backoff.reset();
                    io
                }
                Err(error) => {
                    if !pace_after_failed_accept(error, &accept_warn, &metrics, &mut backoff, &done)
                        .await
                    {
                        break;
                    }
                    continue;
                }
            };
            tracing::debug!(
                listener = ?io.local_of(),
                peer = ?io.peer_of(),
                thread = ?std::thread::current().id(),
                "the listener yielded a socket"
            );
            let places = Arc::clone(&places);
            let waiters = Arc::clone(&waiters);
            let waits = waits.clone();
            let refusals = refusals.clone();
            let sender = done.clone();
            let metrics = Arc::clone(&metrics);
            let total = Arc::clone(&total);
            tokio::spawn(async move {
                // The place first, bounded, as the TLS path does: a
                // connection over the cap waits at most `wait`, then the
                // server closes it, so a storm fails fast instead of
                // queueing behind `MAX_CONNECTION_AGE`. The wait runs in
                // the connection's own task, so one waiter never holds the
                // accept loop, and the waiters are capped, so a storm
                // holds at most `max` waiting sockets.
                let Some(place) = place_within(&places, &waiters, max, wait, &waits).await else {
                    metrics.connection_refused();
                    warn_refused(&refusals, max, wait);
                    // The io drops here: the client sees the connection
                    // close before any reply.
                    return;
                };
                let peer = io.peer_of();
                let _ = sender
                    .send(Ok(Connection {
                        io,
                        _place: place,
                        preface: Some((PREFACE_BYTES, Box::pin(tokio::time::sleep(handshake)))),
                        stalled: Stalled::new(peer, stalled_bytes, metrics, total),
                        born,
                        first_poll: true,
                    }))
                    .await;
            });
        }
    });
    ReceiverStream::new(served)
}

/// The peer of a connected stream, for the audit's refusal lines and
/// the call span's `peer` field: the connect info a request carries
/// below the router is the only peer a call still has.
#[allow(unreachable_pub)]
pub trait PeerOf {
    /// The peer of the stream, when it is a socket.
    fn peer_of(&self) -> Option<SocketAddr>;

    /// The stream's own address, when it is a socket: for an accepted
    /// TCP socket this is the listener's, which names the server. A Unix
    /// socket names none.
    fn local_of(&self) -> Option<SocketAddr> {
        None
    }
}

impl PeerOf for tokio::net::TcpStream {
    fn peer_of(&self) -> Option<SocketAddr> {
        self.peer_addr().ok()
    }

    fn local_of(&self) -> Option<SocketAddr> {
        self.local_addr().ok()
    }
}

impl<IO: PeerOf> PeerOf for TlsStream<IO> {
    fn peer_of(&self) -> Option<SocketAddr> {
        self.get_ref().0.peer_of()
    }

    fn local_of(&self) -> Option<SocketAddr> {
        self.get_ref().0.local_of()
    }
}

/// A Unix socket has no peer address: the audit's refusal lines name no
/// peer for it.
impl PeerOf for UnixStream {
    fn peer_of(&self) -> Option<SocketAddr> {
        None
    }
}

/// A connection that keeps its place of [`accept`] until it is dropped.
struct Connection<IO> {
    io: IO,
    _place: OwnedSemaphorePermit,
    /// Until the preface has come: its bytes still to come, and the end of
    /// the wait for them.
    preface: Option<(usize, Pin<Box<Sleep>>)>,
    /// What the stalled replies of this connection hold, and their limit.
    stalled: Stalled,
    /// When the socket reached the accept's task, for the first-poll mark.
    born: std::time::Instant,
    /// Whether the server has read the connection yet: the first read
    /// logs itself once, with the wait it had.
    first_poll: bool,
}

impl<IO> Connected for Connection<IO> {
    type ConnectInfo = Stalled;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.stalled.clone()
    }
}

impl<IO: AsyncRead + PeerOf + Unpin> AsyncRead for Connection<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        if std::mem::take(&mut this.first_poll) {
            tracing::debug!(
                listener = ?this.io.local_of(),
                waited = ?this.born.elapsed(),
                thread = ?std::thread::current().id(),
                "the server read the connection for the first time"
            );
        }
        // Over the limit, the connection is closed, so hyper frees what its
        // stalled streams hold.
        if this.stalled.over() {
            return Poll::Ready(Err(this.stalled.error()));
        }
        // The waker, so a crossing of the limit is seen without a frame from
        // the client.
        this.stalled.register(cx.waker());
        let before = buf.filled().len();
        let polled = Pin::new(&mut this.io).poll_read(cx, buf);
        if let Some((left, wait)) = &mut this.preface {
            match polled {
                Poll::Ready(Ok(())) => {
                    let read = buf.filled().len() - before;
                    *left = left.saturating_sub(read);
                    // The preface is in, or the client closed.
                    if *left == 0 || read == 0 {
                        this.preface = None;
                    }
                }
                Poll::Pending if wait.as_mut().poll(cx).is_ready() => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "no HTTP/2 preface within the handshake timeout",
                    )));
                }
                _ => {}
            }
        }
        polled
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for Connection<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.stalled.over() {
            return Poll::Ready(Err(self.stalled.error()));
        }
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

/// Checks that only this user can reach `dir`: a real directory (not a
/// symlink), owned by the effective user, with no access for group or
/// others, under directories that no other user can change
/// ([`check_ancestors`](crate::runtime::auth::check_ancestors)). Then nobody
/// else can read what is inside, or put a file there. Returns `dir` with its
/// parent resolved: use it, not `dir`, so no symlink is followed again.
/// Fails with `PermissionDenied`, naming the rule that `dir` breaks.
///
/// # Errors
///
/// [`io::ErrorKind::PermissionDenied`] if `dir` breaks one of the rules.
pub fn check_private_dir(dir: &Path) -> io::Result<std::path::PathBuf> {
    let me = rustix::process::geteuid().as_raw();
    let dir = crate::runtime::auth::check_ancestors(dir, me)?;
    let meta = std::fs::symlink_metadata(&dir)?;
    let refuse = |why: &str| {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{} {why}", dir.display()),
        ))
    };
    if !meta.is_dir() {
        return refuse("is not a directory (a symlink is refused too)");
    }
    if meta.uid() != me {
        return refuse("is owned by another user");
    }
    if meta.mode() & 0o077 != 0 {
        return refuse("is open to other users; make it mode 0700");
    }
    Ok(dir)
}

/// Listens on a Unix socket at `path`, in a directory that only this user
/// can reach: a real directory, owned by this user, closed to others. So
/// nobody else can connect, even in the
/// moment before the socket gets mode 0600, or put another socket in its
/// place. Replaces a socket file that nothing listens on, such as one left
/// by a crash.
///
/// # Errors
///
/// [`io::ErrorKind::PermissionDenied`] if the directory is not private,
/// [`io::ErrorKind::AddrInUse`] if a process listens on `path`, or I/O errors.
pub async fn bind_unix(path: &Path) -> io::Result<UnixListener> {
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} names no socket file", path.display()),
        )
    })?;
    // The checked directory, so no symlink in `path` is followed again.
    let path = &check_private_dir(dir)?.join(name);
    match std::fs::symlink_metadata(path) {
        // A socket file that nothing listens on is ours from an earlier
        // run that died: remove it and bind again.
        Ok(meta) if meta.file_type().is_socket() => {
            if UnixStream::connect(path).await.is_ok() {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("a process listens on {}", path.display()),
                ));
            }
            std::fs::remove_file(path)?;
        }
        // Anything else already there - above all a symlink, which bind
        // would follow to a path this check never saw - is refused with
        // the path named.
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "{} exists and is not a socket file; remove it and start again",
                    path.display()
                ),
            ));
        }
        Err(_) => {}
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::net::SocketAddr;
    use std::path::Path;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
        KeyUsagePurpose,
    };
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio_rustls::rustls::pki_types::{
        CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _,
    };
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};
    use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
    use tonic::{Code, Request, Response, Status, Streaming};

    use super::*;
    use crate::Bytes;
    use crate::runtime::object::Header;
    use crate::runtime::reply::{self, Body, IN_FLIGHT_BYTES, Outgoing};
    use crate::runtime::tls::ServerTls;
    use crate::v1::objects_client::ObjectsClient;
    use crate::v1::objects_server::{Objects, ObjectsServer};
    use crate::v1::{
        Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind,
        ObjectPart, ResolveRefRequest, ResolveRefResponse,
    };
    use tokio::net::TcpStream;

    /// A wait that ends at once when nothing holds the call back.
    const HELD: Duration = Duration::from_millis(500);

    /// Short timers, each of 100 ms, except `slow`.
    fn timers(slow: &str) -> Timers {
        let (short, long) = (Duration::from_millis(100), Duration::from_secs(3600));
        let pick = |name| if name == slow { long } else { short };
        Timers {
            handshake: pick("handshake"),
            keepalive: pick("keepalive"),
            keepalive_timeout: pick("keepalive"),
            age: pick("age"),
            grace: pick("age"),
            wait: short,
        }
    }

    /// Replies to `GetObjects` with one object in three parts of 128 KiB, so
    /// a client that does not read stalls the reply after its first part.
    /// The reply runs through the real [`reply::send_objects`], so it counts
    /// what it leaves on its connection. Refuses `ResolveRef` at once, with
    /// no data, so a full window does not hold that reply.
    #[derive(Clone)]
    struct Stalls;

    #[tonic::async_trait]
    impl Objects for Stalls {
        async fn resolve_ref(
            &self,
            request: Request<ResolveRefRequest>,
        ) -> Result<Response<ResolveRefResponse>, Status> {
            let peer = Stalled::of(&request).and_then(|s| s.peer());
            Err(Status::not_found(format!("no ref; peer is {peer:?}")))
        }

        async fn get_root_tree(
            &self,
            _: Request<GetRootTreeRequest>,
        ) -> Result<Response<GetRootTreeResponse>, Status> {
            Err(Status::unimplemented("not in the stub"))
        }

        type GetObjectsStream = reply::ObjectStream;

        async fn get_objects(
            &self,
            request: Request<GetObjectsRequest>,
        ) -> Result<Response<Self::GetObjectsStream>, Status> {
            // More parts than a client's 64 KiB window and hyper's read
            // ahead take, so a client that does not read stalls the reply
            // after its first parts.
            let chunk = Bytes::from(vec![0; 128 << 10]);
            let parts = 10;
            let header = Header {
                kind: ObjectKind::Blob,
                id: [2; 32],
                encoding: Encoding::Raw,
                encoded_size: parts * chunk.len(),
                size: parts * chunk.len(),
            };
            let object = Outgoing {
                header,
                body: Body::Chunks(vec![chunk; parts]),
            };
            // The budget of this stub: one object at a time, as a reply
            // holds.
            let budget = Arc::new(Semaphore::new(IN_FLIGHT_BYTES));
            let permit = reply::try_take(&budget, 1).unwrap();
            let (tx, stream, slot) = reply::channel();
            // The stall is short, so the test does not wait for the real
            // one; the counting is what the test watches.
            let stall = Duration::from_millis(100);
            tokio::spawn(reply::send_objects(
                (Ok(object), permit),
                [(); 0],
                tx,
                reply::ReplyStall::new(stall, Stalled::of(&request), slot),
                1,
                |()| async { None },
            ));
            Ok(Response::new(stream))
        }
    }

    /// A leaf certificate and its key, both PEM.
    struct TestCert {
        cert_pem: String,
        key_pem: String,
    }

    /// A certificate authority for one test run: it signs the server and
    /// client certificates of the test. Everything stays in memory, except
    /// the PEM files a test writes into its temp directory.
    struct TestAuthority {
        params: CertificateParams,
        key: KeyPair,
    }

    impl TestAuthority {
        fn new() -> Self {
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
            Self {
                params,
                key: KeyPair::generate().unwrap(),
            }
        }

        /// Signs a leaf for `names`, used as `purpose`.
        fn sign(&self, names: &[&str], purpose: ExtendedKeyUsagePurpose) -> TestCert {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(
                names
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            params.extended_key_usages = vec![purpose];
            let issuer = Issuer::from_params(&self.params, &self.key);
            let cert = params.signed_by(&key, &issuer).unwrap();
            TestCert {
                cert_pem: cert.pem(),
                key_pem: key.serialize_pem(),
            }
        }

        /// The certificate of the authority itself, as a client trusts it.
        fn cert(&self) -> TestCert {
            let cert = self.params.self_signed(&self.key).unwrap();
            TestCert {
                cert_pem: cert.pem(),
                key_pem: self.key.serialize_pem(),
            }
        }
    }

    /// The certificates of one test: an authority, a server certificate for
    /// `localhost` and `127.0.0.1`, a client certificate of that authority,
    /// and a second authority with a client certificate of its own, so a
    /// test can trust or present the wrong one.
    struct TestCerts {
        authority: TestAuthority,
        server: TestCert,
        client: TestCert,
        stranger: TestAuthority,
        stranger_client: TestCert,
    }

    fn test_certs() -> TestCerts {
        let authority = TestAuthority::new();
        let server = authority.sign(
            &["localhost", "127.0.0.1"],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let client = authority.sign(&["client"], ExtendedKeyUsagePurpose::ClientAuth);
        let stranger = TestAuthority::new();
        let stranger_client = stranger.sign(&["client"], ExtendedKeyUsagePurpose::ClientAuth);
        TestCerts {
            authority,
            server,
            client,
            stranger,
            stranger_client,
        }
    }

    /// Writes `pem` into the temp directory of a test, as `name`, 0600:
    /// the keys are secrets, and the checked read refuses one that group
    /// or others can read.
    fn write(dir: &Path, name: &str, pem: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let path = dir.join(name);
        std::fs::write(&path, pem).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        path
    }

    /// A `ServerTls` for `server`, with mTLS when `client_ca` is given.
    fn server_tls(dir: &Path, server: &TestCert, client_ca: Option<&TestCert>) -> ServerTls {
        let cert = write(dir, "server.pem", &server.cert_pem);
        let key = write(dir, "server.key", &server.key_pem);
        let client_ca = client_ca.map(|ca| write(dir, "client-ca.pem", &ca.cert_pem));
        ServerTls::load(&cert, &key, client_ca.as_deref()).unwrap()
    }

    /// A client of the stub at `addr`, trusting the CA `ca`, presenting
    /// `identity` with mTLS. Fails when the server refuses the handshake.
    async fn client(
        addr: SocketAddr,
        ca: &TestCert,
        identity: Option<&TestCert>,
    ) -> Result<ObjectsClient<Channel>, tonic::transport::Error> {
        let endpoint = Endpoint::from_shared(format!("https://{addr}")).unwrap();
        let mut tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(&ca.cert_pem));
        if let Some(cert) = identity {
            tls = tls.identity(Identity::from_pem(&cert.cert_pem, &cert.key_pem));
        }
        let channel = endpoint.tls_config(tls)?.connect().await?;
        Ok(ObjectsClient::new(channel))
    }

    /// A raw TLS client of `addr`, trusting the CA `ca`, with `identity`.
    /// For tests that talk to the server by hand.
    async fn raw_client(
        addr: SocketAddr,
        ca: &TestCert,
        identity: Option<&TestCert>,
    ) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
        raw_connect(addr, ca, identity).await.unwrap()
    }

    /// A raw TLS client of `addr`, trusting the CA `ca`, with `identity`.
    /// The handshake's own result stays with the caller: a refusal can
    /// end the handshake from the server's side, and the caller reads
    /// what the server sent back either way.
    async fn raw_connect(
        addr: SocketAddr,
        ca: &TestCert,
        identity: Option<&TestCert>,
    ) -> io::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(ca.cert_pem.as_bytes()).unwrap())
            .unwrap();
        let builder = ClientConfig::builder_with_provider(crate::runtime::tls::provider())
            .with_safe_default_protocol_versions()
            .unwrap();
        let config = match identity {
            None => builder.with_root_certificates(roots).with_no_client_auth(),
            Some(cert) => {
                let der = CertificateDer::from_pem_slice(cert.cert_pem.as_bytes()).unwrap();
                let key = PrivateKeyDer::from_pem_slice(cert.key_pem.as_bytes()).unwrap();
                builder
                    .with_root_certificates(roots)
                    .with_client_auth_cert(vec![der], key)
                    .unwrap()
            }
        };
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        connector
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
    }

    /// The stub, served within `limits` with `timers`, TLS with
    /// `server_tls` whose handshakes end within `tls_timeout`, and its
    /// address.
    async fn serve_tls(
        limits: Limits,
        timers: Timers,
        server_tls: &ServerTls,
        tls_timeout: Duration,
    ) -> (SocketAddr, Arc<Metrics>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let config = server_tls.config();
        let metrics = Arc::new(Metrics::default());
        tokio::spawn({
            let metrics = Arc::clone(&metrics);
            async move {
                let router = |mut server: Server| server.add_service(ObjectsServer::new(Stalls));
                let pending = std::future::pending();
                let timers = Timers {
                    handshake: tls_timeout,
                    ..timers
                };
                Box::pin(limits.serve_with(
                    &Accepting::of(&limits, &timers),
                    router,
                    tcp(listener),
                    Some(config),
                    pending,
                    metrics,
                ))
                .await
            }
        });
        (addr, metrics)
    }

    /// A log writer that collects what the test binary logs.
    #[derive(Clone, Default)]
    struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = Captured;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn log() -> Captured {
        static LOG: std::sync::OnceLock<Captured> = std::sync::OnceLock::new();
        LOG.get_or_init(Captured::default).clone()
    }

    /// Serializes the tests that count accept-warning lines in the shared
    /// captured log: each writes exactly one, so they must not overlap.
    /// Async-aware, because the window spans the test's awaits.
    static ACCEPT_WARN_LOG: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn init_log() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_max_level(tracing::level_filters::LevelFilter::WARN)
                .with_writer(log())
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        });
    }

    /// A listener that never yields a socket: every accept fails at once,
    /// as the process out of file descriptors makes every real listener
    /// fail.
    struct AlwaysFail(Arc<std::sync::atomic::AtomicU64>);

    impl Stream for AlwaysFail {
        type Item = io::Result<TcpStream>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Poll::Ready(Some(Err(io::Error::other("every accept fails"))))
        }
    }

    /// A persistent accept failure must not spin a core: the accept task
    /// paces itself (10 ms doubling to 1 s, reset by a success), counts
    /// the failures in `scion_accept_errors_total`, and writes its
    /// rate-limited warning.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_listener_that_fails_every_accept_is_paced_and_counted() {
        let _log_window = ACCEPT_WARN_LOG.lock().await;
        init_log();
        // The captured log is process-wide; only this test's own lines
        // count, so the scan starts at the offset this test begins at.
        let log_start = log().0.lock().unwrap().len();
        let limits = Limits::default();
        let metrics = Arc::new(Metrics::default());
        let attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        tokio::spawn({
            let metrics = Arc::clone(&metrics);
            let attempts = Arc::clone(&attempts);
            async move {
                let pending = std::future::pending();
                let _ = limits
                    .serve_with(
                        &Accepting::of(&limits, &TIMERS),
                        |mut server: Server| server.add_service(ObjectsServer::new(Stalls)),
                        AlwaysFail(Arc::clone(&attempts)),
                        None,
                        pending,
                        metrics,
                    )
                    .await;
            }
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        let attempts = attempts.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            attempts > 0 && attempts <= 20,
            "failed accepts in 3 s: {attempts} - a persistent failure must be paced, \
             not retried at once"
        );
        let text = metrics.render();
        let line = text
            .lines()
            .find(|l| l.starts_with("scion_accept_errors_total "))
            .unwrap_or_else(|| panic!("the render has no accept errors:\n{text}"));
        assert_ne!(
            line, "scion_accept_errors_total 0",
            "the failed accepts are not counted:\n{text}"
        );
        let text = String::from_utf8(log().0.lock().unwrap()[log_start..].to_vec()).unwrap();
        let warns: Vec<&str> = text
            .lines()
            .filter(|l| {
                l.contains("the listener failed to accept; waiting before it accepts again")
            })
            .collect();
        assert_eq!(
            warns.len(),
            1,
            "the accept warning must be written once in 3 s, not spam or silence:\n{text}"
        );
        let failures = warns[0]
            .split("failures=")
            .nth(1)
            .and_then(|n| n.split(' ').next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);
        assert!(
            failures >= 1,
            "the warning must name at least one failure: {}",
            warns[0]
        );
    }

    /// An incoming stream whose first item is an accept failure; the rest
    /// come from the real listener it wraps.
    struct FailFirst<S> {
        failed: bool,
        inner: S,
    }

    impl<S> Stream for FailFirst<S>
    where
        S: Stream<Item = io::Result<TcpStream>> + Unpin,
    {
        type Item = io::Result<TcpStream>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if !self.failed {
                self.failed = true;
                return Poll::Ready(Some(Err(io::Error::other("the first accept fails"))));
            }
            Pin::new(&mut self.inner).poll_next(cx)
        }
    }

    /// One failed accept must not end the serve: the listener keeps taking
    /// connections after it. A failed accept that ends the serve is a
    /// server that stops taking connections.
    #[tokio::test]
    async fn a_failed_accept_does_not_end_the_serve() {
        let _log_window = ACCEPT_WARN_LOG.lock().await;
        init_log();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = FailFirst {
            failed: false,
            inner: tcp(listener),
        };
        let limits = Limits::default();
        let metrics = Arc::new(Metrics::default());
        tokio::spawn(async move {
            let router = |mut server: Server| server.add_service(ObjectsServer::new(Stalls));
            let pending = std::future::pending();
            let _ = limits
                .serve_with(
                    &Accepting::of(&limits, &TIMERS),
                    router,
                    incoming,
                    None,
                    pending,
                    metrics,
                )
                .await;
        });
        let mut client = ObjectsClient::new(endpoint(addr).connect().await.unwrap());
        assert_eq!(quick(&mut client).await, Code::NotFound);
    }

    /// The same promise through the TLS accept path: one failed accept
    /// there must not end the serve either.
    #[tokio::test]
    async fn a_failed_tls_accept_does_not_end_the_serve() {
        let _log_window = ACCEPT_WARN_LOG.lock().await;
        init_log();
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, Some(&certs.authority.cert()));
        let config = server_tls.config();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = FailFirst {
            failed: false,
            inner: tcp(listener),
        };
        let limits = Limits::default();
        let metrics = Arc::new(Metrics::default());
        tokio::spawn(async move {
            let router = |mut server: Server| server.add_service(ObjectsServer::new(Stalls));
            let pending = std::future::pending();
            let _ = limits
                .serve_with(
                    &Accepting::of(&limits, &TIMERS),
                    router,
                    incoming,
                    Some(config),
                    pending,
                    metrics,
                )
                .await;
        });
        let mut client = client(addr, &certs.authority.cert(), Some(&certs.client))
            .await
            .unwrap();
        assert_eq!(quick(&mut client).await, Code::NotFound);
    }

    /// The stub, served within `limits` with `timers`, and its address.
    async fn serve(limits: Limits, timers: Timers) -> (SocketAddr, Arc<Metrics>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metrics = Arc::new(Metrics::default());
        tokio::spawn({
            let metrics = Arc::clone(&metrics);
            async move {
                let router = |mut server: Server| server.add_service(ObjectsServer::new(Stalls));
                let pending = std::future::pending();
                limits
                    .serve_with(
                        &Accepting::of(&limits, &timers),
                        router,
                        tcp(listener),
                        None,
                        pending,
                        metrics,
                    )
                    .await
            }
        });
        (addr, metrics)
    }

    fn endpoint(addr: SocketAddr) -> Endpoint {
        Endpoint::from_shared(format!("http://{addr}")).unwrap()
    }

    /// A `GetObjects` reply that the caller never reads.
    async fn stall(client: &mut ObjectsClient<Channel>) -> Streaming<ObjectPart> {
        let request = GetObjectsRequest::default();
        client.get_objects(request).await.unwrap().into_inner()
    }

    /// The code of a call that the stub answers at once.
    async fn quick(client: &mut ObjectsClient<Channel>) -> Code {
        let request = ResolveRefRequest::default();
        client.resolve_ref(request).await.unwrap_err().code()
    }

    /// The deadline the refusal tests put on their client's handshake
    /// and on its read of the refusal: the server answers a failed
    /// handshake at once (it drops the stream with an alert), so
    /// anything that hangs past this is not the refusal.
    const REFUSAL_BOUND: Duration = Duration::from_secs(10);

    /// Whether the mTLS server of `addr` refuses a raw TLS client of
    /// `ca` and `identity`. The answer is what the wire gives: the
    /// handshake ending in an error (the server's alert inside it), a
    /// read that ends with an alert or an error, or the clean close.
    /// The refusal is read from the wire, not through a stub's
    /// channel: the client's side of the TLS 1.3 handshake can end
    /// before the server has checked its certificate, and a channel
    /// that holds such a session reconnects under the refusal — its
    /// calls wait on a readiness that never comes, instead of
    /// answering the refusal. A served byte is not a refusal; a server
    /// that neither alerts nor closes fails with the bound.
    async fn a_raw_client_is_refused(
        addr: SocketAddr,
        ca: &TestCert,
        identity: Option<&TestCert>,
    ) -> bool {
        let mut peer = match raw_connect(addr, ca, identity).await {
            Ok(peer) => peer,
            Err(_refused_in_the_handshake) => return true,
        };
        let mut buf = [0u8; 64];
        match tokio::time::timeout(REFUSAL_BOUND, peer.read(&mut buf)).await {
            Err(elapsed) => {
                panic!("the server neither alerted nor closed in {REFUSAL_BOUND:?}: {elapsed}")
            }
            // The close and the alert both prove the refusal.
            Ok(Ok(0) | Err(_)) => true,
            Ok(Ok(_)) => false,
        }
    }

    /// Whether the server of `addr` serves a raw TLS client of `ca`
    /// and `identity`: the client sends the HTTP/2 client preface and
    /// an empty SETTINGS frame, and reads the server's answer. The
    /// positive control of the refusal tests: the same server and the
    /// same raw client path as [`a_raw_client_is_refused`] must serve
    /// a client the server's own CA signed, so a refusal test cannot
    /// pass on a handshake error that is not a refusal.
    async fn a_raw_client_is_served(addr: SocketAddr, ca: &TestCert, identity: &TestCert) -> bool {
        use tokio::io::AsyncWriteExt as _;
        let mut peer = match raw_connect(addr, ca, Some(identity)).await {
            Ok(peer) => peer,
            Err(_refused_in_the_handshake) => return false,
        };
        // The HTTP/2 client preface, then an empty SETTINGS frame.
        let preface: &[u8] =
            b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00";
        let read = tokio::time::timeout(REFUSAL_BOUND, async {
            peer.write_all(preface).await?;
            let mut buf = [0u8; 64];
            peer.read(&mut buf).await
        })
        .await;
        match read {
            Err(elapsed) => panic!("the served control hung in {REFUSAL_BOUND:?}: {elapsed}"),
            // The close and the alert both mean the refusal.
            Ok(Err(_) | Ok(0)) => false,
            // The server answered with its own frames: it served the
            // connection.
            Ok(Ok(_)) => true,
        }
    }

    /// Whether the process closes a raw TCP connection that sends `sent` and
    /// then never answers, within 10 s.
    async fn closes_a_peer_that_sends(addr: SocketAddr, sent: &'static [u8]) -> bool {
        let closed = tokio::task::spawn_blocking(move || {
            let mut peer = std::net::TcpStream::connect(addr)?;
            peer.write_all(sent)?;
            peer.set_read_timeout(Some(Duration::from_secs(10)))?;
            // Ends when the process closes the connection, with an end of
            // file or a reset.
            peer.read_to_end(&mut Vec::new())
        });
        match closed.await.unwrap() {
            Ok(_) => true,
            Err(e) => !matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ),
        }
    }

    /// A client that stops reading keeps its streams open. With the streams
    /// of its connection all open, its next call waits, and goes on once one
    /// of them ends.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_connection_opens_at_most_its_streams() {
        let streams = 4;
        let limits = Limits {
            streams: NonZeroU32::new(streams).unwrap(),
            ..Limits::default()
        };
        let (addr, _) = serve(limits, TIMERS).await;
        // The smallest windows, so no reply ends while it is unread.
        let channel = endpoint(addr)
            .initial_stream_window_size(65_535)
            .initial_connection_window_size(65_535)
            .connect()
            .await
            .unwrap();
        let mut client = ObjectsClient::new(channel);
        let mut stalled = Vec::new();
        for _ in 0..streams {
            stalled.push(stall(&mut client).await);
        }

        let mut next = client.clone();
        let mut next = pin!(quick(&mut next));
        assert!(
            tokio::time::timeout(HELD, &mut next).await.is_err(),
            "a stream beyond the cap was opened"
        );
        drop(stalled.pop());
        let code = tokio::time::timeout(Duration::from_secs(10), next)
            .await
            .expect("the call goes on once a stream ends");
        assert_eq!(code, Code::NotFound);
    }

    /// With all its connections open, a process leaves a new one unread, and
    /// serves it once another closes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_process_serves_at_most_its_connections() {
        let connections = 2;
        let limits = Limits {
            connections: NonZeroUsize::new(connections).unwrap(),
            ..Limits::default()
        };
        let (addr, _) = serve(limits, TIMERS).await;
        let endpoint = endpoint(addr);
        let mut open = Vec::new();
        for _ in 0..connections {
            let mut client = ObjectsClient::new(endpoint.connect().await.unwrap());
            // Served, so the process holds a place for it.
            assert_eq!(quick(&mut client).await, Code::NotFound);
            open.push(client);
        }

        let mut extra = ObjectsClient::new(endpoint.connect_lazy());
        let mut call = pin!(quick(&mut extra));
        assert!(
            tokio::time::timeout(HELD, &mut call).await.is_err(),
            "a connection beyond the cap was served"
        );
        // Its connection closes once no handle is left.
        drop(open.pop());
        let code = tokio::time::timeout(Duration::from_secs(10), call)
            .await
            .expect("the connection is served once another closes");
        assert_eq!(code, Code::NotFound);
    }

    /// A client that keeps its connection, idle, for life still gives its
    /// place up once the connection is old, so a waiting one gets served.
    /// The wait is long on purpose: this is not the refusal test, the new
    /// connection must outwait the age, not the place wait.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_old_connection_gives_its_place_up() {
        let limits = Limits {
            connections: NonZeroUsize::MIN,
            ..Limits::default()
        };
        let mut timers = timers("keepalive");
        timers.wait = Duration::from_secs(3600);
        let (addr, _) = serve(limits, timers).await;
        let endpoint = endpoint(addr);
        let mut old = ObjectsClient::new(endpoint.connect().await.unwrap());
        assert_eq!(quick(&mut old).await, Code::NotFound);

        let mut new = ObjectsClient::new(endpoint.connect_lazy());
        let code = tokio::time::timeout(Duration::from_secs(10), quick(&mut new))
            .await
            .expect("the old connection gives its place up");
        assert_eq!(code, Code::NotFound);
        drop(old);
    }

    /// The TLS path refuses under the cap the same way: the second
    /// connection's handshake does not complete inside the bound, while
    /// the first keeps working, and the refusal is counted.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_connection_over_the_tls_cap_fails_fast() {
        let wait = Duration::from_millis(500);
        let limits = Limits {
            connections: NonZeroUsize::MIN,
            ..Limits::default()
        };
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, Some(&certs.authority.cert()));
        // The handshake deadline is the real one (a cold mTLS handshake
        // does not finish in a test instant); the place wait stays short,
        // the refusal is what the test watches.
        let (addr, metrics) = serve_tls(
            limits,
            cap_timers(wait),
            &server_tls,
            Duration::from_secs(20),
        )
        .await;
        // The first connection is served and holds the only place.
        let mut first = client(addr, &certs.authority.cert(), Some(&certs.client))
            .await
            .unwrap();
        assert_eq!(quick(&mut first).await, Code::NotFound);
        // A second mTLS client connects: served or refused, the handshake
        // ends inside the bound either way; the refused count tells which.
        let started = tokio::time::Instant::now();
        let second = tokio::time::timeout(
            wait * 3,
            raw_connect(addr, &certs.authority.cert(), Some(&certs.client)),
        )
        .await
        .expect("the second handshake ends inside the bound");
        let elapsed = started.elapsed();
        assert!(
            elapsed < wait * 3,
            "the second handshake ends inside the bound, took {elapsed:?}"
        );
        assert!(
            second.is_err(),
            "the second connection is refused, not served"
        );
        assert_eq!(quick(&mut first).await, Code::NotFound);
        assert_eq!(refused_total(&metrics), 1, "the refusal is counted");
    }

    /// A wait warning is one line per interval under concurrency: of many
    /// tasks ticking together, exactly one writes, and every wait is
    /// counted once across the line and the flush.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_wait_warning_is_one_line_under_concurrency() {
        let warn = WaitWarn::new(Duration::from_secs(3600));
        let barrier = Arc::new(tokio::sync::Barrier::new(50));
        let mut waits = Vec::new();
        for _ in 0..50 {
            let warn = warn.clone();
            let barrier = Arc::clone(&barrier);
            waits.push(tokio::spawn(async move {
                barrier.wait().await;
                warn.tick()
            }));
        }
        let mut lines = Vec::new();
        for wait in waits {
            if let Some(count) = wait.await.unwrap() {
                lines.push(count);
            }
        }
        assert_eq!(lines.len(), 1, "one line per interval, not one per task");
        // A return to store-and-forget would lose the waits that raced the
        // line: the line's count and what is left are the whole interval's.
        let rest = warn.flush_all().unwrap_or_default();
        assert_eq!(
            lines[0] + rest,
            50,
            "every wait is counted once: line {} + left {rest}",
            lines[0]
        );
    }

    /// The flusher writes the last interval's count even when no new
    /// connection came, so a count never sits stale.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_wait_count_is_flushed_when_the_interval_passes() {
        let warn = WaitWarn::new(Duration::from_millis(50));
        assert_eq!(warn.tick(), Some(1), "the first wait writes at once");
        assert_eq!(warn.tick(), None, "a wait within the interval is counted");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(warn.flush(), Some(1), "the interval's count goes out");
        assert_eq!(warn.flush(), None, "nothing is left to flush");
    }

    /// The waits line names the cap and the count it covers: the operator
    /// reading the log learns which cap and how many connections.
    #[tokio::test]
    async fn the_waits_line_names_the_cap_and_the_count() {
        init_log();
        let warn = WaitWarn::new(Duration::from_millis(1));
        let waited = warn.tick().unwrap();
        warn_waited(7, waited);
        let text = String::from_utf8(log().0.lock().unwrap().clone()).unwrap();
        let line = text
            .lines()
            .rev()
            .find(|l| l.contains("a connection waits"))
            .unwrap_or_else(|| panic!("no waits line in the log:\n{text}"));
        assert!(line.contains("max=7"), "the line names the cap: {line}");
        assert!(
            line.contains("waited="),
            "the line carries the count: {line}"
        );
    }

    /// The refusals line names the cap, the count, and the wait: an
    /// operator learns which cap closed how many connections after how
    /// long.
    #[tokio::test]
    async fn the_refusals_line_names_the_cap_the_count_and_the_wait() {
        init_log();
        warn_refusals(7, 3, Duration::from_millis(250));
        let text = String::from_utf8(log().0.lock().unwrap().clone()).unwrap();
        let line = text
            .lines()
            .rev()
            .find(|l| l.contains("a connection that did not"))
            .unwrap_or_else(|| panic!("no refusals line in the log:\n{text}"));
        assert!(line.contains("max=7"), "the line names the cap: {line}");
        assert!(
            line.contains("waited=3"),
            "the line carries the count: {line}"
        );
        assert!(
            line.contains("wait=250ms"),
            "the line carries the wait: {line}"
        );
    }

    /// A refused connection's warning is rate limited by the refusals'
    /// own limiter: the first refusal writes, a second within the
    /// interval does not.
    #[tokio::test]
    async fn warn_refused_writes_the_rate_limited_refusal() {
        init_log();
        let refusals = WaitWarn::new(Duration::from_millis(1));
        warn_refused(&refusals, 7, Duration::from_millis(250));
        let text = String::from_utf8(log().0.lock().unwrap().clone()).unwrap();
        let line = text
            .lines()
            .rev()
            .find(|l| l.contains("a connection that did not"))
            .unwrap_or_else(|| panic!("no refusal line in the log:\n{text}"));
        assert!(
            line.contains("waited=1"),
            "the first refusal writes: {line}"
        );
    }

    /// The flusher writes each interval's count even with no new event,
    /// and stops when the serve has ended. With the spawn gone, or with
    /// the end check inverted, no interval line is ever written.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_flusher_writes_each_interval_and_stops_at_the_end() {
        init_log();
        let warn = WaitWarn::new(Duration::from_millis(10));
        let over = Arc::new(AtomicBool::new(false));
        warn.spawn_flusher(
            {
                let over = Arc::clone(&over);
                move || over.load(Ordering::Relaxed)
            },
            move |_| {},
        );
        // A count with no new event: only the flusher can carry it out.
        warn.since.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(60)).await;
        // The flusher's line goes through the caller's closure - here a
        // no-op - so the observable is the limiter's own state: the count
        // left the limiter, which only the flusher's flush does.
        assert_eq!(
            warn.since.load(Ordering::Relaxed),
            0,
            "the flusher carried the interval's count out"
        );
        // The end: what is left is written and the task stops.
        over.store(true, Ordering::Relaxed);
        warn.since.fetch_add(2, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            warn.since.load(Ordering::Relaxed),
            0,
            "the flusher wrote what was left at the end"
        );
    }

    /// `spawn_cap_flushers` spawns both flushers: the waits line and the
    /// refusals line each carry their own count out at the interval.
    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_cap_flushers_spawns_both_flushers() {
        init_log();
        let limits = Limits::default();
        let mut accepting = Accepting::of(&limits, &timers("keepalive"));
        // The real interval is 10 s; a 10 ms limiter makes the flushers'
        // interval work observable in the test.
        accepting.waits = WaitWarn::new(Duration::from_millis(10));
        accepting.refusals = WaitWarn::new(Duration::from_millis(10));
        let over = Arc::new(AtomicBool::new(false));
        spawn_cap_flushers(
            {
                let over = Arc::clone(&over);
                move || over.load(Ordering::Relaxed)
            },
            accepting.max,
            accepting.wait,
            &accepting.waits,
            &accepting.refusals,
        );
        // Counts with no new events: only the flushers can carry them out.
        accepting.waits.since.fetch_add(1, Ordering::Relaxed);
        accepting.refusals.since.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            accepting.waits.since.load(Ordering::Relaxed),
            0,
            "the waits flusher carried its count out"
        );
        assert_eq!(
            accepting.refusals.since.load(Ordering::Relaxed),
            0,
            "the refusals flusher carried its count out"
        );
        let text = String::from_utf8(log().0.lock().unwrap().clone()).unwrap();
        assert!(
            text.lines().any(|l| l.contains("a connection waits")),
            "the waits line is in the log:\n{text}"
        );
        assert!(
            text.lines()
                .any(|l| l.contains("a connection that did not")),
            "the refusals line is in the log:\n{text}"
        );
    }

    /// The backoff resets on a success: a failure, a success, and another
    /// failure wait the first wait again, not the doubled one - a serve
    /// that keeps the doubled pace across a success punishes the next
    /// storm's clients.
    #[tokio::test]
    async fn the_backoff_resets_on_a_success() {
        // The asserts are windows: the wait's floor is the backoff, the
        // ceiling leaves room for the scheduler.
        let within = |got: Duration, floor: Duration| {
            got >= floor && got < floor + Duration::from_millis(100)
        };
        let mut backoff = Backoff::new();
        let start = std::time::Instant::now();
        backoff.wait().await;
        assert!(
            within(start.elapsed(), FIRST_ACCEPT_BACKOFF),
            "the first wait is the first backoff"
        );
        // A success resets, so the next failure waits the first wait again.
        backoff.reset();
        let mark = std::time::Instant::now();
        backoff.wait().await;
        assert!(
            within(mark.elapsed(), FIRST_ACCEPT_BACKOFF),
            "the wait after the reset is the first wait, not the doubled one"
        );
        // Without a reset the doubling holds, and caps at the maximum.
        let mark = std::time::Instant::now();
        backoff.wait().await;
        assert!(within(mark.elapsed(), FIRST_ACCEPT_BACKOFF * 2), "doubles");
        let mark = std::time::Instant::now();
        backoff.wait().await;
        assert!(
            within(mark.elapsed(), FIRST_ACCEPT_BACKOFF * 4),
            "the doubling holds"
        );
        // The doubling continues 80, 160, 320, 640, and stops at the cap.
        let mut floor = FIRST_ACCEPT_BACKOFF * 8;
        for _ in 0..4 {
            let mark = std::time::Instant::now();
            backoff.wait().await;
            assert!(within(mark.elapsed(), floor), "the doubling holds");
            floor *= 2;
        }
        let mark = std::time::Instant::now();
        backoff.wait().await;
        assert!(within(mark.elapsed(), MAX_ACCEPT_BACKOFF), "caps");
        let mark = std::time::Instant::now();
        backoff.wait().await;
        assert!(within(mark.elapsed(), MAX_ACCEPT_BACKOFF), "the cap holds");
    }

    /// The stalled-bytes cap: a hold under the limit keeps the connection
    /// alive, past it the read side fails. The per-connection count alone
    /// crosses it - not the process total, and not the inverted sense.
    #[tokio::test]
    async fn the_stall_cap_crosses_on_the_connection_count_alone() {
        let limit = 100;
        let st = Stalled::new(
            Some(SocketAddr::from(([127, 0, 0, 1], 9))),
            limit,
            Arc::new(Metrics::default()),
            Arc::new(TotalBytes::new(1 << 20)),
        );
        // The Stall stays alive: its drop gives the bytes back, so the
        // holds must overlap to cross the limit, as real replies do.
        let first = st.hold(60);
        assert!(!st.over(), "under the limit the connection lives");
        // A hold under the limit must not spend the once-only cutoff
        // warning: the guard's logged flag stays clear until the crossing.
        assert!(
            !st.0.logged.load(Ordering::Relaxed),
            "no cutoff warning under the limit"
        );
        let second = st.hold(60);
        assert!(st.over(), "past the limit the read side fails");
        drop(second);
        drop(first);
    }

    /// The serve's last line writes what is left, without the interval
    /// check: the counts the binaries exit with are in the log, not
    /// dropped.
    #[tokio::test]
    async fn warn_the_last_writes_what_is_left() {
        init_log();
        let limits = Limits::default();
        let accepting = Accepting::of(&limits, &timers("keepalive"));
        accepting.waits.tick();
        accepting.refusals.tick();
        Limits::warn_the_last(&accepting);
        let text = String::from_utf8(log().0.lock().unwrap().clone()).unwrap();
        assert!(
            text.lines().any(|l| l.contains("a connection waits")),
            "the last waits line is in the log:\n{text}"
        );
        assert!(
            text.lines()
                .any(|l| l.contains("a connection that did not")),
            "the last refusals line is in the log:\n{text}"
        );
        assert_eq!(
            accepting.waits.flush_all(),
            None,
            "the waits are written, not left"
        );
        assert_eq!(
            accepting.refusals.flush_all(),
            None,
            "the refusals are written, not left"
        );
    }

    /// The serve itself writes what is left when it ends: a refusal, the
    /// signal before the interval, and after the serve returns the
    /// accepting's refusals are gone - the serve's last line took them. A
    /// `warn_the_last` removed from the serve fails this.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_serve_writes_what_is_left_when_it_ends() {
        let wait = Duration::from_millis(500);
        let limits = Limits {
            connections: NonZeroUsize::MIN,
            ..Limits::default()
        };
        let accepting = Accepting::of(&limits, &cap_timers(wait));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // The signal is a oneshot the test fires when the refusal is in:
        // the serve awaits `fired`, the test sends through `signal`.
        let (signal, fired) = tokio::sync::oneshot::channel::<()>();
        let serve = tokio::spawn({
            let metrics = Arc::new(Metrics::default());
            // The accepting is shared: the serve writes its last line into
            // it, and the test reads what is left after it returns.
            let accepting = accepting.clone();
            async move {
                let router = |mut server: Server| server.add_service(ObjectsServer::new(Stalls));
                let shutdown = async move {
                    let _ = fired.await;
                };
                limits
                    .serve_with(&accepting, router, tcp(listener), None, shutdown, metrics)
                    .await
            }
        });
        // The first connection holds the only place.
        let mut first = ObjectsClient::new(endpoint(addr).connect_lazy());
        assert_eq!(quick(&mut first).await, Code::NotFound);
        // A second client is refused inside the wait, before the interval
        // of the rate limiter could pass.
        let mut second = tokio::net::TcpStream::connect(addr).await.unwrap();
        second.write_all(PREFACE).await.unwrap();
        tokio::time::sleep(wait * 2).await;
        // The serve ends with its signal, well inside the limiter's
        // interval: only the serve's last line can carry the refusal.
        let _ = signal.send(());
        let outcome = tokio::time::timeout(Duration::from_secs(10), serve)
            .await
            .expect("the serve ends")
            .expect("the serve is ok");
        assert!(outcome.is_ok(), "the serve is ok: {outcome:?}");
        assert_eq!(
            accepting.refusals.flush_all(),
            None,
            "the serve's last line took the refusal"
        );
        assert_eq!(accepting.waits.flush_all(), None, "nothing waited");
    }

    /// A peer that connects and sends nothing is closed: it gets no place
    /// for life, though keepalive never starts without the preface.
    #[tokio::test]
    async fn a_peer_without_the_preface_is_closed() {
        let (addr, _) = serve(Limits::default(), timers("age")).await;
        assert!(closes_a_peer_that_sends(addr, b"").await);
    }

    /// A peer that sends the HTTP/2 preface and then never answers is closed
    /// by keepalive: a dead client does not keep its connection, or what its
    /// replies hold, forever.
    #[tokio::test]
    async fn keepalive_closes_a_silent_peer() {
        let (addr, _) = serve(Limits::default(), timers("age")).await;
        // The client preface, then an empty SETTINGS frame. No frame of the
        // server is ever answered, so its ping is not either.
        let preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0";
        assert!(closes_a_peer_that_sends(addr, preface).await);
    }

    /// A peer that sends only part of the preface and then stalls is cut at
    /// the handshake timeout: the preface counter runs until the preface is
    /// all in, so a few bytes do not buy a silent connection a place
    /// forever. (It also fails if any read at all stops the counter.)
    #[tokio::test]
    async fn a_partial_preface_is_cut_at_the_handshake_timeout() {
        let (addr, _) = serve(Limits::default(), timers("age")).await;
        assert!(closes_a_peer_that_sends(addr, b"PRI * HT").await);
    }

    /// The served connection's peer reaches the stub: the audit's refusal
    /// lines name it, so a serve that attaches None makes every call
    /// anonymous. Over plain TCP.
    #[tokio::test]
    async fn the_served_peer_reaches_the_stub() {
        let (addr, _) = serve(Limits::default(), TIMERS).await;
        let mut client = ObjectsClient::new(endpoint(addr).connect().await.unwrap());
        let error = client
            .resolve_ref(ResolveRefRequest::default())
            .await
            .unwrap_err();
        assert!(
            error.message().contains("peer is Some(127.0.0.1"),
            "the stub sees the loopback peer: {}",
            error.message()
        );
    }

    /// The same promise through the TLS accept path: the peer of a
    /// `TlsStream` reaches the stub too.
    #[tokio::test]
    async fn the_served_tls_peer_reaches_the_stub() {
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, Some(&certs.authority.cert()));
        let (addr, _) = serve_tls(
            Limits::default(),
            TIMERS,
            &server_tls,
            Duration::from_secs(20),
        )
        .await;
        let mut client = client(addr, &certs.authority.cert(), Some(&certs.client))
            .await
            .unwrap();
        let error = client
            .resolve_ref(ResolveRefRequest::default())
            .await
            .unwrap_err();
        assert!(
            error.message().contains("peer is Some(127.0.0.1"),
            "the stub sees the loopback peer: {}",
            error.message()
        );
    }

    /// The keepalive getters hand tonic/hyper values that keep a dead peer
    /// noticeable. A zero interval would ping without pause, and a zero
    /// timeout kills the connection at its first ping (hyper's
    /// `maybe_timeout` fires the moment the ping is sent), so the documented
    /// constants must survive the getters.
    #[tokio::test]
    async fn the_keepalive_getters_hand_tonic_working_values() {
        let (interval, answer) = client_keepalive();
        assert!(
            interval > Duration::ZERO,
            "a zero keepalive interval pings without pause"
        );
        assert!(
            answer > Duration::ZERO,
            "a zero keepalive timeout kills the connection at the first ping"
        );
        assert!(
            client_tcp_keepalive() > Duration::ZERO,
            "a zero tcp keepalive is not a keepalive"
        );
    }

    /// A TLS reply that outruns the socket's send buffer must still arrive
    /// in full: `rustls`' `poll_write` may return with ciphertext still in the
    /// session (its own doc says so), and the connection's flush is what
    /// drives the rest out. A flush that does nothing strands the tail of a
    /// final reply until the next write, which for a final reply never
    /// comes - the h2 task never re-registers its waker.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_tls_reply_arrives_in_full_even_after_a_socket_pause() {
        init_log();
        // The listener's send buffer is small; Linux accepted sockets
        // inherit it, so the 128 KiB parts cannot leave rustls' session in
        // one poll and the flush is what empties it.
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        rustix::net::sockopt::set_socket_send_buffer_size(&std_listener, 4096).unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let listener = TcpListener::from_std(std_listener).unwrap();
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, Some(&certs.authority.cert()));
        let config = server_tls.config();
        let addr = listener.local_addr().unwrap();
        let limits = Limits::default();
        let metrics = Arc::new(Metrics::default());
        tokio::spawn(async move {
            let router = |mut server: Server| server.add_service(ObjectsServer::new(Stalls));
            let pending = std::future::pending();
            let _ = limits
                .serve_with(
                    &Accepting::of(&limits, &TIMERS),
                    router,
                    tcp(listener),
                    Some(config),
                    pending,
                    metrics,
                )
                .await;
        });
        let mut client = client(addr, &certs.authority.cert(), Some(&certs.client))
            .await
            .unwrap();
        let mut parts = stall(&mut client).await;
        let mut got = 0;
        loop {
            let part = tokio::time::timeout(Duration::from_secs(30), parts.message())
                .await
                .unwrap_or_else(|_| panic!("the reply stalled past 30 s"))
                .unwrap();
            match part {
                Some(_) => got += 1,
                None => break,
            }
        }
        assert_eq!(got, 10, "the whole reply arrives");
    }

    /// The client preface, then an empty SETTINGS frame: what a real client
    /// sends at once, so a connection that is served would live.
    const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0";

    /// Timers for the cap tests: the handshake and the place wait are
    /// short (a refusal must come inside them), keepalive and the age are
    /// long (the held connection must live through the test).
    fn cap_timers(wait: Duration) -> Timers {
        let long = Duration::from_secs(3600);
        Timers {
            handshake: Duration::from_millis(100),
            keepalive: long,
            keepalive_timeout: long,
            age: long,
            grace: long,
            wait,
        }
    }

    /// The cap's refused count, from a scrape: the series an operator reads.
    fn refused_total(metrics: &Metrics) -> u64 {
        for line in metrics.render().lines() {
            if let Some(value) = line.strip_prefix("scion_connections_refused_total ") {
                return value.parse().unwrap();
            }
        }
        0
    }

    /// A connection over the cap of one is closed inside the wait, while
    /// the first connection keeps working, and the refusal is counted.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_connection_over_the_cap_fails_fast() {
        let wait = Duration::from_millis(500);
        let limits = Limits {
            connections: NonZeroUsize::MIN,
            ..Limits::default()
        };
        let (addr, metrics) = serve(limits, cap_timers(wait)).await;
        // The first connection is served and holds the only place.
        let mut first = ObjectsClient::new(endpoint(addr).connect_lazy());
        assert_eq!(quick(&mut first).await, Code::NotFound);
        // A second client connects and sends its preface at once: served or
        // refused, it hears back inside the wait either way.
        let mut second = tokio::net::TcpStream::connect(addr).await.unwrap();
        second.write_all(PREFACE).await.unwrap();
        let started = tokio::time::Instant::now();
        let mut buf = [0u8; 16];
        let ended = tokio::time::timeout(wait * 3, second.read(&mut buf)).await;
        let elapsed = started.elapsed();
        // The read ends inside the bound either way: a served connection
        // also answers its preface at once, so the read alone does not
        // tell a refusal from a serve. The refused count below does.
        assert!(
            ended.is_ok() && elapsed < wait * 3,
            "the second connection is closed inside the bound, took {elapsed:?}"
        );
        // The first connection keeps working.
        assert_eq!(quick(&mut first).await, Code::NotFound);
        assert_eq!(refused_total(&metrics), 1, "the refusal is counted");
    }

    /// A place freed in time still serves a connection that waits for it:
    /// the wait is long enough for the held connection to go away first.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_place_freed_in_time_still_serves_a_waiting_connection() {
        let wait = Duration::from_secs(5);
        let limits = Limits {
            connections: NonZeroUsize::MIN,
            ..Limits::default()
        };
        let (addr, metrics) = serve(limits, cap_timers(wait)).await;
        let mut first = ObjectsClient::new(endpoint(addr).connect_lazy());
        assert_eq!(quick(&mut first).await, Code::NotFound);
        // A second client connects and waits for the first's place.
        let mut second = tokio::net::TcpStream::connect(addr).await.unwrap();
        second.write_all(PREFACE).await.unwrap();
        // The first connection goes away inside the wait.
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(first);
        // The waiting connection is served: the server answers its preface.
        let mut buf = [0u8; 64];
        let read = tokio::time::timeout(Duration::from_secs(5), second.read(&mut buf))
            .await
            .expect("the server answers inside the bound");
        assert!(read.unwrap() > 0, "the waiting connection is served");
        assert_eq!(refused_total(&metrics), 0, "nothing was refused");
    }

    /// The line about connections that wait for a place is rate-limited:
    /// one line per interval, carrying the count, not one per connection.
    #[test]
    fn a_wait_warning_is_rate_limited() {
        let warn = WaitWarn::new(Duration::from_secs(60));
        assert_eq!(warn.tick(), Some(1), "the first wait writes the line");
        assert_eq!(
            warn.tick(),
            None,
            "a wait within the interval is only counted"
        );
        assert_eq!(warn.tick(), None);
        // An interval of zero passes at every wait, so every wait writes.
        let eager = WaitWarn::new(Duration::ZERO);
        assert_eq!(eager.tick(), Some(1));
        assert_eq!(eager.tick(), Some(1));
    }

    /// One stalled reply is already over the limit, so a client that stops
    /// reading on one stream gets its connection closed, though it sends
    /// nothing more: the count wakes the read side. Another connection is
    /// served once the stalled one is closed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_connection_whose_stalled_replies_hold_too_much_is_closed() {
        let limits = Limits {
            connections: NonZeroUsize::MIN,
            stalled_bytes: NonZeroU64::new(reply::STALLED_REPLY_BYTES).unwrap(),
            ..Limits::default()
        };
        // Long keepalive and age, so only the count can close the first
        // connection.
        let long = Duration::from_secs(3600);
        let timers = Timers {
            handshake: Duration::from_millis(100),
            keepalive: long,
            keepalive_timeout: long,
            age: long,
            grace: long,
            wait: long,
        };
        let (addr, _) = serve(limits, timers).await;
        let mut stalling = ObjectsClient::new(
            endpoint(addr)
                .initial_stream_window_size(65_535)
                .initial_connection_window_size(65_535)
                .connect()
                .await
                .unwrap(),
        );
        let mut stalled_stream = stall(&mut stalling).await;

        // The other connection waits for a place, and is served once the
        // process closes the stalled one.
        let mut other = ObjectsClient::new(endpoint(addr).connect_lazy());
        let mut next = pin!(quick(&mut other));
        let code = tokio::time::timeout(Duration::from_secs(10), &mut next)
            .await
            .expect(
                "the connection whose stalled replies hold too much is closed; the other is served",
            );
        assert_eq!(code, Code::NotFound);
        // The stalled stream of the closed connection holds nothing more.
        assert!(matches!(stalled_stream.message().await, Err(_) | Ok(None)));
    }

    /// Two connections, each holding one stalled reply under its own cap,
    /// together over the process's stall total: the connection whose count
    /// crosses the total is closed, and the one under it goes on holding.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_connections_over_the_stall_total_close_the_crossing_one() {
        let limits = Limits {
            connections: NonZeroUsize::new(8).unwrap(),
            stalled_bytes: NonZeroU64::new(2 * reply::STALLED_REPLY_BYTES).unwrap(),
            stalled_total: NonZeroU64::new(6 * (1 << 20)).unwrap(),
            ..Limits::default()
        };
        // Long keepalive and age, so only a count can close a connection.
        let long = Duration::from_secs(3600);
        let timers = Timers {
            handshake: Duration::from_millis(100),
            keepalive: long,
            keepalive_timeout: long,
            age: long,
            grace: long,
            wait: long,
        };
        let (addr, metrics) = serve(limits, timers).await;
        let mut first = a_stalling_client(addr).await;
        // The stream stays bound: its reply's stall is what the gauge
        // settles on.
        let _first_stream = stall(&mut first).await;
        let mut second = a_stalling_client(addr).await;
        let mut second_stream = stall(&mut second).await;

        // The second connection's hold crossed the process total (the first
        // connection's 4 MiB plus its own), so its stalled reply ends and
        // its bytes go free; the frames it had buffered drain first.
        let closed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match second_stream.message().await {
                    Err(_) | Ok(None) => break,
                    Ok(Some(_)) => {}
                }
            }
        })
        .await;
        assert!(closed.is_ok(), "the connection over the total is closed");
        // The first connection keeps its stall: the gauge settles at its
        // one reply's estimate, not at nothing.
        until(|| stalled_gauge(&metrics) == reply::STALLED_REPLY_BYTES).await;

        // A new client is served: the closed connection freed its place
        // and its share of the total.
        let mut third = ObjectsClient::new(endpoint(addr).connect_lazy());
        let code = tokio::time::timeout(Duration::from_secs(10), quick(&mut third))
            .await
            .expect("a new connection is served after the close");
        assert_eq!(code, Code::NotFound);
    }

    /// What the process's gauge says the stalled replies hold, from a
    /// scrape: this is the series an operator reads.
    fn stalled_gauge(metrics: &Metrics) -> u64 {
        for line in metrics.render().lines() {
            if let Some(value) = line.strip_prefix("scion_stalled_reply_bytes ") {
                return value.parse().unwrap();
            }
        }
        0
    }

    /// Waits until `done` holds, at most 10 s.
    async fn until(done: impl Fn() -> bool) {
        let started = tokio::time::Instant::now();
        while !done() {
            assert!(started.elapsed() < Duration::from_secs(10), "timed out");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// A client with the smallest windows, so a reply it does not read
    /// stalls.
    async fn a_stalling_client(addr: SocketAddr) -> ObjectsClient<Channel> {
        ObjectsClient::new(
            endpoint(addr)
                .initial_stream_window_size(65_535)
                .initial_connection_window_size(65_535)
                .connect()
                .await
                .unwrap(),
        )
    }

    /// Limits under which one stalled reply is served, and two together
    /// would close the connection.
    fn stall_limits() -> Limits {
        Limits {
            stalled_bytes: NonZeroU64::new(2 * reply::STALLED_REPLY_BYTES).unwrap(),
            ..Limits::default()
        }
    }

    /// A stalled reply counts its estimate on the process's gauge while its
    /// client does not read, and gives it back when the client resets the
    /// stream, which frees the parts: the connection goes on holding
    /// nothing for the stalls it served.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stalled_reply_gives_its_count_back_when_its_stream_is_reset() {
        let (addr, metrics) = serve(stall_limits(), TIMERS).await;
        let mut client = a_stalling_client(addr).await;
        let stalled_stream = stall(&mut client).await;
        until(|| stalled_gauge(&metrics) >= reply::STALLED_REPLY_BYTES).await;

        // The reset frees the parts, and with them the count.
        drop(stalled_stream);
        until(|| stalled_gauge(&metrics) == 0).await;
        assert_eq!(
            quick(&mut client).await,
            Code::NotFound,
            "the connection served no call after the reset"
        );
    }

    /// A client that closes its connection while a reply of it is stalled
    /// gives the estimate back: the connection's teardown drops the reply's
    /// body, and with it the parked count.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_client_that_closes_with_a_stalled_reply_gives_the_count_back() {
        let (addr, metrics) = serve(stall_limits(), TIMERS).await;
        let mut client = a_stalling_client(addr).await;
        let stalled_stream = stall(&mut client).await;
        until(|| stalled_gauge(&metrics) >= reply::STALLED_REPLY_BYTES).await;

        // The close of the connection frees every stalled stream of it.
        drop(stalled_stream);
        drop(client);
        until(|| stalled_gauge(&metrics) == 0).await;
    }

    /// Stall-and-reset cycles on one connection do not ratchet its count:
    /// every reply gives its estimate back when its stream is reset, so the
    /// connection stays under the close limit however many stalls it
    /// served. (Before the give-back, each reply held its estimate for
    /// life, and the second cycle already crossed this limit.)
    #[tokio::test(flavor = "multi_thread")]
    async fn stall_and_reset_cycles_stay_under_the_close_limit() {
        let (addr, metrics) = serve(stall_limits(), TIMERS).await;
        let mut client = a_stalling_client(addr).await;
        for _ in 0..5 {
            let stalled_stream = stall(&mut client).await;
            until(|| stalled_gauge(&metrics) >= reply::STALLED_REPLY_BYTES).await;
            drop(stalled_stream);
            until(|| stalled_gauge(&metrics) == 0).await;
        }
        assert_eq!(
            quick(&mut client).await,
            Code::NotFound,
            "the connection was closed under the stall cycles"
        );
    }

    /// TCP connections have Nagle's algorithm off, so a small reply does not
    /// wait for the client's ACK.
    #[tokio::test]
    async fn tcp_connections_have_no_delay() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let accepted = pin!(tcp(listener)).next().await.unwrap().unwrap();
        assert!(accepted.nodelay().unwrap());
    }

    /// A client that trusts the CA of the server is served over TLS.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_tls_client_is_served() {
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        let (addr, _) = serve_tls(Limits::default(), TIMERS, &server_tls, HANDSHAKE_TIMEOUT).await;
        let mut client = client(addr, &certs.authority.cert(), Some(&certs.client))
            .await
            .unwrap();
        assert_eq!(quick(&mut client).await, Code::NotFound);
    }

    /// An mTLS server refuses a client without a certificate.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_mtls_server_refuses_a_client_without_a_certificate() {
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, Some(&certs.authority.cert()));
        let (addr, _) = serve_tls(Limits::default(), TIMERS, &server_tls, HANDSHAKE_TIMEOUT).await;
        // The positive control: the same server and the same raw
        // client path serve a client the server's CA signed, so the
        // refusal below cannot pass on a handshake error that is not a
        // refusal.
        assert!(
            a_raw_client_is_served(addr, &certs.authority.cert(), &certs.client).await,
            "the server refused a client of its own CA"
        );
        assert!(
            a_raw_client_is_refused(addr, &certs.authority.cert(), None).await,
            "the server served a client without a certificate"
        );
    }

    /// An mTLS server serves a client with a certificate of its CA.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_mtls_server_serves_a_client_with_a_certificate() {
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, Some(&certs.authority.cert()));
        let (addr, _) = serve_tls(Limits::default(), TIMERS, &server_tls, HANDSHAKE_TIMEOUT).await;
        let mut client = client(addr, &certs.authority.cert(), Some(&certs.client))
            .await
            .unwrap();
        assert_eq!(quick(&mut client).await, Code::NotFound);
    }

    /// A client that trusts another CA is refused: the certificate of the
    /// server does not verify.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_client_with_the_wrong_ca_is_refused() {
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        let (addr, _) = serve_tls(Limits::default(), TIMERS, &server_tls, HANDSHAKE_TIMEOUT).await;
        assert!(client(addr, &certs.stranger.cert(), None).await.is_err());
    }

    /// An mTLS server refuses a client whose certificate another CA signed.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_mtls_server_refuses_a_client_of_another_ca() {
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, Some(&certs.authority.cert()));
        let (addr, _) = serve_tls(Limits::default(), TIMERS, &server_tls, HANDSHAKE_TIMEOUT).await;
        // As above: the control runs on the same server and the same
        // raw client path.
        assert!(
            a_raw_client_is_served(addr, &certs.authority.cert(), &certs.client).await,
            "the server refused a client of its own CA"
        );
        assert!(
            a_raw_client_is_refused(addr, &certs.authority.cert(), Some(&certs.stranger_client))
                .await,
            "the server served a client of another CA"
        );
    }

    /// A peer that opens a TLS connection and sends nothing is dropped by
    /// the TLS handshake timeout.
    #[tokio::test]
    async fn a_peer_without_the_tls_handshake_is_dropped() {
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        let (addr, _) = serve_tls(
            Limits::default(),
            TIMERS,
            &server_tls,
            Duration::from_millis(100),
        )
        .await;
        assert!(closes_a_peer_that_sends(addr, b"").await);
    }

    /// A peer that ends the TLS handshake and then sends nothing holds no
    /// place: the preface timer of the decrypted connection closes it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_peer_silent_after_the_tls_handshake_is_closed() {
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        // All timers are short (timers("age")), so the peer is closed soon
        // after it goes silent: the TLS handshake timeout can no longer
        // reach it once the handshake ended, so only the preface timer can.
        let (addr, _) = serve_tls(
            Limits::default(),
            timers("age"),
            &server_tls,
            Duration::from_millis(100),
        )
        .await;
        let mut peer = raw_client(addr, &certs.authority.cert(), None).await;
        // The server aborts the connection (no clean TLS close), so the
        // read ends with an error; what matters is that it ends: the peer
        // holds no place for life.
        let read =
            tokio::time::timeout(Duration::from_secs(10), peer.read_to_end(&mut Vec::new())).await;
        assert!(read.is_ok(), "the server closed the silent peer");
    }

    /// A peer whose TLS handshake ends late, and that goes silent right
    /// after it, is closed within one deadline from its accept: the
    /// handshake and the preface share the deadline, so the close comes
    /// before a second full handshake timeout could run.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_silent_tls_peer_is_closed_within_one_deadline() {
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        let deadline = Duration::from_secs(1);
        let (addr, _) = serve_tls(Limits::default(), TIMERS, &server_tls, deadline).await;
        // The peer connects at once, waits out most of the deadline, then
        // ends its handshake and sends nothing more. The clock runs from the
        // connect, which is the server's accept to within a millisecond.
        let started = std::time::Instant::now();
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(deadline * 5 / 6).await;
        let mut roots = RootCertStore::empty();
        roots
            .add(
                CertificateDer::from_pem_slice(certs.authority.cert().cert_pem.as_bytes()).unwrap(),
            )
            .unwrap();
        let builder = ClientConfig::builder_with_provider(crate::runtime::tls::provider())
            .with_safe_default_protocol_versions()
            .unwrap();
        let config = builder.with_root_certificates(roots).with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let mut peer = connector
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap();
        let read =
            tokio::time::timeout(Duration::from_secs(10), peer.read_to_end(&mut Vec::new())).await;
        assert!(read.is_ok(), "the server closed the silent peer");
        // One deadline from the accept, with slack for the handshake round
        // trip and a loaded runner. The old code (a full preface timer after
        // the handshake) closes at about deadline + delay, 1.83 s here,
        // past this bound.
        assert!(
            started.elapsed() < deadline * 3 / 2,
            "the silent peer held its place {:?}, past the deadline",
            started.elapsed()
        );
    }

    /// A peer that opens a TLS connection and sends nothing holds its place,
    /// but no accept: a second client is served at once, while the silent
    /// peer's handshake timeout is still far away. With the handshake run in
    /// sequence instead (the mutation), the second client would wait for the
    /// silent peer's timeout, past 20 s.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_silent_tls_handshake_does_not_block_other_clients() {
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        // The silent peer's handshake timeout: far beyond the next test
        // step, so only the blocking of a sequential handshake can end it.
        let (addr, _) = serve_tls(
            Limits::default(),
            TIMERS,
            &server_tls,
            Duration::from_secs(60),
        )
        .await;
        // A peer that connects and sends nothing: no ClientHello.
        let mut silent = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let served = tokio::time::timeout(Duration::from_secs(20), async {
            let mut client = client(addr, &certs.authority.cert(), None).await?;
            Ok::<_, tonic::transport::Error>(quick(&mut client).await)
        })
        .await
        .expect("a silent handshake does not block the next client")
        .expect("the client connects");
        assert_eq!(served, Code::NotFound);
        // The silent peer is still holding its connection: a read would
        // block, not return.
        let mut buf = [0; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), silent.read(&mut buf))
                .await
                .is_err(),
            "the silent peer is not yet closed"
        );
        drop(silent);
    }
    /// A serve's end gives a connected TLS client a clean close: the
    /// drain's h2 shutdown runs the connection's `poll_shutdown`, rustls
    /// sends its `close_notify`, and the client reads an end of stream -
    /// not the abrupt `UnexpectedEof` of a peer that never sent it. The
    /// mutant that replaces the connection's `poll_shutdown` with
    /// `Ok(())` dies here: the close notify never goes out and the
    /// client's read answers with the error instead.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_serve_end_closes_tls_cleanly() {
        init_log();
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let listener = TcpListener::from_std(std_listener).unwrap();
        let certs = test_certs();
        let dir = crate::runtime::private_tempdir();
        let server_tls = server_tls(dir.path(), &certs.server, Some(&certs.authority.cert()));
        let config = server_tls.config();
        let addr = listener.local_addr().unwrap();
        let limits = Limits::default();
        let metrics = Arc::new(Metrics::default());
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let router = |mut server: Server| server.add_service(ObjectsServer::new(Stalls));
            let shutdown = async move {
                let _ = stop_rx.changed().await;
            };
            let _ = limits
                .serve_with(
                    &Accepting::of(&limits, &TIMERS),
                    router,
                    tcp(listener),
                    Some(config),
                    shutdown,
                    metrics,
                )
                .await;
            let _ = done_tx.send(());
        });
        let mut stream = raw_connect(addr, &certs.authority.cert(), Some(&certs.client))
            .await
            .unwrap();
        stream.write_all(PREFACE).await.unwrap();
        stream.flush().await.unwrap();
        // The server's SETTINGS: the connection is past the transport
        // sniffing and into the h2 serve, so the drain below runs the h2
        // close path.
        let mut buf = [0u8; 4096];
        tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("the server's SETTINGS never came"))
            .unwrap();
        // One task reads what the server sends and answers the drain's
        // PING, like a real h2 client's runtime does; without the pong
        // the drain waits for it. The read's end is the verdict: an end
        // of stream is the clean close, rustls' `UnexpectedEof` is the
        // abrupt one.
        let (mut rd, mut wr) = tokio::io::split(stream);
        let reader = tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            let mut pending: Vec<u8> = Vec::new();
            loop {
                match rd.read(&mut buf).await {
                    Ok(0) => return None,
                    Ok(n) => {
                        pending.extend_from_slice(&buf[..n]);
                        let mut off = 0;
                        while pending.len() >= off + 9 {
                            let len = u32::from_be_bytes([
                                0,
                                pending[off],
                                pending[off + 1],
                                pending[off + 2],
                            ]) as usize;
                            if pending.len() < off + 9 + len {
                                break;
                            }
                            // A PING without the ACK flag is answered
                            // with its ACK, the payload echoed.
                            if pending[off + 3] == 0x06 && pending[off + 4] == 0 {
                                let mut pong = [0u8; 17];
                                pong[2] = 8;
                                pong[3] = 0x06;
                                pong[4] = 1;
                                pong[9..17].copy_from_slice(&pending[off + 9..off + 17]);
                                if wr.write_all(&pong).await.is_err() {
                                    return Some(io::Error::new(
                                        io::ErrorKind::BrokenPipe,
                                        "the drain's ping could not be answered",
                                    ));
                                }
                                wr.flush().await.ok()?;
                            }
                            off += 9 + len;
                        }
                        pending.drain(..off);
                    }
                    Err(e) => return Some(e),
                }
            }
        });
        stop_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(30), done_rx)
            .await
            .unwrap_or_else(|_| panic!("the serve did not end"))
            .unwrap();
        let abrupt = tokio::time::timeout(Duration::from_secs(30), reader)
            .await
            .unwrap_or_else(|_| panic!("the client's read never ended"))
            .unwrap();
        assert!(
            abrupt.is_none(),
            "the client read an error, not an end of stream: {abrupt:?}"
        );
    }
}
