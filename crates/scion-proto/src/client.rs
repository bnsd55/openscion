//! The client half of the connection age: a call that never reached the
//! wire is sent again.
//!
//! The server asks its connections to close at `MAX_CONNECTION_AGE`
//! (the HTTP/2 `GOAWAY` goes out then; see the serve builder in
//! [`runtime::listen`](crate::runtime::listen)). A client whose request is still queued in
//! hyper's dispatch at that moment loses the call: hyper names a call
//! it never sent either `hyper::Error::is_canceled()` (still in the
//! dispatch queue) or dispatch-gone (taken by the dispatcher, its
//! HTTP/2 stream still pending open when the dispatcher ends on the
//! `GOAWAY`; see `fut_ctx` in hyper's `proto/h2/client.rs`), and tonic
//! drops it, so a back-to-back writer loses a call at the age even
//! though the server never saw it. [`Resend`] wraps the channel and
//! sends such a call again, up to `MAX_ATTEMPTS` tries, with the
//! very request it was built with, on a connection of its own:
//! `connect_lazy` on the same endpoint, so the connection the call
//! was lost on - which would refuse the resent stream - is bypassed,
//! and the fresh connection is dropped when the call ends.
//!
//! A call whose body hyper already took gets a second proof, because
//! the slot no longer holds the request: the middleware keeps a copy
//! of the frames hyper took, complete when the body reached its end,
//! and sends the call again only when the server itself refused the
//! stream as never processed - a `RST_STREAM` with `REFUSED_STREAM`
//! from the remote, or a `GOAWAY` delivered to this stream (h2 hands
//! a GOAWAY only to streams above its last stream id). Anything else
//! that happens after the take says nothing about what the server
//! read, and is never resent - an upload that started once and was
//! seen once is never sent twice.
//!
//! Trust boundary: the second proof takes the server's `REFUSED_STREAM`
//! and `GOAWAY` at their word (RFC 9113 8.7 and 6.8: a server that
//! refused a stream did not process it, and a GOAWAY's last stream id
//! covers every stream at or below it). A server that broke that rule
//! could cause a duplicate write.

use std::error::Error as _;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::PoisonError;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use http::Request;
use http_body::{Body as HttpBody, Frame, SizeHint};
use hyper::Error as HyperError;
use tonic::Status;
use tonic::body::Body;
use tonic::transport::{Channel, Endpoint, Error as TransportError};
use tower::Service;

/// How many times a call may be sent: the first send and two more.
/// Each resend must re-earn its proof - either the slot still holds
/// the body (hyper never polled it), or the kept copy is complete and
/// the server itself refused the stream unprocessed.
const MAX_ATTEMPTS: usize = 3;

/// The most body hyper may have taken from a call while the middleware
/// still keeps a copy of it, as refcounted `Bytes` frames. gRPC's own
/// default for this kind of retry: a call whose body was bigger than
/// this when it was refused loses its copy and fails like it did
/// before the resend existed.
const RESEND_BODY_KEEP_BYTES: usize = 256 * 1024;

/// The client half of the connection age. A tower service that wraps
/// the channel: a call whose error's source chain holds a
/// `hyper::Error` - hyper's transport failure - is handed to the
/// channel again, with the same request, when one of two proofs says
/// the send was safe to repeat: the body hyper never polled is still
/// whole in the slot, or the kept copy of what hyper took is complete
/// and the server itself refused the stream as never processed.
#[derive(Clone, Debug)]
pub struct Resend {
    inner: Channel,
    /// The endpoint the channel was built from: a resend opens a
    /// connection of its own with it, so the connection the call was
    /// lost on - which would refuse the resent stream - is bypassed.
    endpoint: Endpoint,
}

impl Resend {
    /// The channel, wrapped: every scion client's channel goes through
    /// this, so a call that never reached the wire reaches it the
    /// second time, on a connection of its own. The endpoint must be
    /// the one the channel was built from.
    #[must_use]
    pub fn new(inner: Channel, endpoint: Endpoint) -> Self {
        Self { inner, endpoint }
    }
}

impl Service<Request<Body>> for Resend {
    type Response = http::Response<Body>;
    type Error = TransportError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, TransportError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        // The ready channel handle moves into the future: it holds the
        // buffer slot that poll_ready reserved, and attempt 0 runs on
        // it as it is. Self takes a fresh clone, so a polled clone
        // never holds a slot it does not use - enough polled clones
        // would run the buffer dry and hang poll_ready.
        let fresh_handle = self.inner.clone();
        let inner = std::mem::replace(&mut self.inner, fresh_handle);
        Box::pin(run(inner, self.endpoint.clone(), request))
    }
}

/// Whether the error's source chain carries a `hyper::Error` - the
/// transport failure of the connection. This alone does not justify a
/// resend; it lets the gate look further. The request may be unsent -
/// still in the dispatch queue (`Canceled`), or taken by the
/// dispatcher with its HTTP/2 stream pending open (dispatch-gone) -
/// or the server may have refused the stream unprocessed. The chain
/// walk fails closed: no `hyper::Error`, no resend. What the gate
/// does with it is [`Slot::may_resend`]'s call: a body hyper never
/// polled resends as it is; a taken body resends only on the
/// server's own refused-unprocessed frames.
fn is_unsent(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut chain = Some(error);
    while let Some(error) = chain {
        if error.downcast_ref::<HyperError>().is_some() {
            return true;
        }
        chain = error.source();
    }
    false
}

/// The remote's own proof that a stream it refused was never
/// processed, so sending it again is safe even though hyper already
/// took the body. Two frames carry it: a `RST_STREAM` with
/// `REFUSED_STREAM` from the remote, and a `GOAWAY` delivered to this
/// stream - h2 hands the GOAWAY error only to streams above the
/// frame's last stream id (h2-0.4.20/src/proto/streams/streams.rs:
/// 812-829), so the delivery itself is the check that the server
/// never saw this one. Any other failure after the take - a plain
/// connection close, a reset with another reason - says nothing about
/// what the server read, and is never resent.
fn refused_without_processing(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut chain = Some(error);
    while let Some(error) = chain {
        if let Some(h2) = error.downcast_ref::<h2::Error>() {
            return h2.is_remote()
                && ((h2.is_reset() && h2.reason() == Some(h2::Reason::REFUSED_STREAM))
                    || h2.is_go_away());
        }
        chain = error.source();
    }
    false
}

/// A short name for the transport failure, for the resend log line.
fn kind_of(error: &HyperError) -> &'static str {
    if error.is_canceled() {
        "canceled"
    } else if error.is_user() {
        "dispatch gone"
    } else {
        let mut chain: Option<&(dyn std::error::Error + 'static)> = Some(error);
        while let Some(error) = chain {
            if let Some(h2) = error.downcast_ref::<h2::Error>() {
                if h2.is_reset() && h2.reason() == Some(h2::Reason::REFUSED_STREAM) {
                    return "refused stream";
                }
                if h2.is_go_away() {
                    return "go away";
                }
            }
            chain = error.source();
        }
        if error.is_closed() {
            "channel closed"
        } else if error.is_incomplete_message() {
            "incomplete message"
        } else if error.is_timeout() {
            "timeout"
        } else {
            "protocol"
        }
    }
}

async fn run(
    mut inner: Channel,
    endpoint: Endpoint,
    request: Request<Body>,
) -> Result<http::Response<Body>, TransportError> {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_owned();
    // The body waits in the slot; the proxy of the attempt that owns it
    // takes it out on its first poll. While the slot still holds it,
    // hyper never read a byte of the request, and the request can be
    // sent again as it is. The generation says which attempt's proxy
    // may take it: a proxy of an older attempt, still alive in hyper
    // after its connection was lost, sends nothing instead.
    let slot = Arc::new(Mutex::new(Slot {
        body: Some(body),
        generation: 0,
        sent: Some(Sent::default()),
    }));
    let mut attempt = 0;
    loop {
        if attempt > 0 {
            tracing::debug!(
                rpc = %path,
                attempt,
                transport = "fresh connection",
                proof = if slot
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .body
                    .is_some()
                {
                    "unsent"
                } else {
                    "refused unprocessed"
                },
                "sending a call again"
            );
        }
        slot.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .generation = attempt;
        // Every attempt is a call of its own: the ready channel of the
        // first, a connection of its own after - the connection the
        // call was lost on would refuse the resent stream.
        let mut fresh;
        let channel = if attempt == 0 {
            // The handle from poll_ready: it holds the reserved buffer
            // slot, and it is ready to be called as it is.
            &mut inner
        } else {
            fresh = endpoint.connect_lazy();
            &mut fresh
        };
        // The fresh connection must say it is ready first.
        if attempt > 0 {
            poll_fn(|cx| channel.poll_ready(cx)).await?;
        }
        let request = Request::from_parts(
            parts.clone(),
            Body::new(ProxyBody::new(Arc::clone(&slot), attempt)),
        );
        let response = channel.call(request).await;
        match response {
            Ok(response) => return Ok(response),
            Err(error) => {
                let unsent = is_unsent(&error)
                    && slot
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .may_resend(&error);
                if attempt + 1 == MAX_ATTEMPTS || !unsent {
                    if attempt > 0 {
                        tracing::warn!(
                            rpc = %path,
                            attempts = attempt + 1,
                            "a call ran out of resend attempts"
                        );
                    }
                    return Err(error);
                }
                let kind = error
                    .source()
                    .and_then(|source| source.downcast_ref::<HyperError>())
                    .map_or("other", kind_of);
                tracing::debug!(
                    rpc = %path,
                    attempt = attempt + 1,
                    kind,
                    "call lost before the wire"
                );
                attempt += 1;
            }
        }
    }
}

/// The slot a call's body waits in for its next attempt. The
/// generation names the attempt that may take the body: a proxy of an
/// older attempt, still alive in hyper after its connection was lost,
/// must never take the body a newer attempt is sending.
#[derive(Debug)]
struct Slot {
    body: Option<Body>,
    generation: usize,
    /// The frames hyper took, kept as refcounted clones while the body
    /// streams and its total stays under [`RESEND_BODY_KEEP_BYTES`].
    /// The stream's end marks the copy complete - the whole body is in
    /// the list, and the list lives until the call ends. Going over
    /// the cap, or a trailers frame, drops the copy: without a
    /// complete copy a refused-after-the-take call is not resent,
    /// because sending half a call twice is a duplicate write.
    sent: Option<Sent>,
}

/// The kept frames of a body hyper took.
#[derive(Debug, Default)]
struct Sent {
    frames: Vec<bytes::Bytes>,
    /// The running size of the kept frames, so no poll re-sums them.
    total: usize,
    complete: bool,
}

impl Slot {
    /// The call may be sent again on one of two proofs. One: hyper
    /// never polled the body - the slot still holds it, so no byte
    /// left the client. Two: hyper took the body, but the copy it
    /// streamed from is complete and the remote refused the stream as
    /// unprocessed, so the whole call is safe to put on the wire
    /// again. Anything else is not a resend: a body without its end,
    /// or a refusal that says nothing about what the server read.
    fn may_resend(&self, error: &(dyn std::error::Error + 'static)) -> bool {
        if self.body.is_some() {
            return true;
        }
        match &self.sent {
            Some(sent) => sent.complete && refused_without_processing(error),
            None => false,
        }
    }
}

/// The body of a possibly-resent call: the real body waits in the
/// slot, and the owning attempt's first poll takes it out. Every
/// frame hyper takes is kept as a refcount clone while the body
/// streams; once the body ends the copy is complete, and a resent
/// attempt may replay it - but only when the server itself refused
/// the stream as never processed.
#[derive(Debug)]
struct ProxyBody {
    slot: Arc<Mutex<Slot>>,
    generation: usize,
    inner: Option<Body>,
    /// The next kept frame a resent attempt replays from the slot.
    replay: usize,
}

impl ProxyBody {
    fn new(slot: Arc<Mutex<Slot>>, generation: usize) -> Self {
        Self {
            slot,
            generation,
            inner: None,
            replay: 0,
        }
    }
}

impl HttpBody for ProxyBody {
    type Data = <Body as HttpBody>::Data;
    type Error = <Body as HttpBody>::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        // The slot decides what this attempt streams, then the lock
        // goes away: the polls below borrow the body.
        let (taken, replay) = {
            let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
            if slot.generation != self.generation {
                // This attempt was superseded: its connection was
                // lost, and a newer attempt owns the body now. An
                // error frame makes hyper reset this stream; a clean
                // end would send END_STREAM and the server would see a
                // complete request with an empty body, which for a
                // client-streaming upload is a valid zero-message call
                // it may act on.
                return Poll::Ready(Some(Err(Status::cancelled(
                    "the call was lost before it was sent",
                ))));
            }
            (
                slot.body.take(),
                slot.sent
                    .as_ref()
                    .map(|sent| (sent.complete, sent.frames.get(self.replay).cloned())),
            )
        };
        // The real-body poll: the attempt that got the body streams
        // from it, and the bookkeeping below keeps every frame hyper
        // takes as a refcount clone until the body ends or the copy
        // passes the cap. It runs on every poll of the owning attempt -
        // the body's end usually arrives on a later poll than the take.
        let poll = match taken {
            Some(mut body) => {
                let poll = Pin::new(&mut body).poll_frame(cx);
                self.inner = Some(body);
                poll
            }
            None => match self.inner.as_mut() {
                Some(body) => Pin::new(body).poll_frame(cx),
                // The body of this attempt is gone: a resent attempt
                // replays the kept frames, the whole body as hyper
                // took it, and returns early - a replayed frame never
                // runs the bookkeeping below, which would grow the
                // copy with its own echoes and corrupt the resent body.
                None => match replay {
                    Some((true, Some(bytes))) => {
                        self.replay += 1;
                        return Poll::Ready(Some(Ok(Frame::data(bytes))));
                    }
                    Some((true, None)) => return Poll::Ready(None),
                    // Without a complete copy - the cap dropped it, or
                    // hyper never ended the stream - an error frame
                    // makes hyper reset this stream; a clean end would
                    // hand the server a request of its own.
                    _ => {
                        return Poll::Ready(Some(Err(Status::cancelled(
                            "the call was lost before it was sent",
                        ))));
                    }
                },
            },
        };
        match poll {
            Poll::Ready(Some(Ok(frame))) => {
                let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
                match (&mut slot.sent, frame.data_ref()) {
                    (Some(sent), Some(bytes)) => {
                        sent.total += bytes.len();
                        sent.frames.push(bytes.clone());
                        if sent.total > RESEND_BODY_KEEP_BYTES {
                            slot.sent = None;
                        }
                    }
                    // A trailers frame, or the copy already gone over
                    // the cap: the copy stops being a proof.
                    _ => slot.sent = None,
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) => {
                // The body reached its end: the copy is complete, and
                // it lives until the call ends.
                let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(sent) = slot.sent.as_mut() {
                    sent.complete = true;
                }
                Poll::Ready(None)
            }
            poll => poll,
        }
    }

    fn size_hint(&self) -> SizeHint {
        // Unknown on every attempt, the first send included: hyper
        // polls the body either way, and the resent HEADERS then carry
        // the same no-content-length head the first ones did. The
        // exact hint is a trap twice over - it lets hyper send
        // HEADERS with END_STREAM without ever polling the body, and
        // it makes the resend's headers differ from the first send's.
        SizeHint::default()
    }

    fn is_end_stream(&self) -> bool {
        match &self.inner {
            Some(body) => body.is_end_stream(),
            // False until taken, for the same reason: hyper must poll
            // the body to end the stream, so the first poll is the one
            // that moves the body out of the slot.
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::v1::GetObjectsRequest;
    use crate::v1::ResolveRefRequest;
    use crate::v1::objects_client::ObjectsClient;
    use crate::v1::objects_server::ObjectsServer;
    use http_body::Body as HttpBody;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::net::TcpListener;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::body::Body;
    use tonic::transport::{Channel, Endpoint, Server};
    use tonic::{Code, Request, Response, Status};

    use super::{MAX_ATTEMPTS, ProxyBody, RESEND_BODY_KEEP_BYTES, Resend, Sent, Slot, is_unsent};

    /// The stub: `ResolveRef` answers at once, so back-to-back calls
    /// queue behind each other in hyper's dispatch; `GetObjects`
    /// ends the connection under a call the server has already
    /// received, and counts its calls, so a test can tell a resent
    /// call from a served one.
    #[derive(Clone, Default)]
    struct Stub {
        get_objects_calls: Arc<AtomicUsize>,
        resolve_ref_calls: Arc<AtomicUsize>,
    }

    /// A connection that keeps the open-count honest: dropped when
    /// the serve drops the connection, and never before.
    struct CountedIo {
        io: tokio::net::TcpStream,
        open: Arc<AtomicUsize>,
    }

    impl Drop for CountedIo {
        fn drop(&mut self) {
            self.open.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl AsyncRead for CountedIo {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.io).poll_read(cx, buf)
        }
    }

    impl tonic::transport::server::Connected for CountedIo {
        type ConnectInfo =
            <tokio::net::TcpStream as tonic::transport::server::Connected>::ConnectInfo;

        fn connect_info(&self) -> Self::ConnectInfo {
            self.io.connect_info()
        }
    }

    impl AsyncWrite for CountedIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.io).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.io).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.io).poll_shutdown(cx)
        }
    }

    #[tonic::async_trait]
    impl crate::v1::objects_server::Objects for Stub {
        type GetObjectsStream = crate::runtime::reply::ObjectStream;

        async fn resolve_ref(
            &self,
            _: Request<ResolveRefRequest>,
        ) -> Result<Response<crate::v1::ResolveRefResponse>, Status> {
            self.resolve_ref_calls.fetch_add(1, Ordering::SeqCst);
            // A call long enough that another one queues behind it,
            // the bench's shape with its writers back to back.
            tokio::time::sleep(Duration::from_millis(20)).await;
            Err(Status::not_found("no ref"))
        }

        async fn get_objects(
            &self,
            _: Request<GetObjectsRequest>,
        ) -> Result<Response<Self::GetObjectsStream>, Status> {
            self.get_objects_calls.fetch_add(1, Ordering::SeqCst);
            // The call was received and is in flight: the connection
            // ends under it, the way a cut does. The panic lands in the
            // connection's own task, so the serve goes on.
            panic!("the stub ends the connection under a served call");
        }

        async fn get_root_tree(
            &self,
            _: Request<crate::v1::GetRootTreeRequest>,
        ) -> Result<Response<crate::v1::GetRootTreeResponse>, Status> {
            Err(Status::unimplemented("not in the stub"))
        }
    }

    /// The stub, served with the connection age of the fixed server:
    /// the age alone, so the GOAWAY goes out at it, and no grace.
    async fn serve(age: Duration, stub: Stub) -> SocketAddr {
        serve_counting(age, stub).await.0
    }

    /// The same, with the count of the connections the serve accepted
    /// and the count of the ones still open.
    async fn serve_counting(
        age: Duration,
        stub: Stub,
    ) -> (SocketAddr, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let open = Arc::new(AtomicUsize::new(0));
        let accepted_in = Arc::clone(&accepted);
        let open_in = Arc::clone(&open);
        let counted = TcpListenerStream::new(listener).map(move |io| {
            io.map(|io| {
                accepted_in.fetch_add(1, Ordering::SeqCst);
                open_in.fetch_add(1, Ordering::SeqCst);
                CountedIo {
                    io,
                    open: Arc::clone(&open_in),
                }
            })
        });
        tokio::spawn(async move {
            Server::builder()
                .max_connection_age(age)
                // Two streams at once: a third call queues in hyper's
                // dispatch, the bench's shape with its writers back to
                // back, so the GOAWAY finds a call in the queue.
                .max_concurrent_streams(2)
                .add_service(ObjectsServer::new(stub))
                .serve_with_incoming(counted)
                .await
                .unwrap();
        });
        (addr, accepted, open)
    }

    /// A client whose calls are sent again when they were never sent.
    async fn resend_client(addr: SocketAddr) -> ObjectsClient<Resend> {
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
        let channel = endpoint.connect().await.unwrap();
        ObjectsClient::new(Resend::new(channel, endpoint))
    }

    /// A client on the bare channel, without the resend: what every
    /// scion client did before, and hyper's own behavior.
    async fn raw_client(addr: SocketAddr) -> ObjectsClient<Channel> {
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        ObjectsClient::new(channel)
    }

    /// A client whose calls cross the connection age: the calls that
    /// hyper never sent are sent again, so back-to-back writers at the
    /// age get no error. Red without the resend: the queued calls get
    /// hyper's Canceled. (#1419)
    #[tokio::test]
    async fn a_call_queued_at_the_goaway_is_sent_again() {
        let addr = serve(Duration::from_secs(1), Stub::default()).await;
        let mut client = resend_client(addr).await;
        // One call before the age, so the first connection is served.
        assert_eq!(
            client
                .resolve_ref(ResolveRefRequest::default())
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        // Writers back to back across the age, the bench's own shape:
        // a call in flight and the next one queued when the GOAWAY
        // lands. The stub answers each call after 20 ms, so the queue
        // is never empty at the age.
        tokio::time::sleep(Duration::from_millis(900)).await;
        let mut writers = Vec::new();
        for _ in 0..3 {
            let mut writer = client.clone();
            writers.push(tokio::spawn(async move {
                let mut results = Vec::new();
                let start = std::time::Instant::now();
                while start.elapsed() < Duration::from_millis(300) {
                    results.push(writer.resolve_ref(ResolveRefRequest::default()).await);
                }
                results
            }));
        }
        let mut calls = 0usize;
        for writer in writers {
            for result in writer.await.expect("the writer's task ran") {
                calls += 1;
                assert_eq!(
                    result.expect_err("the stub refuses every call").code(),
                    Code::NotFound,
                    "a call that was never sent is sent again and reaches the stub"
                );
            }
        }
        assert!(calls > 10, "the writers ran: {calls} calls");
    }

    /// The pin against hyper: a call the client loses at the age
    /// carries a `hyper::Error` in its chain - hyper's mark for a
    /// transport failure at the connection's end, on a request that was
    /// never sent: still in the dispatch queue (`Canceled`), or taken
    /// by the dispatcher with its HTTP/2 stream pending open
    /// (dispatch-gone) - and the stub's served count shows the server
    /// never ran it. A hyper bump that changes what the connection's
    /// end does to a queued call fails here. (#1419)
    #[tokio::test]
    async fn a_call_lost_at_the_age_was_never_sent() {
        let stub = Stub::default();
        let addr = serve(Duration::from_secs(1), stub.clone()).await;
        let client = raw_client(addr).await;
        assert_eq!(
            client
                .clone()
                .resolve_ref(ResolveRefRequest::default())
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        tokio::time::sleep(Duration::from_millis(900)).await;
        let served_before = stub.resolve_ref_calls.load(Ordering::SeqCst);
        let mut writers = Vec::new();
        for _ in 0..3 {
            let mut writer = client.clone();
            writers.push(tokio::spawn(async move {
                let mut results = Vec::new();
                let start = std::time::Instant::now();
                while start.elapsed() < Duration::from_millis(300) {
                    results.push(writer.resolve_ref(ResolveRefRequest::default()).await);
                }
                results
            }));
        }
        let mut issued = 0usize;
        let mut failed = 0usize;
        for writer in writers {
            for result in writer.await.expect("the writer's task ran") {
                issued += 1;
                // The stub's own answer is a refusal; anything else is
                // a call the client lost.
                if let Err(status) = result {
                    if status.code() == Code::NotFound {
                        continue;
                    }
                    failed += 1;
                    assert!(
                        is_unsent(&status),
                        "a failed call at the age must carry a hyper error: {status:?}"
                    );
                }
            }
        }
        assert!(
            failed > 0,
            "the race did not fire: a call must be lost at the age"
        );
        let served = stub.resolve_ref_calls.load(Ordering::SeqCst) - served_before;
        assert!(
            served + failed <= issued,
            "a lost call was served: {served} served, {failed} lost, {issued} sent"
        );
    }

    /// A call the server received is never sent again. The raw
    /// listener reads the request to the end of its stream - hyper
    /// surely took the body - and answers nothing, so the call ends in
    /// an error.
    /// The clean code sends the call once and accepts no more
    /// connections; the cap's mutant keeps resending a call whose body
    /// is already gone, and the server would see the same write three
    /// times. No write the server got is ever sent again. (#1419)
    #[tokio::test]
    async fn a_call_the_server_received_is_never_sent_again() {
        // The raw listener: the server's side of the HTTP/2 handshake,
        // then the request read to END_STREAM, then the connection
        // drops with no response. Each accept is counted.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let serve_accepted = Arc::clone(&accepted);
        tokio::spawn(async move {
            loop {
                let Ok((io, _)) = listener.accept().await else {
                    return;
                };
                serve_accepted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut io = io;
                    // The server's side of the handshake: an empty
                    // SETTINGS frame. A blocking write: try_write can
                    // answer WouldBlock before the reactor has marked
                    // the socket writable, and a dropped handshake
                    // would resend a call the body was never read on.
                    let settings: &[u8] = &[0, 0, 0, 4, 0, 0, 0, 0, 0];
                    if tokio::io::AsyncWriteExt::write_all(&mut io, settings)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    // The client's preface, then frames until a DATA
                    // frame carries END_STREAM: the whole request body
                    // is on the server now. Then drop the connection
                    // without a response.
                    let mut preface = [0u8; 24];
                    if tokio::io::AsyncReadExt::read_exact(&mut io, &mut preface)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    loop {
                        let mut header = [0u8; 9];
                        if tokio::io::AsyncReadExt::read_exact(&mut io, &mut header)
                            .await
                            .is_err()
                        {
                            return;
                        }
                        let length =
                            u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
                        let kind = header[3];
                        let end_stream = header[4] & 0x1 != 0;
                        let mut payload = vec![0u8; length];
                        if length > 0
                            && tokio::io::AsyncReadExt::read_exact(&mut io, &mut payload)
                                .await
                                .is_err()
                        {
                            return;
                        }
                        // A DATA frame with END_STREAM: the whole call
                        // is here, the body is taken for good.
                        if kind == 0 && end_stream {
                            return;
                        }
                    }
                });
            }
        });
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
        let channel = endpoint.connect().await.unwrap();
        let mut client = ObjectsClient::new(Resend::new(channel, endpoint));
        // The hang trap: without the cap the mutant retries forever
        // and outlives the wait.
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            client.resolve_ref(ResolveRefRequest::default()),
        )
        .await;
        assert!(result.is_ok(), "the call ends in time");
        let outcome = result.unwrap();
        assert!(
            outcome.is_err(),
            "the server answers nothing, so the call ends in an error: {outcome:?}"
        );
        // A late resend would need another connection; after a short
        // wait there is none beyond the one the call ran on.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let accepted = accepted.load(Ordering::SeqCst);
        assert_eq!(
            accepted, 1,
            "a call the server received is sent once, on one connection: {accepted} accepted"
        );
    }

    /// What a raw HTTP/2 server does to the one request it gets.
    #[derive(Clone, Copy, PartialEq)]
    enum Refusal {
        /// Read the request to its end, then `RST_STREAM` with a reason.
        Reset(h2::Reason),
        /// Read the request to its end, then a hand-written GOAWAY
        /// whose last stream id is zero, then close: every local
        /// stream is above that id, so this one was never processed.
        GoAwayAboveEveryStream,
        /// Read the request to its end, then h2's own abrupt
        /// shutdown: its GOAWAY's last stream id covers the request,
        /// so the server may have processed it.
        GoAwayCoveringTheCall,
        /// Read the request to its end, then answer `NOT_FOUND`.
        Answer,
    }

    /// The raw path behind `GoAwayAboveEveryStream`: the handshake,
    /// the request read frame by frame to the end of its stream, then
    /// a hand-written GOAWAY whose last stream id is zero - every
    /// local stream is above it - and the connection closes.
    async fn go_away_above_every_stream(mut io: tokio::net::TcpStream) {
        let settings: &[u8] = &[0, 0, 0, 4, 0, 0, 0, 0, 0];
        if tokio::io::AsyncWriteExt::write_all(&mut io, settings)
            .await
            .is_err()
        {
            return;
        }
        let mut preface = [0u8; 24];
        if tokio::io::AsyncReadExt::read_exact(&mut io, &mut preface)
            .await
            .is_err()
        {
            return;
        }
        loop {
            let mut header = [0u8; 9];
            if tokio::io::AsyncReadExt::read_exact(&mut io, &mut header)
                .await
                .is_err()
            {
                return;
            }
            let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
            let kind = header[3];
            let end_stream = header[4] & 0x1 != 0;
            let mut payload = vec![0u8; length];
            if length > 0
                && tokio::io::AsyncReadExt::read_exact(&mut io, &mut payload)
                    .await
                    .is_err()
            {
                return;
            }
            if kind == 0 && end_stream {
                break;
            }
        }
        let goaway: &[u8] = &[0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let _ = tokio::io::AsyncWriteExt::write_all(&mut io, goaway).await;
    }

    /// The raw HTTP/2 server behind the refusal tests: each
    /// connection reads the one request to the end of its stream -
    /// hyper surely took the body - and then does its plan's action.
    /// The accepts are counted.
    fn serve_refusals(listener: TcpListener, plan: Vec<Refusal>) -> Arc<AtomicUsize> {
        let accepted = Arc::new(AtomicUsize::new(0));
        let serve_accepted = Arc::clone(&accepted);
        tokio::spawn(async move {
            loop {
                let Ok((io, _)) = listener.accept().await else {
                    return;
                };
                let step = serve_accepted.fetch_add(1, Ordering::SeqCst);
                let Some(action) = plan.get(step).copied() else {
                    continue;
                };
                tokio::spawn(async move {
                    if action == Refusal::GoAwayAboveEveryStream {
                        go_away_above_every_stream(io).await;
                        return;
                    }
                    let Ok(mut connection) = h2::server::handshake(io).await else {
                        return;
                    };
                    if action == Refusal::GoAwayCoveringTheCall {
                        // The body must be fully read before the
                        // covering GOAWAY, and the connection must
                        // keep being polled while it streams, or the
                        // window updates never flush.
                        let Some(Ok((request, _respond))) = connection.accept().await else {
                            return;
                        };
                        let (done, mut done_rx) = tokio::sync::oneshot::channel::<()>();
                        tokio::spawn(async move {
                            let mut body = request.into_body();
                            while let Some(Ok(chunk)) = body.data().await {
                                let _ = body.flow_control().release_capacity(chunk.len());
                            }
                            let _ = done.send(());
                        });
                        loop {
                            tokio::select! {
                                result = connection.accept() => {
                                    if result.is_none() {
                                        return;
                                    }
                                }
                                _ = &mut done_rx => {
                                    connection.abrupt_shutdown(
                                        h2::Reason::INTERNAL_ERROR,
                                    );
                                    let _ = std::future::poll_fn(|cx| {
                                        connection.poll_closed(cx)
                                    })
                                    .await;
                                    return;
                                }
                            }
                        }
                    }
                    loop {
                        let accepted_stream = connection.accept().await;
                        let Some(Ok((request, mut respond))) = accepted_stream else {
                            println!("SERVER accept[{step}]: {accepted_stream:?}");
                            return;
                        };
                        println!("SERVER accept[{step}]: got a stream");
                        tokio::spawn(async move {
                            let mut body = request.into_body();
                            while let Some(Ok(chunk)) = body.data().await {
                                let _ = body.flow_control().release_capacity(chunk.len());
                            }
                            match action {
                                Refusal::Reset(reason) => {
                                    respond.send_reset(reason);
                                }
                                Refusal::Answer => {
                                    let response = http::Response::builder()
                                        .status(200)
                                        .header("content-type", "application/grpc")
                                        .body(())
                                        .expect("a valid response head");
                                    let Ok(mut send) = respond.send_response(response, false)
                                    else {
                                        return;
                                    };
                                    let mut trailers = http::HeaderMap::new();
                                    trailers.insert("grpc-status", "5".parse().expect("a status"));
                                    let _ = send.send_data(bytes::Bytes::new(), false);
                                    let _ = send.send_trailers(trailers);
                                }
                                _ => {}
                            }
                        });
                    }
                });
            }
        });
        accepted
    }

    /// A stream the server refused after taking the whole body is sent
    /// again: `REFUSED_STREAM` is the server's own word that it never
    /// processed the stream, and the second proof of the gate is the
    /// complete copy of what hyper took. The call reaches the server's
    /// answer on the second connection. (#1419)
    #[tokio::test]
    async fn a_stream_refused_after_the_take_is_sent_again() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = serve_refusals(
            listener,
            vec![Refusal::Reset(h2::Reason::REFUSED_STREAM), Refusal::Answer],
        );
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
        let channel = endpoint.connect().await.unwrap();
        let mut client = ObjectsClient::new(Resend::new(channel, endpoint));
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            client.resolve_ref(ResolveRefRequest::default()),
        )
        .await;
        assert!(result.is_ok(), "the call ends in time");
        let error = result.unwrap().unwrap_err();
        println!("DEBUG error: {error:?}");
        assert_eq!(
            error.code(),
            Code::NotFound,
            "the resent call reaches the server's answer"
        );
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            2,
            "one connection for the refusal, one for the resent call"
        );
    }

    /// A reset with a reason that says nothing about processing - a
    /// server that answered `INTERNAL_ERROR` - is never sent again: the
    /// server may have read the whole call. (#1419)
    #[tokio::test]
    async fn a_stream_reset_for_another_reason_is_never_sent_again() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = serve_refusals(listener, vec![Refusal::Reset(h2::Reason::INTERNAL_ERROR)]);
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
        let channel = endpoint.connect().await.unwrap();
        let mut client = ObjectsClient::new(Resend::new(channel, endpoint));
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            client.resolve_ref(ResolveRefRequest::default()),
        )
        .await;
        assert!(result.is_ok(), "the call ends in time");
        assert!(
            result.unwrap().is_err(),
            "the server answered nothing, so the call ends in an error"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "an unexplained reset after the take is never resent"
        );
    }

    /// A body bigger than the copy cap loses its copy, and a refusal
    /// after the take is never resent: the middleware never holds more
    /// than the cap. (#1419)
    #[tokio::test]
    async fn a_body_over_the_copy_cap_is_never_sent_again() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = serve_refusals(listener, vec![Refusal::Reset(h2::Reason::REFUSED_STREAM)]);
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
        let channel = endpoint.connect().await.unwrap();
        let mut client = ObjectsClient::new(Resend::new(channel, endpoint));
        let big = "x".repeat(RESEND_BODY_KEEP_BYTES * 2);
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            client.resolve_ref(ResolveRefRequest {
                repo: big,
                name: String::new(),
            }),
        )
        .await;
        assert!(result.is_ok(), "the call ends in time");
        assert!(
            result.unwrap().is_err(),
            "the server refused the call and the copy is gone"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "over the cap, a refused call is not resent"
        );
    }

    /// A GOAWAY whose last stream id is below the call is the server's
    /// word that it never processed the stream: h2 delivers that error
    /// only to streams above the last id. The call is sent again and
    /// reaches the answer. (#1419)
    #[tokio::test]
    async fn a_go_away_below_the_call_is_sent_again() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = serve_refusals(
            listener,
            vec![Refusal::GoAwayAboveEveryStream, Refusal::Answer],
        );
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
        let channel = endpoint.connect().await.unwrap();
        let mut client = ObjectsClient::new(Resend::new(channel, endpoint));
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            client.resolve_ref(ResolveRefRequest::default()),
        )
        .await;
        assert!(result.is_ok(), "the call ends in time");
        assert_eq!(
            result.unwrap().unwrap_err().code(),
            Code::NotFound,
            "the resent call reaches the server's answer"
        );
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            2,
            "one connection for the GOAWAY, one for the resent call"
        );
    }

    /// A GOAWAY whose last stream id covers the call is the server's
    /// word that the stream may have been processed: it is never sent
    /// again, whatever the reason. (#1419)
    #[tokio::test]
    async fn a_go_away_covering_the_call_is_never_sent_again() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = serve_refusals(listener, vec![Refusal::GoAwayCoveringTheCall]);
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
        let channel = endpoint.connect().await.unwrap();
        let mut client = ObjectsClient::new(Resend::new(channel, endpoint));
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            client.resolve_ref(ResolveRefRequest::default()),
        )
        .await;
        assert!(result.is_ok(), "the call ends in time");
        assert!(
            result.unwrap().is_err(),
            "the server answered nothing, so the call ends in an error"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "a stream the server may have processed is never resent"
        );
    }

    /// A server that refuses every stream after taking the whole body,
    /// on every connection: `REFUSED_STREAM` each time. The second
    /// proof earns each resend, and the cap is what ends the call -
    /// exactly `MAX_ATTEMPTS` connections, then the error. This pins
    /// the cap without a timing race: the test that used to reach it
    /// needed the connection age to land between three writers' sends
    /// and flaked. (#1419)
    #[tokio::test]
    async fn a_server_that_refuses_every_stream_loses_the_call_at_the_cap() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = serve_refusals(
            listener,
            vec![Refusal::Reset(h2::Reason::REFUSED_STREAM); 20],
        );
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
        let channel = endpoint.connect().await.unwrap();
        let mut client = ObjectsClient::new(Resend::new(channel, endpoint));
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            client.resolve_ref(ResolveRefRequest::default()),
        )
        .await;
        assert!(result.is_ok(), "the call ends in time");
        assert_eq!(
            result.unwrap().unwrap_err().code(),
            Code::Unavailable,
            "the server refused the call unprocessed, so the gate resends until the cap"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            MAX_ATTEMPTS,
            "the cap bounds the connections"
        );
    }

    /// A resent call leaves no connection behind: the fresh channel
    /// of the resend lives only as long as the call, so after the
    /// writers end, the only connection still open is the one the
    /// original client holds. (#1419)
    #[tokio::test]
    async fn a_resend_leaves_no_connection_behind() {
        let (addr, accepted, open) = serve_counting(Duration::from_secs(1), Stub::default()).await;
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
        let channel = endpoint.connect().await.unwrap();
        let client = ObjectsClient::new(Resend::new(channel, endpoint));
        assert_eq!(
            client
                .clone()
                .resolve_ref(ResolveRefRequest::default())
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        tokio::time::sleep(Duration::from_millis(900)).await;
        let mut writers = Vec::new();
        for _ in 0..3 {
            let mut writer = client.clone();
            writers.push(tokio::spawn(async move {
                let start = std::time::Instant::now();
                while start.elapsed() < Duration::from_millis(300) {
                    let _ = writer.resolve_ref(ResolveRefRequest::default()).await;
                }
            }));
        }
        for writer in writers {
            writer.await.expect("the writer's task ran");
        }
        // The resend opened a connection of its own at least once.
        assert!(
            accepted.load(Ordering::SeqCst) >= 2,
            "the resend used a connection of its own: {} accepted",
            accepted.load(Ordering::SeqCst)
        );
        // The fresh channels die with their calls; only the original
        // client's idle connection stays.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            1,
            "the fresh connections are closed with their calls"
        );
    }

    /// A proxy of an older attempt ends its stream with an error once
    /// a newer attempt owns the body: polled after the resend started,
    /// it sends a cancellation - which makes hyper reset the stream -
    /// and never a clean end, which would reach the server as a
    /// complete request with an empty body, a zero-message call a
    /// client-streaming handler may act on. (#1419)
    #[test]
    fn a_superseded_proxy_resets_its_stream() {
        let slot = Arc::new(Mutex::new(Slot {
            body: Some(Body::empty()),
            generation: 1,
            sent: Some(Sent::default()),
        }));
        let mut old = ProxyBody {
            replay: 0,
            slot: Arc::clone(&slot),
            generation: 0,
            inner: None,
        };
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        match HttpBody::poll_frame(Pin::new(&mut old), &mut cx) {
            Poll::Ready(Some(Err(error))) => {
                assert_eq!(error.code(), tonic::Code::Cancelled);
            }
            other => panic!("a superseded proxy must reset its stream, not send {other:?}"),
        }
        assert!(
            slot.lock().unwrap().body.is_some(),
            "the body stays with the attempt that owns it"
        );
    }

    /// A body hyper has not taken yet hides that it ends: no end-of-
    /// stream mark and no size, so hyper must poll the body to end the
    /// request, and the first poll - the one that takes the body out -
    /// is the only way the request reaches the wire. A body that said
    /// "empty" up front could reach the server whole while the slot is
    /// still full, and a resent call would then run twice. (#1419)
    #[test]
    fn an_untaken_body_hides_its_end() {
        let body = Body::new(ProxyBody::new(
            Arc::new(Mutex::new(Slot {
                body: Some(Body::empty()),
                generation: 0,
                sent: Some(Sent::default()),
            })),
            0,
        ));
        assert!(
            !HttpBody::is_end_stream(&body),
            "an untaken body hides its end"
        );
        assert_eq!(
            HttpBody::size_hint(&body).exact(),
            None,
            "an untaken body has no size hint"
        );
    }

    /// A call the server has received is never sent again: the stub's
    /// handler ends the connection under a served call, and the client
    /// gets the error, not a second attempt. The chain may carry a
    /// hyper error here - the reset the dead connection left behind -
    /// but the body was hyper's by then, the slot is empty, and an
    /// upload that started can never go out twice. (#1419)
    #[tokio::test]
    async fn a_call_the_server_saw_is_never_resent() {
        let stub = Stub::default();
        let addr = serve(Duration::from_secs(3600), stub.clone()).await;
        let mut client = resend_client(addr).await;
        let error = client
            .get_objects(GetObjectsRequest::default())
            .await
            .unwrap_err();
        assert_eq!(
            stub.get_objects_calls.load(Ordering::SeqCst),
            1,
            "the served call was sent once: {error:?}"
        );
    }
}
