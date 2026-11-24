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
//! A call that was sent and then lost is never resent: by then hyper
//! has polled the request body - the proxy body took the real body
//! out of its slot on its first poll - so the slot no longer holds
//! it, and an upload that started is never sent twice.

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

/// How many times a call that never reached the wire may be sent: the
/// first send and two more. A resend is only ever made while the slot
/// still holds the body, so each one must re-earn it.
const MAX_ATTEMPTS: usize = 3;

/// The client half of the connection age. A tower service that wraps
/// the channel: a call whose error's source chain holds a
/// `hyper::Error` - hyper's transport failures, both of them marks of
/// a request that was never sent - and whose body hyper never polled
/// is handed to the channel again, with the same request.
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
/// transport failure of the connection, which at a connection's end
/// means a request that was never sent: still in the dispatch queue
/// (`Canceled`), or taken by the dispatcher with its HTTP/2 stream
/// pending open (dispatch-gone). The chain walk fails closed: no
/// `hyper::Error`, no resend. The slot check in [`run`] does the rest:
/// a body hyper polled is a request on the wire, never resent, whether
/// the server processed it or refused the stream.
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

/// A short name for the transport failure, for the resend log line.
fn kind_of(error: &HyperError) -> &'static str {
    if error.is_canceled() {
        "canceled"
    } else if error.is_user() {
        "dispatch gone"
    } else if error.is_closed() {
        "channel closed"
    } else if error.is_incomplete_message() {
        "incomplete message"
    } else if error.is_timeout() {
        "timeout"
    } else {
        "protocol"
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
    }));
    let mut attempt = 0;
    loop {
        if attempt > 0 {
            tracing::debug!(
                rpc = %path,
                attempt,
                transport = "fresh connection",
                "sending a call again that hyper never put on the wire"
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
                        .body
                        .is_some();
                if attempt + 1 == MAX_ATTEMPTS || !unsent {
                    if attempt > 0 {
                        tracing::warn!(
                            rpc = %path,
                            attempts = attempt + 1,
                            "a call that never reached the wire ran out of attempts"
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
}

/// The body of a possibly-resent call: the real body waits in the
/// slot, and the owning attempt's first poll takes it out. Once it is
/// out, hyper has the request on the wire, and no second attempt is
/// ever made.
#[derive(Debug)]
struct ProxyBody {
    slot: Arc<Mutex<Slot>>,
    generation: usize,
    inner: Option<Body>,
}

impl ProxyBody {
    fn new(slot: Arc<Mutex<Slot>>, generation: usize) -> Self {
        Self {
            slot,
            generation,
            inner: None,
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
        if self.inner.is_none() {
            let taken = {
                let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
                if slot.generation != self.generation {
                    // This attempt was superseded: its connection was
                    // lost, and a newer attempt owns the body now.
                    // An error frame makes hyper reset this stream; a
                    // clean end would send END_STREAM and the server
                    // would see a complete request with an empty body,
                    // which for a client-streaming upload is a valid
                    // zero-message call it may act on.
                    return Poll::Ready(Some(Err(Status::cancelled(
                        "the call was lost before it was sent",
                    ))));
                }
                slot.body.take()
            };
            match taken {
                Some(body) => self.inner = Some(body),
                // The body of this attempt is already gone. An error
                // frame resets the stream; a clean end would hand the
                // server a complete empty request, and a panic inside
                // hyper's connection task would end every call on the
                // connection.
                None => {
                    return Poll::Ready(Some(Err(Status::cancelled(
                        "the call was lost before it was sent",
                    ))));
                }
            }
        }
        match self.inner.as_mut() {
            Some(body) => Pin::new(body).poll_frame(cx),
            // The take above always fills the body, so this arm never
            // runs; it is here so a poll of this body can never panic.
            None => Poll::Ready(Some(Err(Status::cancelled(
                "the call was lost before it was sent",
            )))),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            Some(body) => body.size_hint(),
            // Unknown until taken, never the real body's hint: hyper
            // sends HEADERS with END_STREAM when the hint is exactly
            // zero, without ever polling the body, and an empty
            // client-streaming call could then reach the server while
            // the slot is still full. Unknown makes hyper poll.
            None => SizeHint::default(),
        }
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

    use super::{ProxyBody, Resend, Slot, is_unsent};

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
        }));
        let mut old = ProxyBody {
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
