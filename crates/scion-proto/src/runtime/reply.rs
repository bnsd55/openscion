//! How a `GetObjects` reply streams objects. The server and every cache tier
//! follow the same rules, so they share this code.
//!
//! A reply hands the parts of its objects ([`crate::runtime::object`]) to its client
//! one at a time, through a one-slot channel. Before a reply reads an
//! object's bytes, it takes the object's share of a byte budget: all the
//! memory that the reply holds for the object. The reply keeps the object,
//! and its share, until it has handed the object's last part to the channel.
//! Each part that goes into the channel is a buffer of its own, so a part
//! that waits for a client that stopped reading never keeps the rest of its
//! object in memory.
//!
//! A reply sends the next object only while it has sent less than
//! `REPLY_BUDGET_BYTES`. It ends when its client takes no part for
//! [`STALL_TIMEOUT`], or when the budget has no room for the next object for
//! [`BUSY_TIMEOUT`]. A reply finds its first object before the call answers.
//! If the budget has no room for it, the call fails with [`busy`]. So a reply
//! always holds at least one object, and never ends `OK` with none. A reply
//! that ends in the middle of an object ends with `DEADLINE_EXCEEDED`, as
//! `ReadFile` does, never `OK`.
//!
//! Not counted in the budget: the parts that a reply leaves in its channel
//! and in HTTP/2 when its client stops reading ([`crate::runtime::listen`]).

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::Stream;
use tonic::Status;

use crate::runtime::object::{Header, MAX_ENCODED_BYTES, UNPACK_BYTES};
use crate::v1::ObjectPart;
use crate::{Bytes, CHUNK_BYTES, MAX_MESSAGE_BYTES, MAX_OBJECT_BYTES};

/// A reply stops before the next object once it has sent this many encoded
/// bytes, so it holds less than this plus one object. The client asks again
/// for the rest, so one call stays short.
pub(crate) const REPLY_BUDGET_BYTES: usize = MAX_MESSAGE_BYTES;

/// The stall-path marks the probe reads: how many times a reply's stall
/// path took a hold, and how many bytes replies sent into their channels
/// before they stopped. Test-only.
pub(crate) static HOLD_CALLS: AtomicUsize = AtomicUsize::new(0);
pub(crate) static SENT_INTO_TX: AtomicUsize = AtomicUsize::new(0);

/// The most reads one reply keeps in flight while it sends, in the request's
/// order: enough to overlap the hops of a read with the sends of the objects
/// before it, without holding much of the reply budget for objects the
/// client may never take. Chosen at the knee of the runner sweep (2, 4, 8,
/// 16 over the many-small-files cell, the cache cold), which the PR that
/// introduced it quotes.
pub const READS_IN_FLIGHT: usize = 4;

/// The byte budget of one process: the most memory that all its replies
/// together hold for their objects.
pub const IN_FLIGHT_BYTES: usize = 4 * MAX_OBJECT_BYTES;

/// A reply ends when its client takes no part for this long, so a client
/// that stops reading does not hold the budget. (A request timeout covers
/// only the start of a streamed reply.)
pub const STALL_TIMEOUT: Duration = Duration::from_secs(120);

/// How long a reply waits for room in the budget for its next object. For
/// the first object, the call then fails with [`busy`]. For a later one, the
/// reply ends `OK` and the client asks again. A tier waits longer than this
/// for each step of its upstream, so it never gives up on a server that is
/// only busy.
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(30);

/// What a reply leaves in HTTP/2 when its client stops reading, as its
/// connection counts it ([`crate::runtime::listen::Stalled`]): the part in the
/// channel, the parts that hyper has read ahead, and the h2 send buffer —
/// under 4 MiB, so this pays for it all.
pub const STALLED_REPLY_BYTES: u64 = 4 << 20;

// Any one object fits in the budget, even when a tier holds it packed and
// unpacked with a decoder, and the budget counts in u32 permits.
const _: () = assert!(MAX_ENCODED_BYTES + MAX_OBJECT_BYTES + UNPACK_BYTES <= IN_FLIGHT_BYTES);
const _: () = assert!(IN_FLIGHT_BYTES <= u32::MAX as usize);

/// The encoded bytes of an object for a reply.
#[derive(Debug)]
pub enum Body {
    /// Chunks of [`CHUNK_BYTES`] (the last shorter), each a buffer of its
    /// own, as [`crate::runtime::object::read_chunks`] makes them. Each goes out as a
    /// part as it is.
    Chunks(Vec<Bytes>),
    /// One buffer. Each part is copied out of it as it goes, so a part never
    /// keeps the whole buffer.
    Whole(Bytes),
}

/// An object for a reply.
#[derive(Debug)]
pub struct Outgoing {
    /// What its first part says.
    pub header: Header,
    /// Its encoded bytes.
    pub body: Body,
}

/// An object for a reply, or why it cannot be sent, with its share of the
/// budget.
pub type Found = (Result<Outgoing, Status>, OwnedSemaphorePermit);

/// A part on its way to the client.
pub struct Sent {
    part: Result<ObjectPart, Status>,
    /// Whether the part ends its object (or is an error).
    last: bool,
}

/// The `GetObjects` reply: the parts from the channel. If the channel closes
/// in the middle of an object, the reply ends with `DEADLINE_EXCEEDED`.
pub struct ObjectStream {
    rx: mpsc::Receiver<Sent>,
    /// The last part taken did not end its object.
    in_object: bool,
    /// Where the reply parks its stall count, if it stalled. Dropping the
    /// body drops the parked count with it: the stream is what the stalled
    /// bytes are counted against.
    stall: StallSlot,
}

impl std::fmt::Debug for ObjectStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStream")
            .field("in_object", &self.in_object)
            .field("stall", &self.stall)
            .finish_non_exhaustive()
    }
}

impl Stream for ObjectStream {
    type Item = Result<ObjectPart, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        match this.rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(sent)) => {
                this.in_object = !sent.last;
                Poll::Ready(Some(sent.part))
            }
            Poll::Ready(None) if this.in_object => {
                this.in_object = false;
                Poll::Ready(Some(Err(Status::deadline_exceeded(
                    "the client took no part for the stall limit, in the middle of an object; ask again",
                ))))
            }
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

/// What a reply does when its client stops taking parts: the timeout it
/// waits per part, the handle of the connection that counts the parts a
/// stall leaves in HTTP/2, and the slot that parks the count.
pub struct ReplyStall {
    stall: Duration,
    stalled: Option<crate::runtime::listen::Stalled>,
    slot: StallSlot,
}

impl ReplyStall {
    /// The reply waits `stall` per part, parks the parts a stall leaves in
    /// HTTP/2 onto `stalled`'s connection count through `slot`.
    #[must_use]
    pub fn new(
        stall: Duration,
        stalled: Option<crate::runtime::listen::Stalled>,
        slot: StallSlot,
    ) -> Self {
        Self {
            stall,
            stalled,
            slot,
        }
    }
}

/// A new reply: the sender for [`send_objects`], the stream for tonic, and
/// the slot that carries the reply's stall count — `send_objects` parks it
/// there when the reply stalls, and the count goes when the stream does.
#[must_use]
pub fn channel() -> (mpsc::Sender<Sent>, ObjectStream, StallSlot) {
    let (tx, rx) = mpsc::channel(1);
    let stall = StallSlot::default();
    let stream = ObjectStream {
        rx,
        in_object: false,
        stall: stall.clone(),
    };
    (tx, stream, stall)
}

/// Where a stalled reply parks the [`crate::runtime::listen::Stall`] its
/// count comes back with, until the reply's body drops: its stream ended —
/// it finished, its client reset it, or the connection closed. One slot per
/// reply, made with the reply by [`channel`].
#[derive(Clone, Default)]
pub struct StallSlot(Arc<Mutex<Option<crate::runtime::listen::Stall>>>);

impl StallSlot {
    /// Parks `stall`. A reply parks at most once, on the stall that ends
    /// it; a second park gives the first count back.
    /// Takes the parked stall, if any, so its bytes count no more: the
    /// caller holds a fresh count for the same reply, and the old one must
    /// not add to it while both are live (the process total would see a
    /// connection twice).
    fn release(&self) {
        drop(
            self.0
                .lock()
                .expect("the stall slot lock is not poisoned")
                .take(),
        );
    }

    fn park(&self, stall: crate::runtime::listen::Stall) {
        let _ = self
            .0
            .lock()
            .expect("the stall slot lock is not poisoned")
            .replace(stall);
    }
}

impl std::fmt::Debug for StallSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parked = self.0.try_lock().is_ok_and(|stall| stall.is_some());
        f.debug_struct("StallSlot")
            .field("parked", &parked)
            .finish()
    }
}

/// The share of the budget for `bytes` bytes: at least 1, at most
/// [`IN_FLIGHT_BYTES`], which the asserts above keep within u32.
fn share(bytes: usize) -> u32 {
    u32::try_from(bytes.clamp(1, IN_FLIGHT_BYTES)).unwrap_or(u32::MAX)
}

/// Takes the share of `budget` for `bytes` bytes. Waits until the share is
/// free, at most `wait`: the budget is held by objects that clients have not
/// taken yet. `None` if the wait ends, or the budget is closed.
pub async fn take(
    budget: &Arc<Semaphore>,
    bytes: usize,
    wait: Duration,
) -> Option<OwnedSemaphorePermit> {
    let acquire = Arc::clone(budget).acquire_many_owned(share(bytes));
    tokio::time::timeout(wait, acquire).await.ok()?.ok()
}

/// Takes the share of `budget` for `bytes` bytes if it is free now.
#[must_use]
pub fn try_take(budget: &Arc<Semaphore>, bytes: usize) -> Option<OwnedSemaphorePermit> {
    Arc::clone(budget).try_acquire_many_owned(share(bytes)).ok()
}

/// Gives back the part of `permit` above the share for `bytes` bytes, once
/// the work that needed more memory is done.
pub fn trim(permit: &mut OwnedSemaphorePermit, bytes: usize) {
    let keep = share(bytes) as usize;
    drop(permit.split(permit.num_permits().saturating_sub(keep)));
}

/// The status of a call whose first object found no room in the budget
/// within [`BUSY_TIMEOUT`]. The client asks again later.
#[must_use]
pub fn busy() -> Status {
    Status::resource_exhausted("busy: no memory for this reply; retry later")
}

/// The parts of an object, made one at a time as the reply sends them.
struct Parts {
    /// Until the first part is made.
    header: Option<Header>,
    rest: Rest,
}

/// The encoded bytes of an object that are not in a part yet.
enum Rest {
    Chunks(VecDeque<Bytes>),
    Whole { buffer: Bytes, offset: usize },
}

impl Parts {
    fn new(object: Outgoing) -> Self {
        let rest = match object.body {
            Body::Chunks(chunks) => Rest::Chunks(chunks.into()),
            Body::Whole(buffer) => Rest::Whole { buffer, offset: 0 },
        };
        Self {
            header: Some(object.header),
            rest,
        }
    }

    /// The next part, and whether it is the last. `None` after the last.
    fn next(&mut self) -> Option<(ObjectPart, bool)> {
        let header = self.header.take();
        let (data, last) = match &mut self.rest {
            Rest::Chunks(chunks) => match chunks.pop_front() {
                Some(data) => (data, chunks.is_empty()),
                None if header.is_some() => (Bytes::new(), true),
                None => return None,
            },
            Rest::Whole { buffer, offset } => {
                if *offset == buffer.len() && header.is_none() {
                    return None;
                }
                let end = buffer.len().min(*offset + CHUNK_BYTES);
                // A part that is the whole buffer goes as it is. Any other
                // is copied out, so it does not keep the whole buffer.
                let data = if *offset == 0 && end == buffer.len() {
                    buffer.clone()
                } else {
                    Bytes::copy_from_slice(&buffer[*offset..end])
                };
                *offset = end;
                (data, end == buffer.len())
            }
        };
        let part = match header {
            Some(header) => header.first_part(data),
            None => ObjectPart {
                data,
                ..ObjectPart::default()
            },
        };
        Some((part, last))
    }
}

/// Sends `first`, which the caller found before the call answered, then one
/// object per key, part by part, as the client takes them. `produce` finds an
/// object, takes its share of the budget with [`take`], and only then reads
/// the bytes, so a reply that waits for budget holds no object. Stops after
/// an error, once the reply has sent `REPLY_BUDGET_BYTES`, when the client
/// has gone, when the read of a key gives up (`None`), or when the client
/// does not take a part within `stall`. A read has no time limit here: a
/// cache tier bounds its upstream fetches itself.
///
/// The reply keeps up to `reads_in_flight` reads running ahead of the object
/// it is sending, in an ordered window that hands out their results in the
/// request's order, whatever order they finish in. Each read holds its own
/// share of the budget from the moment it takes it, so a window of `N` holds
/// at most `N` shares for its reads, and the reply holds one more for the
/// object it is sending. The window is polled between the parts of the
/// object going out, so reads overlap the sends; a slow or stalled client
/// stops the sends, and with them the polling, until the stall ends the
/// reply and every share goes back.
///
/// A stall leaves the reply's parts in HTTP/2, where no budget counts them
/// ([`crate::runtime::listen`]), so `stalled` — the handle of the connection, from
/// [`crate::runtime::listen::Stalled::of`] — takes the estimate
/// [`STALLED_REPLY_BYTES`] onto its connection's count, and the reply parks
/// it in `slot`, which gives it back when the reply's body drops. `None`
/// when there is no such connection (tests, or a transport without the
/// count).
pub async fn send_objects<K, F>(
    first: Found,
    keys: impl IntoIterator<Item = K>,
    tx: mpsc::Sender<Sent>,
    stall: ReplyStall,
    reads_in_flight: usize,
    produce: impl Fn(K) -> F,
) where
    F: Future<Output = Option<Found>>,
{
    let mut keys = keys.into_iter();
    let mut window = Window::new(reads_in_flight);
    let mut next = Some(first);
    let mut sent = 0;
    let ReplyStall {
        stall,
        stalled,
        slot,
    } = stall;
    // The first reads start before the first object goes out, so they
    // overlap the send from the start.
    window.run_ahead(&mut keys, &produce).await;
    while let Some((object, permit)) = next {
        let mut parts = match object {
            Ok(object) => Parts::new(object),
            Err(status) => {
                let error = Sent {
                    part: Err(status),
                    last: true,
                };
                match tokio::time::timeout(stall, tx.send(error)).await {
                    // Sent: the client takes it, or has gone with it.
                    Ok(_) => {}
                    // It stopped reading: the parts stay in HTTP/2.
                    Err(_) => {
                        if let Some(stalled) = &stalled {
                            slot.release();
                            HOLD_CALLS.fetch_add(1, Ordering::Relaxed);
                            slot.park(stalled.hold(STALLED_REPLY_BYTES));
                        }
                    }
                }
                SENT_INTO_TX.fetch_add(sent, Ordering::Relaxed);
                return;
            }
        };
        while let Some((part, last)) = parts.next() {
            let len = part.data.len();
            let part = Sent {
                part: Ok(part),
                last,
            };
            match tokio::time::timeout(stall, tx.send(part)).await {
                Ok(Ok(())) => sent += len,
                // The client is gone or stalled. In the middle of an object,
                // the stream then ends with `DEADLINE_EXCEEDED`.
                Ok(Err(_)) => {
                    SENT_INTO_TX.fetch_add(sent, Ordering::Relaxed);
                    return;
                }
                // Stalled: the parts stay in HTTP/2 until the connection
                // ends or the client resets the stream. Count them, so a
                // connection that stalls too much is closed with them.
                Err(_) => {
                    if let Some(stalled) = &stalled {
                        slot.release();
                        HOLD_CALLS.fetch_add(1, Ordering::Relaxed);
                        slot.park(stalled.hold(STALLED_REPLY_BYTES));
                    }
                    return;
                }
            }
            // The reads in flight run while this object goes out. Each part
            // that left is one turn of the polling: nothing awaits here, but
            // every poll pushes a read over its next await point.
            window.run_ahead(&mut keys, &produce).await;
        }
        // The object's bytes are gone, and so is its share.
        drop((parts, permit));
        if sent >= REPLY_BUDGET_BYTES {
            SENT_INTO_TX.fetch_add(sent, Ordering::Relaxed);
            return;
        }
        // The next object in the request's order, from the window's front.
        // `None`: no keys are left, or the read at the front gave up. Both
        // end the reply, as the old one-read-at-a-time loop ended it.
        next = window.next(&mut keys, &produce).await;
    }
}

/// The ordered window of reads in flight: at most `limit` reads run at a
/// time, and the front read's result leaves first, whatever order the reads
/// finish in. Each entry is a read that has not finished, or the result it
/// finished with, still waiting for the entries ahead of it to go out.
struct Window<F> {
    limit: usize,
    entries: VecDeque<Entry<F>>,
}

/// One slot of the order: a read that runs, or the result it finished
/// with, still waiting for the entries ahead of it to go out.
enum Entry<F> {
    Running(Pin<Box<F>>),
    Done(Option<Found>),
}

impl<F: Future<Output = Option<Found>>> Window<F> {
    fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            entries: VecDeque::new(),
        }
    }

    /// Starts a read for each free slot, while keys are left.
    fn fill<K, P>(&mut self, keys: &mut impl Iterator<Item = K>, produce: &P)
    where
        P: Fn(K) -> F,
    {
        while self.entries.len() < self.limit
            && let Some(key) = keys.next()
        {
            self.entries
                .push_back(Entry::Running(Box::pin(produce(key))));
        }
    }

    /// Polls every read that has not finished, in the request's order, so
    /// they all run: a finished read parks its result in its entry, and a
    /// read that is behind another keeps running ahead of it. A read that
    /// fails or gives up parks that, in its place in the order.
    fn poll_all(&mut self, cx: &mut Context<'_>) {
        for entry in &mut self.entries {
            if let Entry::Running(read) = entry
                && let Poll::Ready(found) = read.as_mut().poll(cx)
            {
                *entry = Entry::Done(found);
            }
        }
    }

    /// One turn of the polling, without waiting: fills the free slots and
    /// polls what runs. Ready at once, always: the sends never wait on it.
    async fn run_ahead<K, P>(&mut self, keys: &mut impl Iterator<Item = K>, produce: &P)
    where
        P: Fn(K) -> F,
    {
        std::future::poll_fn(|cx| {
            self.fill(keys, produce);
            self.poll_all(cx);
            Poll::Ready(())
        })
        .await;
    }

    /// Waits for the read at the front of the order and yields its result.
    /// `None` when no keys are left, or when the read at the front gave up:
    /// the reply ends there. The reads behind it keep running until the
    /// reply drops them, and their shares go with them.
    async fn next<K, P>(&mut self, keys: &mut impl Iterator<Item = K>, produce: &P) -> Option<Found>
    where
        P: Fn(K) -> F,
    {
        std::future::poll_fn(|cx| {
            self.fill(keys, produce);
            self.poll_all(cx);
            match self.entries.front() {
                Some(Entry::Done(_)) => {
                    let Entry::Done(found) = self.entries.pop_front().unwrap() else {
                        unreachable!("the front was done")
                    };
                    Poll::Ready(found)
                }
                // The fill consumed the keys; an empty window with a result
                // nowhere in sight only happens when the keys ran out.
                None if self.entries.is_empty() => Poll::Ready(None),
                _ => Poll::Pending,
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio_stream::StreamExt as _;
    use tonic::Code;

    use super::*;
    use crate::runtime::object::read_chunks;
    use crate::v1::{Encoding, ObjectKind};

    /// A raw blob of `len` bytes, in one buffer or in chunks.
    fn blob(len: usize, whole: bool) -> Outgoing {
        let header = Header {
            kind: ObjectKind::Blob,
            id: [1; 32],
            encoding: Encoding::Raw,
            encoded_size: len,
            size: len,
        };
        let data = vec![9; len];
        let body = if whole {
            Body::Whole(Bytes::from(data))
        } else {
            Body::Chunks(read_chunks(&data[..], len).unwrap())
        };
        Outgoing { header, body }
    }

    /// Waits until `done` holds, at most 10 s.
    async fn until(done: impl Fn() -> bool) {
        let started = tokio::time::Instant::now();
        while !done() {
            assert!(started.elapsed() < Duration::from_secs(10), "timed out");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    #[tokio::test]
    async fn an_object_keeps_its_share_until_its_last_part_is_handed_on() {
        let chunk = CHUNK_BYTES;
        let len = 3 * chunk + 10;
        for whole in [true, false] {
            let budget = Arc::new(Semaphore::new(IN_FLIGHT_BYTES));
            let permit = take(&budget, len, BUSY_TIMEOUT).await.unwrap();
            let (tx, mut stream, _slot) = channel();
            let object = blob(len, whole);
            // Where the bytes of a whole buffer are.
            let buffer = match &object.body {
                Body::Whole(buffer) => buffer.as_ptr_range(),
                Body::Chunks(_) => std::ptr::null()..std::ptr::null(),
            };
            let first = (Ok(object), permit);
            let stall = Duration::from_secs(10);
            let reply = tokio::spawn(send_objects(
                first,
                [(); 0],
                tx,
                ReplyStall::new(stall, None, StallSlot::default()),
                1,
                |()| async { None },
            ));
            let held = || IN_FLIGHT_BYTES - budget.available_permits();

            // Each part is a buffer of its own. The share stays until the
            // reply has handed on the last part: after the client took the
            // third.
            let mut got = Vec::new();
            for _ in 0..2 {
                got.push(stream.next().await.unwrap().unwrap());
                assert_eq!(held(), len, "whole: {whole}");
            }
            got.push(stream.next().await.unwrap().unwrap());
            until(|| held() == 0).await;
            got.push(stream.next().await.unwrap().unwrap());
            let lens: Vec<usize> = got.iter().map(|p| p.data.len()).collect();
            assert_eq!(lens, [chunk, chunk, chunk, 10]);
            // A part of a whole buffer is a copy, not a slice that keeps it.
            let inside = |part: &ObjectPart| buffer.contains(&part.data.as_ptr());
            assert!(!got.iter().any(inside), "whole: {whole}");
            assert_eq!(got[0].encoded_size, len as u64);
            assert!(got[1..].iter().all(|p| p.encoded_size == 0));
            assert!(stream.next().await.is_none());
            reply.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_client_that_stops_in_the_middle_of_an_object_gets_an_error_and_frees_the_budget() {
        let len = 2 * CHUNK_BYTES + 1;
        let budget = Arc::new(Semaphore::new(IN_FLIGHT_BYTES));
        let permit = take(&budget, len, BUSY_TIMEOUT).await.unwrap();
        let (tx, mut stream, _slot) = channel();
        let stall = Duration::from_millis(50);
        let first = (Ok(blob(len, true)), permit);
        let reply = tokio::spawn(send_objects(
            first,
            [(); 0],
            tx,
            ReplyStall::new(stall, None, StallSlot::default()),
            1,
            |()| async { None },
        ));

        // The client takes one part and stops. The second part waits in the
        // channel, the third stalls, and the reply ends. Its object and its
        // share go; the part in the channel is a copy of 1 MiB.
        let first = stream.next().await.unwrap().unwrap();
        reply.await.unwrap();
        assert_eq!(budget.available_permits(), IN_FLIGHT_BYTES);
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.data.len(), CHUNK_BYTES);
        drop(first);
        let status = stream.next().await.unwrap().unwrap_err();
        assert_eq!(status.code(), Code::DeadlineExceeded, "{status:?}");
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn trim_gives_back_the_share_above_what_stays() {
        let budget = Arc::new(Semaphore::new(100));
        let mut permit = try_take(&budget, 60).unwrap();
        trim(&mut permit, 20);
        assert_eq!(budget.available_permits(), 80);
        trim(&mut permit, 0);
        assert_eq!(budget.available_permits(), 99);
    }

    /// A reply that stalls on its connection counts its estimate onto the
    /// process's gauge, and gives it back when the reply's body drops —
    /// here, the test dropping the stream. This is what keeps a connection
    /// that serves stall after stall from ratcheting its count up to the
    /// close limit for bytes it no longer holds.
    #[tokio::test]
    async fn a_stalled_reply_counts_until_its_body_drops() {
        use crate::runtime::listen::Stalled;
        use crate::runtime::metrics::Metrics;

        let metrics = Arc::new(Metrics::default());
        let stalled = Stalled::new(
            None,
            STALLED_REPLY_BYTES * 2,
            Arc::clone(&metrics),
            Arc::new(crate::runtime::listen::TotalBytes::new(u64::MAX)),
        );
        let budget = Arc::new(Semaphore::new(IN_FLIGHT_BYTES));
        let permit = take(&budget, 3 * CHUNK_BYTES, BUSY_TIMEOUT).await.unwrap();
        let (tx, mut stream, slot) = channel();
        let stall = Duration::from_millis(50);
        let first = (Ok(blob(3 * CHUNK_BYTES, true)), permit);
        let reply = tokio::spawn(send_objects(
            first,
            [(); 0],
            tx,
            ReplyStall::new(stall, Some(stalled), slot),
            1,
            |()| async { None },
        ));

        // The first part goes out; the next one stalls, and the reply ends.
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.data.len(), CHUNK_BYTES);
        reply.await.unwrap();
        until(|| metrics.stalled_reply_bytes() >= STALLED_REPLY_BYTES).await;

        // Dropping the body drops the parked count with it.
        drop(stream);
        until(|| metrics.stalled_reply_bytes() == 0).await;
    }

    /// A window of `N` reads in flight: the results leave in the request's
    /// order, whatever order the reads finish in; at most `N` reads run at
    /// a time, each holding its own share of the budget; and the reply
    /// holds one more share for the object being sent.
    #[tokio::test]
    async fn reads_run_ahead_in_order_and_never_past_the_window() {
        const N: usize = 4;
        const KEYS: usize = 12;
        // A budget of exactly N shares plus the one being sent, so a read
        // past the bound would wait for budget instead of failing the test.
        let share = 1000usize;
        let budget = Arc::new(Semaphore::new((N + 1) * share));
        let (tx, mut stream, _slot) = channel();
        // Each read holds a share and finishes out of order: the later the
        // key, the longer the read takes. A read's own probe counts how
        // many run at once, and the finish list shows the test's reads
        // really do finish out of order.
        let in_flight = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(Mutex::new(Vec::new()));
        let first = (
            Ok(blob(1, true)),
            take(&budget, share, BUSY_TIMEOUT).await.unwrap(),
        );
        let left = budget.clone();
        let done_list = finished.clone();
        let most_seen = most.clone();
        let reply = tokio::spawn(send_objects(
            first,
            0..KEYS,
            tx,
            ReplyStall::new(Duration::from_secs(10), None, StallSlot::default()),
            N,
            move |key: usize| {
                // Clones before the block: the block owns its handles, the
                // closure keeps its own, so the closure is a plain `Fn`.
                let (in_flight, most, finished, budget) = (
                    in_flight.clone(),
                    most.clone(),
                    finished.clone(),
                    budget.clone(),
                );
                async move {
                    let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    most.fetch_max(now, Ordering::SeqCst);
                    // The reads finish in the reverse of their order, so
                    // the window has to reorder them to keep the promise.
                    tokio::time::sleep(Duration::from_millis(20 * (KEYS - key) as u64)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    {
                        let mut finished = finished.lock().unwrap();
                        finished.push(key);
                    }
                    // The share for the object's bytes, as `produce` takes
                    // it before the object goes out.
                    let permit = take(&budget, share, BUSY_TIMEOUT).await.unwrap();
                    // A size of its own, so the order of the parts names
                    // the order of the keys.
                    Some((Ok(blob(share + key, true)), permit))
                }
            },
        ));

        // The object found before the reply began, then every object in
        // the request's order: the size names the key that sent it.
        let first_part = stream.next().await.unwrap().unwrap();
        assert_eq!(first_part.data.len(), 1);
        for key in 0..KEYS {
            let part = stream.next().await.unwrap().unwrap();
            assert_eq!(part.data.len(), share + key, "key {key} out of order");
            assert_eq!(part.encoded_size, (share + key) as u64);
        }
        assert!(stream.next().await.is_none());
        reply.await.unwrap();

        // The reads finished in the reverse of their order, and all of
        // them ran; the most at once was the bound; and every share came
        // back when the reply ended.
        let done = done_list.lock().unwrap().clone();
        let mut sorted = done.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..KEYS).collect::<Vec<_>>());
        assert_ne!(done, sorted, "the test's reads did not finish out of order");
        assert_eq!(most_seen.load(Ordering::SeqCst), N);
        assert_eq!(left.available_permits(), (N + 1) * share);
    }
}
