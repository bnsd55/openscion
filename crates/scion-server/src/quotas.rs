//! Per-principal limits: requests in flight and upload bytes in flight.
//!
//! The limits in [`service`](crate::service) and [`store`](crate::store)
//! bound the whole server. These bound one principal, so that one client
//! cannot take every queue slot or spool byte. A limit of `None` turns the
//! check off.
//!
//! The counts live in memory, one row per principal that a call named. A
//! guard takes its units on entry and gives them back on every exit path,
//! also when the client goes away or a job panics. The pending-land limit is
//! not counted here: a pending land outlives its call, so the crate's
//! `land` module counts its rows in the database.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use scion_proto::runtime::auth::Caller;

use crate::error::{Error, Result};

/// The per-principal limits, and the counts. A clone shares the counts.
#[derive(Clone, Debug, Default)]
pub struct Quotas {
    requests: Option<usize>,
    upload_bytes: Option<usize>,
    /// The share of the server's small limits one principal may hold,
    /// under `--access`. All `None` in single-principal mode.
    shares: Shares,
    // SHORTCUT: the counts live in memory, so a restart forgets them; only
    // the pending-lands cap counts rows of the store. Count the in-flight
    // work in the store, or share the counts across processes, if one
    // process ever stops being the whole server.
    usage: Arc<Mutex<HashMap<String, Usage>>>,
}

/// Which server limit a share take counts. The share is the principal's
/// part of a limit the whole server shares, under `--access` (#1391).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShareKind {
    /// One of the blame slots.
    Blame,
    /// One of the diff slots.
    Diff,
    /// One of the graph-query slots (`MergeBase`, `IsAncestor`, `Log` with
    /// a path).
    GraphQuery,
    /// Bytes of the read budget a reply holds.
    ReadBytes,
    /// One upload permit.
    UploadPermit,
}

impl ShareKind {
    /// The limit's name, for the refusal's message.
    fn name(self) -> &'static str {
        match self {
            ShareKind::Blame => "blame",
            ShareKind::Diff => "diff",
            ShareKind::GraphQuery => "graph-query",
            ShareKind::ReadBytes => "read-budget",
            ShareKind::UploadPermit => "upload",
        }
    }

    fn units(self, usage: &Usage) -> usize {
        match self {
            ShareKind::Blame => usage.blames,
            ShareKind::Diff => usage.diffs,
            ShareKind::GraphQuery => usage.graph_queries,
            ShareKind::ReadBytes => usage.read_bytes,
            ShareKind::UploadPermit => usage.upload_permits,
        }
    }

    /// The kind's share limit, when the server runs under `--access`.
    fn limit_of(self, shares: &Shares) -> Option<usize> {
        match self {
            ShareKind::Blame => shares.blames,
            ShareKind::Diff => shares.diffs,
            ShareKind::GraphQuery => shares.graph_queries,
            ShareKind::ReadBytes => shares.read_bytes,
            ShareKind::UploadPermit => shares.upload_permits,
        }
    }

    fn take(self, usage: &mut Usage, units: usize) {
        match self {
            ShareKind::Blame => usage.blames += units,
            ShareKind::Diff => usage.diffs += units,
            ShareKind::GraphQuery => usage.graph_queries += units,
            ShareKind::ReadBytes => usage.read_bytes += units,
            ShareKind::UploadPermit => usage.upload_permits += units,
        }
    }

    fn give_back(self, usage: &mut Usage, units: usize) {
        match self {
            ShareKind::Blame => usage.blames = usage.blames.saturating_sub(units),
            ShareKind::Diff => usage.diffs = usage.diffs.saturating_sub(units),
            ShareKind::GraphQuery => {
                usage.graph_queries = usage.graph_queries.saturating_sub(units);
            }
            ShareKind::ReadBytes => usage.read_bytes = usage.read_bytes.saturating_sub(units),
            ShareKind::UploadPermit => {
                usage.upload_permits = usage.upload_permits.saturating_sub(units);
            }
        }
    }
}

/// The share of the server's small limits that one principal may hold
/// under `--access`: half of each, rounded up (#1391). `None` is no
/// share for that limit. A server of one token runs with no shares:
/// every client is the same principal, so a share would be a global cap.
#[derive(Clone, Copy, Debug, Default)]
pub struct Shares {
    blames: Option<usize>,
    diffs: Option<usize>,
    graph_queries: Option<usize>,
    read_bytes: Option<usize>,
    upload_permits: Option<usize>,
}

impl Shares {
    /// No shares: single-principal mode keeps the whole limits.
    #[must_use]
    pub fn off() -> Self {
        Self::default()
    }

    /// Half of each small limit, rounded up, over the server's consts:
    /// 2 of 4 blames, 2 of 4 diffs, 2 of 4 graph queries, 128 MiB of the
    /// 256 MiB read budget, 8 of 16 upload permits. No flags: the half
    /// is policy over the existing caps.
    #[must_use]
    pub fn for_access() -> Self {
        Self {
            blames: Some(usize::div_ceil(crate::service::MAX_BLAMES, 2)),
            diffs: Some(usize::div_ceil(crate::service::MAX_DIFFS, 2)),
            graph_queries: Some(usize::div_ceil(crate::service::MAX_GRAPH_QUERIES, 2)),
            read_bytes: Some(usize::div_ceil(crate::service::READ_BUDGET_BYTES, 2)),
            upload_permits: Some(usize::div_ceil(crate::service::MAX_UPLOADS, 2)),
        }
    }
}

impl Quotas {
    /// Limits each principal to `requests` calls in flight and
    /// `upload_bytes` upload bytes in flight. `None` turns a limit off.
    #[must_use]
    pub fn new(requests: Option<usize>, upload_bytes: Option<usize>) -> Self {
        Self {
            requests,
            upload_bytes,
            shares: Shares::off(),
            usage: Arc::default(),
        }
    }

    /// Sets the per-limit shares one principal may hold, under
    /// `--access`. A take of a kind checks the kind's share.
    #[must_use]
    pub fn with_shares(mut self, shares: Shares) -> Self {
        self.shares = shares;
        self
    }

    /// No limits. A server of one token runs this unless a flag says
    /// otherwise: every client is the same principal, so a cap would be a
    /// global one.
    #[must_use]
    pub fn off() -> Self {
        Self::new(None, None)
    }

    /// Takes one of the in-flight requests of `caller`, until the [`Call`]
    /// drops.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceExhausted`] if the principal is over its limit.
    pub(crate) fn enter(&self, caller: &Caller) -> Result<Call> {
        let Some(limit) = self.requests else {
            return Ok(self.nothing(caller));
        };
        let mut usage = self.lock();
        let entry = usage.entry(caller.0.clone()).or_default();
        if entry.requests >= limit {
            return Err(Error::ResourceExhausted(format!(
                "principal {:?} has its {limit} requests in flight \
                 (--max-principal-requests {limit}); retry later",
                caller.0
            )));
        }
        entry.requests += 1;
        Ok(Call {
            usage: Arc::clone(&self.usage),
            principal: caller.0.clone(),
            limit: None,
            bytes: 0,
            counts_requests: true,
        })
    }

    /// Takes `bytes` of the in-flight upload budget of `caller`. The caller
    /// adds the bytes of each later message with [`Call::add`], and gives
    /// everything back by dropping the guard.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceExhausted`] if the principal is over its limit.
    pub(crate) fn upload(&self, caller: &Caller, bytes: usize) -> Result<Call> {
        let limit = self.upload_bytes;
        let Some(limit) = limit else {
            return Ok(self.nothing(caller));
        };
        let mut usage = self.lock();
        let entry = usage.entry(caller.0.clone()).or_default();
        if entry.upload_bytes + bytes > limit {
            return Err(Error::ResourceExhausted(format!(
                "principal {:?} would have more than {limit} upload bytes in flight \
                 (--max-principal-upload-bytes {limit}); retry with fewer or smaller uploads",
                caller.0
            )));
        }
        entry.upload_bytes += bytes;
        Ok(Call {
            usage: Arc::clone(&self.usage),
            principal: caller.0.clone(),
            limit: Some(limit),
            bytes,
            counts_requests: false,
        })
    }

    fn nothing(&self, caller: &Caller) -> Call {
        Call {
            usage: Arc::clone(&self.usage),
            principal: caller.0.clone(),
            limit: None,
            bytes: 0,
            counts_requests: false,
        }
    }

    /// Takes `units` of the share of `kind` that `caller` may hold, until
    /// the returned [`Share`] drops. The share is the principal's part of
    /// a limit the whole server shares, under `--access` (#1391): a take
    /// over it is refused at once, not after a wait — it is the
    /// principal's own cap.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceExhausted`] if the principal holds its whole
    /// share, or would go over it with `units` more.
    pub(crate) fn take(&self, caller: &Caller, kind: ShareKind, units: usize) -> Result<Share> {
        let Some(limit) = kind.limit_of(&self.shares) else {
            return Ok(Share {
                usage: Arc::clone(&self.usage),
                principal: caller.0.clone(),
                kind,
                units: 0,
            });
        };
        let mut usage = self.lock();
        let entry = usage.entry(caller.0.clone()).or_default();
        if kind.units(entry) + units > limit {
            return Err(Error::ResourceExhausted(format!(
                "principal {:?} would hold more than its {limit} {} share, \
                 half of the server's limit under --access; retry later",
                caller.0,
                kind.name(),
            )));
        }
        kind.take(entry, units);
        Ok(Share {
            usage: Arc::clone(&self.usage),
            principal: caller.0.clone(),
            kind,
            units,
        })
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Usage>> {
        self.usage.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The requests in flight of one principal, as the tests read them.
    /// The tests of the caps wait on this, on the server's own state,
    /// instead of polling through the network: a probe's reply rides the
    /// same connection whose flow control a stalled stream holds, and
    /// under load its arrival says nothing about the quota.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn in_flight(&self, principal: &str) -> usize {
        self.lock().get(principal).map_or(0, |entry| entry.requests)
    }
}

/// What one principal holds at the moment.
#[derive(Debug, Default)]
struct Usage {
    requests: usize,
    upload_bytes: usize,
    /// Held units of the server's small limits, by kind (#1391).
    blames: usize,
    diffs: usize,
    graph_queries: usize,
    read_bytes: usize,
    upload_permits: usize,
}

/// One call's share of a server limit. Gives it back on drop, on every
/// exit path, a job panic included: the guard moves wherever the call's
/// semaphore permit lives, and drops beside it.
#[derive(Debug)]
pub(crate) struct Share {
    usage: Arc<Mutex<HashMap<String, Usage>>>,
    principal: String,
    kind: ShareKind,
    units: usize,
}

impl Drop for Share {
    /// Gives the units back, on every exit path.
    fn drop(&mut self) {
        let mut usage = self.usage.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = usage.get_mut(&self.principal) else {
            return;
        };
        self.kind.give_back(entry, self.units);
    }
}

/// What one call holds. Gives it back on drop, on every exit path.
#[derive(Debug, Default)]
pub(crate) struct Call {
    usage: Arc<Mutex<HashMap<String, Usage>>>,
    principal: String,
    /// The limit for later [`Call::add`]s, when one is set.
    limit: Option<usize>,
    /// The in-flight bytes of the upload this guards, if any.
    bytes: usize,
    counts_requests: bool,
}

impl Call {
    /// Adds the bytes of one more upload message.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceExhausted`] if the principal would go over its
    /// limit. The bytes of the message are not taken.
    pub(crate) fn add(&mut self, bytes: usize) -> Result<()> {
        let Some(limit) = self.limit else {
            return Ok(());
        };
        let mut usage = self.usage.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = usage.entry(self.principal.clone()).or_default();
        if entry.upload_bytes + bytes > limit {
            return Err(Error::ResourceExhausted(format!(
                "principal {:?} would have more than {limit} upload bytes in flight \
                 (--max-principal-upload-bytes {limit}); retry with fewer or smaller uploads",
                self.principal
            )));
        }
        entry.upload_bytes += bytes;
        self.bytes += bytes;
        Ok(())
    }
}

impl Drop for Call {
    /// Gives back what this call held, on every exit path.
    fn drop(&mut self) {
        let mut usage = self.usage.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = usage.get_mut(&self.principal) else {
            return;
        };
        if self.counts_requests {
            entry.requests = entry.requests.saturating_sub(1);
        }
        entry.upload_bytes = entry.upload_bytes.saturating_sub(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caller(name: &str) -> Caller {
        Caller(name.into())
    }

    #[test]
    fn a_call_takes_one_slot_and_gives_it_back() {
        let quotas = Quotas::new(Some(1), None);
        let guard = quotas.enter(&caller("a")).unwrap();
        let status = quotas.enter(&caller("a")).unwrap_err();
        assert!(matches!(status, Error::ResourceExhausted(_)), "{status:?}");
        assert!(status.to_string().contains("--max-principal-requests 1"));
        drop(guard);
        quotas.enter(&caller("a")).unwrap();
    }

    #[test]
    fn principals_count_separately() {
        let quotas = Quotas::new(Some(1), None);
        let _a = quotas.enter(&caller("a")).unwrap();
        let _b = quotas.enter(&caller("b")).unwrap();
        quotas.enter(&caller("b")).unwrap_err();
        // The failed take took nothing.
        quotas.enter(&caller("b")).unwrap_err();
    }

    #[test]
    fn upload_bytes_add_up_and_come_back() {
        let quotas = Quotas::new(None, Some(10));
        let mut upload = quotas.upload(&caller("a"), 6).unwrap();
        upload.add(4).unwrap();
        assert!(upload.add(1).is_err());
        drop(upload);
        // The bytes came back, so a new upload fits again.
        let mut upload = quotas.upload(&caller("a"), 10).unwrap();
        assert!(upload.add(1).is_err());
        drop(upload);
        quotas.upload(&caller("a"), 10).unwrap();
    }

    #[test]
    fn a_dropped_upload_gives_back_its_own_bytes_only() {
        let quotas = Quotas::new(None, Some(10));
        // One call takes 6 and adds 2: its own hold is 8 bytes.
        let mut first = quotas.upload(&caller("a"), 6).unwrap();
        first.add(2).unwrap();
        // A second call takes 1 more while the first is still held.
        let second = quotas.upload(&caller("a"), 1).unwrap();
        // The first's drop gives back its own 8; the second's 1 stays
        // held, so the whole cap does not fit again.
        drop(first);
        quotas.upload(&caller("a"), 10).unwrap_err();
        drop(second);
        // All of it came back now that both calls are gone.
        quotas.upload(&caller("a"), 10).unwrap();
    }

    #[test]
    fn principals_share_no_bytes() {
        let quotas = Quotas::new(None, Some(10));
        let _a = quotas.upload(&caller("a"), 10).unwrap();
        quotas.upload(&caller("b"), 10).unwrap();
    }

    /// A take over the principal's share is refused at once, and a drop
    /// gives the units back (#1391).
    #[test]
    fn a_take_over_its_share_is_refused_and_a_drop_gives_back() {
        let quotas = Quotas::off().with_shares(Shares {
            blames: Some(2),
            ..Shares::default()
        });
        let first = quotas.take(&caller("p"), ShareKind::Blame, 1).unwrap();
        let second = quotas.take(&caller("p"), ShareKind::Blame, 1).unwrap();
        let status = quotas.take(&caller("p"), ShareKind::Blame, 1).unwrap_err();
        assert!(
            matches!(status, Error::ResourceExhausted(ref why) if why.contains("its 2 blame share")),
            "{status:?}"
        );
        assert!(status.to_string().contains("under --access"), "{status}");
        // The refused take took nothing: the whole share still fits.
        drop(first);
        drop(second);
        let _third = quotas.take(&caller("p"), ShareKind::Blame, 2).unwrap();
    }

    /// Principals hold their own shares: one at its cap never blocks
    /// another (#1391).
    #[test]
    fn principals_hold_their_own_shares() {
        let quotas = Quotas::off().with_shares(Shares {
            diffs: Some(1),
            ..Shares::default()
        });
        let _p = quotas.take(&caller("p"), ShareKind::Diff, 1).unwrap();
        let _b = quotas.take(&caller("b"), ShareKind::Diff, 1).unwrap();
    }

    /// No share for a kind caps nothing: the whole server limit is one
    /// principal's to hold (single-principal mode, #1391).
    #[test]
    fn no_share_caps_nothing() {
        let quotas = Quotas::off();
        let held: Vec<_> = (0..64)
            .map(|_| quotas.take(&caller("p"), ShareKind::Blame, 1).unwrap())
            .collect();
        drop(held);
    }

    /// The `--access` shares are half of each server limit, rounded up
    /// (#1391).
    #[test]
    fn the_access_shares_are_half_rounded_up() {
        let shares = Shares::for_access();
        let limit = |kind: ShareKind| kind.limit_of(&shares);
        assert_eq!(limit(ShareKind::Blame), Some(2));
        assert_eq!(limit(ShareKind::Diff), Some(2));
        assert_eq!(limit(ShareKind::GraphQuery), Some(2));
        assert_eq!(
            limit(ShareKind::ReadBytes),
            Some(usize::div_ceil(crate::service::READ_BUDGET_BYTES, 2))
        );
        assert_eq!(limit(ShareKind::UploadPermit), Some(8));
    }

    #[test]
    fn no_limits_take_nothing() {
        let quotas = Quotas::off();
        let held: Vec<_> = (0..1000)
            .map(|_| quotas.enter(&caller("a")).unwrap())
            .collect();
        let mut upload = quotas.upload(&caller("a"), usize::MAX - 1).unwrap();
        upload.add(usize::MAX - 1).unwrap();
        drop(upload);
        drop(held);
    }
}
