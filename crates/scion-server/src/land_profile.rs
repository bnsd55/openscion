//! Branch-only instrumentation for issue #769, step 2: the breakdown of
//! the land's serialized slice. Every land records how long each phase
//! took and how much of that was statement round trips (the wall time
//! of `ask()`, not Postgres's own exec), then logs one line. The
//! profile harness averages the lines per cell. This module rides the
//! land-profile branch only: it is a measuring instrument, not product
//! code, and nothing outside `land.rs`, `pg.rs` and the harness reads
//! it.
//!
//! Attribution is a thread-local scratch slot: a land's phases and its
//! statements interleave within one task's poll (each statement blocks
//! in `ask()`), so the slot sees every statement the land issues. A
//! statement issued with no land in the slot counts into the free
//! bucket, which the log leaves out.

use std::cell::Cell;
use std::time::Duration;

/// The phases of one land, in the order they run. `Other` catches
/// statements issued outside a land.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Phase {
    /// The handler's checks: the caller and quota checks, the id
    /// parses, the reader rules.
    PreChecks,
    /// The handler's audit intent append, before the land records.
    PreIntent,
    /// The rest of the handler's pre-land wall: the worker's dispatch
    /// and the permit wait.
    PreRest,
    /// The reply's build, after the land's phases end and before the
    /// call answers.
    Post,
    /// The `record_land` commit, before the ref's slot.
    Record,
    /// Waiting for the ref's slot lock. No statements: the wait is
    /// queueing, its wall time is the queue's own.
    SlotWait,
    /// The outcome read: the land's row, then the plan's reads, in one
    /// autocommit pass.
    Outcome,
    /// The plan's path-index work: the walk, the check, the prune.
    Plan,
    /// Filling the root tree of the land's target.
    FillRoot,
    /// The blob lengths the tree build reads.
    Lens,
    /// The settle commit: the changeset, its trees, the ref's CAS, the
    /// land's row. The next three are its sub-spans.
    Settle,
    /// Inside the settle commit: storing the changeset and its trees.
    Build,
    /// Inside the settle commit: the ref's compare-and-swap.
    Cas,
    /// Inside the settle commit: the land's row moves to landed.
    SetState,
    /// Anything else (the eviction drop).
    Other,
    /// The changeset commit of a `CreateChangeset` call.
    CreateCommit,
    /// The row commit of a `PutBlob` call.
    PutBlobRow,
    /// A gc sweep's write transaction.
    GcSweep,
    /// The derive steps a land's old base fills.
    DeriveSteps,
    /// The blob files' fsync after a write; not on the connection.
    FlushFile,
    /// The build's preloaded reads and the deferred deletes' arming.
    PreloadDerived,
    /// The build's derive of the parent's root tree.
    DeriveRoot,
    /// The build's apply: the path tree rows' node encode, hash and put.
    ApplyTrees,
    /// The changeset row and the repo-blob rows.
    InsertChangeset,
    /// The derived rows' put and the old versions' removes.
    PutDerived,
    /// The generation read between the cas and the verdict.
    Generation,
}

pub(crate) const PHASES: [Phase; 26] = [
    Phase::PreChecks,
    Phase::PreIntent,
    Phase::PreRest,
    Phase::Record,
    Phase::SlotWait,
    Phase::Outcome,
    Phase::Plan,
    Phase::FillRoot,
    Phase::Lens,
    Phase::Settle,
    Phase::Build,
    Phase::Cas,
    Phase::SetState,
    Phase::Post,
    Phase::Other,
    Phase::CreateCommit,
    Phase::PutBlobRow,
    Phase::GcSweep,
    Phase::DeriveSteps,
    Phase::FlushFile,
    Phase::PreloadDerived,
    Phase::DeriveRoot,
    Phase::ApplyTrees,
    Phase::InsertChangeset,
    Phase::PutDerived,
    Phase::Generation,
];

impl Phase {
    fn slot(self) -> usize {
        PHASES.iter().position(|p| *p == self).expect("named phase")
    }

    /// The field name the log line and the harness use.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Phase::PreChecks => "pre_checks",
            Phase::PreIntent => "pre_intent",
            Phase::PreRest => "pre_rest",
            Phase::Post => "post",
            Phase::Record => "record",
            Phase::SlotWait => "slot_wait",
            Phase::Outcome => "outcome",
            Phase::Plan => "plan",
            Phase::FillRoot => "fill_root",
            Phase::Lens => "lens",
            Phase::Settle => "settle",
            Phase::Build => "build",
            Phase::Cas => "cas",
            Phase::SetState => "set_state",
            Phase::Other => "other",
            Phase::CreateCommit => "create_commit",
            Phase::PutBlobRow => "put_blob_row",
            Phase::GcSweep => "gc_sweep",
            Phase::DeriveSteps => "derive_steps",
            Phase::FlushFile => "flush_file",
            Phase::PreloadDerived => "preload_derived",
            Phase::DeriveRoot => "derive_root",
            Phase::ApplyTrees => "apply_trees",
            Phase::InsertChangeset => "insert_changeset",
            Phase::PutDerived => "put_derived",
            Phase::Generation => "generation",
        }
    }
}

/// One phase's counters: wall time, the statements it ran, and the
/// round-trip wall time of those statements.
#[derive(Default, Clone, Copy, Debug)]
pub struct Slot {
    /// The phase's wall time.
    pub ns: u128,
    /// The phase's statement count.
    pub stmts: u64,
    /// The phase's statements' round-trip time.
    pub stmt_ns: u128,
}

/// The counters of one land. The current phase is a stack: the guards
/// nest (build, cas and `set_state` ride inside the settle's commit), and
/// a flat "current" leaked the enclosing phase's statements into the
/// last inner phase that dropped.
#[derive(Default, Clone, Copy, Debug)]
pub struct Trace {
    /// One slot per phase, in `PHASES` order.
    pub slots: [Slot; PHASES.len()],
    stack: [Option<Phase>; PHASES.len()],
    depth: usize,
    /// The plan attempts the land used.
    pub attempts: u64,
}

thread_local! {
    static CURRENT: Cell<Option<Trace>> = const { Cell::new(None) };
    static PRE: Cell<Option<std::time::Duration>> = const { Cell::new(None) };
    static PRE_PARTS: Cell<Option<(std::time::Duration, std::time::Duration)>> =
        const { Cell::new(None) };
}

/// Hands the handler's pre-land wall to the land's trace: the handler
/// measures it on its own task (the trace cannot cross threads), and
/// the next `measure` on this thread takes it into the Pre phases.
pub(crate) fn seed_pre(pre: std::time::Duration) {
    PRE.with(|c| c.set(Some(pre)));
}

/// Splits the pre-land wall into its parts: the checks and the audit
/// intent's append; the rest is the dispatch and the permit wait.
pub(crate) fn seed_pre_parts(checks: std::time::Duration, intent: std::time::Duration) {
    PRE_PARTS.with(|c| c.set(Some((checks, intent))));
}

impl Trace {
    fn get() -> Option<Trace> {
        CURRENT.with(Cell::get)
    }

    fn put(trace: &Trace) {
        CURRENT.with(|c| c.set(Some(*trace)));
    }

    fn clear() {
        CURRENT.with(|c| c.set(None));
    }

    /// Runs `f` with a fresh land slot installed, and returns the
    /// trace. Nests only in tests, where the inner trace wins.
    pub(crate) fn measure<R>(f: impl FnOnce() -> R) -> (R, Trace) {
        let saved = Self::get();
        Self::put(&Trace::default());
        let mut trace = Trace::default();
        PRE.with(|c| {
            if let Some(pre) = c.get() {
                let mut total = pre.as_nanos();
                c.set(None);
                PRE_PARTS.with(|p| {
                    if let Some((checks, intent)) = p.get() {
                        let checks_ns = checks.as_nanos().min(total);
                        let intent_ns = intent.as_nanos().min(total - checks_ns);
                        trace.slots[Phase::PreChecks.slot()].ns = checks_ns;
                        trace.slots[Phase::PreIntent.slot()].ns = intent_ns;
                        total -= checks_ns + intent_ns;
                        p.set(None);
                    }
                });
                trace.slots[Phase::PreRest.slot()].ns = total;
            }
        });
        Self::put(&trace);
        let out = f();
        let trace = Self::get().unwrap_or_default();
        match saved {
            Some(saved) => Self::put(&saved),
            None => Self::clear(),
        }
        (out, trace)
    }

    fn add_wall(self, phase: Phase, ns: u128) -> Self {
        let mut trace = self;
        trace.slots[phase.slot()].ns += ns;
        trace
    }

    fn add_stmt(self, ns: u128) -> Self {
        let mut trace = self;
        let slot = trace
            .stack
            .get(trace.depth.wrapping_sub(1))
            .copied()
            .flatten()
            .unwrap_or(Phase::Other)
            .slot();
        trace.slots[slot].stmts += 1;
        trace.slots[slot].stmt_ns += ns;
        trace
    }

    fn push_phase(self, phase: Phase) -> Self {
        let mut trace = self;
        if trace.depth < trace.stack.len() {
            trace.stack[trace.depth] = Some(phase);
            trace.depth += 1;
        }
        trace
    }

    fn pop_phase(self) -> Self {
        let mut trace = self;
        if trace.depth > 0 {
            trace.depth -= 1;
            trace.stack[trace.depth] = None;
        }
        trace
    }

    fn add_attempt(self) -> Self {
        let mut trace = self;
        trace.attempts += 1;
        trace
    }

    /// The wall time of one phase, in nanoseconds.
    pub(crate) fn wall_ns(self, phase: Phase) -> u128 {
        self.slots[phase.slot()].ns
    }

    /// The round-trip wall time of the phase's statements.
    pub(crate) fn stmt_ns(self, phase: Phase) -> u128 {
        self.slots[phase.slot()].stmt_ns
    }

    /// The phase's statement count.
    pub(crate) fn stmts(self, phase: Phase) -> u64 {
        self.slots[phase.slot()].stmts
    }

    /// The whole trace's statement count and round-trip time.
    pub(crate) fn stmt_total(self) -> (u64, u128) {
        let mut stmts = 0;
        let mut ns = 0;
        for slot in &self.slots {
            stmts += slot.stmts;
            ns += slot.stmt_ns;
        }
        (stmts, ns)
    }
}

/// A phase guard: names the phase statements belong to, and adds its
/// wall time when dropped.
pub(crate) struct Guard {
    phase: Phase,
    started: std::time::Instant,
}

impl Guard {
    pub(crate) fn enter(phase: Phase) -> Guard {
        if let Some(trace) = Trace::get() {
            Trace::put(&trace.push_phase(phase));
        }
        Guard {
            phase,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let ns = self.started.elapsed().as_nanos();
        if let Some(trace) = Trace::get() {
            Trace::put(&trace.add_wall(self.phase, ns).pop_phase());
        }
    }
}

/// One lane customer's counters, cumulative since the process started.
/// The write lane holds at most one connection; every write transaction
/// queues for it (queued), then holds it (held). The land's own
/// transactions and the other customers' (the changeset commit, the
/// blob row, a gc sweep, the derive steps) all ride these counters;
/// the harness diffs them across the bench window like any gauge.
#[derive(Default)]
pub(crate) struct LaneSlot {
    pub calls: std::sync::atomic::AtomicU64,
    pub queued_ns: std::sync::atomic::AtomicU64,
    pub held_ns: std::sync::atomic::AtomicU64,
    pub stmts: std::sync::atomic::AtomicU64,
    pub stmt_ns: std::sync::atomic::AtomicU64,
    pub commit_ns: std::sync::atomic::AtomicU64,
}

pub(crate) static LANE: [LaneSlot; PHASES.len()] = {
    #[allow(clippy::declare_interior_mutable_const)]
    const SLOT: LaneSlot = LaneSlot {
        calls: std::sync::atomic::AtomicU64::new(0),
        queued_ns: std::sync::atomic::AtomicU64::new(0),
        held_ns: std::sync::atomic::AtomicU64::new(0),
        stmts: std::sync::atomic::AtomicU64::new(0),
        stmt_ns: std::sync::atomic::AtomicU64::new(0),
        commit_ns: std::sync::atomic::AtomicU64::new(0),
    };
    [SLOT; PHASES.len()]
};

thread_local! {
    /// The customer phase the non-land call sites name, while their
    /// write transaction runs. A land's own transactions need no guard:
    /// the phase trace's stack already names them.
    static LANE_CUSTOMER: Cell<Option<Phase>> = const { Cell::new(None) };
    /// The transaction's own customer, pinned when it enters the lane:
    /// the stack top moves as the transaction's nested guards (the
    /// settle's build, cas, set_state) come and go, but the whole
    /// transaction is one customer's hold.
    static LANE_TXN: Cell<Option<Phase>> = const { Cell::new(None) };
}

/// Names the lane customer the enclosing call site belongs to, for the
/// duration of the guard. Nesting is not expected; the inner wins.
pub(crate) fn lane_customer(phase: Phase) -> LaneCustomerGuard {
    LANE_CUSTOMER.with(|c| c.set(Some(phase)));
    LaneCustomerGuard
}

pub(crate) struct LaneCustomerGuard;

impl Drop for LaneCustomerGuard {
    fn drop(&mut self) {
        LANE_CUSTOMER.with(|c| c.set(None));
    }
}

/// The lane customer this thread's current write transaction belongs
/// to: the land phase stack's top when a land's trace is live, else
/// the guard-named customer, else Other.
/// The lane customer for the running transaction: the one pinned at
/// the lane's entry, else the stack top, else the guard-named one.
fn lane_phase_txn() -> Phase {
    LANE_TXN
        .with(Cell::get)
        .or_else(|| {
            Trace::get().and_then(|trace| {
                trace
                    .stack
                    .get(trace.depth.wrapping_sub(1))
                    .copied()
                    .flatten()
            })
        })
        .or_else(|| LANE_CUSTOMER.with(Cell::get))
        .unwrap_or(Phase::Other)
}

fn lane_phase() -> Phase {
    let phase = Trace::get().and_then(|trace| {
        trace
            .stack
            .get(trace.depth.wrapping_sub(1))
            .copied()
            .flatten()
    });
    if let Some(phase) = phase {
        return phase;
    }
    LANE_CUSTOMER.with(Cell::get).unwrap_or(Phase::Other)
}

/// The nanoseconds as u128, clamped into a u64 for the counters.
fn u64_ns(ns: u128) -> u64 {
    u64::try_from(ns).unwrap_or(u64::MAX)
}

/// A write transaction queued for the write connection for `ns`, then
/// holds it. The queue time lands before the hold; the hold's total
/// comes from the lane guard's drop. Splitting here keeps a queued
/// wait from reading as held work.
#[allow(clippy::cast_possible_truncation)] // ns is clamped to u64::MAX first
pub(crate) fn lane_queued(ns: Duration) {
    let ns = u64_ns(ns.as_nanos());
    let phase = lane_phase();
    LANE_TXN.with(|c| c.set(Some(phase)));
    let slot = &LANE[phase.slot()];
    slot.calls
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    slot.queued_ns
        .fetch_add(ns, std::sync::atomic::Ordering::Relaxed);
}

/// The write transaction's hold ended after `ns` more (the queue is
/// already counted).
#[allow(clippy::cast_possible_truncation)] // ns is clamped to u64::MAX first
pub(crate) fn lane_held(ns: Duration) {
    let ns = u64_ns(ns.as_nanos());
    let slot = &LANE[lane_phase_txn().slot()];
    slot.held_ns
        .fetch_add(ns, std::sync::atomic::Ordering::Relaxed);
    LANE_TXN.with(|c| c.set(None));
}

/// The transaction's COMMIT round trip took `ns`: the commit is its
/// own gauge, so a hold splits into statement time and commit time.
pub(crate) fn lane_commit(ns: Duration) {
    let ns = u64_ns(ns.as_nanos());
    let slot = &LANE[lane_phase_txn().slot()];
    slot.commit_ns
        .fetch_add(ns, std::sync::atomic::Ordering::Relaxed);
}

/// The open held spans, for the one-writer invariant: `current` is the
/// count of open spans right now, `max` the highest it reached. The
/// lane is one connection, so both must stay at 1.
pub(crate) static LANE_OPEN: (std::sync::atomic::AtomicU64, std::sync::atomic::AtomicU64) = (
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
);

/// One held span opened: the count goes up, and the max follows it.
pub(crate) fn lane_open() {
    let (cur, max) = &LANE_OPEN;
    let now = cur.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    max.fetch_max(now, std::sync::atomic::Ordering::Relaxed);
}

/// One held span closed.
pub(crate) fn lane_close() {
    LANE_OPEN
        .0
        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
}

/// The /metrics lines of the lane counters, as Prometheus gauges. The
/// harness diffs them across the bench window and divides by the lands.
#[must_use]
pub fn render_lane() -> Option<String> {
    use std::fmt::Write as _;
    let mut out = String::new();
    for phase in PHASES {
        let slot = &LANE[phase.slot()];
        let name = phase.name();
        let calls_n = slot.calls.load(std::sync::atomic::Ordering::Relaxed);
        if calls_n == 0 {
            continue;
        }
        let queued = slot.queued_ns.load(std::sync::atomic::Ordering::Relaxed);
        let held = slot.held_ns.load(std::sync::atomic::Ordering::Relaxed);
        let stmts = slot.stmts.load(std::sync::atomic::Ordering::Relaxed);
        let stmt_ns = slot.stmt_ns.load(std::sync::atomic::Ordering::Relaxed);
        // ns into seconds; a u64's last bits do not matter at this scale.
        #[allow(clippy::cast_precision_loss)]
        let queued_s = queued as f64 / 1e9;
        #[allow(clippy::cast_precision_loss)]
        let held_s = held as f64 / 1e9;
        #[allow(clippy::cast_precision_loss)]
        let round_s = stmt_ns as f64 / 1e9;
        let commit_spent = slot.commit_ns.load(std::sync::atomic::Ordering::Relaxed);
        #[allow(clippy::cast_precision_loss)]
        let commit_s = commit_spent as f64 / 1e9;
        let _ = write!(
            out,
            "scion_lane_calls{{phase=\"{name}\"}} {calls_n}\n\
             scion_lane_queued_seconds{{phase=\"{name}\"}} {queued_s:.6}\n\
             scion_lane_held_seconds{{phase=\"{name}\"}} {held_s:.6}\n\
             scion_lane_stmts{{phase=\"{name}\"}} {stmts}\n\
             scion_lane_stmt_seconds{{phase=\"{name}\"}} {round_s:.6}\n\
             scion_lane_commit_seconds{{phase=\"{name}\"}} {commit_s:.6}\n"
        );
    }
    let (cur, max) = &LANE_OPEN;
    let _ = write!(
        out,
        "scion_lane_open_holds {}\n\
         scion_lane_max_open_holds {}\n",
        cur.load(std::sync::atomic::Ordering::Relaxed),
        max.load(std::sync::atomic::Ordering::Relaxed)
    );
    (!out.is_empty()).then_some(out)
}

/// One statement's round trip done: the wall time from the client side.
pub(crate) fn stmt_done(spent: Duration) {
    if let Some(trace) = Trace::get() {
        Trace::put(&trace.add_stmt(spent.as_nanos()));
    }
    let ns = u64_ns(spent.as_nanos());
    let slot = &LANE[lane_phase().slot()];
    slot.stmts
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    slot.stmt_ns
        .fetch_add(ns, std::sync::atomic::Ordering::Relaxed);
}

/// One plan attempt: the land decided to plan again.
pub(crate) fn attempt_done() {
    if let Some(trace) = Trace::get() {
        Trace::put(&trace.add_attempt());
    }
}

/// The log line's fields, in phase order: `wall_ms` and `stmt_ms` and
/// `stmts` per phase, then the attempts and the whole-land statement
/// totals. The harness greps `target=land_profile`.
pub(crate) fn log(land_id: i64, total: Duration, trace: &Trace) {
    let (stmts, stmt_ns) = trace.stmt_total();
    for phase in PHASES {
        tracing::info!(
            target: "land_profile",
            land_id,
            phase = phase.name(),
            wall_ms = f64::from(u32::try_from(trace.wall_ns(phase) / 1_000).unwrap_or(u32::MAX)) / 1_000.0,
            stmt_ms = f64::from(u32::try_from(trace.stmt_ns(phase) / 1_000).unwrap_or(u32::MAX)) / 1_000.0,
            stmts = trace.stmts(phase),
        );
    }
    tracing::info!(
        target: "land_profile",
        land_id,
        phase = "total",
        wall_ms = total.as_secs_f64() * 1000.0,
        stmt_ms = f64::from(u32::try_from(stmt_ns / 1_000).unwrap_or(u32::MAX)) / 1_000.0,
        stmts,
        attempts = trace.attempts,
    );
}
