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
    /// Anything else (the flush, the eviction drop).
    Other,
}

pub(crate) const PHASES: [Phase; 15] = [
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

/// One statement's round trip done: the wall time from the client side.
pub(crate) fn stmt_done(spent: Duration) {
    if let Some(trace) = Trace::get() {
        Trace::put(&trace.add_stmt(spent.as_nanos()));
    }
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
