//! The malloc arena cap of the long-lived serving processes (#1421).
//!
//! Under thread contention glibc opens more arenas, and each arena
//! keeps its own freed memory, while the live heap stays near 11 MB -
//! at 24k lands, 73.5 MB uncapped against 35.8 MB capped, p99
//! unchanged. The cap holds that climb at 2.
//!
//! The cap applies where a process serves for a long time: the server,
//! scn mount, the cache, and the daemon. One-shot commands keep the
//! default.

/// The arena count a serving process caps itself at when the operator
/// says nothing: one main arena plus one shared worker arena. The
/// soak's number (73.5 MB uncapped against 35.8 MB capped at 24k
/// lands, p99 unchanged) and the concurrency bench's (16 agents: no
/// throughput or latency cost against uncapped, runner variance
/// dominating the table on #1421, peak RSS 46 MB down to 30 MB) both
/// picked 2. A named const so the final number is a one-line change.
pub const ARENA_MAX_DEFAULT: i32 = 2;

/// The startup decision: apply the built-in cap, or leave the
/// allocator to the operator's `MALLOC_ARENA_MAX`. Pure, so a test
/// pins both arms without a process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArenaCap {
    /// `MALLOC_ARENA_MAX` is set: the operator's value wins.
    LeftToEnv(String),
    /// Nothing is set: apply the built-in cap.
    Apply(i32),
}

/// The startup decision, from what the environment carries.
#[must_use]
pub fn arena_cap_decision(arena_max_env: Option<&str>) -> ArenaCap {
    match arena_max_env {
        Some(value) => ArenaCap::LeftToEnv(value.to_owned()),
        None => ArenaCap::Apply(ARENA_MAX_DEFAULT),
    }
}

/// Decides the arena cap, applies it to this process, and returns the
/// one-line text of the outcome it took - the one function that
/// decides, acts and reports, so the text cannot say what did not
/// happen. Call it at the very start of a serving binary's `main`:
/// before the tokio runtime builds its worker threads, because a cap
/// set after a thread exists reaches only the arenas that thread has
/// not minted yet.
///
/// `scn mount` has no log subscriber, so it prints the returned text
/// to stderr instead - a decision an operator cannot see is the silent
/// path.
///
/// Outcomes: `malloc arenas capped at N` (applied) / `malloc arenas
/// left to MALLOC_ARENA_MAX=v` (the operator's variable named) / `not
/// capped: mallopt(M_ARENA_MAX, N) failed` / `not capped: the
/// allocator is not glibc`.
pub fn cap_malloc_arenas() -> String {
    let decision = arena_cap_decision(std::env::var("MALLOC_ARENA_MAX").ok().as_deref());
    match decision {
        ArenaCap::LeftToEnv(value) => {
            let text = format!("malloc arenas left to MALLOC_ARENA_MAX={value}");
            tracing::info!("{}", text);
            text
        }
        ArenaCap::Apply(n) => {
            // The platform call lives in the crate's one platform-FFI
            // module; the open's own rule keeps the token out of here.
            #[cfg(all(target_os = "linux", target_env = "gnu"))]
            if crate::runtime::auth::mallopt_arena_max(n) == 0 {
                // A failure leaves the default arena behavior, which is
                // the pre-#1421 world: say so, warn, and carry on.
                let text = format!("not capped: mallopt(M_ARENA_MAX, {n}) failed");
                tracing::warn!("{}", text);
                text
            } else {
                let text = format!("malloc arenas capped at {n}");
                tracing::info!("{}", text);
                text
            }
            // Not glibc, not the climb #1421 measured: nothing to cap.
            #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
            {
                let _ = n;
                let text = "not capped: the allocator is not glibc".to_owned();
                tracing::info!("{}", text);
                text
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ARENA_MAX_DEFAULT, ArenaCap, arena_cap_decision};

    /// No environment: the built-in cap applies.
    #[test]
    fn an_unset_environment_applies_the_built_in_cap() {
        assert_eq!(arena_cap_decision(None), ArenaCap::Apply(ARENA_MAX_DEFAULT));
    }

    /// The operator's value wins, whatever it is, and is carried as
    /// they wrote it.
    #[test]
    fn the_operators_value_wins_over_the_built_in_cap() {
        assert_eq!(
            arena_cap_decision(Some("4")),
            ArenaCap::LeftToEnv("4".to_owned())
        );
        assert_eq!(
            arena_cap_decision(Some("1")),
            ArenaCap::LeftToEnv("1".to_owned())
        );
    }
}
