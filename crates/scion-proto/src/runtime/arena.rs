//! The malloc arena cap of the long-lived serving processes (#1421).
//!
//! glibc's allocator mints one arena per contending thread, up to 8
//! per core by default, and never returns the memory of a retired
//! arena to the OS. A server whose thread count rises over a run can
//! therefore step its RSS by tens of megabytes while its live heap
//! stays flat - the soak measured the step (18-30 MB at ~56 minutes
//! into #1421's runs) and a heap profile showed only ~11 MB live.
//! Capping the arenas at 2 held RSS at 35.8 MB where the uncapped
//! server reached 73.5 MB at 24k lands, with p99 unchanged.
//!
//! The cap applies where a process serves for a long time: the server,
//! the cache, and the daemon. One-shot commands keep the default.

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

/// Applies [`arena_cap_decision`] to this process, at the very start
/// of a serving binary's `main`: before the tokio runtime builds its
/// worker threads, because a cap set after a thread exists reaches
/// only the arenas that thread has not minted yet. On a Linux glibc
/// process this calls `mallopt(M_ARENA_MAX, ..)` once; on any other
/// target it compiles to nothing. The decision logs once: the value
/// applied, or that the operator's environment carries one.
pub fn cap_malloc_arenas() {
    let decision = arena_cap_decision(std::env::var("MALLOC_ARENA_MAX").ok().as_deref());
    match decision {
        ArenaCap::LeftToEnv(value) => {
            tracing::info!("malloc arenas left to MALLOC_ARENA_MAX={value}");
        }
        ArenaCap::Apply(n) => {
            // The platform call lives in the crate's one platform-FFI
            // module; the open's own rule keeps the token out of here.
            // A failure leaves the default arena behavior, which is the
            // pre-#1421 world: warn and carry on.
            #[cfg(target_os = "linux")]
            if crate::runtime::auth::mallopt_arena_max(n) == 0 {
                tracing::warn!(
                    "mallopt(M_ARENA_MAX, {n}) failed; the allocator \
                     keeps its default arena behavior"
                );
            } else {
                tracing::info!("malloc arenas capped at {n}");
            }
            // Not glibc, not the climb #1421 measured: nothing to cap.
            #[cfg(not(target_os = "linux"))]
            let _ = n;
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
