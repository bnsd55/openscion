//! The host's own numbers, read without a subprocess.
//!
//! The start-up memory probe once read `hw.memsize` with a
//! `sysctl(1)` child, and on macOS that read lasted as long as a
//! client that had connected through the backlog: the child's pipe
//! kept the read waiting, and the server accepted nothing until it
//! ended (#795). The platform call has no Rust binding, so the one
//! module of the workspace that holds platform symbols answers it,
//! and this module is its safe caller.

/// The host's physical memory, in bytes: `hw.memsize` on macOS. `None`
/// where the platform has no such value or the call fails, and the
/// start-up line says the limit is unknown.
#[must_use]
pub fn memsize() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        super::auth::hw_memsize()
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::memsize;

    /// The host reports a plausible physical memory: more than a
    /// gigabyte. Where the platform has no such value, nothing is
    /// claimed and the check is empty.
    #[test]
    fn the_host_reports_a_plausible_memory() {
        #[cfg(target_os = "macos")]
        assert!(memsize().is_some_and(|bytes| bytes > 1 << 30));
    }
}
