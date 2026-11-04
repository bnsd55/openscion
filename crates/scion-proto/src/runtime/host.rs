//! The host's own numbers, read without a subprocess.
//!
//! The start-up memory probe once read `hw.memsize` with a
//! `sysctl(1)` child, and on macOS that read lasted as long as a
//! client that had connected through the backlog: `Command::output`
//! reads a pipe whose write end macOS marks close-on-exec only after
//! the pipe is made, and a child spawned in that window keeps it open
//! past its own exec, so the read waited for that child to end (#795).
//! The platform's `sysctlbyname` has no safe binding, so the symbols
//! are declared here and wrapped in a safe function that returns owned
//! data, as the ACL calls of [`crate::runtime::auth::ffi`] are.

#![allow(unsafe_code)]

use std::os::raw::{c_char, c_int, c_void};

/// `hw.memsize`: the host's physical memory, in bytes.
const HW_MEMSIZE: &[u8] = b"hw.memsize\0";

unsafe extern "C" {
    /// The BSD `sysctlbyname(3)`: the named value into `oldp`, its size
    /// in `oldlenp`. No new value is set: `newp` is null and `newlen`
    /// zero.
    fn sysctlbyname(
        name: *const c_char,
        oldp: *mut c_void,
        oldlenp: *mut usize,
        newp: *mut c_void,
        newlen: usize,
    ) -> c_int;
}

/// The host's physical memory, in bytes: `hw.memsize` on macOS. `None`
/// where the platform has no such value or the call fails, and the
/// start-up line says the limit is unknown.
#[must_use]
pub fn memsize() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        let mut size: u64 = 0;
        let mut len = std::mem::size_of::<u64>();
        // SAFETY: the name is NUL-terminated, the buffer is as long as
        // the call is told, and no value is asked back.
        let ok = unsafe {
            sysctlbyname(
                HW_MEMSIZE.as_ptr().cast(),
                (&raw mut size).cast(),
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        (ok == 0 && size > 0).then_some(size)
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
