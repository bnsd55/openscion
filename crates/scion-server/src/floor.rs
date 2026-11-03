//! The memory floor: what one server process's budgets and caps add up
//! to, and what the host that runs it has.
//!
//! The process bounds its memory with caps, not reservations: a blame
//! pool of 1 GiB holds only what blame calls take, and the stall total
//! only what stalled replies leave in HTTP/2. On a host with more memory
//! than the caps assume, nothing happens. On a host with less, the page
//! cache gives way first, and under load the process can reach the OOM
//! killer. So at start the server reads the host's limit, sets both
//! numbers as gauges, and when the host is under the floor writes one
//! error line that names the limit, its source, the floor, and the
//! shares that make it. The server does not refuse to start: the caps
//! keep their promises at any size, and a smaller host serves until its
//! memory runs out, which the line and the gauges make visible.

// Every pub item of this module is re-exported by the test seam
// (`test_support` in lib.rs), and unreachable_pub does not see
// through a re-export, so this marker would fire on all of them
// under any feature set.
#![allow(unreachable_pub)]

use std::path::{Path, PathBuf};

/// What every `Blame` call together may hold
/// (`BLAME_MEMORY_BUDGET` in service.rs).
pub(crate) const BLAME: u64 = crate::service::BLAME_MEMORY_BUDGET as u64;

/// What the derive walks in flight may hold (`DERIVE_WALK_BUDGET` in
/// derived.rs).
pub(crate) const DERIVE_WALK: u64 = crate::derived::DERIVE_WALK_BUDGET as u64;

/// What the uploads that read at once may hold: one request message of
/// up to `MAX_MESSAGE_BYTES` each (`MAX_UPLOADS` in service.rs).
pub(crate) const UPLOADS: u64 =
    (crate::service::MAX_UPLOADS * scion_proto::MAX_MESSAGE_BYTES) as u64;

/// What the `GetObjects` replies in flight may hold (`IN_FLIGHT_BYTES`
/// in scion-proto's runtime/reply.rs).
pub(crate) const IN_FLIGHT: u64 = scion_proto::runtime::reply::IN_FLIGHT_BYTES as u64;

/// What the `ReadFile` replies together hold (`READ_BUDGET_BYTES` in
/// service.rs).
pub(crate) const READ: u64 = crate::service::READ_BUDGET_BYTES as u64;

/// What the unary replies together hold (`REPLY_MEMORY_BUDGET` in
/// reply.rs).
pub(crate) const UNARY_REPLY: u64 = crate::reply::REPLY_MEMORY_BUDGET as u64;

/// What the land indexes of the refs in use hold (`MAX_INDEX_BYTES` in
/// land.rs; the test build shrinks that constant, so the floor keeps the
/// production value here).
pub(crate) const LAND_INDEX: u64 = 128 << 20;
#[cfg(not(test))]
const _: () = assert!(crate::land::MAX_INDEX_BYTES as u64 == LAND_INDEX);

/// What the gc's prune reads at once (`PRUNE_BODY_BUDGET` in gc.rs).
pub(crate) const PRUNE_BODY: u64 = crate::gc::PRUNE_BODY_BUDGET as u64;

/// What the stalled replies of every connection together may hold (the
/// listener's process-wide stall total, `STALLED_TOTAL_BYTES` in
/// scion-proto's runtime/listen.rs).
pub(crate) const STALL_TOTAL: u64 = scion_proto::runtime::listen::STALLED_TOTAL_BYTES;

/// Slack for what the budgets do not count: the interpreter, the
/// allocators' overhead, the connection and TLS buffers, the stacks of
/// the worker threads.
pub(crate) const HEADROOM: u64 = 1536 << 20;

/// The memory the server's host needs for the budgets to keep their
/// promises.
pub(crate) const FLOOR: u64 = BLAME
    + DERIVE_WALK
    + UPLOADS
    + IN_FLIGHT
    + READ
    + UNARY_REPLY
    + LAND_INDEX
    + PRUNE_BODY
    + STALL_TOTAL
    + HEADROOM;

/// The files the host-limit reader reads, so a test can point them at a
/// temporary directory.
#[derive(Clone)]
pub struct Paths {
    /// `/proc/self/cgroup`: which cgroup the process runs in.
    pub self_cgroup: PathBuf,
    /// The cgroup filesystem root, `/sys/fs/cgroup` on Linux.
    pub cgroup_root: PathBuf,
    /// `/proc/meminfo`.
    pub meminfo: PathBuf,
}

impl Paths {
    /// The paths of a Linux host.
    #[must_use]
    pub fn production() -> Self {
        Self {
            self_cgroup: PathBuf::from("/proc/self/cgroup"),
            cgroup_root: PathBuf::from("/sys/fs/cgroup"),
            meminfo: PathBuf::from("/proc/meminfo"),
        }
    }
}

/// A test's override of the production paths, behind the test hooks: the
/// check has one caller per process, so a plain static needs no ordering.
#[cfg(any(test, feature = "test-hooks"))]
static TEST_PATHS: std::sync::Mutex<Option<Paths>> = std::sync::Mutex::new(None);

/// Points the start-up check at a test's paths. Pass `None` to reset.
///
/// # Panics
///
/// Panics if the paths override lock is poisoned.
#[cfg(any(test, feature = "test-hooks"))]
pub fn test_set_paths(paths: Option<Paths>) {
    *TEST_PATHS
        .lock()
        .expect("the paths override lock is not poisoned") = paths;
}

/// The paths the start-up check reads: a test's override, or the
/// production paths of a Linux host.
///
/// # Panics
///
/// Panics if the paths override lock is poisoned.
#[must_use]
pub fn production_paths() -> Paths {
    #[cfg(any(test, feature = "test-hooks"))]
    {
        let override_ = TEST_PATHS
            .lock()
            .expect("the paths override lock is not poisoned")
            .clone();
        if let Some(paths) = override_ {
            return paths;
        }
    }
    Paths::production()
}

/// The host's memory limit, and where the reading came from.
pub(crate) struct HostLimit {
    pub bytes: u64,
    pub source: &'static str,
}

/// Reads the host's memory limit: the cgroup the process runs in first
/// (a container's limit is the one that bounds it), then the host's
/// total, then `hw.memsize` where the files do not exist. `None`: no
/// source answered, and the start-up line says the limit is unknown.
pub(crate) fn host_limit(paths: &Paths, macos: impl Fn() -> Option<u64>) -> Option<HostLimit> {
    if let Ok(cgroups) = std::fs::read_to_string(&paths.self_cgroup) {
        // cgroup v2, the unified hierarchy: `0::/path`.
        let v2 = cgroups.lines().find_map(|line| line.strip_prefix("0::"));
        if let Some(path) = v2 {
            let file = paths
                .cgroup_root
                .join(path.trim().trim_start_matches('/'))
                .join("memory.max");
            if let Some(bytes) = read_cgroup_limit(&file) {
                return Some(HostLimit {
                    bytes,
                    source: "cgroup v2 memory.max",
                });
            }
        }
        // cgroup v1: one hierarchy per controller, `12:memory:/path`.
        let v1 = cgroups.lines().find_map(|line| {
            let mut fields = line.split(':');
            match (fields.next(), fields.next(), fields.next()) {
                (_, Some("memory"), Some(path)) => Some(path),
                _ => None,
            }
        });
        if let Some(path) = v1 {
            let file = paths
                .cgroup_root
                .join("memory")
                .join(path.trim().trim_start_matches('/'))
                .join("memory.limit_in_bytes");
            if let Some(bytes) = read_cgroup_limit(&file) {
                return Some(HostLimit {
                    bytes,
                    source: "cgroup v1 memory.limit_in_bytes",
                });
            }
        }
    }
    if let Ok(meminfo) = std::fs::read_to_string(&paths.meminfo) {
        for line in meminfo.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                let kb: u64 = rest
                    .trim()
                    .trim_end_matches(" kB")
                    .replace(' ', "")
                    .parse()
                    .unwrap_or(0);
                if kb > 0 {
                    return Some(HostLimit {
                        bytes: kb * 1024,
                        source: "/proc/meminfo MemTotal",
                    });
                }
            }
        }
    }
    macos().map(|bytes| HostLimit {
        bytes,
        source: "hw.memsize",
    })
}

/// A cgroup limit file: `max` (no limit) reads as none, so the reader
/// falls through to the next source; a v1 file at its `u64::MAX`-shaped
/// unlimited also reads as none. A number is bytes.
fn read_cgroup_limit(file: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(file).ok()?;
    let text = text.trim();
    if text == "max" {
        return None;
    }
    text.parse()
        .ok()
        .filter(|bytes| *bytes > 0 && *bytes < (1 << 62))
}

/// macOS `hw.memsize`, the source a non-Linux host falls through to.
#[cfg(target_os = "macos")]
fn macos_memsize() -> Option<u64> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    std::str::from_utf8(&out.stdout)
        .ok()?
        .trim()
        .parse()
        .ok()
        .filter(|bytes| *bytes > 0)
}

/// A non-Linux host without the sysctl answers nothing, so the start-up
/// line says the limit is unknown.
#[cfg(not(target_os = "macos"))]
fn macos_memsize() -> Option<u64> {
    None
}

/// The start-up check: read the host's limit, set the two gauges, and
/// when the host is under the floor write one error line that names the
/// limit, its source, the floor, and the shares that make it. The server
/// starts either way.
// SHORTCUT: the shares stay fixed whatever the host has, and a host
/// under the floor starts anyway; scale the shares to the host limit
/// when a host under the floor becomes a supported shape (the floor row
/// of section 9 of architecture.md).
pub(crate) fn check_startup_memory(
    metrics: &scion_proto::runtime::metrics::Metrics,
    paths: &Paths,
) {
    metrics.set_memory_floor_bytes(FLOOR);
    if let Some(limit) = host_limit(paths, macos_memsize) {
        metrics.set_host_memory_bytes(limit.bytes);
        if limit.bytes < FLOOR {
            tracing::error!(
                host_bytes = limit.bytes,
                host_source = limit.source,
                floor_bytes = FLOOR,
                headroom_bytes = HEADROOM,
                blame_bytes = BLAME,
                derive_walk_bytes = DERIVE_WALK,
                uploads_bytes = UPLOADS,
                in_flight_bytes = IN_FLIGHT,
                read_bytes = READ,
                unary_reply_bytes = UNARY_REPLY,
                land_index_bytes = LAND_INDEX,
                gc_bytes = PRUNE_BODY,
                stall_total_bytes = STALL_TOTAL,
                "the host has less memory than the server's floor: \
                 the page cache gives way first, and under load the \
                 process can reach the OOM killer"
            );
        }
    } else {
        tracing::warn!(floor_bytes = FLOOR, "cannot read the host's memory limit");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(dir: &Path) -> Paths {
        Paths {
            self_cgroup: dir.join("self_cgroup"),
            cgroup_root: dir.to_path_buf(),
            meminfo: dir.join("meminfo"),
        }
    }

    /// The floor is the shares plus the headroom, and the profile's
    /// stated number is that sum.
    #[test]
    fn the_floor_is_the_shares_plus_the_headroom() {
        assert_eq!(
            FLOOR,
            BLAME
                + DERIVE_WALK
                + UPLOADS
                + IN_FLIGHT
                + READ
                + UNARY_REPLY
                + LAND_INDEX
                + PRUNE_BODY
                + STALL_TOTAL
                + HEADROOM
        );
        assert_eq!(FLOOR, 6608 << 20);
    }

    #[test]
    fn the_host_limit_reads_the_cgroup_v2_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(dir.path());
        std::fs::write(&paths.self_cgroup, "0::/system.slice/scion.service\n").unwrap();
        let leaf = paths.cgroup_root.join("system.slice/scion.service");
        std::fs::create_dir_all(&leaf).unwrap();
        std::fs::write(leaf.join("memory.max"), "68719476736\n").unwrap();
        assert_eq!(host_limit(&paths, || None).unwrap().bytes, 68_719_476_736);
        assert_eq!(
            host_limit(&paths, || None).unwrap().source,
            "cgroup v2 memory.max"
        );
    }

    #[test]
    fn an_unlimited_cgroup_falls_through_to_meminfo() {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(dir.path());
        std::fs::write(&paths.self_cgroup, "0::/\n").unwrap();
        std::fs::write(paths.cgroup_root.join("memory.max"), "max\n").unwrap();
        std::fs::write(&paths.meminfo, "MemTotal:       32876392 kB\n").unwrap();
        let limit = host_limit(&paths, || None).unwrap();
        assert_eq!(limit.bytes, 32_876_392 * 1024);
        assert_eq!(limit.source, "/proc/meminfo MemTotal");
    }

    #[test]
    fn a_v1_unlimited_value_falls_through_to_meminfo() {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(dir.path());
        std::fs::write(&paths.self_cgroup, "12:memory:/docker/c1\n").unwrap();
        let leaf = paths.cgroup_root.join("memory/docker/c1");
        std::fs::create_dir_all(&leaf).unwrap();
        std::fs::write(leaf.join("memory.limit_in_bytes"), "9223372036854771712\n").unwrap();
        std::fs::write(&paths.meminfo, "MemTotal:       16048588 kB\n").unwrap();
        let limit = host_limit(&paths, || None).unwrap();
        assert_eq!(limit.bytes, 16_048_588 * 1024);
        assert_eq!(limit.source, "/proc/meminfo MemTotal");
    }

    #[test]
    fn with_no_file_source_the_macos_reader_answers() {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(dir.path());
        let limit = host_limit(&paths, || Some(8_589_934_592)).unwrap();
        assert_eq!(limit.bytes, 8_589_934_592);
        assert_eq!(limit.source, "hw.memsize");
        assert!(host_limit(&paths, || None).is_none());
    }

    #[test]
    fn the_check_sets_both_gauges_and_starts_under_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(dir.path());
        std::fs::write(&paths.meminfo, "MemTotal:       1048576 kB\n").unwrap();
        let metrics = scion_proto::runtime::metrics::Metrics::default();
        check_startup_memory(&metrics, &paths);
        assert_eq!(metrics.host_memory_bytes(), 1_048_576 * 1024);
        assert_eq!(metrics.memory_floor_bytes(), FLOOR);
        let text = metrics.render();
        assert!(
            text.contains("scion_host_memory_bytes 1073741824"),
            "{text}"
        );
        assert!(
            text.contains(&format!("scion_memory_floor_bytes {FLOOR}")),
            "{text}"
        );
    }
}
