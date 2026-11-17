//! The file park of the land path's interleaving tests (#888): named
//! points where a test holds one land while another runs, so an
//! interleaving replays exactly instead of by thread luck.
//!
//! The seam is files, not a socket. The env lever names a directory; a
//! test drops `<point>.<key>.arm` in it, the park answers with
//! `<point>.<key>.hit`, and the test ends the park with
//! `<point>.<key>.release`. A fault needs no server code: the test
//! kills the server or ends the parked call while it sits.
//!
//! One arm is one park: the arm file is consumed here, so a land that
//! comes through the point again - a replan does - goes on without a
//! second park.
//!
//! Everything here is behind the `test-hooks` feature (with the crate's
//! own tests); a release build compiles the module and every call into
//! it away.

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// The park's own ceiling: a test that dies must not leave a land
/// parked forever. A park that runs past it logs the point and the key
/// and the land goes on; the pending land stays one a retry finishes.
const PARK_CEILING: Duration = Duration::from_secs(120);

/// The lever's directory, read once: `None` in a build whose tests set
/// no lever, or a server run that carries none.
fn dir() -> Option<&'static PathBuf> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| std::env::var_os("SCION_TEST_PAUSE_DIR").map(PathBuf::from))
        .as_ref()
}

/// Parks the caller at the point `point` when a test armed that point
/// for the land with request key `key`:
///
/// - `<point>.<key>.arm` present: the arm is consumed,
/// - `<point>.<key>.hit` is written, and
/// - the caller waits for `<point>.<key>.release`, at most
///   [`PARK_CEILING`].
///
/// A park holds no Postgres transaction and no pool reader - the
/// caller's plan read has closed and its settle has not begun. What
/// else it holds is the call site's to say: the after-plan point holds
/// the ref slot lock the plan and settle share by design, so a land on
/// another ref goes on beside it and a mover on this ref must bypass
/// the slot, as the import does.
pub(crate) fn hit(point: &str, key: &str) {
    let Some(dir) = dir() else {
        return;
    };
    let arm = dir.join(format!("{point}.{key}.arm"));
    if std::fs::remove_file(&arm).is_err() {
        // No arm for this point and key: the common case, one stat.
        return;
    }
    if std::fs::write(dir.join(format!("{point}.{key}.hit")), b"").is_err() {
        return;
    }
    let release = dir.join(format!("{point}.{key}.release"));
    let started = Instant::now();
    while !release.exists() {
        if started.elapsed() >= PARK_CEILING {
            tracing::warn!(
                point = %point,
                key = %key,
                "a test park ran past its ceiling; the land goes on"
            );
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
