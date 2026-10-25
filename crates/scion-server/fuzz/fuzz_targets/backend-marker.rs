#![forbid(unsafe_code)]
//! The backend marker file is bytes on the disk an operator or a bad
//! disk wrote: `Backend::open` reads it before anything else. Any bytes
//! end in a clean open or a clean refusal, never a panic, and the
//! refusal names the damage (the tests pin the messages; here the
//! property is only that the parser stays calm).

#![no_main]

use libfuzzer_sys::fuzz_target;
use std::fs::Permissions;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("backend");
    std::fs::write(&marker, data).unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&marker, Permissions::from_mode(0o600)).unwrap();

    // Both opens ride the marker check before anything else: a SQLite
    // open (no URL) and a Postgres-shaped one (the URL never reached -
    // the check precedes the contact). Clean open or clean refusal;
    // never a panic.
    let _ = scion_server::Backend::open(dir.path(), None, None);
    let _ = scion_server::Backend::open(
        dir.path(),
        Some("postgres://127.0.0.1:5/scion"),
        None,
    );
});
