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
    let dir = scion_server::test_support::private_tempdir();
    let marker = dir.path().join("backend");
    std::fs::write(&marker, data).unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&marker, Permissions::from_mode(0o600)).unwrap();

    // Both opens ride the marker check before anything else: an open
    // whose URL never resolves (the check precedes the contact, and the
    // host is not local). Clean open or clean refusal; never a panic.
    let _ = scion_server::Backend::open(dir.path(), "postgres://127.0.0.1:5/scion", None);
});
