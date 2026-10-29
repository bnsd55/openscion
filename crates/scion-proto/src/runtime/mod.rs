//! The runtime code that serves the contract, collected in one module.

pub mod auth;
pub mod check;
pub mod deadline;
pub mod listen;
pub mod metrics;
pub mod object;
pub mod reply;
pub mod request_id;
pub mod tls;

/// A temp directory the run owns outright: mode 0700, whatever the
/// umask. The checked reads refuse a file under a directory that group
/// or others can write, and a plain temp directory is 0777 less the
/// umask.
#[cfg(test)]
fn private_tempdir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}
