//! The daemon's socket path is never followed through a symlink: `bind_unix`
//! refuses anything at the final component that is not a socket file of
//! ours, re-binds a stale socket that nothing listens on, and names the
//! path in every refusal.

use std::os::unix::fs::PermissionsExt as _;

use std::os::unix::net::UnixListener;

use scion_proto::runtime::listen::bind_unix;

fn private_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

/// A symlink at the socket path is refused: bind must not follow it to a
/// path the directory check never saw.
#[tokio::test]
async fn a_symlink_at_the_socket_path_refuses_the_start() {
    let dir = private_dir();
    let target = dir.path().join("trap.sock");
    let socket = dir.path().join("daemon.sock");
    std::os::unix::fs::symlink(&target, &socket).unwrap();
    let error = bind_unix(&socket).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists, "{error}");
    assert!(
        error.to_string().contains("daemon.sock"),
        "the refusal names the path: {error}"
    );
    assert!(!target.exists(), "the bind followed the symlink");
}

/// A socket file that nothing listens on - one an earlier run left - is
/// removed and bound again, as before.
#[tokio::test]
async fn a_stale_socket_of_ours_still_restarts() {
    let dir = private_dir();
    let socket = dir.path().join("daemon.sock");
    UnixListener::bind(&socket).unwrap();
    bind_unix(&socket).await.expect("the stale socket re-binds");
    let mode = std::fs::metadata(&socket).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
}

/// A regular file at the socket path is refused, with the path named: the
/// daemon never writes over a file it did not make.
#[tokio::test]
async fn a_regular_file_at_the_socket_path_is_refused() {
    let dir = private_dir();
    let socket = dir.path().join("daemon.sock");
    std::fs::write(&socket, b"not a socket").unwrap();
    let error = bind_unix(&socket).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists, "{error}");
    assert!(
        error.to_string().contains("not a socket file"),
        "the refusal says what is wrong: {error}"
    );
    assert_eq!(
        std::fs::read(&socket).unwrap(),
        b"not a socket",
        "the file is untouched"
    );
}
