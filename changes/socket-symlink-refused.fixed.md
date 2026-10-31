A daemon whose socket path held a symlink followed it and bound the
socket at the symlink's target, a path the private-directory check
never saw. `bind_unix` now refuses anything at the socket path that is
not a socket file of this user's earlier run: a symlink, a regular
file, or a directory is refused with the path named, a stale socket
that nothing listens on is re-bound as before, and nothing is followed
or overwritten
(`scion-proto/tests/socket_symlink.rs`).
