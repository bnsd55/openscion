The metrics surface gains one counter: `scion_connections_refused_total`,
the connections the server closed because no place freed within its wait
at the connection cap (5 s, `CONNECTION_WAIT` in
`crates/scion-proto/src/runtime/listen.rs`). A connection over the cap
now fails fast instead of waiting unread for up to half an hour, and the
warning line about waiting connections is rate-limited to one per 10
seconds, with the count the line covers.
