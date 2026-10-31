`scion-cache` refused three kinds of bad ticket key at start with
messages that named `--ticket-key`, the flag of `scion-server serve`.
The cache takes `--ticket-public-key`, and its refusals now say so
("give at most two --ticket-public-key", "must be 64 hex characters",
"bad --ticket-public-key: ..."), so the error points at the flag the
operator can actually fix.
