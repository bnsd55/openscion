`scion-cache` names its own flag, `--ticket-public-key`, when it refuses
a key at start (more than two keys, a key that is not 64 hex
characters, or bytes that are not a public key). The refusals named
`--ticket-key`, which is the server's flag and which the cache does
not take.
