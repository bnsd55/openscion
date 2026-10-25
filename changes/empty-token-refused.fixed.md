An access file that named the digest of an empty token authenticated
every client that sent an empty bearer, and `hash-token --token ''`
printed that digest, so an operator could mint it by accident. The
access-file parser refuses the empty token's digest with the fix named
("hash the real token with `hash-token`"), `hash-token` refuses empty
and whitespace-only input in every form, the server refuses a
whitespace-only `--token` at start, and a server whose access file
names the empty digest does not start
(`scion-server/tests/empty_token.rs`). Tokens with leading or trailing
spaces keep their bytes: only a token that is all whitespace is
refused.
