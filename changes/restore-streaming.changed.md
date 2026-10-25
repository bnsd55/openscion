A restore holds one archive section at a time and replays it in
bounded transactions, instead of holding a repo's whole changeset
stream in memory: a restore of a large repo no longer needs RSS the
size of its history. The archive format is unchanged; archives written
by an older build restore as before
(`crates/scion-server/tests/archive.rs`).
