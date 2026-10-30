# SQLite removed

The server keeps its metadata in Postgres only: the schema the
`--postgres-url` URL names is the metadata store. A SQLite data
directory is not opened - there are no users, so there is no
migration; a directory that holds one is refused at the open. The
backup is `pg_dump` beside the blob files, and the restore restores
the dump into the schema before the first serve.

The metadata store is Postgres only: the SQLite store, its schema
migrations and the rusqlite dependency are gone from the server crate
and from `scn` (the tier keeps its own SQLite object cache). A store's
metadata was never carried across engines, and this build carries none
forward: a data directory whose metadata a SQLite store holds stays
with the build that made it - there is no migration from SQLite. The
`--postgres-url` flag is the store's own URL now; passing it warns that
the flag is removed in v0.2.0. The store-upgrade and backup-n1 drills,
and the CI jobs that ran them, are removed; they come back after
v0.1.0, against the Postgres-only open.

The cache daemon keeps its local SQLite index: `<data>/index.db` of
`scion-daemon` is the tier crate's own index of the objects it holds,
and it never held server metadata.
