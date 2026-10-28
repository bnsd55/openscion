# SQLite removed

The server keeps its metadata in Postgres only: the schema the
`--postgres-url` URL names is the metadata store. A SQLite data
directory is not opened - there are no users, so there is no
migration; a directory that holds one is refused at the open. The
backup is `pg_dump` beside the blob files, and the restore restores
the dump into the schema before the first serve.

The cache daemon keeps its local SQLite index: `<data>/index.db` of
`scion-daemon` is the tier crate's own index of the objects it holds,
and it never held server metadata.
