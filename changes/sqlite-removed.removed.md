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
