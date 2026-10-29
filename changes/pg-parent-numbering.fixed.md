Postgres numbered a changeset's parents from one (WITH ORDINALITY
starts at one) where the reads ask for zero, so every first-parent
read (`n = 0`) matched nothing on real data: a chain fetch walked one
changeset and stopped, and the savings the first-parent chain fetch
promises never happened. The insert now shifts the ordinality down, so
the first parent sits at n = 0, and a migration renumbers the
one-based rows of an existing store (only the rows of a changeset
whose smallest n is 1 move, in two steps that never hold a duplicate
of the primary key, and the migration changes nothing on a store that
is already zero-based). The guard tests pin the numbering, chain a
300-deep ladder through the ordinary path, migrate a one-based store,
and hold the bulk seed to the rows the ordinary path writes.

Proof: crates/scion-server/tests/pg_parent_numbering.rs
