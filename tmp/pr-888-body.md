## Tests: the land path parks at named points, and the compare-and-swap loss replays

Refs #888 (S1 of the four ordered scripts; S2-S4 remain).

### The park

`crates/scion-server/src/pause.rs`, behind the existing `test-hooks` feature (with the crate's own tests). No socket: the env lever `SCION_TEST_PAUSE_DIR` names a directory, one per data directory, set by the shared harness on every spawn. A test drops `<point>.<key>.arm`; the land writes `<point>.<key>.hit` and polls for `<point>.<key>.release`. One arm is one park: a replan goes on without a second park. A park is bounded at 120 s, then logs the point and key and the land goes on. The test's wait for `.hit` uses the 60 s floor.

The first point is `after-plan`, in `finish`, between the plan's read and the settle. Its comment says what it holds: no Postgres transaction, no pool reader - and the ref slot lock, which spans the plan and the settle by design. That lock is the one finding against the plan (below).

### The first forced order

`crates/scion-server/tests/land_pause.rs`: land A plans on the root and parks; land B lands whole on `dev` while A sits (nothing outside A's own ref is held); `main` then moves the way an import moves it - one compare-and-swap against the version A's plan read, on a connection that never takes the land's ref slot; A's settle loses the compare-and-swap, the finish replans inside the same call, and the chain ends root, the import's head, A's result.

### Mutation table

| plant | where | result |
|---|---|---|
| the compare-and-swap loss ignored | `settle`, the `cas_ref` check | red: the chain assert fails (A's result sat on the root while `main` stayed at the import's head); planted, run, reverted by hand |

### The finding against the plan (P3)

P3 said the park holds no ref slot lock, and S1 proves it with land B settling on the same ref while A parks. It cannot: the ref slot lock spans `finish` (the plan and the settle share it, per the SHORTCUT comment at the loop). The first cut parked after the plan in one process, and B blocked until its client timeout - exactly the falsifier P3 named. The design that keeps the order: B lands whole on another ref, and the mover on A's ref is the import class, which bypasses the slot - the mover the settle's compare-and-swap exists for (the settle's own comment names it). If S1 must keep land B on the same ref inside the window, the park has to release the slot across the park, a structural change to `finish`; that would fit a later slice.

### The real writer beside a live server

The import command is the writer that can move a ref beside a live server, and it is real today: it opens the store with `Backend::open_shared` ("a process that does not land, such as an import: the database sorts its writers out itself"), the open without the one-server fence, and the serve path says in so many words that an import can run beside the server. It moves refs with the same compare-and-swap (`import.rs`, the `cas_ref` against the ref's current version, with its own retry loop). So the compare-and-swap loss the test pins is a path production takes: a land whose plan read a head the import then moved. The fence covers only a second server process, not the import.

### Checks

- `land_pause` green; the plant red by hand, then reverted.
- `land_model` (7), `land_twin_race` (2), `land_statement_bound` (1) green beside the new test.
- clippy, fmt clean; `scripts/check-test-seams.sh` (with `--self-test`) passes - the new module is inside the existing doc(hidden)/test-hooks gate, no new CI job.
