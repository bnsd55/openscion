# The production audit

The blocking audit for v1.0. Every finding carries exactly one status:

- **fixed** — with the PR that closed it, or **fixed by design** with
  the one-line reason the design closes it.
- **bounded** — with the measured ceiling as a number, and the
  `operations.md` line that states it. A row without both is not
  bounded.
- **outside the production profile** — with the `support.md` line that
  excludes it.
- **open** — no status yet. An open finding is a v1.0 blocker; the
  maintainers open an issue for each one until it carries a status. Nothing
  in this document is merely "known".

Sections follow the production review of 2026-10-23. The harness
vacuity table of the same day feeds section 8. Each finding names
its evidence point `file:line`, checked once against the commit this
document landed on.

## 1. Release surface

| Finding | Evidence | Status |
| --- | --- | --- |
| `scn mount --unstable-writable`: a planted parent symlink can redirect an overlay write outside the mount (final-name-only check). the maintainer's decision: the writable mount leaves the v1 release binary — the write path becomes a build feature, off in release | `crates/scn/src/vfs.rs:1469`, `docs/cli.md:57`, `changes/313.fixed.md` | fixed (#640: the write path is a build feature, off in release; the build flag names it) |
| Env aliases `SCION_DATA`, `SCION_TOKEN`, `SCION_POSTGRES_URL`, `SCION_POSTGRES_URL_FILE` exist beside their flags | `crates/scion-server/src/main.rs:30,36-44,1290` | fixed by design (each alias is the documented spelling of a flag in `docs/cli.md`; the flag list is the release surface, and the URL env exists so secrets stay off argv) |
| Test hooks in release binaries: the store fault hooks and the derive test pool build only with `--features test-hooks` | `crates/scion-server/Cargo.toml:61`, `crates/scion-server/src/store.rs:920` | fixed by design (the feature is off in every release build; `release.yml` builds plain `--release`) |

## 2. Durability seams

| Finding | Evidence | Status |
| --- | --- | --- |
| The Postgres store identity lives in two durable places: the `store_uuid` row in the database and the `backend` marker file in the data directory. A crash between the two writes, or a lost marker, refuses the open loudly ("this database names the store ..., but the data directory carries no marker of it") | `crates/scion-server/src/backend.rs:108,143`, `crates/scion-server/src/pg.rs:870` | fixed by design — the refusal is loud and never silent, and the runbook's re-pairing (write the marker back with the UUID the error names) is proven by `crates/scion-server/tests/backend_refusals.rs::a_lost_marker_written_back_as_the_runbook_says_reopens_the_store`; the runbook line is `operations.md` "Re-pairing a lost marker" |
| On Postgres, a server restarted right after a crash can refuse with "another server holds this store" until Postgres ends the dead session's advisory lock | `crates/scion-server/src/pg.rs:1745`, `docs/operations.md` "A start after an unclean kill" | fixed by design — the runbook says the supervisor's `Restart=on-failure` is the whole recovery on the same host (the lock dies at once with the dead process's sockets), and on a lost host no bounded wait inside `serve` helps (Postgres learns of the death only through TCP keepalives, hours by default), so `serve` fails fast and the supervisor retries; `operations.md` quotes the refusal's exact text and names `pg_terminate_backend` / `tcp_keepalives_idle` for the lost host |
| Migrations run one transaction each under a lock with a 5 s clock; an open that waits longer answers busy; a newer database is refused | `docs/operations.md` "The open" (the 5 s clock and the busy answer), `crates/scion-server/src/db.rs` | bounded — ceiling: 1 transaction per migration, the lock wait capped at 5 s; `operations.md` "The open migrates the database's schema ... under a database lock with the same 5 s clock" |
| Rename durability: the blob file fsyncs before the rename and the directory after it; the power-loss suite proves sync-before-reply | `crates/scion-server/src/store.rs:931,1004`, `crates/scion-server/tests/power.rs` | fixed on Linux (the native cell); the macOS gap (no tool can cut power or drop a barrier there) is outside the production profile — `support.md`'s platform matrix runs the power-loss and dm-log-writes cells on Linux only |
| Archive restore holds a whole repository's changesets in RAM while reading the archive | `crates/scion-server/src/archive.rs` (the streaming replay), `crates/scion-server/tests/archive_restore_stream.rs::a_big_restore_stays_under_its_rss_ceiling` | fixed (#575) — the replay streams the frames from a `BufReader`, one record at a time, and commits per byte budget (`DEFAULT_TX_BYTES`, 64 MiB); the RSS ceiling test proves a big archive's restore stays under its measured bound |
| The import writes changesets, parents, repo rows, derived rows, history nodes and the git import in separate transactions; the order and the batching are proven by the statement-count suites | `crates/scion-server/tests/changeset_batch.rs`, `tests/derived_batch.rs`, `tests/history_batch.rs` | fixed (#545, #548, #551) |

## 3. Unbounded work

| Finding | Evidence | Status |
| --- | --- | --- |
| Protocol reads are length-capped before `read_to_end` | `crates/scion-proto/src/runtime/auth/mod.rs:352`, cap `MAX_CHECKED_FILE = 1 MiB` at `:278` | bounded — ceiling: 1 MiB per checked read, `cap + 1` then refused; `docs/protocol-v1.md` states the message size limits |
| One unary reply can aggregate a whole directory or changeset past the per-object memory budget (`whole_share` charged by encoded size, the aggregate uncapped) | `crates/scion-server/src/service.rs:485` (`whole_share_of`), `:1825` (the changeset's length-only query), `:1986` (`tree_size`) | fixed (#586): the share is taken from a length-only query before the object is built, an object over `MAX_OBJECT_BYTES` is refused before any byte is read, and the share is held for the whole reply (`stream_whole_holding`); the guard is `tests/reply_budget_h2.rs` (`a_client_that_resets_its_unread_replies_frees_the_budget`, which asserts the budget binds at `bodies.len() < 40` and the served reply is whole) |
| Slow clients can pin large unary replies outside the memory accounting | `crates/scion-server/src/reply.rs:36` (`REPLY_MEMORY_BUDGET = 128 << 20`), `crates/scion-proto/src/runtime/metrics.rs:896` (`CountingBody`) | fixed (#586): the reply-memory layer counts every in-flight reply's bytes and refuses new unary replies past the 128 MiB budget; a client that resets its unread replies frees the budget (the guard is the same `tests/reply_budget_h2.rs` test) |
| `gc` prune reads and deletes in batches of 1,000 rows | `crates/scion-server/src/gc.rs` (`prune_rows`) | fixed (#529 batched the reads and the deletes; the guard is `tests/gc_prune_batch.rs`) |
| `scn commit` reads a regular file whole into RAM: the size limit is "whatever fits" | `crates/scn/src/cmd/commit.rs` | fixed (#582 streams from the file in 1 MiB chunks; the ceiling is the RSS guard, `crates/scn/tests/commit_stream.rs`, 24 MiB file under a 32 MiB ceiling) |
| `scn` JSON output silently replaces non-UTF-8 bytes | `crates/scn/src/client.rs:328` (`lossy`), `:336` (`put_json_bytes`) | fixed (#576): the lossy text stays, and every non-UTF-8 name or message carries the exact bytes in a sibling `<field>_b64` (standard base64) so scripts can round-trip them; the guard is `crates/scn/tests/json_raw_bytes.rs` (`assert_b64` and the seed suites) |
| Import batches are time-based per transaction and count-based for blobs | `crates/scion-server/src/import.rs:52` (`TX_BUDGET = 1 s`), `store.rs` (`BlobBatch`) | bounded — ceiling: 1 s of work or one commit per import transaction; `docs/benchmarks.md` records the measured commit size and rate the batches were tuned at |

## 4. Database loops

| Finding | Evidence | Status |
| --- | --- | --- |
| The Postgres writer pings the database with an autocommit `SELECT 1` while holding the writer mutex | `crates/scion-server/src/pg.rs:905` | bounded — ceiling: 1 statement per writer-idle probe (not per call); the probe's role is stated in `operations.md`'s connection-cap paragraph (the server holds 9 connections). Untested: the probe is a timer with no seam (the 60 s interval is a literal in the spawned thread), so no test pins its cadence |
| No production site pays a statement per repository item over the wire: Postgres chunks every multi-row write (1,000 rows per `unnest` insert) or answers one `ANY($1)` statement per list, and every paged read is a keyset page with a matching `ORDER BY` | `pg.rs:1330,2401,2573,2634,2979,3313,3599,3710-3910`; `fsck.rs:240,300`; `land.rs:1170`; `import.rs:236` | fixed by design (chunked; no REPO-SIZED site) |

### The sweep, per site

| Site | The loop grows with | Statements per iteration | Verdict |
| --- | --- | --- | --- |
| `db.rs:228` `insert_changeset`: per-parent generation read, then the per-parent `changeset_parents` inserts | the parents of one changeset | 2 statements per parent, local | BATCHED — a client commit has exactly one parent (`write.rs:278` refuses otherwise); only git import carries merges, and it runs ≤ 1,000 commits per transaction (`import.rs:44,236`), all local |
| `db.rs:278` `repo_missing_blobs`: per-blob existence read | the blob list of one upload request | 1 local SELECT per blob | BATCHED — local (the comment at `db.rs:276` names it); Postgres answers one `ANY($1)` statement per 1,000 (`pg.rs:2573`) |
| `db.rs:875` `get_trees`: per-id body read | the ids of one request | 1 local SELECT per id | BATCHED — local; Postgres chunks at `meta::DERIVED_BATCH` (`pg.rs:3313`) |
| `db.rs:1193` `put_git_imports`: per-pair insert | the pairs of one import batch | 1 local INSERT per pair | BATCHED — local; Postgres inserts one `unnest` per batch (`pg.rs:3599`) |
| `db.rs:1346` `delete_repo_blobs`: per-blob delete | the blobs gc removes | 1 local DELETE per blob | BATCHED — local; Postgres answers one `ANY($2)` statement (`pg.rs:3809`) |
| `db.rs:1384` `changeset_bodies_after`/`tree_bodies_after`, `derived_rows_after`; `pg.rs:3710-3910` the same readers | the whole table | 1 statement per ≤ 1,000-row keyset page | BATCHED — keyset pagination, each page ordered by its key |
| `db.rs:1536` `lands_with_missing_changesets` | two fixed columns | 2 statements | BOUNDED (2) |
| `fsck.rs:240,300` the check page loops | every changeset and tree | 1 statement per page | BATCHED |
| `land.rs:1041,1170` the path index walks (`first_parent_page`) | the repo's history length | 1 statement per 1,000-change page | BATCHED — the page is one recursive CTE with `ORDER BY walk.depth` |
| `pg.rs:1330,2401` the change flushes (`flush_history_rows`, `flush_changeset_rows`) | the transaction's pending rows | 1 `unnest` insert per 1,000 rows | BATCHED |
| `pg.rs:2283` the per-changeset parent-generation read | the parents the transaction did not insert | 1 statement with `= ANY($2)` | BATCHED |
| `pg.rs:2634,2979` `first_parent_page`/`get_derived_chain` recursive CTEs | the history the walk covers | 1 statement per page | BATCHED |

### Queries a caller's order depends on

Every paged read and every recursive CTE carries the `ORDER BY` its
keyset or walk needs (`db.rs:354,649,1362,1415,1432,1553`;
`pg.rs:2646,2994,3710,3734,3742,3757,3768,3800,3844,3861,3874,3890,3951`).
Three queries lack `ORDER BY`, and no caller depends on their order —
each caller answers per row and the reply is a set:

- `dangling_refs` (`pg.rs:3912`, `db.rs:1510`) — fsck reports each row.
- `stale_lands` (`pg.rs:3462`) — the janitor rejects each land by id.
- `lands_with_missing_changesets` (`db.rs:1518`, `pg.rs:3929`) — fsck
  reports each row.

### Postgres isolation, per transaction

| Transaction | Level | Read-then-write paths at that level | Lost update? |
| --- | --- | --- | --- |
| Writes (`pg.rs:1355`, plain `BEGIN`) | READ COMMITTED (Postgres's default) — one in-process writer mutex, and every write transaction of the schema holds the schema's one advisory key until it commits, so two writers serialize even across processes | `cas_ref` keeps its version predicate inside the UPDATE; `record_land`'s count-then-insert runs under the advisory key (`pg.rs:3407`); `settle` re-reads the ref and writes only under the version it read (`land.rs:810`); `put_derived`'s agree-check reads and writes in one writer-serialized transaction | No — `two_lands_that_record_together_respect_the_pending_cap` (`pg.rs:4735`) proves the count, `a_conflict_recorded_while_an_import_moved_the_ref_is_not_final` (`land.rs:1982`, run on both backends) proves the verdict's CAS window |
| Reads (`pg.rs:1389`) | REPEATABLE READ, READ ONLY — one snapshot per read, with `SET LOCAL` statement and idle timeouts | none (read-only) | No |
| A server refuses a data directory whose marker names another backend at the open | `crates/scion-server/src/backend.rs:128,183` | fixed by design (refusal, not fallback) |

## 5. Failure paths

### 5a. The panic inventory

Every `unwrap`, `expect`, `panic!`, `unreachable!` and `todo!` in
non-test code of `scion-server`, `scn`, `scion-proto` and
`scion-tier` (test-tail modules, `tests/` and the fuzz targets
excluded). Verdicts:

- **SAFE** — an invariant the code proves on the line before (named).
- **REACHABLE** — a client request, a file on disk, the database, or
  the OS can reach it; each one is fixed in the panic-fix PR with a
  red/green test.

| Site | What reaches it | Verdict |
| --- | --- | --- |
| `scion-proto/src/ticket.rs:150,194,195,229` (`try_into().unwrap()`) | a ticket from the wire | SAFE — each `try_into` follows a `take(n)` that returns exactly `n` bytes or errors, so the conversion is uninhabitable |
| `scion-proto/src/ticket.rs:365` (`unreachable!`) | the parse loop's exit | SAFE — the loop returns at every entry (the comment names it) |
| `scion-proto/src/ticket.rs:388` (`trees.get(&id).unwrap()`) | a tree id just written to the same table | SAFE — the id is inserted two lines above |
| `scion-proto/src/runtime/listen.rs:69,73,168` (`NonZero::new().unwrap()`) | process start | SAFE — the constants are literal non-zero values |
| `scion-proto/src/runtime/listen.rs:559,633` (semaphore `.expect("never closed")`) | accept-loop teardown | SAFE — the semaphore is never closed while the listener lives; closing happens only in Drop |
| `scion-proto/src/runtime/reply.rs:176` (stall-slot lock `.expect("not poisoned")`) | a panicked stall holder | SAFE — it fails loud (a panic, not a silent continue): the poisoned slot cannot be repaired, and the process dies naming it |
| `scion-server/src/access.rs:295,308` (`unreachable!`) | the prefix-match loop | SAFE — the loop breaks on the first match, so the `_` arm is dead (the comment names the invariant) |
| `scion-server/src/archive.rs:651` (`to.parent().expect`) | a crafted archive manifest | SAFE — `to` is built from a `BlobId` (`archive/blobs/xx/yy`), never a bare name; the parent is always present |
| `scion-server/src/blame.rs:819` (`PathElem::new().expect`) | a repo path from the database | SAFE — `RepoPath::new` validated every part on the way in (no empty part, no `.`/`..`, no nul), and re-splitting a valid path yields exactly those parts |
| `scion-server/src/blame.rs:881` (`line_u32`) | a blamed file with 2^32 lines | SAFE — `MAX_OBJECT_BYTES` is 100 MiB (`store.rs:84`), so a stored file has far fewer than 2^32 one-byte lines |
| `scion-server/src/blame.rs:967` (16 of 32 hash bytes) | the hasher's output | SAFE — `blake3` always yields 32 bytes |
| `scion-server/src/blame.rs:1033,1111,1112` (cursor/table `u32::try_from`) | a cursor field or window table | SAFE — the fields are built from one path (≤ `MAX_PATH_BYTES`) and one window over a store far below 2^32 changesets |
| `scion-server/src/graph.rs:576` (`.expect("a queued changeset has a mark")`) | the walk's own queue | SAFE — the mark is inserted when the changeset is queued, two lines above |
| `scion-server/src/main.rs:794,858,975,1064,1392,1402` (clap-conflict `unreachable!`/`expect`) | the command line | SAFE — clap refuses the conflicting flags before these arms run |
| `scion-server/src/main.rs:1274` (`.expect("exactly one source is given")`) | the access-file flags | SAFE — clap's `conflicts_with` allows exactly one source |
| `scion-server/src/pg.rs:325` (`.expect("a taken reader has a client")`) | the reader pool's taken flag | SAFE — `take` sets the client before returning the guard, and the guard's Drop clears it |
| `scion-server/src/pg.rs:599` (`.expect("a raw job carries its closure")`) | the job queue | SAFE — the sender sets `raw` exactly for `Kind::Raw` jobs, checked one line above |
| `scion-server/src/pg.rs:621` (`.expect("spawn the postgres worker")`) | the OS: thread-spawn `EAGAIN` under a thread limit | REACHABLE — fixed in the panic-fix PR (the open answers the error instead of crashing) |
| `scion-server/src/pg.rs:645,658,666` (`unreachable!`) | a reply of the wrong shape | SAFE — each worker branch answers exactly the reply kind its job kind asked for |
| `scion-server/src/pg.rs:905` (`writer.lock().unwrap()`) | a poisoned writer lock | SAFE — see 5b: a panic mid-probe leaves nothing behind |
| `scion-server/src/service.rs:1295` (`walk.next.unwrap()`) | the log walk's state | SAFE — the `walk.next.is_none()` case returns 13 lines above |
| `scion-server/src/service.rs:1841` (`.expect("asserted to fit in u32")`) | a reply size | SAFE — the `assert!` above bounds it |
| `scion-server/src/tickets.rs:44` (`.expect("system randomness")`) | the OS entropy source | SAFE — `getrandom::fill` cannot fail on the supported platforms (Linux, macOS; `support.md`'s platform matrix); the doc comment states it |
| `scion-server/src/tickets.rs:103` (`.expect("a key is 32 bytes")`) | the 32-byte secret minted above | SAFE — `getrandom::fill(&mut [0u8; 32])` fills exactly 32 |
| `scion-server/src/views.rs:262` (`Tree::new(kept).expect`) | kept entries of a stored tree | SAFE — a subset of an already-valid tree re-validates (every name passed `PathElem` on the way in) |
| `scion-server/src/pg.rs`, `test_support/*`, the fuzz targets | — | outside the audit (test-only builds; `test-hooks` is off in release) |
| `scn/src/cmd/bench.rs:114,125`, `cmd/blame.rs:64`, `cmd/commit.rs:130,138` (`IdentityTime::new`/`Changeset::new` `.expect`) | argv and git config | SAFE — the fields come from `-m` argv (bounded by `ARG_MAX`) and git's config parser, both far below the format's `u32::MAX` field limit |
| `scn/src/cmd/mount.rs:589,590` (signal `.expect`) | the OS signal masks | SAFE — `signal()` on an already-listened signal fails only on a broken runtime; the platforms guarantee it |
| `scn/src/main.rs:208,216`, `scion-tier/src/bin/*:120,128,157,165,197` (clap-conflict `unreachable!`/`expect`) | the command line | SAFE — clap refuses the conflicting flags first |
| `scn/src/vfs.rs:351,607,658,779,1167,1229,1278` (path/kind `expect`/`unreachable`) | the mount's own node table | SAFE — the guards above each call check the same field; a file node always carries its overlay path (the `Node` invariant the mount maintains) |
| `scn/src/vfs.rs:713` (double `.unwrap()`) | an NFS setattr on a forged file id | SAFE — `get(&id)` returns None before it (answered `NFS3ERR_STALE`), and the kind check above returns for dirs, so the remaining node is a file with a path |
| `scn/src/vfs.rs:720` (`file_mode().try_into().unwrap()`) | the mode constant | SAFE — 0o644 fits both Linux `u32` and macOS `u16`; the `useless_conversion` allow names it |
| `scn/src/vfs.rs:811` (`.get(&id).unwrap()`) | a lookup one line after the same get | SAFE — the match above already answered the `None` case |
| `scn/src/vfs.rs:1320,1384,1392` (`take(4).try_into().expect`) | a truncated index file | SAFE — `take(n)` returns exactly `n` bytes or an error; four bytes always fit `u32` |
| `scn/src/vfs.rs:1510` (`.expect("cannot read /dev/urandom")`) | the OS entropy source | SAFE — `/dev/urandom` always opens on the supported platforms; the mount fails at startup, before it serves |
| `scion-core`, `scion-proto` ticket decode, and every `take`-then-`try_into` in the codecs | the wire | SAFE — the same `take` contract |

### 5b. The lock-poison inventory

Every `Mutex`/`RwLock` that guards store or derived state. Policy:
`unwrap_or_else(PoisonError::into_inner)` — a poisoned lock is used,
not propagated. The question per lock: what a holder that panics
leaves behind, and whether the next holder checks it.

| Lock | Guards | A panicking holder leaves | Verdict |
| --- | --- | --- | --- |
| `derived.rs:178` `flights: Mutex<HashMap<FlightKey, Arc<Flight>>>` | which repo/kind walks are live | a live flight whose walk never retires: every later joiner waits for a walk counter that never moves — a caller with no deadline waits forever. Two exit paths leaked it: the panic unwind past `retire`, and the permit wait's early `?` on the owner's deadline | UNSAFE — fixed in the panic-fix PR: one drop guard (`RetireOnDrop`), created the moment the join makes the caller an owner, retires the flight on every exit; the panic still propagates. Tests: a panicking derive (the next caller owns a fresh walk) and a permit timeout (red before: the next caller hung to its DeadlineExceeded; green: it finishes the chain) |
| `derived.rs:180` `in_flight: Mutex<usize>` | the walk-permit count | a stale permit count | SAFE — `Permit`'s Drop runs on unwind and decrements |
| `derived.rs` `flight.walks` per-flight counter + `woken` | the walk-end signal for waiters | a counter that never advances | covered by the `flights` fix above: with `retire` on every owner exit the waiters wake and take over a fresh walk |
| `service.rs`/`listen.rs` semaphores and slot maps | connection/stream accounting | a leaked slot | SAFE — every holder holds it across `.await`-free counter ops; the panic points sit outside the critical sections, and the slot Drop runs on unwind |

The fix PR (#568) carries: the flight-leak fix — one drop guard over
both leak paths — with two tests (a panicking derive; a permit timeout
whose red run hung the next caller to its DeadlineExceeded), and the
worker-spawn fix with the error path (EAGAIN cannot be forced
in-process; stated in the PR).

| `todo!`/`unimplemented!` in production code: none | grep across the five crates | fixed (none exist) |
| Warn-and-continue: the import records and counts per-commit failures against the ledger; the two `eprintln!` skip lines are inside test-only helpers | `crates/scion-server/src/pg.rs:4459`, `gc.rs:591,1154` (both test helpers) | fixed by design (no production warn-and-continue on a correctness boundary) |

## 6. Defaults

| Finding | Evidence | Status |
| --- | --- | --- |
| The default data directory is `scion-data` relative to the server's working directory | `crates/scion-server/src/main.rs:29` | fixed (#578): a missing data directory is refused, not created — every subcommand except restore answers "<path> does not exist; create it or pass --data" (`main.rs`, `refuse_missing_data_dir`); fsck's refusal exits 2, its "could not run" code; `tests/refuse_data_dir.rs` holds every subcommand to the refusal and asserts nothing is created |
| Secrets on argv: no binary takes a secret as a flag; `--postgres-url-file` and `SCION_POSTGRES_URL` are the supported URL forms | `crates/scion-server/src/main.rs` (the pre-parse refusal) | fixed (the argv form is removed: a call that still passes one is refused before any parsing) |
| Ticket and seed files are 0600; the access file refuses group/other writable and symlinks | `crates/scion-server/src/main.rs:1333,1433,1493` | bounded — ceiling: 0600 on every secret file, the access file refuses go-w and symlinks at the open; `operations.md` "The open checks" states the refusal |
| Listeners default to loopback | `crates/scion-server/src/main.rs:60` (`default_value = "127.0.0.1:7878"`) | bounded — ceiling: the default bind is 127.0.0.1:7878; `docs/operations.md`'s serve examples restate it |
| Production state in temp dirs: the server never writes store state outside `--data`; every temp-dir user is a harness script | `crates/scion-server/src/main.rs:29` (the only store path) | fixed by design (the data dir is the only durable state; `TMPDIR` appears in no serve path) |
| Paths the process creates with the process umask inherit the gap: `export --out` (a new archive world-readable under a permissive umask) and the restore's staging directory (0755 members under a 0700 store) | `archive.rs` `make_dir` (the `--out` root, archive.rs:128,1458); the restore staging create | the restore row: fixed (#669, 4d5187f1 - the staging is created 0700 with a red proof); the export row: open, PR #666 (export: `--out` created 0700) - HIGH until it merges |
| A data directory whose mode lets other users read it is refused at every open of the store | `refuse_bad_dir`, store.rs:258 (serve, import, fsck, gc, derive) | open, PR #670 |
| `checkout` accepted the hostile-tree names (`..`, NUL, `..`-prefixed members) a crafted changeset can carry | the checkout name check | fixed (#662): `..`, NUL and `..`-prefixed members are refused; the mutation run found the guard unbound - no test failed a mutant that removed it - and #662's tests name that gap |

## 7. Compatibility

| Finding | Evidence | Status |
| --- | --- | --- |
| The store format, the protocol and the archive format are versioned documents; the unknown-field policy is stated per format, and each format's goldens enforce it | `docs/format-v1.md`, `docs/protocol-v1.md`, `docs/archive-format.md`, `docs/compatibility.md` | fixed — the table below |

### The contract goldens

| Format | The golden | The additive rule the test enforces | The doc pairing |
| --- | --- | --- | --- |
| Store (`docs/format-v1.md`) | `crates/scion-core/tests/golden.rs`: expected bytes hand-assembled from the grammar, independent of the encoder | an unknown tag, version, flag or git-algo byte is refused with the named error (`rejects_bad_tags_flags_and_algos`); red-proven: a decoder that accepts unknown tags fails it | the lint watches `golden.rs`: a change without a version-line move fails |
| Protocol (`docs/protocol-v1.md`) | `crates/scion-proto/tests/wire_golden.rs` + `protocol_wire_v1.golden`: the wire bytes of three fixed messages, hand-assembled from the proto3 grammar; the encoder must write exactly them and the golden must read back to the same values | unknown fields (varint 999, length-delimited 998) appended to every golden message decode with the known values unchanged (the doc's MUST-ignore); red-proven: one flipped golden byte fails the encoder test | the lint watches `protocol_wire_v1.golden`, `protocol_surface_v1.golden` and `proto/scion.proto` |
| Archive (`docs/archive-format.md`) | `crates/scion-server/tests/archive_golden.rs` + `archive_v1.golden`: one export's files and normalized manifest, byte-for-byte, on both backends | a manifest with an unknown version or format is refused with the named error before any write (`an_archive_of_an_unknown_version_or_format_is_refused`); red-proven: a reader that accepts version 2 restores the archive and fails the test | the lint watches `archive_v1.golden` and `archive_golden.rs` |

The pairing is enforced on the diff: `scripts/check-golden-pairing.sh`
(the CI lint job, on the pull request's own `HEAD^1..HEAD`). The gate
itself had gaps found on 2026-10-26 (all being fixed in #668, open):
two table rows (`crates/scion-server/tests/archive_v1.golden` and
`archive_golden.rs`) carry no rule word, so the `rule` field read the
golden's path and the row always took the "describe" branch; and the
rows whose pattern starts with `^` (`^# Scion object format v` at
check-golden-pairing.sh:48, `^Version [0-9]+ is this document` at
:53-54) can never match a diff line, which always begins with `+`, `-`
or a header prefix. Pinned
bytes (the wire golden, the store pins, the archive golden) fail
without their format document's version line in the same pull request.
`proto/scion.proto` and the surface golden are additive - they need no
new version (the doc's additive table) - so they need only the same
pull request changing `docs/protocol-v1.md`, any line: the contract
must describe the new surface. A golden file the pull request adds is
its initial pin: the in-tree tests hold that first pin against the
doc, and the lint holds every later change.
| Migrations carry no rollback path; the evidence is forward-only: N-1 opens N's store, N refuses a newer database, and the failed-migration runbook restores from backup | `crates/scion-server/tests/schema_drift.rs`, `scripts/version-matrix.sh:10-13`, `docs/operations.md` "A failed migration" | fixed by design (forward-only, with the matrix and the runbook as the evidence) |
| Fixtures of the current build only: the version matrix runs the pinned N-1 binaries against the current store both directions | `scripts/version-matrix.sh` | fixed (#540-era legs; the matrix never passes as a skip) |
| Backups and the failed-migration runbook are tested against a live server, not hand-written fixtures | `crates/scion-server/tests/backup.rs`, `crash.rs:342` | fixed |

## 8. Evidence and CI

The harness vacuity audit of 2026-10-23 is the base table of this
section; its fixes landed as #561. Highlights, with that audit's verdicts:

| Finding | Evidence | Status |
| --- | --- | --- |
| The soaks and the smoke never named the store they served, and soak-fault never counted its kills | `scripts/soak-fault.sh`, `scripts/soak.sh`, `scripts/release-smoke.sh` | fixed (#561: the backend row, the kills count, the no-sample segment rule) |
| The wall-clock budget in the derived flight tests could lose to a thread start | `crates/scion-server/src/derived.rs` | fixed (#566: the tests expire the deadline on the test's word, after the state proves the order) |
| The batch equality guards run both sides through the same flush, so a shared flush bug passes row-by-row equality | `tests/changeset_batch_eq.rs` | bounded — ceiling: 0 shared-flush coverage in the eq witness; the catching suites for quiet wrong content are `tests/history.rs` (the path log answers the git-referenced IDs) and `tests/audit_evidence.rs` (`a_tampered_history_node_reads_as_corrupt`, `fsck_reports_a_tampered_history_node`: a swapped history-node body answers Corrupt "does not match its ID" and fsck reports history-node-bad, both backends); the blind spot is recorded in that audit's notes |
| Main-only jobs without dispatch proof: every harness named in that audit's table now carries a branch dispatch run id | `.github/workflows/soak.yml`, `ci.yml`, `version-matrix.yml` | fixed (dispatches 39891410810, 39926987173, 39949854816, 39923804727) |
| Env-required suites that skip silently | `tests/crash_pg.rs:596`, `power.rs:80`, `dm_log_writes.rs:33`, `disk_full.rs:32` | bounded — ceiling: 0 silent skips; each prints its skip line, and the CI job that owns it sets the REQUIRE variable so a missing build fails the job |
| A randomized land model as an independent oracle | the land model script (#492) | fixed |

## 9. Supply chain

| Finding | Evidence | Status |
| --- | --- | --- |
| The workspace and the three fuzz lockfiles are committed | `Cargo.lock`, `crates/*/fuzz/Cargo.lock` (4 files) | bounded — ceiling: exactly 4 lockfiles, all committed; the release smoke and the CI builds run `--locked` |
| Every third-party action is pinned to a commit SHA with the version in a comment | `.github/workflows/ci.yml:24` and every workflow | fixed (pinned at introduction; the scorecard workflow watches for drift) |
| Service containers carry version tags, not mutable `latest` | `ci.yml` (the matrix `postgres:16.15/17.11/18.6`, each `@sha256:`-pinned in its include entry), `ci.yml` and `bench.yml` (`postgres:17.11@sha256:...`) | fixed — every services image pins its tag's multi-arch index; `scripts/lint-workflows.sh` fails a services `image:` line without `@sha256:` |
| Artifacts are built from the protected release tag only | `docs/RELEASING.md`, `docs/verify.md`, `docs/slsa.md` | fixed by design (the release workflow builds on the tag; the provenance doc states the SLSA level) |

## 10. Test strength (mutation testing)

A mutation-testing pass per module: each run flips one comparison or
guard at a time, and a surviving mutant names a check whose loss no
test would notice. The table lists the modules run so far, the mutant
count, what survived, and the pull request whose tests killed each real
gap.

| Module (the mutants' run) | Mutants | What survived, and what killed it |
| --- | --- | --- |
| `gc.rs` (#650) | 94 | 7 shapes: the grace report, the batch walk's end, three blob-root guards, a gone file's age, a corrupt changeset's naming - one test each, linked in #650 |
| `write.rs` (#649) | 2 shapes | the author-NAME limit (only the email arm was tested) and the message boundary (no test pinned the exact limit) - both bound with red proofs in #649 |
| the land path (#652) | 3 shapes | the empty name/ref arm, the name limit's boundary, the replan attempt cap - one test each, with the expected mutant outcomes named in #652 |
| `fsck.rs` (#655, #660) | 55 (the full sweep: 39 assertion-caught, 10 timeout-caught, 6 missed) | the 6 missed shapes, one killing test each in #660: the tree, history-node and repo-blob walks stop at a full page, a land whose changeset is gone reads clean, an unknown derived kind reads as damage instead of old; #655's four operator-view tests (an unreadable blob root, a lying tree size, a page-boundary early stop, a missing blob root) complete the run; the restore's staging pin (#659) adds the late-truncation layer |
| `ticket.rs` (#648) | 2 shapes | the walk's tree-id content-hash guard (a crafted tree reached a file the real tree does not name) and the ticket lifetime (no test pinned the 15 minutes) - both bound in #648. HIGH while it lasted: the tree-id check was unbound |
| the cache's signature check (#647) | 1 shape | the tier cache accepted `parse_unverified`: a signature check with no tier-level test. #647 drives a real cache with an unsigned ticket, an unknown key, and a flipped byte - each refused, the upstream untouched. HIGH while it lasted |
| the restore path (#659, #669) | 1 pin + 1 shape | the stream cut: the restore fails naming the torn stream, the target holds no store, the staging scratch is gone (#659); the staging directory made under the process umask left a 0700 parent store with 0755 members - the staging is created 0700 explicitly, whatever the umask, with a red proof (#669, 4d5187f1) |
| the auth paths (#961, run 42391198417) | 177 tested: 107 caught, 15 unviable, 55 missed | 3 real gaps, one killing test each in #985: the token-digest comparison folded to XOR would pass about 1 random token in 256 (the fold zeroes when the 32 byte-xors XOR to zero) - a test gap, main is not vulnerable; the checked file's 1 MiB cap and the comparison that enforces it; the 0700 secret's boundary. The client Bearer interceptor's dropped token is bound on main by the audit suite's valid-token writes (audit.rs:342), not by this run's crate. 42 misses are macOS-only (the ffi ACL module's tests are macOS-gated); the Linux run cannot reach them. 9 equivalent: the distinct-bit open flags (| and ^ compose the same value), and the ancestor open's flags still open the same resolved directory read-only |
| the read rules, `access.rs` (#961; runs 42377322779 over the boundary suites and 42394665585 over the unit suite) | 82: the boundary run caught 60, the unit run 64, 51 by both; 7 unviable | 2 real gaps, missed by both suites, one killing test each in #981: a principal of legal characters over the 64-byte limit had no refusing case, and the audit start line's `access_tokens` count had no test at all. The other 22 misses are one suite's blind spot, not gaps: 13 missed by the boundary run alone the unit suite kills (both rule-set caps at and over the bound, a write without a whole read, a comment line, a two-prefix line, a principal with a space), 9 missed by the unit run alone the boundary suites kill (`read_digest` and `view_digests`, the read arm's field-count guard). Sums: 60 = 51 + 9 caught + 15 missed = 75 viable + 7 unviable = 82; 64 = 51 + 13 caught + 11 missed |
| the auth paths, macOS half (#961, #986, run 42415734120) | 153 tested: 120 caught, 17 unviable, 33 missed; the union with the Linux run misses 28 | the 28, by verdict: 8 real-gap mutants, four killing tests in #999 - an ancestor directory whose ACL grants read was answered "no ACL" on every ancestor (a parent that grants another account read access was accepted); a walk that broke at the first deny, or ended early, read a deny-then-allow ACL as deny-only; the refusal's kind text ("an allow entry") was unpinned; the qualifier's arm - an allow entry naming a specific account must render "for account"; 3 not pinned: the account number in the refusal is host-dependent (the ACL's id space differs from stat's), low severity - the refusal and the entry kind are pinned; 2 equivalent: hw_memsize - the mutants differ only when sysctl fails or answers 0, which no test can force, and the success value is the host's; 13 equivalent: the flag compositions - distinct powers of two, so | and ^ compose the same value, and the mutated ancestor opens still open the same resolved directory read-only; plus the ACL_NEXT_ENTRY = 1 mutant, measured equivalent-in-context on macOS (acl_get_entry reads the id as a working index); 5 misses are bound by the other run (the Linux setfacl tests kill fd_has_hidden_acl's !-deletes; the Linux clean-tree tests kill the Linux-fn body swaps), and 1 by a server suite (the Bearer drop: audit.rs:342's valid-token writes). the 28 the runs share = 8 + 3 + 2 + 14 + 1; the 5 the Linux run bound are the difference, and every count here sums to the run's 33 |
| the per-principal quotas, `quotas.rs` (#1001; runs 42429841896 over the boundary suites and 42429850332 over the unit suite) | 40 = 16 caught by both suites + 8 caught by one suite only (5 the boundary run missed, the unit suite kills; 3 the unit run missed, the boundary suites kill) + 6 missed by both (1 real, bound by the new test; 3 removed with the dead accessor; 2 equivalent) + 10 unviable | the real gap: `Call::add`'s own-hold counter `self.bytes += bytes` flipping to `*=` makes the drop give back a wrong count that saturates to 0, so a principal's in-flight bytes under-count and the next upload wrongly fits while another call still holds bytes - killed by the unit test `a_dropped_upload_gives_back_its_own_bytes_only` (red proof with the mutant planted). The 3 removed: the dead `upload_bytes()` accessor, deleted in #1001 (no caller outside tests; the limits ride the private field). The 2 equivalent: `off()` and `nothing()` under `Default::default()` produce the same values |
| the object replies, `objects.rs` (#1001; runs 42429919049 over the boundary suites and 42429926893 over the unit suite) | 63 = 20 caught by both suites + 14 caught by one suite only (9 the boundary run missed, the unit suite kills; 5 the unit run missed, the boundary suites kill) + 8 missed by both (7 real, one killing test each; 1 in a dead check that hid a real bug, fixed by #1017) + 21 unviable | the 7 gaps, one killing test each: the plan's tree-size cap at the exact limit (`>` to `==` and to `>=`: a tree the size of MAX_OBJECT_BYTES must still plan Raw - `a_tree_at_the_object_limit_plans_raw`); the empty file's Raw plan (the `Ok(0)` arm deleted: only the empty blob's 0-byte file may plan Raw - `an_empty_blob_file_plans_raw`); the plan's zstd bound (a stored file over its bound must read Corrupt - `a_packed_file_over_its_zstd_bound_is_corrupt`); the request's deadline (the Waits' `until` initializer deleted: the reply's waits must read the call's grpc-timeout - the in-process deadline test, red 60.7 s / green 3.6 s); the walk cap's arithmetic (`held + bytes > cap` to `*` and to `>=`: two trees summing to the exact cap - `the_walk_cap_takes_the_sum_of_two_trees_to_its_exact_bound`). The 8th: the mutant sat in a check that could never fire (`Some(None).is_none()` is always false), which hid a real bug (#1016: a refused walk share still served); #1017 fixed the refusal and removed the check, and its test `a_refused_walk_share_ends_the_reply_busy` pins the refusal. The other 14 misses are one suite's blind spot: 9 the unit run kills (the plan's read_bytes arithmetic twice, the (Blob, Raw) arm, the walk-share if's `>=`, the refusal's `==` and `>=`, prove's page bound `>=`, the cap's `==`, the held-counter `*=`), 5 the boundary suites kill (the cap's `<`, resolve_ref and get_root_tree answering Ok(default), `of()`'s kind replaced by the default twice) |
| the commit graph, `graph.rs` (#1018; the boundary run in two shards - 42449207785 and 42459266014 - over the land model and twin-race, history and its restore bound, the graph's pg budget and numbering, import, read edges and contract suites; 42449222507 over the unit suite) | 139: the boundary run caught 76, missed 46, 17 unviable; the unit run caught 90, missed 21, timed out 11, 17 unviable | by either suite, best outcome: 103 caught (63 by both), 15 missed by both, 4 the boundary missed and the unit only hung on (counted caught-by-timeout), 17 unviable (sums 139). The 15 missed by both, by verdict (one line per mutant, counted by script): 7 killed, one test each - `on_first_parents`' three body swaps and its final `==` flip (a base on the tip's first-parent history is `Some(true)`, one off it `Some(false)`; the land path trusts the answer - `a_base_on_the_first_parent_history_is_true_and_one_off_it_false`); `in_steps`' visit-bound `>` to `==` and `>=` (an answer landing exactly on the bound must go through - `the_visit_bound_lets_an_answer_on_it_through`); `tally`'s `&` to `^` (the guard then refuses every mark, the live counts freeze, and the walk drains the one-sided tail past the bound - `a_paint_walk_ends_when_a_side_dies_not_when_the_queue_drains`). 1 equivalent: `Paint::next`'s `|` to `^` (`FROM_A` and `FROM_B` are disjoint single bits, so the composed value is identical and no state differs, graph.rs:543-547). 7 untested: `meeting`'s two `>` to `>=` (cost only - at `newest_merge == floor` the newest merge is the first-parent meet itself, so the paint walk answers the same bases at more cost, graph.rs:535-538); `Index::node`'s `&&` to `||` (cost only - the line only picks the prefetch batch; a missed prefetch loads on demand, graph.rs:757-778); `in_steps`' three error arms (the stop's info log line only - the result is unchanged); and the step-split `<` to `<=` (the read split across the walk's steps only) |
| the tree builder, `tree.rs` (#1030; the boundary run in two shards - 42477678613 and 42477778457 - over the land model and twin-race, the tree batch suites, history batch and import; 42477684684 over the unit suite) | 57: the boundary run caught 26, missed 7, 24 unviable; the unit run caught 22, missed 11, 24 unviable | by either suite, best outcome: 26 caught (22 by both, 4 boundary-only), 7 missed by both, 24 unviable (sums 57). The 7 missed by both: 5 killed by suites outside the run - the batching check's `!` deleted (the land loses its preload and deferred deletes; the create and land statement bounds fail: `a_one_put_create_costs_a_fixed_number_of_statements`, `a_land_costs_a_constant_more_over_a_hundred_paths_and_stays_under_its_bound`), the test hook's `prefetch_steps` answering 0 or 1, and the retire's `+` to `*` and `==` to `!=` (tree_batch_wide's step count, pinned on both sides: the fixture's 100,000 single-directory paths take exactly 100,000 steps, and the two walker mutants double it to 200,000); 2 untested: cost only - the over-queuing `&&` to `||` (the duplicates re-read resolved trees; the cursor steps and the answers are unchanged) and the frozen-cursor `+=` to `*=` (one step per path, the walk ends after the first round, the apply reads the deeper trees one by one); the apply is the correctness path for each |
| the view derivations, `views.rs` (#961; runs 42394477745 over the boundary suites and 42402430077 over the unit suite, both complete; two earlier attempts - 42377322779 cancelled at 24 of 44, and the unit filter 42394668084 at its job cap - are covered by these two) | 44: the boundary run caught 18, missed 16, timed out 7, 3 unviable; the unit run caught 28, missed 5, timed out 8, 3 unviable | by either suite, best outcome: 29 caught, 5 missed, 7 timed out, 3 unviable (sums 44). Of the 5 missed by both suites: 3 are real gaps - the part-bound guard's restore and the walk's pending-byte estimate multiplying name bytes instead of adding, one killing test each in #981; the walk counter answering 0 under `+=` → `*=`, bound in #990 (the cold-derivation assert). 2 are equivalent: the dedupe map's clear is a RAM-bounding heuristic inside the walk's own reservation, and a map that never clears costs re-walks, never wrong results. The 7 timeouts are unit-suite hangs (a walk that never converges, a part per step) - reactions, not silences; `write_memo_part` → `Ok(())` the boundary run caught and the unit run only timed out |
| the audit file, `audit.rs` (#1001; runs 42429702569 over the boundary suites and 42429766878 over the unit suite, both complete) | 77: the boundary run caught 36, missed 22, timed out 8, 11 unviable; the unit run caught 48, missed 16, timed out 2, 11 unviable | by either suite, best outcome: 50 caught (34 by both), 14 missed by both, 2 timed out, 11 unviable (sums 77). 12 of the 14 missed by both are equivalent: the nine `|` flips on the open flags compose the same values (distinct single-bit flags; the parent-directory open stays a read-only open of the directory `check_ancestors` just resolved), and the writer's batch bound (`< QUEUE` to `==`, `>`, `<=`) only regroups fsyncs, never a byte. Two are not pinned: the ownership arm (`st_uid == 0` to `!=`) and the guard's first `||` to `&&` are equivalent only for a non-root server - a non-root process cannot open another uid's 0600 file, so the open refuses before the guard runs - but nothing stops a root serve, and a root process opens any file: under root the mutants accept a foreign-owned 0600 audit file the guard must refuse (#1006). The 2 timeouts are the append loop's fault-step counter under `<=` and `*=`: hangs, not silences |
| the data directory's checks, `store.rs` `refuse_bad_dir` (#1011; the planted method - the file's 167 mutants narrow to the 4 in `refuse_bad_dir`, each planted by hand one at a time and reverted after, with both suites run per planting: `--test private_dirs --test backend_refusals` and `--lib store::`; no CI run for 4 mutants) | 4 | all 4 caught by both suites: the stub (`refuse_bad_dir` to `Ok(())`) dies on the group/others-readable refusal (private_dirs.rs:322) and the unit suite's symlink and mode refusals; the mask folds (`&` to `|`, `&` to `^`) and the sense fold (`!=` to `==`) die from both directions - the good-dir opens fail (a 0700 directory answers nonzero under all three) and the refusal tests fail the other way (a 0755 directory passes the `^` and `==` folds). The mode check is uid-independent - it reads the directory's mode, never the process's uid - so it refuses a group/others-readable data directory for a root server exactly as for a user server; under root it is the guard that keeps the store private at all, since the ancestor walk's ownership arm then accepts only root-owned ancestors, the same shape #1006 names for the audit file |
| the listener, `listen.rs` (#1008; three runs over the same 192 - the lib-filter run over `--lib listen::` in two shards, runs 42444503483 + 42444001281, the in-package socket-suite run 42446217261 (`--test socket_symlink --test secret_file_attacks`), and the server-suite run over the four boundary suites in four shards, runs 42445744678 + 42449921428 + 42449928499 + 42449935889) | 192: the lib run caught 56, missed 44, timed out 1; the socket run caught 10, missed 91; the server run caught 16, missed 85; the same 91 unviable in all three (the whole-function type errors: `ReceiverStream`/`Poll` have no `from_iter`, `SocketAddr` has no `Default`) | by any run, best outcome: 70 caught (58 by one run only, 12 by two, none by all three - a one-run catch is that suite's blind spot, and the other runs or the new tests kill it), 31 missed by all three, 91 unviable (sums 192). The 31, one verdict each, names verbatim from the run's tsv (the `crates/scion-proto/src/runtime/listen.rs:` prefix stripped): killed - `1079:24: delete ! in accept_tls` by `a_failed_tls_accept_does_not_end_the_serve` (#1022) and `1181:24: delete ! in accept` by `a_failed_accept_does_not_end_the_serve` (#1022): with the `!` deleted the failed accept ends the serve instead of the loop retrying; `1333:43: replace == with != in <impl AsyncRead for Connection<IO>>::poll_read` by `a_partial_preface_is_cut_at_the_handshake_timeout` (#1022, caught in the proof run): the inverted sense never clears the preface window, so the handshake timeout never fires; `1375:9: replace <impl AsyncWrite for Connection<IO>>::poll_flush -> Poll<io::Result<()>> with Poll::from(Ok(()))` by `a_tls_reply_arrives_in_full_even_after_a_socket_pause` (#1022): the flush call dropped, the reply's tail strands in rustls' session and the read times out; `127:9: replace Backoff::reset with ()`, `133:72: replace * with /`, and `133:72: replace * with +` by `the_backoff_resets_on_a_success`, which asserts the real waits over a window (the `+` keeps the pace linear, 10/20/30, and the window catches it at the 40 ms floor); `180:5: replace client_keepalive -> (Duration, Duration) with (Default::default(), Default::default())` and `190:5: replace client_tcp_keepalive -> Duration with Default::default()` by `the_keepalive_getters_hand_tonic_working_values` - zero is not "off": hyper schedules the next ping at last_read + interval and a zero timeout fires the moment the ping is sent, so the connection dies at its first ping (hyper-1.12.0/src/proto/h2/ping.rs:455-458, 495-505); `843:5: replace warn_waited with ()` by `the_waits_line_names_the_cap_and_the_count`, `852:5: replace warn_refusals with ()` by `the_refusals_line_names_the_cap_the_count_and_the_wait`, `877:5: replace warn_refused with ()` by `warn_refused_writes_the_rate_limited_refusal`, `737:9: replace WaitWarn::spawn_flusher with ()` and `737:33: replace == with != in WaitWarn::spawn_flusher` by `the_flusher_writes_each_interval_and_stops_at_the_end` (the `!=` inversion dies at the flusher's first tick), `871:5: replace spawn_cap_flushers with ()` by `spawn_cap_flushers_spawns_both_flushers` (both counts strand), `614:9: replace Limits::warn_the_last with ()` by `warn_the_last_writes_what_is_left` - each with a one-by-one red proof against the local lib suite; `357:76: replace + with * in Stalled::hold`, `359:43: replace >= with < in Stalled::hold`, and `360:13: replace && with \|\| in Stalled::hold` by `the_stall_cap_crosses_on_the_connection_count_alone` (the holds overlap - a real reply's drops give the bytes back - so the crossing is on the per-connection count alone; the `&&` -> `\|\|` guard fires under the limit and spends the once-only warning); `1429:22: replace match guard !dir.as_os_str().is_empty() with true in bind_unix` by `a_bare_socket_name_binds_in_the_current_directory` (#1019, on main): a bare relative socket name has an empty parent and must take the "." fallback; `1379:9: replace <impl AsyncWrite for Connection<IO>>::poll_shutdown -> Poll<io::Result<()>> with Poll::from(Ok(()))` by `a_serve_end_closes_tls_cleanly`: a serve's end drives the h2 close path - h2's State::Closing runs the codec's shutdown into the IO's `poll_shutdown` (h2 connection.rs:341-344, after the drain's PING is answered) - so the mutant's no-op leaves the close_notify unsent and the client reads rustls' `UnexpectedEof` instead of an end of stream; red proof with the mutant planted. equivalent-in-context - `1356:9: replace <impl AsyncWrite for Connection<IO>>::poll_write -> Poll<io::Result<usize>> with Poll::from(Ok(0))` and `1356:9: replace <impl AsyncWrite for Connection<IO>>::poll_write -> Poll<io::Result<usize>> with Poll::from(Ok(1))`: both wrapped streams report `is_write_vectored()` true (tokio stream.rs:1518-1520, tokio-rustls server.rs:523-525), so h2's framed writer drives `poll_write_buf`/`poll_write_vectored` (tokio-util poll_buf.rs:132-138) and `poll_write` never runs in this tree; `1371:9: replace <impl AsyncWrite for Connection<IO>>::is_write_vectored -> bool with false` - h2 takes `poll_write`, which forwards to the same inner write, the same bytes - and `1371:9: replace <impl AsyncWrite for Connection<IO>>::is_write_vectored -> bool with true`, the value both streams already report; `1262:9: replace <impl PeerOf for TlsStream<IO>>::peer_of -> Option<SocketAddr> with None`: the TLS serve reads the peer from the raw TCP stream before the wrapper exists (listen.rs:972), nothing calls `TlsStream::peer_of` (planted, green); `654:69: replace - with / in now_millis`: reached only when elapsed millis overflow u64, about 584 million years of uptime. not pinned - `1330:51: replace - with + in <impl AsyncRead for Connection<IO>>::poll_read`: the count inflates only when a poll lands bytes into a buffer that already holds filled bytes; hyper's preface reader and every suite poll with the buffer cleared, so `read - before == filled + before` wherever the guard runs - a hand-driven `poll_read` unit test holding the buffer across two polls would pin it (missed by all suites, the proof run included); `654:74: replace + with * in now_millis`: with `* 1` the clock reads 0 during the process's first millisecond, and zero means "never wrote", so a line written in that window is stored as never-wrote and a second line can pass in the same interval - only the first call of the process, in its first millisecond, can see it, and tests share one process; `1256:9: replace <impl PeerOf for tokio::net::TcpStream>::local_of -> Option<SocketAddr> with None` and `1266:9: replace <impl PeerOf for TlsStream<IO>>::local_of -> Option<SocketAddr> with None`: the local addr feeds only a `debug!` line (the listener field, listen.rs:1190, and poll_read's first-poll line), which the WARN-level test log never emits - an operator at debug level sees a missing listener field; a test could tell only by capturing at debug level (both planted, green) |

| the client checkout path, `materialize.rs` (#1014; runs 42447103276 + 42447119093 over the boundary suites and 42447135474 + 42447153159 over the unit suite, each pair a two-shard split, all complete) | 168: the boundary runs caught 64, the unit runs 131, 63 by both; 27 missed by both; 9 unviable, 0 timeouts (each run's counts sum to 84, the union to 168) | of the 27 missed by both suites: 8 are killed by 6 new tests in #1020, each with the mutant planted one at a time and the single test run red: the removal marker's record (three mutants - `None`, a default `Some`, a no-op set - die on the marker naming the temp directory), the rename guard answering `true` for a real ENOENT (the temp directory removed behind the walk's back fails the checkout), the empty-parent guard (a bare destination name works in the working directory), `open_dest` losing `O_DIRECTORY` (the 0600 FIFO at the destination is refused within the 5 s bound instead of blocking), `copy_up`'s mode mask widening to 0777, and the fold filter's kind clause forced false (a lost mkdir reply is refused because a removed recorded file counts as a fold); 16 are equivalent: the flag compositions (`RDONLY` is 0, so `|` and `^` compose the same value; distinct single bits XOR to the same value), the `&` folds on opens inside the temp directory `make_temp_dir` made 0700 (nothing a local user plants is reachable there), `start_next_to`'s two `&` folds (when the weakened open runs, `open_dest` (materialize.rs:176) has already opened that same path as a directory, so swapping the parent for a FIFO in between takes a writer of the grandparent - the user running the checkout, or root - which is the local trust the 0700 temp directory already leans on; the FIFO-parent plants came back green, and a regular-file ancestor fails `make_temp_dir` the same way), and the fold filter's outer any agreeing with the inner at the recorded key; 2 are untested: the `Drop` warning counter, which nothing in the suite observes; 1 is not pinned: the `is_mine` swap arm, which needs a directory another uid owns at the destination, and one uid cannot stage that |
| the backup archive, `archive.rs` (#1051; the run in seven shards over the archive, archive-attacks, archive-golden, archive-restore-stream, git-import-batch, changeset-batch-eq and history-restore-bound suites - the seven suites that write, verify or restore an archive through the library; no unit run: the file has no unit tests and the only lib-side callers of its readers are the test_support hooks, which archive_restore_stream already drives; DRAFT - two of seven shards (42638259563, 42650077738) and the measure shard 0/14 are in, the tail pending) | 281 planned; so far 82 covered: 42 caught, 22 missed, 18 unviable (sums 82; the rest lands with shards 2-6) | the 22 so far, by verdict, counted by script: 19 killed by the branch's tests, each with the mutant planted one at a time and the single test run red - the manifest's counts equal the sections' sums, the lands' collision and input checks pinned from the clean side, a plain file where a section should be refused, a changeset body that does not hash to its ID named, the restore's report and transaction counters exact, the schema-drop's two refusals each saying what it protects, and the audit file carried back on a failed replay and on a failed publish; 4 not pinned: the restore's whole-file lever, whose memory ceiling is measured on Linux only (archive_restore_stream.rs:297) and which mutants.yml cannot arm; 1 equivalent-in-context: the restore's record_land deadline (+ to -) - the deadline is never stored and the restore is the sole writer of the fresh schema. One real gap found by the run's shards: verify accepted a provenance row whose changeset the archive does not hold (the format's MUST was checked for refs and lands, not provenance); the restore died later on the store's foreign key with the target left empty - fixed by #1062 with its own red-first test |

## The open list (the v1.0 blockers this audit leaves)

1. The `--unstable-writable` write path out of the release binary (§1,
   the table's last open row) — closed by #640, which takes the write
   path into a build feature, off in release.

Closed since the audit:

- The Postgres lock-restart runbook, and whether `serve` should
  wait for the lock (#609, §2: the runbook names the supervisor's
  restart as the recovery, and `serve` fails fast by design).
- The archive restore's repository-sized RAM hold (#575, §2: the replay
  streams and commits per byte budget, with the RSS ceiling test).
- The unary reply caps and `lossy` JSON (#586, #576, §3), and the `scn
  commit` whole-file read (#582, §3).
- The database-loop sweep: SQL in loops, `ORDER BY`, isolation (#570,
  §4: every loop listed with its verdict, no repo-sized site).
- The data-dir default create-vs-refuse question (#578, §6: a
  missing data directory is refused, not created).
- The additive contract goldens that enforce the format policies
  (#571, §7: the store, protocol and archive goldens, with the
  doc-pairing lint).
- Digest pins for the service-container tags (#595, §9: every services
  image pins its tag's multi-arch index, and the workflow lint holds
  it).
