# Postgres 17 qualification gap list

The promotion decision (`docs/ROADMAP.md` v0.6) moves Postgres 17 to
production when
four things hold: (a) the contract test passes on Postgres 17, (b) the
real Postgres fault rows pass, (c) backup and restore pass on Postgres
17, and (d) the release-candidate trial runs on Postgres 17. Postgres
is the metadata store the project supports today (`docs/support.md`):
17 runs on every push, 16 and 18 nightly and on
release candidates.

This document is the gap list: one row per requirement and per fault
row of `docs/faults.md`, what proves it on Postgres 17 today, and the
verdict:

- **EVIDENCE** — the test or drill really runs against Postgres 17 in
  CI, asserts the backend it names, and can fail (a red is visible).
- **WEAK** — it runs but could pass vacuously, or runs only when a
  human arms it; the reason is stated.
- **MISSING** — nothing runs it on Postgres 17.

Every row was checked against the code and the workflows on
2026-10-23, at the merge of #571, and refreshed against `main` on
2026-11-20 (the refresh re-checked every anchor below; the run links
are the 2026-10-23 sweep's). The last fully green `main` CI run
that proves the EVIDENCE rows is
[39985773698](https://github.com/bnsd55/openscion/actions/runs/39985773698)
(#577): `test (postgres 17.11)`, `power-loss` and `mount (NFS)` all
green. Where a claim could not be verified it says so.

The Postgres 17 evidence rides on one job family:

- `test (postgres ${{ matrix.version }})` (`.github/workflows/ci.yml`,
  job `test-postgres`): the whole `open-scion-server` suite over the
  job's Postgres service (every suite is Postgres now: the suite's
  common server asserts the `backend` marker is `postgres` and that no
  `scion.db` exists), then the real-fault steps — the tmpfs disk-full
  step (`disk_full`), the block-level power cuts (`dm_log_writes`),
  and the takeover (the database stops and restarts;
  `backend_refusals -- --ignored`). Postgres 17.11 runs on every
  ready, non-docs-only push and PR, and always on `main`, the nightly
  and a dispatch; 16.15 and 18.6 are nightly and dispatch only
  (the job's `matrix.version`).
- `power-loss` (`.github/workflows/ci.yml`): the LazyFS suite over a
  Postgres 17.11 service, one flow — the suite's own servers assert
  the Postgres marker. Main, nightly and dispatch only — never on a
  pull request.
- `mount (NFS)` (`.github/workflows/ci.yml`, job `mount (NFS)`): one
  Postgres-backed flow (the mount script runs with
  `SCION_TEST_POSTGRES_URL` set). Also main, nightly and dispatch
  only.

## 1. The four promotion requirements

| Requirement | What it says | Evidence today | Verdict |
| --- | --- | --- | --- |
| (a) the contract test on Postgres 17 | The end-to-end contract flow (import, serve, land, read, export) answers the same on Postgres 17 | `crates/scion-server/tests/contract.rs` drives a real server over the job's Postgres service (the spawned commands get `SCION_POSTGRES_URL` from `common::postgres_url`) and runs inside `test (postgres 17.11)`'s whole-suite step on every ready, non-docs-only push and PR, `main`, nightly and dispatch. The contract goldens of #571 (`crates/scion-server/tests/archive_golden.rs`, `audit_golden.rs`, the proto wire goldens) ride the same suite | EVIDENCE |
| (b) the real Postgres fault rows | The fault matrix's Postgres rows are proven by tests that run on Postgres and can fail | See section 2 | EVIDENCE with two WEAK rows (the dispatch-only soak-fault arm, the drills) |
| (c) backup and restore on Postgres 17 | A Postgres-backed store backs up and restores whole | `crates/scion-server/tests/backup.rs` `a_backup_taken_while_serving_restores_whole` — a live server, a backup taken mid-write (the metadata snapshot is the runbook's `pg_dump --no-owner` of the live database; the job hands the service container to the steps through `SCION_TEST_PG_CONTAINER`), the restore into a fresh store, then the equality check. Runs in `test (postgres 17.11)` per push. The operations runbook is `docs/operations.md` section 3 (`pg_dump` the database, restore the data directory, reopen) | EVIDENCE for the test; the runbook has never been walked on a real Postgres 17 install — that is requirement (d)'s trial, not a CI gap |
| (d) the release-candidate trial on Postgres 17 | One RC serves real work for 30 days per `docs/production-trial.md`, with the trial file named before day 1 | `docs/production-trial.md` is the template only; `docs/release-notes/` holds no trial file; v0.6 has no RC yet. The two store drills have Postgres steps (operator-drill T3's `pg_dump` backup; second-operator's marker step and restart drill), but only a person runs them; the release and vulnerability drills have no store step. The RC trial runs the drills on the Postgres 17 install and records the outputs | MISSING — by schedule, not by neglect: the trial runs when the RC is cut. The follow-up is in section 4 |

## 2. The fault rows, one per `docs/faults.md` row

The matrix's two-half rule holds throughout: on Postgres the data
directory holds the marker and the blob files, the metadata lives in
the database's own WAL, and each fault row cuts the half it names.

| Fault row (faults.md) | Postgres 17 evidence | Verdict |
| --- | --- | --- |
| Wrong data directory for the database (UUID refusal) | `backend_refusals.rs` in the whole-suite step on Postgres; the takeover step runs its `--ignored` tests | EVIDENCE |
| SQLite open of a Postgres-backed directory | Same | EVIDENCE |
| Another server took the store (fence) | `backend_refusals.rs::a_lost_server_lock_fences_the_backend`, Postgres-only by design | EVIDENCE |
| Database unreachable (not a takeover) | `backend_refusals.rs::an_unreachable_database_is_not_a_takeover` | EVIDENCE |
| Another writer holds the write lock | `backend_refusals.rs::a_write_blocked_by_another_writer_answers_busy_in_seconds`, both backends in one run | EVIDENCE |
| Server killed mid-write (SIGKILL) | `crates/scion-server/tests/crash_pg.rs` — the real binary over Postgres: `a_kill_during_a_land_leaves_the_ref_old_or_new` and `a_kill_during_a_schema_upgrade_leaves_one_whole_version` | EVIDENCE — faults.md's rows name these tests |
| Kill during a schema upgrade | `crash_pg.rs::a_kill_during_a_schema_upgrade_leaves_one_whole_version` (the row above repeats here because faults.md splits them) | EVIDENCE |
| Tier daemon killed mid-fill | No Postgres row by design: the tier keeps its own object cache and takes none of the server's metadata | not a gap (documented exclusion) |
| Blob write hits a full disk (ENOSPC) | `storage.rs` runs on Postgres in the whole-suite step; the blob half is the data directory either way | EVIDENCE |
| SQLite metadata disk full | SQLite-only by definition; the Postgres analogue is the 53100 row below | not a gap |
| A really full Postgres database (53100) | `disk_full.rs::a_full_postgres_database_answers_out_of_space_and_needs_no_repair` in the tmpfs disk-full step of `test-postgres`, per push on the Postgres tier | EVIDENCE — faults.md's row names this step correctly (its one stale anchor, a ci.yml line number, is fixed in the same sweep as this list) |
| One file write of a batch fails | `storage.rs::a_batch_that_loses_a_file_keeps_its_earlier_files`, backend-independent blob half | EVIDENCE |
| Blob file deleted under the server | `backend_refusals.rs::a_fsck_of_a_postgres_backed_store_finds_missing_blob_files` | EVIDENCE |
| NFS mount | the `mount (NFS)` job, its Postgres-backed flow (the mount script runs with `SCION_TEST_POSTGRES_URL` set); the script proves the marker names Postgres before the flow | EVIDENCE |
| Power cut while clients write | the `power-loss` job, its Postgres-backed LazyFS flow (the suite's servers assert the `backend` marker is `postgres`) | EVIDENCE for the reply rows |
| Racing puts of one blob + power cut | `power.rs::racing_puts_of_one_blob_survive_a_cut` in the `power-loss` job's Postgres-backed LazyFS flow: the write park that makes the race certain is a file seam under the data directory (`store.rs`'s `WritePark::from_env`, armed through the test's park helpers), backend-independent, and the suite's servers assert the `backend` marker is `postgres`. faults.md's row already says the rows run on Postgres | EVIDENCE |
| Block-level replay (dm-log-writes) | `test-postgres`'s block-level power-cut step (`dm_log_writes`), per push on the Postgres tier | EVIDENCE |
| EIO / EROFS failing disk | `storage.rs` rows on Postgres in the whole-suite step; the metadata half rides the database's WAL | EVIDENCE |
| Whole server on a real full disk (tmpfs) | `disk_full.rs` on Postgres in the tmpfs step (blob path and inodes) | EVIDENCE |
| SIGTERM drain under load | `health.rs` runs on Postgres in the whole-suite step | EVIDENCE |
| Soak-fault: kill -9 the server under load, on Postgres | The `soak-fault-postgres` job (`.github/workflows/soak.yml`) arms the Postgres arm of `scripts/soak-fault.sh` (`pg_restart`): a dispatch-only Postgres 17 service container pinned tag@sha256, `SCION_TEST_POSTGRES_URL`, `SCION_FAULT_PG=1`, and `SCION_FAULT_PG_CMD` restarting the container between kills. The summary names the backend, the kills and the Postgres restarts, and the run fails unless every kill restarted Postgres | WEAK — dispatch-only (the 72-hour hosting decision), never per push or nightly. Green: [40017979410](https://github.com/bnsd55/openscion/actions/runs/40017979410) (postgres -> postgres, 3 kills, 3 restarts); red: [40071077720](https://github.com/bnsd55/openscion/actions/runs/40071077720) (the restart command replaced by a stop: "Postgres never came back"). Promotion to EVIDENCE comes with the 72-hour hosting decision |
| The lock-restart refusal row (a server restarting right after a crash refuses until Postgres reaps the dead session's lock) | Open in `docs/production-audit.md` section 2; the runbook must say the supervisor retries the open with backoff | open row of the audit, not a CI gap — feeds the trial's runbook walk |
| Operator / release / vulnerability drills | WEAK - the two store drills have Postgres steps (`pg_dump --no-owner` in operator-drill T3; second-operator's marker step and its Postgres restart drill T9), but only a person runs them; the trial (d) is where they run on Postgres 17. The release and vulnerability drills have no store step — they never touch the metadata backend, and the trial's record says so | WEAK as drills — the trial (d) runs them on the Postgres 17 install. Follow-up 4 |

## 3. The WEAK and MISSING rows, and what closes them

| Gap | Smallest closer | Cost |
| --- | --- | --- |
| faults.md staleness (its evidence names moved: `crash_pg.rs`, the tmpfs step's home, and one ci.yml line number) | faults.md's rows now name `crash_pg.rs` and the `test-postgres` tmpfs step; the one stale ci.yml line reference in the 53100 row is fixed in the same sweep as this list | Doc-only; minutes; no new CI |
| ~~The soak-fault Postgres arm never runs~~ | Closed by #585: the dispatch-only `soak-fault-postgres` job sets the three variables and pins the image; the script fails a run whose kills did not restart Postgres | Done; ~10 minutes per dispatch run |
| ~~The racing-puts power row skips on Postgres~~ | Closed by the park seam's move to files under the data directory (`store.rs`'s `WritePark::from_env`): the row runs in the `power-loss` job's Postgres-backed flow, and faults.md's row says so | Done; the row is EVIDENCE above |
| The drills run only when a person runs them | The trial (below) runs the store drills against the Postgres 17 RC install and records the outputs in the trial file; the release and vulnerability drills name no backend, and the trial's record says so | Part of (d); no CI change |
| The RC trial on Postgres 17 (d) | Cut the RC, name `docs/release-notes/production-trial-<version>-pg17.md` before day 1 per the template, run 30 days with the drill walk-throughs | 30 days wall clock plus one host; not CI-automatable; starts when v0.6's RC is cut |

## 4. The follow-ups, cheapest first

1. ~~**faults.md row repair**~~ (done): the SIGKILL rows name
   `crash_pg.rs`, and the 53100 row names the `test-postgres` tmpfs
   step. The one stale anchor left in faults.md, its ci.yml line
   number for the tmpfs step, is fixed in the same sweep as this
   list.
2. ~~**Arm the Postgres soak-fault arm**~~ (closed by #585): the
   dispatch-only `soak-fault-postgres` job sets the three variables;
   the summary row and the restarts check prove the fault fired.
3. ~~**The racing-puts power row on Postgres**~~ (closed): the park
   seam is a file seam under the data directory now, so the row runs
   in the power job's Postgres-backed flow; see section 2's EVIDENCE
   row.
4. **The RC trial (d)**: schedule with the v0.6 RC; run the drills on
   Postgres 17 as part of it; the trial file lands in
   `docs/release-notes/`.

The lock-restart runbook row (§2 of the production audit, closed by
[#609](https://github.com/bnsd55/openscion/pull/609)) and the #575
restore finding stay where they are; neither is a Postgres 17 qualification
gap beyond what the trial's runbook walk already covers.
