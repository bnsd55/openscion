# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project will follow [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

New entries go at the top of the `[v0.1.0] - unreleased` section below,
until that section gets its release date.

## [v0.1.0] - unreleased

The first release: the server, its cache tiers, and the `scn` client.

### Added

- [docs/production-audit.md](docs/production-audit.md), the blocking
  audit for v1.0: nine sections of findings, each with one status —
  fixed (with the PR), bounded (with the measured ceiling and where
  operations.md states it), outside the production profile (with the
  support.md line), or open. An open finding is a v1.0 blocker (#315).
- # The docs' named tests are checked
  
  Every `path.rs::fn_name` the docs and README name is now machine-
  checked (docs_links.rs): the file is in the repo, it holds the fn,
  and the fn is a test - `#[test]`, `#[tokio::test]` or one a
  `proptest!` generates - not a helper. A failure names the doc, its
  line and the missing name. Eight references in docs/threat-model.md
  named helpers, not tests; they now name the covering test or keep the
  helper in prose, so no claim reads as proven when its test is gone.
- # The admin lines cover every state-changing command
  
  The last admin actions join the audit. The `derive` subcommand records
  one `admin`/`intent` line before its backfill and one `admin`/`ok` after
  it, with the kind and the rows the run wrote and removed; a failed run
  records `admin`/`failed` with the class. Making the counts real,
  `backfill` returns a `DeriveReport` (the kind's name, the rows it wrote,
  the rows of older versions it removed) instead of a bare name.
  
  An open that migrates a store's schema records one
  `admin`/`ok` line `{what: migrate, from, to}` as soon as the audit can
  open, before the command's own pair: the migration ran inside the
  store's open, before the audit could exist, so no intent can precede
  it. A fresh open creates the schema and reports no migration.
  
  The `ticket-key` subcommand records one intent before the key exists
  and one ok after it is on the disk. The lines carry the what and
  nothing else: never the seed, the key bytes, or the key file's path,
  in any encoding the test searches for.
  
  The golden's `admin`/`intent` and `admin`/`ok` sections grow the new
  fields; no new (event, outcome) kind appears. With this slice every
  state-changing CLI command writes its admin lines, and #385's admin
  item is complete: gc, import, restore, derive, the schema migration,
  and ticket-key generation all record what they changed.
- A backup and restore runbook for a server that keeps serving:
  the metadata snapshot first (`pg_dump`), the
  blob files second, the store's UUID marker with the metadata; what the
  backup promises, why gc must stay out of it, and a test that restores
  a backup taken while writes land.
- The crash suite's Postgres rows. The real `scion-server` binary
  serves a Postgres database, and the test kills it with SIGKILL - the
  crash a cut without a FIN or RST leaves - at steps of a measured whole
  run.
  
  Two rows:
  
  - A kill while a wide land runs leaves the ref at the head it had or
    at a changeset the move put there - one whose first parent is that
    head, and whole enough to read back. One restarted server answers
    the replay of the same request key, and the ref must end exactly
    where the reply says it did. The kills run until they have seen
    both endings: a land the kill cut before the move, and one it cut
    after.
  - A kill while the schema upgrades leaves the database at one whole
    version, and the next start finishes the upgrade and serves a
    repo the client resolves.
  
  Their clients carry their own 5 s connect timeout and 30 s per-call
  deadline until a shared builder lands. Each try runs well under
  300 s.
- # Wave 2: the durability point's backup proofs run the guide's commands
  
  The shared helper gains a block selector: `runbook_block_with` and
  `run_guide_block_with` take a needle and pick the first block under the
  heading that contains it, so a section with several blocks (a backup's
  snapshot, its object copy) hands a test the step it performs.
  
  The backup and restore-bench proofs now take their snapshot and copy
  the blob files with the runbook's own lines - `pg_dump` and
  `cp -R ... objects` - with the test's directories in place of the
  documented ones; restore-bench runs its full measurement through
  them. The four physical-failure proofs (power cut, dm-log cut,
  kill timing, injected fsync failure) carry `(behaviour, no command)`
  markers: their subject is the failure's own timing, which no shell
  command reproduces. Six rows leave NOT_YET_DRIVING; six remain.
- One land on Postgres runs its work as serial client-to-database
  statements inside the per-ref critical section, and every land queued
  behind the one that holds the ref waits for all of them. The analysis
  lives in the land performance issue.
  
  The store layer counts the statements a process runs against Postgres,
  behind the test hooks, and a Postgres-only guard measures one land
  through the server's Land RPC - the land already on the head, and one
  that must rebase - and fails the run when either is above its count.
  
  A transaction now reads each changeset's generation once: a
  generation is fixed at insert and never updated, so the derived walks
  and the land that ask for the same few ids again get them from the
  transaction's own cache, and a merge reads every parent's generation
  in one statement instead of one per parent. The guard's limits fell
  from 51 to 48 and from 56 to 55 with this change.
  
  The multi-row writes went the same way: the trees a change builds land
  as one statement per table instead of one per row, and so do the blobs
  a changeset puts and the rows of its parents - the work that scales
  with the change's width. A third guard case pins that: a land of 50
  files ran 106 statements before the batch and 56 after.
  
  A repeated parent now stores the way the per-row loop did (the batched
  read had diverged), and a test holds the batch to the rows the loop
  writes.
  
  The land's outcome check rides the plan's read transaction: one read
  answers both whether a twin call settled the land and what the land
  must do now, where the loop read them in two steps before. The counts
  do not move (the loop's read ran without transaction boilerplate
  already); what moves is that the plan can no longer run for a land
  that a twin call has settled. The read runs in autocommit statements,
  so two reads can see different states - a settle that lands between
  them is stopped at `set_land_state`'s pending guard ("no longer
  pending"), which is the backstop.
- The fault matrix's disk, power and mount rows run on Postgres: the
  `mount (NFS)`, `power-loss` and `test (postgres)` jobs each run their
  suite, and `docs/faults.md` says which half of each fault a run proves
  (the blob files in the data directory; the metadata in Postgres's own
  WAL). The tier row is marked N/A: the tier keeps its own object cache
  and takes no metadata backend.
  
  The busy row blocks through an open transaction of a second
  connection, and answers busy inside the same four-to-twenty-second
  window.
  
  On Postgres, a database that answers SQLSTATE `53100` (out of space)
  maps to `Storage::disk_full` and the fixed `ResourceExhausted` reply, a
  unit test of the mapping in `pg.rs` pins the SQLSTATE, the class and the
  reply text; the stale comments that pointed at error.rs say the same.
- The three contract formats now carry the goldens that enforce their
  additive rules: the store's byte pins and unknown-tag refusal
  (crates/scion-core/tests/golden.rs), the protocol's wire golden and
  unknown-field-ignore test (crates/scion-proto/tests/wire_golden.rs
  with protocol_wire_v1.golden), and the archive's byte golden and
  unknown-version refusal (crates/scion-server/tests/archive_attacks.rs
  against archive_v1.golden). The pairing is enforced on the pull
  request's own diff: scripts/check-golden-pairing.sh (the CI lint job).
  A pinned-byte change (the wire golden, the store pins, the archive
  golden) fails without its format document's version line in the same
  pull request. The proto file and the surface golden are additive: they
  need no new version, only the same pull request changing
  docs/protocol-v1.md (any line - the contract must describe the new
  surface), with its own message.
- The Postgres recovery rows of the production audit close in the
  operations guide: re-pairing a lost backend marker (write the two-line
  marker back with the store UUID the refusal names - and never for a
  directory the database was never served with), and the one-server rule
  after an unclean kill (the first start refuses until Postgres ends the
  dead session's advisory lock; the service manager's restart is the
  whole recovery on the same host, and pg_terminate_backend or
  tcp_keepalives_idle for a lost one)
  (`crates/scion-server/tests/backend_refusals.rs::a_lost_marker_written_back_as_the_runbook_says_reopens_the_store`).
- # The second operator's drill, in the record's shape
  
  docs/drills/second-operator.md is the drill a human second operator
  runs (#396): ten tasks from install to the alert runbooks, each with
  its runbook section, the runbook's own commands verbatim, its
  done-when, and a blank for the operator's notes; the header holds the
  record of who ran it, when, and against which commit. A command that
  the runbook does not print does not appear: the two steps the runbook
  states in prose (creating the data directory, the gc first run's
  `--dry-run`) say so and hand the choice to the operator's notes. The
  database-container step is marked Linux. The guesses log stays: a
  filled row is a gap in the runbook, not a failure.
- The 72-hour soak of issue #239's run-length decision is ready for the
  day a VM is rented for it, and runs nothing until then. A
  dispatch-only `soak-72h` workflow waits for one self-hosted runner
  with the `scion-soak` label (a label no other job names), gives it
  4400 minutes, and runs one uninterrupted 72-hour soak (RUN_MINUTES
  4320, the same ceilings and warm-up window as the nightly). It is
  never a pull_request, push or scheduled run, and the fault job stays a
  nightly of its own.
  
  The job's `GITHUB_TOKEN` is dead after 24 hours, so the long job calls
  nothing with it after its checkout: the samples stream to files on the
  runner's disk while it runs (`metrics.csv`, `metrics-store.csv`, the
  logs), and a separate short job - `needs: soak-72h`, `if: always()`, a
  fresh token - uploads them. With one `scion-soak` runner, the upload
  job lands on the same machine and reads the same disk.
  
  `scripts/soak-runner-setup.md` registers the runner on a fresh Ubuntu
  VM as a normal runner (NOT `--ephemeral`: the workflow's two jobs both
  need it, and an ephemeral runner de-registers after the first, leaving
  the upload waiting forever), sizes the disk at about 300 GB for the
  store (one 30-minute release run: 334 MB -> 8.95 GB beside 22,930
  lands, slowing as it goes) plus system and margin, and removes the
  runner (`config.sh remove`) and the VM after. Because the repository
  is public, the doc also names the fork-PR risk - a fork workflow can
  target the runner's label while it is up - and the owner's two doors:
  the fork-PR approval policy is tightened to all external contributors
  before the runner is registered and put back to
  `first_time_contributors` after, and the runner exists only for the
  rental.
- The supply-chain job now runs cargo-deny in every fuzz
    sub-workspace, each of which holds its own Cargo.lock the root check
    never read — the gap that let RUSTSEC-2026-0382 sit in three fuzz
    lockfiles while the root stayed clean. Each fuzz workspace carries
    its own `deny.toml`: the same advisories floor, the license list the
    harness alone needs, and looser bans where a fuzz workspace differs
    from the product by design. A new script,
    `scripts/check-deny-coverage.sh`, fails the job when a Cargo.lock
    sits where no deny step reads it, so a new sub-workspace cannot slip
    past either.
- The land model check becomes a randomized script: a proptest strategy
  generates a sequence of operations against the real `scion-server` —
  lands from four callers on two refs, retries with the same request key,
  twins of one key in flight together, calls dropped in flight, a SIGKILL
  (plain, or at a pending land), the collector beside the live server,
  and bursts of up to eight lands on one ref at once — and a plain
  in-memory model checks the result: each ref's first-parent history is
  one chain, every acknowledged land is in it exactly once, no land is
  applied twice, no key's state regresses, a kill kept every land that
  had answered, and `fsck` sees no damage. On a failure, proptest shrinks
  the script to the smallest one that still fails. A checked-in seed
  corpus of seven scripts runs first and reads like sentences; CI runs
  one random case per pull request, and the nightly `land-model` job
  raises it to eight.
  
  A conflict reply is judged against the head the server saw when it
  decided, which each call records beside its reply (a batch's reply
  records the head after the batch — an upper bound, so a pass there is
  weaker, never a false accusation) — a bogus conflict whose path a
  later land moved cannot slip through. A CONFLICT with no paths is
  allowed when the detail says why, as the protocol provides.
- The scripts own what they start: common.sh's `start` runs every
    server in a process group of its own, records the pid, the group and
    the command line in the run's work directory, and the exit traps stop
    the group — TERM, then KILL — so a script that dies by error,
    interrupt or timeout leaves no server behind it. `stop` kills only
    after the job is still the one the pidfile recorded. The background
    client loops in soak.sh, soak-fault.sh and capacity.sh are recorded
    the same way and stopped through it; version-matrix.sh's own
    start/stop gains the group and the same check. The group, not the
    program name, is what the stop checks: a job started through a
    wrapper that execed another program is still ours, and a pid the
    system gave away since is skipped with a line saying so. A CI test
    starts a stand-in server, dies by error, proves the process is gone,
    and proves a wrapped job is stopped too (`scripts/test-owned-run.sh`).
- # gc writes the audit's first admin lines
  
  The collector subcommand records one `admin`/`intent` line before its
  run and one result after it: `admin`/`ok` with the report's counts, or
  `admin`/`failed` with the storage class of the error. `--audit-file`
  puts the CLI's lines in the same file the server appends to, so one
  audit log holds the whole history of a data directory. The golden pins
  the new `admin`/`intent` kind and the `admin`/`ok` line's fields; a
  source run that fails after its intent is declared undriven for now,
  with its reason in the golden's census.
- # import and restore write the audit's next admin lines
  
  The import subcommand records one `admin`/`intent` line before its run
  and one `admin`/`ok` after it, with the import's counts: the commits it
  saw, the ones it stored, the ones it verified, and whether it moved the
  target ref. The restore subcommand records the same pair around its
  replay, in the restored store's own `audit.log`, with the counts the
  restore reports: repos, refs, and pending lands. Both take
  `--audit-file`, with the same default as `gc`. The golden's
  `admin`/`intent` and `admin`/`ok` sections grow the fields of the new
  lines. The fix that made this possible: `verify` and `restore` sat
  behind an `unreachable!` in the store-open match since the archive
  landed, so both subcommands panicked instead of running; they now run
  before the store opens.
- The server records an audit log: an append-only JSON-lines file of
  authentication, writes, ref changes, lands and administrative
  changes, with no tokens and no contents.
  
  The file sits in the data directory (mode 0600, `--audit-file`
  relocates it, no disable flag) and one writer thread owns it: callers
  hand lines to a bounded channel, the writer drains the queue, writes
  and fsyncs once per drain, and wakes each caller when its own lines
  are durable. A state change records two lines: an `intent` line
  fsynced before the change, and a `result` line after it, joined by
  request ID; a read records one. Every intent gets its result: a call
  that dies before its job starts is answered `cancelled` by the call's
  drop, and a land the stale-land sweep rejects is answered `stale` by
  the sweep. One case can leave a result missing: a job that dies after
  it claimed the call but before it recorded anything — the server log
  carries what the audit line cannot. If the audit append fails, the
  change is refused and readiness reports not-ready. Refused
  authentication is aggregated per peer per interval into one counted
  line, capped.
  
  The write, land and ref events carry what an operator needs and no
  contents: the RPC, the repo, the ref, the byte count of an upload,
  the land's state and ID, and a ref move's old and new changeset IDs.
  A changeset ID names content the caller already had; a blob or tree
  digest would name contents, so none appears.
  
  On open, a file whose last line has no newline gets one, so a line
  that a full drive cut in the middle stays a fragment of its own and
  cannot swallow the lines that follow.
  
  A land's ref line names the head the move replaced, read where the
  move happens, under the ref's lock, and is written only by the call
  that made the move. A replayed land (the same request key after it
  landed) moved nothing, so its land line names no old and no ref line
  appears for it; an auditor finds the head the move replaced in the
  land line of the call that made the move, under the same land_id.
  The ref lines of a ref carry the ref's true history: each old is the
  head its move's CAS replaced, so linking old to new rebuilds the
  history. The lines of concurrent changes can appear in a different
  order than the changes themselves (each job records its result after
  its change), so the history is linked, not read in line order.
  
  Blob and tree digests are never logged (a digest is a dictionary key
  to contents), and neither are token bytes or token hashes. Rotation
  is by size, keeps a bounded number of old files, and names what it
  dropped in an admin line.
  
  Not covered yet: the ref moves that happen outside the server.
  Import moves a ref to each imported tip (import.rs), and archive
  restore writes refs back (restore_ref); they run as their own
  processes and cannot hand lines to the server's writer, so they must
  append their own lines to the same audit file, intent before the
  move. Until they do, a reader of the log must not take it as
  complete: a ref can move without a line.
- # The metric surface's golden
  
  The `/metrics` registry joins the machine-checked surfaces: a
  checked-in golden (`crates/scion-proto/tests/metrics_v1.golden`) pins
  every `scion_*` name the registry renders, its `# TYPE`, and its
  labels and label values - names, types and labels are the surface,
  the sample values are this run's counts and pin nothing. The test
  exercises the registry end to end first (one request per RPC path,
  one cancelled, one failed, every gauge and histogram), because
  histograms render only once they counted. The stalled gauge hides
  until a reply stalls, so its name, type and label set pin in a unit
  test beside the render. A diff in the golden is a surface change: a
  removed name or a changed type is a break, never a re-pin; re-pin an
  added name on purpose with `SCION_UPDATE_METRICS_GOLDEN=1`.
- # The archive's golden
  
  The archive format joins the machine-checked surfaces: a checked-in
  golden (`tests/archive_v1.golden`) pins every file of the export of
  one fixed store by hash and path - the manifest's clock reading and
  store UUID pin nothing - and pins the changeset ID the restored
  store's `main` resolves to. The same test runs `verify` on the golden
  with no store code and a restore into an empty directory, `fsck`
  clean. A diff in the golden is a format change: a new archive version
  beside v1 per docs/archive-format.md, named in a changes/ fragment.
  Re-pin on purpose with `SCION_UPDATE_ARCHIVE_GOLDEN=1`.
- # The audit log's line surface, pinned by a golden
  
  The audit's lines join the machine-checked surfaces: a checked-in
  golden (`crates/scion-server/tests/audit_v1.golden`) pins every
  (event, outcome) kind a served server writes - the exercise drives
  refused auth, the start line, a put, a create, the failed create, a
  land and a land that runs past its deadline - and for each kind, the
  field names and JSON types of its lines. The values (`ts`, the ids,
  the counts) are this run's and pin nothing. A source census pins the
  kind set the code writes, and the exercise must equal it exactly.
  Re-pin on purpose with `SCION_UPDATE_AUDIT_GOLDEN=1` and a changes
  fragment naming the added kind.
- # Wave 1: the first boot and one-server proofs run the guide's command
  
  The harness gains `spawn_on`/`start_on`: the serve argv comes from the
  first fenced `sh` block under the named heading of docs/operations.md,
  with the test's data directory, token file, and port substituted for
  the documented ones. The first-boot test starts its server with the
  First boot block's own line and asserts the One server block's command
  is the same command; the kill test starts on the One server block. The
  backend_refusals proofs move to `(behaviour, no command)` markers:
  their subject is the database's own lock and container, not a scion
  command. Five rows leave NOT_YET_DRIVING; twelve remain.
- # The two maintainer drills of #393
  
  docs/drills/vulnerability-drill.md walks a mock private vulnerability
  report end to end, from GitHub's private vulnerability reporting
  through the private fork, the CVE decision, the published advisory,
  and the credit, using only SECURITY.md and the Security tab - and
  lists what SECURITY.md does not say for this (where a fix is
  developed, when a CVE is requested, how a security fix ships, the
  publication timeline, triage ownership), without filling the gaps.
  
  docs/drills/release-drill.md runs RELEASING.md as a second maintainer,
  starting with the dry run, and marks every step that today needs the
  owner: the signed tag (their personal SSH key), the refs/tags/v*
  ruleset, and the immutable-releases setting - the v0.8 list. Both files
  carry the record header, the done-whens, and the notes blanks, and
  keep to what the runbooks already say.
- The nightly is CI itself: `ci.yml` gains a schedule trigger (03:00 UTC,
  after the soak's 02:30), so the scheduled full suite is exactly the CI
  suite - the same jobs, the same cases, nothing to drift. The
  concurrency group carries the event name, so a 03:00 schedule waits
  its turn instead of cancelling a push to main that started minutes
  earlier.
  
  `scripts/nightly-streak.sh` prints the current streak: the scheduled
  runs of `ci.yml` on main, counted from the newest completed one
  backwards until the first non-success, which is the v0.8.0 roadmap
  line's "30 consecutive runs" as a fact anyone re-checks with `gh`. A
  small `nightly streak` job prints it on every run; it reads the run
  history only (`actions: read`).
- The production-trial record exists as a form:
  [docs/production-trial.md](docs/production-trial.md) (#395, the v0.9.0
  gate). Its fields hold what the roadmap names: the commit and the
  binaries with their checksums, the backend and its version, the
  filesystem, the workload with its floors fixed in writing before day
  1, a daily table of lands, reads, latencies, memory, disk and errors,
  and sections for upgrades, restores, key and certificate rotations and
  incidents — each with a time, what happened, and the evidence link —
  ending in the pass-or-fail rule against the floors. Every floor value
  is left blank for the owner to fix, and every field says what a second
  operator fills in.
- The operator's alert rules are versioned with the code they watch
  (deploy/prometheus/openscion-alerts.yml)
  
  Nineteen alerts over the metrics the server, the cache and the gc
  collector export today - the server is down, RPC error rate, internal
  errors, land errors and replans, latency p99, disk free (two levels),
  blob sync saturation, the view memo bypassed, stalled replies, the
  Postgres pool and transaction age and collector key, the tier's miss
  rate, the gc run going stale, and Objects refusals. Severities are
  docs/support.md's levels, and each alert's annotation says the
  operator's first move, linked to the operations.md section that has
  it. The Postgres group is true only for the Postgres shape.
  
  ci.yml gains a promtool job: the pinned Prometheus release tarball
  (v3.13.4, the current LTS line, its sha256 in the job) checks the
  rules and runs openscion-alerts-test.yml, where every alert fires on
  a crafted series and does not fire just below its threshold. The
  daemon exports no metrics; an alert on it needs the gap closed first.
- # The guide's own commands become an executable rule
  
  A Proof of a runbook used to mean a reviewer reading the test and the
  guide side by side. Now the check runs itself:
  
  - The block-extraction the alert-runbook tests carried moves into the
    shared test support (`test_support::docs`): `runbook_block` reads the
    first fenced `sh` block under a heading of `docs/operations.md`, with
    named substitutions (an address, a data directory, a token file) in
    place of the documented literals, and `run_guide_block` runs it
    through `sh`. The alert tests keep their blocking-pool wrapper and
    call the shared helper.
  - `docs_ops` gains `every_proof_runs_the_guides_command_or_says_why`:
    for every `Proof:` line, the named test must call the shared helper
    with its section's heading, or the line must say `(drill)` or
    `(behaviour, no command)` with a reason, or the proof must sit in
    `NOT_YET_DRIVING` with every other converting proof. The list must
    match the guide exactly, so no proof is exempt silently and no
    finished conversion keeps its row.
  - The certificate-rotation and ticket-key Proof lines now carry the
    `(behaviour, no command)` marker with their reasons, as do the
    disk-full, failing-disk, and Postgres-restart runbooks, whose steps
    are actions on the machine or the database rather than one command.
  
  ## Proofs that do not yet run their block
  
  `NOT_YET_DRIVING`, in `docs_ops.rs`, names the tests that still run the
  guide's commands their own way; each row is work to delete. Today:
  
  - First boot, One server: the harness spawns the guide's serve command
    itself; conversion is a refactor of the shared spawn.
  - The durability point: power, fsync, and backup tests drive the
    storage through the test seams, not through the guide's shell.
  - A failed migration and 5. Routines: the refusal and gc tests run the
    commands, but not yet through the helper.
  - Stalled replies high: the log-stream test reproduces the stall, not
    the guide's curl.
- # Wave 3: the collector's proof runs the routine's own line
  
  The block selector now finds a heading at either level, so `5. Routines`
  - a numbered part, not a subsection - hands its collector block over.
  The gc-metrics proof builds its collector command from the routine's own
  line, with the test's data directory and metrics file substituted, and
  its dry run is the documented `--dry-run` flag.
  
  The four proofs whose subject is behaviour no command reproduces - the
  refusal and restore of a failed migration, the kill inside a migration,
  the fsck-restore routine, and the stalled-reply close on a paused clock
  - carry `(behaviour, no command)` markers with their reasons.
  
  NOT_YET_DRIVING is empty: every Proof in the guide runs its section's
  command block through the shared helper, or says on its line why it
  cannot. A new proof must do one of the two, and a stale row or a silent
  exemption fails the check.
- The disk-full runbook (docs/operations.md, "Disk full"): the two alerts'
  first move is the gc routine - a dry run, then a real run - then fsck,
  then grow the volume if gc collected too little; a write that failed
  out-of-space leaves nothing to repair, and writes resume with no restart.
  The Proof test runs those steps in order on a real full disk (tmpfs):
  fill, refusal, the gc dry run and real run through the actual command,
  fsck clean, then a write that lands again with no restart and a clean
  reopen.
- The failed-migration runbook (docs/operations.md, "A failed migration"):
  the two failures an operator can see, each with the rule that names it.
  A newer schema refuses the open with both versions named, and the fix is
  the old binary or the restore of section 3 - never a hand-edited
  user_version. A start that waits on other processes says which; stop
  them. The Proof test forges the newer-schema case: the server refuses
  the open with the error, the restore of the backup per section 3 brings
  the store back, and the server serves. The fsck-failure runbook is its
  own subsection of the routines, pointing at the failing-disk runbook's
  Proof test, which runs the restore of section 3 end to end.
- The failing-disk runbook (docs/operations.md, "A failing disk"): a read
  the disk refuses answers internal and the rest keeps serving; fsck runs
  beside the live server and its exit 1 names the damage; the store has no
  repair, so the fix is the restore of section 3, and fsck runs on the
  restored copy before the server starts. The Proof test runs those steps:
  it takes a backup per section 3's order, fails a read with EIO, corrupts
  a blob file behind the store's back, runs the fsck command to its exit 1
  with the blob named, restores into a fresh directory, and reads back
  through a fresh open.
- The first-boot runbook (docs/operations.md, "First boot"): a token file
  and the three commands of a first start, each with its promise - the
  token goes in a 0600 file (umask 077, then printf), not in argv, the
  binary creates the data directory and serves, the health service
  answers for the service name and for liveness, and the first import
  makes a repo the client reads. The Proof test runs the commands in
  order against a fresh data directory - the harness's server now takes
  its token from the file too - and reads the imported ref back.
  
  The certificate-rotation runbook (docs/operations.md, "Rotating the
  server's certificate"): a new certificate of the same CA needs no client
  change - the rotation is write the pair, restart, and the client's
  unmoved CA file still connects. The Proof test rotates the server's
  certificate under a connected client and reads back over https. A new
  CA is the drill record of docs/drills/, since no test can hold a
  running fleet's client CA files.
- The Postgres-restart runbook (docs/operations.md, "A Postgres restart"):
  a database that restarts under the server needs no server restart - the
  calls answer the busy error a client retries against, the store does not
  fence itself, and the next write after the database is back reconnects,
  takes the advisory lock again, and runs. The Proof test is the existing
  takeover test, which stops the job's database, sees the busy answers and
  the fence stay down, brings the database back, and recovers the store
  with no process restart.
- A packet for a hired reviewer: [docs/security-review.md](docs/
    security-review.md) states the review's scope and its out-of-scope
    boundaries, links the trust model instead of copying it, and lists
    the internal attack passes by surface — what each tried, what it
    found, and the pull request that closed it — so a reviewer starts
    past them. It walks a ten-minute local build-and-attack setup, links
    the fuzz target map, and names where the project itself wants the
    most eyes, strongest first. No exploit recipes: every named weakness
    is closed with a test or pinned as documented behavior.
- # The snapshot file's golden
  
  The snapshot file joins the machine-checked surfaces: a checked-in
  golden (`tests/snapshot_v1.golden`) pins the bytes today's writer
  produces for a fixed object set, and the same golden seeds back into
  an empty store. A diff in the golden is a format change: stability.md
  says a new magic beside `scnsnap1\n`, read beside v1, and a changes/
  fragment naming the break. The test header says how to re-pin on
  purpose (`SCION_UPDATE_SNAPSHOT_GOLDEN=1`). The codec and proto
  goldens get their on-purpose update paths written in their headers
  too - the codec's is "a grammar change is a new format, never a
  re-pin", the proto's is "re-pin additions only; a removal is v2".

### Changed

### Fixed

- `scn bench --agents N` runs N agents that each land, as its report
  says. It gave every agent the rounded-up share of `--lands`, so the
  lands ran out before the last agents got any: `--agents 300 --lands
  1000` ran 250 agents and reported 300, and its throughput and latency
  described a lower load. The lands now spread as evenly as they divide,
  and more agents than lands run one agent per land.
- `scion-daemon` with more than one `--seed` reads every file into a cold
  store, as the flag's help ("Repeat for more") and docs/architecture.md
  say. It checked for an empty store before each file, so the first
  file's objects made it skip the rest ("the store holds objects; the
  snapshot is not read"); it now checks once, before the first file.
- `scn mount --help` states the mount's single-user rule, as
  docs/threat-model.md says it does; before, only the start of a mount
  said it. docker/README.md names `docker/certs`, where `docker/demo.sh`
  writes the demo certificates, not `docker/demo/certs`, and
  docs/cli-vs-git.md's sparse checkout row carries the `--into` that
  `scn checkout` requires.
- A lookup of one name in a stored tree checks the executable flag of
  every entry it reads past, as the full decode does: a tree whose
  earlier entry carries a flag byte other than 0 or 1 is refused as
  corrupt instead of answering the later name.
- `scn`, `scion-cache` and `scion-daemon` treat an `HTTPS://` address (a
  scheme in any case) as https: they refuse it without `--tls-ca`, and
  with one they connect over TLS. The check was case-sensitive, so an
  uppercase scheme skipped the CA rule and the TLS configuration, and the
  connection failed with a misleading "is a scion server running" hint.
- A mount session record could name a pid that is no server: the stop
  signaled pid 0, which takes the caller's whole process group, or pid
  1, which is init. A record whose pid is below 2 is never signaled
  now - the unmount keeps it and says so - and every kill the mount
  runs passes `--` before the pid, so procps-ng's kill cannot read a
  pid that follows the signal option as another option and fold it to
  its first digit (`kill -9 -123456` ran kill(-1, SIGKILL), every
  process of the user).
- `scn mount` counts a file's bytes once in its in-memory cache when
  two reads fetch the file at the same time, as NFS clients do on a
  file's first read. Each racing read added the bytes again, so the
  count drifted above what the cache held, the cache evicted too early,
  and a drift past the cache's 128 MiB bound could leave its eviction
  loop spinning on an empty cache with every read of the mount waiting.
- `scn unmount DIR` finds a mount whatever way DIR is written: with a
  `..` (`scn unmount ../view` from another directory) or through a
  symlinked directory above it. The mount table compared paths as typed,
  so such an unmount answered "no mount of scn is recorded", a second
  `scn mount` of the same directory was not refused, and `scn commit
  --from-mount` did not find the mount.
- `scn mount` records a directory or overlay path that is not UTF-8
  with its exact bytes, in a `_b64` sibling as scn's JSON output does,
  so `scn unmount` and `scn commit --from-mount` find the mount. The
  session file kept such a path with its bad bytes replaced, and no
  later command matched it. A UTF-8 path is written as before, and an
  older session file reads as before.
- ms: "the Postgres section says what a start after an unclean kill sees"
  ---
  
  A first start after an unclean kill can exit with "another server
  holds this database": Postgres keeps the dead server's session, and
  with it the one-server advisory lock, until the session is ended. The
  Postgres section now says the recovery and both clocks that decide
  how long the lock stands: on the same host the kernel closes the dead
  process's sockets at once, so a service manager's restart
  (`Restart=on-failure`) is the whole recovery; a lost host is seen
  only through TCP keepalives, whose clock is the database's
  `tcp_keepalives_idle` (unset, the system default, often 2 hours), so
  the section names `pg_terminate_backend` and that clock for an
  operator who cannot wait it out.
- `scn commit --put PATH=FILE` takes a local FILE whose name is not UTF-8,
  as `checkout --into` and `mount` take a local directory: the name's
  bytes name the file. Before, clap refused the whole argument ("invalid
  UTF-8 was detected") before the command ran. PATH, the repo path, is
  still UTF-8, and a refusal now says so.
- `scion-daemon`'s "seeded the store from a snapshot" line counts only the
  objects the store still holds once the seed ends. An object that a later
  object of the same snapshot evicted to stay under `--max-bytes` was
  counted as stored, so a snapshot larger than the store reported that it
  all fit; it now counts as skipped.
- Every promise in stability.md and compatibility.md now names what
  enforces it, and the naming is checked: the ticket prefix's literal
  bytes (`scion\0ticket\0v1\0`) are pinned by a unit test (the version
  byte was, the prefix string was not - a rewrite would have passed
  every test while every held ticket stopped verifying), and the
  schema rows name the migration tests. The drills that named the
  N-1 surfaces (the tagged-store ladder and the N-1 restore drill)
  stand down for v0.1.0 and return with the first release tag, when
  there is a release pair to prove.
- The version matrix always has an N-1, from the first nightly: `PREV_REF`
  beats the release tags, the newest tag that is an ancestor of HEAD and
  not HEAD itself wins next, and with neither, the stand-in commit - the
  one pinned commit on main chosen where the store schema changed -
  stands in until the first release tag replaces it. A run that tests no
  pair fails with the reason; the matrix never
  passes as a skip. Two windows of the N and N-1 pair are proved, both
  directions: the ticket one version's server signs verifies on the other
  version's tier, and a ticket of another key is refused with the ticket
  reason; the snapshot one version's cache writes opens as the other
  version's daemon seed, and a checkout through that daemon reads the
  repo whole. The tier pair's read is a checkout, not a `cat`: `cat`
  reads at the server and never reaches the tier, so only a checkout
  exercises the GetObjects the ticket gates.
- # Archive: a section's reads hold one snapshot
  
  An export's metadata sections claim one read snapshot, and each
  section reads inside one explicit read transaction
  (`Backend::read_snapshot`): REPEATABLE READ for its statements, so a
  land that commits between a section's changesets query and its refs
  query leaves the archive holding exactly the ref the snapshot named.
  The cost is the one the section's snapshot always names: the log
  cannot restart while the section reads.
- `scion-cache` names its own flag, `--ticket-public-key`, when it refuses
  a key at start (more than two keys, a key that is not 64 hex
  characters, or bytes that are not a public key). The refusals named
  `--ticket-key`, which is the server's flag and which the cache does
  not take.
- `scion-cache` refused three kinds of bad ticket key at start with
  messages that named `--ticket-key`, the flag of `scion-server serve`.
  The cache takes `--ticket-public-key`, and its refusals now say so
  ("give at most two --ticket-public-key", "must be 64 hex characters",
  "bad --ticket-public-key: ..."), so the error points at the flag the
  operator can actually fix.
- `scn diff --json` no longer prints `"size": 0` for the old and new side
  of every entry. A diff's file states name a blob, not a length, so the
  reply carries no size and the zero was never true; the field is gone,
  as it went from `scn show --json` in #194. A script that read it gets
  no field instead of a wrong zero (docs/stability.md: a 0.x minor may
  remove a `--json` field).
- The status-code table of docs/protocol-v1.md no longer says that only
  `GetObjects` answers `RESOURCE_EXHAUSTED`: the row now names every kind
  of server limit that answers it (a per-principal quota, a memory
  budget, the unary-reply budget, a reply's size cap, a full disk), as
  the rest of the document already did. The README's partial-reader
  section says that `GetObjects` without a ticket answers
  `PERMISSION_DENIED`, as for every caller, not `UNIMPLEMENTED`.
- A new `--out` directory of `scion-server export` was made 0777 less
  the umask (0755 under Ubuntu's 002, 0755 again under 022), and the
  section files inside at 0644, so an archive that holds every blob,
  changeset and ref of the store was group- or world-readable wherever
  the operator pointed the flag. The archive root is now 0700 and the
  section files 0600, whatever the umask, the way the data directory is.
- `scion-server import` refuses a git commit whose time zone is shorter
  than `+hhmm` (`+1`, or a bare `+`) instead of panicking: such a zone is
  a date git cannot write, and the import now reports it like any other
  identity it cannot keep.
- `scion-server import` aborted with an index panic on a git commit
  whose author or committer time zone is shorter than `+hhmm` (`+1`, or
  a bare `+`): the zone splitter assumed two digits after the sign. gix
  accepts such a zone (the offset becomes zero), so a repo git itself
  wrote could refuse the import with a raw `end byte index 2 is out of
  bounds`. The import now refuses a malformed zone with the zone named,
  and imports the rest as before.
- On Postgres, a graph walk over a merge-heavy history could answer a
  false `commit graph: no node for X` on a healthy store, and a repeated
  subtree read could answer as missing: two of the backend's batch reads
  emptied their reply map as they answered it, so when one batch named
  the same ID twice (two merges sharing a side parent, or one tree under
  two paths), the second ask found nothing. Both reads now answer one
  entry per asked ID, in the order asked, as the metadata contract says.
  No rows were lost; the reads lost them.
- Postgres numbered a changeset's parents from one (WITH ORDINALITY
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
- `scn` answered three broken or absurd inputs with a panic, and now
  answers each with an error that says what to fix:
  
  - `scn commit` and `scn bench` built the changeset's author identity
    and the changeset itself with an unchecked assert whose only failure
    is a field longer than the encoder can write (over 4 GiB); the
    results are now errors that name the field.
  - `scn mount` registered its stop-signal handlers with an unchecked
    result; a system that refuses them now hears why at startup.
  - The mount's secret came from `/dev/urandom` with an unchecked open,
    so a system without the device panicked mid-mount; the mount now
    reports the error with its cause.
- The nightly soak failed its own RSS bound on 2026-10-19 (run
  39219458903, the schedule event on 2bc9099): 34892 KB -> 54140 KB, over
  the 150 percent ceiling. At that commit the schedule branch of the
  workflow still fell back to the 10-minute default, so the nightly ran
  its planned 10 minutes and failed the check at the end, against a
  baseline taken after 60 seconds; the 120-minute schedule came later on
  main. There was no early abort: the only RSS check in the script runs
  after the sampling loop, and the fixed script's only RSS check needs
  the warm baseline, which exists only after SOAK_WARM_SECONDS. A 35-minute local run of the same load saw RSS climb for about
  30 minutes (32288 -> 59184 KB) and then hold flat for the last 16
  minutes (the last 20 samples: max 59184, average 58137, no trend), and
  the failed CI curve itself rose, stepped once at about 4 minutes, and
  then oscillated 49-57 KB for its last 5 minutes with no climb. That is
  warm-up - allocator arenas, the database connections' client-side
  buffers, prepared statements - not a leak: a leak does not plateau on
  two runs.
  
  The bound's baseline is now the first sample after SOAK_WARM_SECONDS
  (1800 s, named in soak.yml): the nightly still checks 90 minutes of RSS
  after the warm-up. A run shorter than the window checks no RSS bound -
  it cannot tell warm-up from a leak (a 10-minute run, as the nightly of
  that commit was, sees only the climb). The other bounds are
  untouched.
  
  The samples now include the store's size on disk: every 10 s the loop
  appends epoch and `du -sk` of the server data directory to a
  metrics-store.csv, next to metrics.csv in the artifacts, so a run can
  be read for disk growth and for Ben's 72-hour soak decision, not only
  for the RSS it shares a cause with.
- The production audit's evidence now holds as tests: a tampered history
  node reads as corrupt through the server path (`get_history_node`
  answers "does not match its ID"), and fsck reports the row
  ("history-node-bad"), on both backends (`tests/audit_evidence.rs`). The
  audit's evidence cells name the lines and dispatch runs that exist, and
  the writer-idle probe's role and its untested cadence are stated in the
  runbook and the audit.
- A docs-only pull request never ran the docs tests: the test job skips
  them, and the docs job ran only rustdoc, so a broken link or a stale
  doc landed and failed main's first run. A docs-tests job now runs the
  docs_*.rs integration tests on every pull request (and the same
  events), and fails when a docs_*.rs file exists that it does not name.
- An access file that named the digest of an empty token authenticated
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
- An import refused nothing when a git commit's date carried git's
  unknown-offset zone `-0000`: the sign check folded the minus into a
  zero and the commit imported with a `+0000` date, silently rewriting
  history. Such a commit is refused now, on the author and the
  committer line alike; every real zone still imports with its offset.
- A daemon whose socket path held a symlink followed it and bound the
  socket at the symlink's target, a path the private-directory check
  never saw. `bind_unix` now refuses anything at the socket path that is
  not a socket file of this user's earlier run: a symlink, a regular
  file, or a directory is refused with the path named, a stale socket
  that nothing listens on is re-bound as before, and nothing is followed
  or overwritten
  (`scion-proto/tests/socket_symlink.rs`).
  read the directory has every blob and every row. A directory that only
  group or others can write was already refused; the refusal now demands
  mode 0700 and the error says so (`chmod 700 <dir>`), and a restore makes
  its target 0700 whatever the umask. An operator whose data directory was
  mode 0755 must run `chmod 700 <dir>` before the server starts again.
- RUSTSEC-2026-0414: `pulldown-cmark` 0.13.4 allows exponential event
  growth with `ENABLE_WIKILINKS`; fixed in 0.13.5, and all four
  lockfiles moved to it. Build-time only: the crate enters as a build
  dependency of the protobuf codegen, its input is the checked-in
  `.proto` comments, and nothing ships the parser. The deps job's
  advisory gate now passes at the root and in every fuzz workspace.
- The server's fuzz lockfile no longer pins the vulnerable gix
  (RUSTSEC-2026-0382): `cargo update -p gix` moved the gix family in
  `crates/scion-server/fuzz/Cargo.lock` to 0.90.0 (#587 moved only the
  root lock). A fuzz lockfile that no longer matches its manifest now
  fails the deps job (`cargo metadata --locked` per fuzz sub-workspace),
  where a stale pin previously re-resolved in place and failed nothing,
  and Dependabot watches each fuzz sub-workspace, so the next advisory
  in one cannot sit unseen (#572 rule: every fault row names its guard).
- gix 0.89 -> 0.90, the advisory GHSA-q54r-jf2p-rfr2 (HIGH, "untrusted .git/config disables filesystem .git-protection path checks via unfiltered protect_options()"). Only the gix crates move in the lock; no API break, no behavior change. Our gix surface reads objects and diffs blobs in memory (import.rs: gix::open, rev_parse_single, find_commit, find_blob; blame.rs: gix::diff::blob) and never checks out a worktree, so the vulnerable path is not reached; the bump is defense in depth until RustSec carries the entry for cargo-deny.

- The mount's overlay writes no longer follow a symlink planted in the
  overlay: a write, a truncate, a mode change, and the deletions note
  refuse one. A symlinked parent directory still redirects (a named
  limit: the upgrade is opening every parent through a handle chain,
  as `materialize.rs` does).

- The server refuses a data directory that group or others can write, or
  that sits under a directory another user can change: the blobs and the
  metadata are only as private as their directory (`#197`).
- The ticket-key seed must be readable by its owner alone, a public-key
  file must not be group-writable, both refuse a symlink, and a second
  `ticket-key` run refuses, so a key cannot change under the tiers
  (`#168`).
- `scion-daemon --seed` refuses a snapshot unless `--seed-full-repo` says
  every user of the node may read the repo whole (`#241`): a snapshot is
  a full-repo credential.
