# Operator drill: a stranger follows the runbooks

This file is the script, not a record. A second operator who has never
seen the project takes it, works alone, and may read only
[docs/operations.md](../operations.md) — no source, no issue, no
teammate. Every step below is taken from that document and names the
section it came from, so a reviewer can check the two against each
other. The drill's output is two things: the evidence each task names,
and the guesses log at the bottom — every place the runbook made the
operator guess, written down while it happens. A guess is a gap in the
runbook, and the gaps become its next revision.

Run each drill on a fresh machine (or VM) as a non-root user. One dated
file per run, from [TEMPLATE.md](TEMPLATE.md), records what happened;
this file never changes with a run.

## T1 — Install

Source: the whole of operations.md (its "The pieces" list names the
binaries; no section says how to obtain one). Until the first release
tag exists, a build from source is the way:

1. Get the three binaries — `scion-server`, `scion-cache`, `scion-daemon`
   — onto the machine:

   ```sh
   cargo build --release
   # then the three from target/release/
   ```

   Done when: all three run `--help` and exit 0.

2. Note what you had to assume to get them (release page? build from
   source? which toolchain?). The runbook does not say; write it in the
   guesses log.

## T2 — First boot: one server

Source: §1 "One server", §2 "Ticket keys", §6 "Health".

Every `serve` in this drill carries the metadata flags of §1's first
boot — the URL file (`--postgres-url-file`) and the token file. The
snippets below leave them out for brevity; without the URL file the
server answers "no Postgres URL".

1. `mkdir /srv/scion && chmod 0700 /srv/scion`

2. `SCION_TOKEN=<TOKEN> ./scion-server --data /srv/scion serve`

   Done when: the log says the server listens on `127.0.0.1:7878`.

3. Stop it (Ctrl-C). Mint a ticket key — without one no object is
   readable. The seed's directories must be root's or yours, and not
   group- or others-writable (§2), so make the keys directory first,
   and capture the public key beside the seed (§2's rotation step 1
   reads it back at the first rotation):

   ```sh
   install -d -m 0700 /srv/scion-keys
   ./scion-server --data /srv/scion ticket-key --out /srv/scion-keys/ticket.key \
     | tee /srv/scion-keys/ticket.pub
   chmod 600 /srv/scion-keys/ticket.key
   ```

   Done when: the command prints the public key as 64 hex characters,
   and a second run of the same command is an error.

4. Start for real:

   ```sh
   ./scion-server --data /srv/scion serve --token-file <TOKEN_FILE> \
     --ticket-key /srv/scion-keys/ticket.key
   ```

5. Probe readiness and liveness: the health service answers
   `grpc.health.v1` on the serving port; the service name for readiness
   is `scion.v1.Scion`, the empty name is liveness.

   Done when: readiness answers `SERVING` for `scion.v1.Scion`, and the
   empty name answers `SERVING` too.

6. Try to start a second server on the same data directory.

   Done when: the second refuses to start (the store lock), and the
   first is unaffected.

## T3 — Backup a running server

Source: §3 "Backup and restore".

0. Give the backup something to hold: import a git repository and put
   one changeset on it (any small repo works — the README's quick
   start does this). Without this, T4's done-when has
   nothing to read.

1. Take the metadata snapshot first, then the blobs — never the other
   order:

   ```sh
   pg_dump --no-owner --file /backups/metadata.sql <the metadata database>
   cp -R /srv/scion/objects /backups/scion-$(date +%F)/objects
   chmod 700 /backups/scion-$(date +%F)/objects
   cp /srv/scion/backend /backups/scion-$(date +%F)/backend
   ```

   Done when: every command exits 0. Do not run `gc` while the backup
   runs.

2. Check what you took: `fsck` on the copy is read-only and may run
   beside a live server.

   Done when: `fsck` on the backup directory exits 0 and prints no
   problem lines.

3. Write down the recovery point the runbook promises for this backup
   (§3 "The durability point"), in your own words, before you look at
   step 1 of T4.

## T4 — Restore

Source: §3 "Backup and restore".

1. Put the metadata, the `backend` marker, and `objects/`, into a
   fresh directory (§3's restore list names all three; the marker is
   the one T3 step 1 copied). The metadata is the dump: load it with
   `psql -f /backups/scion-<date>/metadata.sql <a new database URL>`
   into a fresh database — the dump recreates the schema it
   carries — and the restored server's Postgres URL points at that
   database. If the old server still runs, the restored one needs a
   port it does not hold (`serve --listen`).

2. `fsck` on it.

   Done when: `fsck` exits 0.

3. Start the server on the restored directory with the same token and
   ticket key as T2.

   Done when: the server listens, and a client can read what was
   written before the backup's snapshot. (§3 names what may be lost:
   the writes acknowledged after the snapshot began.)

## T5 — Disk-full recovery

Source: §5 "Routines" (gc), §6 "What to watch" (`scion_data_free_bytes`).

1. The alert the runbook gives: `scion_data_free_bytes` — free bytes on
   the filesystem that holds the data directory, refreshed every 30 s.
   Turn `--metrics-listen 127.0.0.1:9100` on and read the metric.

   Done when: you can see the current free bytes.

2. Fill the disk (a test file to near zero free), and watch the metric
   and the server's behaviour. Linux only: fill a tmpfs mount (a loop
   file or a small `tmpfs` unit needs root), never the disk that holds
   anything else. On macOS there is no safe form without root — skip
   the fill and record it so, then do step 3's gc forms on a normal
   disk.

   Done when: you can say what the server does — and what you did — to
   get space back. The runbook's tool for this is the collector:

   ```sh
   scion-server --data /srv/scion gc --dry-run
   ```

   then, when the report makes sense, without `--dry-run`, and with a
   metrics file if you scrape one. Write down what the runbook does
   NOT tell you about a full disk — §6 only says to alert before it
   reaches zero.

## T6 — Rotate the ticket key

Source: §2 "Ticket keys" (the four steps are quoted there in order).

1. Make the new key: `scion-server --data /srv/scion ticket-key --out
   NEW.key` — it prints the new public key, 64 hex characters. The
   first rotation's OLD key is the one T2 step 3 captured: its `.pub`
   file. On a store whose first boot predates that capture, there is
   no OLD-PUBLIC; restart with the new key alone and skip step 3's
   `--ticket-key-old`.

   Skipping the old key has a cost an operator must weigh first: every
   live ticket the old key signed is refused at once — up to 15
   minutes' worth — and the clients and caches that hold one must mint
   again.

2. Give the tiers BOTH public keys, new first, before the server moves:
   `--ticket-public-key <NEW_HEX> --ticket-public-key <OLD_HEX>`.
   Restart the caches.

3. Restart the server with both keys: `--ticket-key NEW.key
   --ticket-key-old OLD-PUBLIC`, where `OLD-PUBLIC` is a file holding
   the OLD key's public hex, not its seed.

4. After 15 minutes (a ticket's whole life), restart the server without
   `--ticket-key-old`, and the caches with only the new public key. A
   real drill waits the 15 minutes out; a rehearsal may compress the
   wait to seconds (a client that holds no old ticket at the last
   restart proves the same), and the behaviour's test holds it:
   crates/scn/tests/rotation.rs.

Done when: a checkout reads through the whole rotation without a
refused ticket in the log, and the last state carries no old key
anywhere.

## T7 — Upgrade

Source: §4 "Upgrades", §3 for the backup copy.

0. Know which build is N-1. The version matrix names it:
   `scripts/version-matrix.sh --resolve-ref` prints the N-1 ref (a
   release tag once one exists; until then a stand-in commit whose
   store schema differs from main's — the script's header names the
   ladder). Build that ref first, then `cargo build --release` for the
   new side. If the two builds write the same schema version, no
   migration can run and step 3's first done-when cannot happen: pick
   the ladder rung below main's schema, not the newest stand-in.

1. Stop the server (the drain: readiness goes `NOT_SERVING`, listeners
   close, in-flight calls finish, exit 0).

2. Copy the data directory as a backup (§3: with the server stopped the
   copy is exact).

3. Start the new binary on the same directory. Watch the log. A
   database the new build finds behind its schema refuses every open
   but the two that move it (§4): run `scion-server --data /srv/scion
   migrate` between the stop and the start, or start with `serve
   --migrate`. The migration logs its steps.

   Done when: the server listens, `fsck` after it exits 0, and you
   recorded the schema version before and after (`psql "<the metadata
   database>" -c 'SELECT max(version) FROM schema_migrations'`, with
   the URL's search_path naming the store's schema). A migration logs
   its steps: the open logs `migrating the metadata schema` with the
   versions it moves between, so the log pairs with the two version
   numbers as the evidence the migration ran. A §4 sentence promises
   "progress in the log" — what you actually saw goes in the run
   record.

4. Prove the guard the runbook promises: never let a newer database
   open with an older binary. The runbook quotes the refusal —
   `database schema version N is newer than this build` — see it once
   by starting the OLD binary on the upgraded directory.

   Done when: the old binary refuses with that shape of error, and you
   did NOT edit the schema version in the database to fix it (the
   runbook forbids it).

## Guesses log

Fill one line the moment a step makes you decide something the runbook
did not say. A filled row is a finding, not a failure.

| Task | Step | What the runbook left out | What I guessed |
| --- | --- | --- | --- |
| T1 | | | |
| T2 | | | |
| T3 | | | |
| T4 | | | |
| T5 | | | |
| T6 | | | |
| T7 | | | |
