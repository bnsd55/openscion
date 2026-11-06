#!/usr/bin/env bash
# One land, taken apart on the runner class the capacity envelope uses
# (operations.md section 7). It measures only: no product code rides in
# the change, and the run publishes no envelope — its product is the
# breakdown table.
#
# The instrumentation, per part:
# - the land as the client sees it: `scn bench --json` (its own
#   percentiles);
# - the land as the server served it: the `scion_request_seconds`
#   histogram of the Land rpc, snapshotted before and after the bench
#   window;
# - the Postgres statements of exactly that window: pg_stat_statements,
#   reset before the window and read after it, so every row is a
#   statement the lands ran (calls and total time). The round-trip
#   count is the calls sum, with the caveat that the wire can pipeline
#   several statements into one round trip;
# - the readers beside the writers (the cells with 20 readers): the
#   same checkout loop the capacity harness runs, through the cache and
#   the daemon, so the slice's wait columns have the reader load the
#   capacity envelope saw.
#
# The database this harness drops and creates follows the capacity
# harness's naming rule, for the same reason: an operator URL that
# names a real database is refused before any DROP.
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"

# The grid: the writers the capacity envelope names, each with no
# readers and with the envelope's heavy reader count; two repetitions
# per cell (the envelope's rule; the first rep warms, the second is
# the steady state). PROFILE_LANDS is the lands per rep: 2 x 1000 is
# the envelope's 2000 lands per cell.
PROFILE_CELLS=${PROFILE_CELLS:-"2 50 200"}
PROFILE_READERS=${PROFILE_READERS:-20}
PROFILE_LANDS=${PROFILE_LANDS:-1000}
PROFILE_REPS=${PROFILE_REPS:-2}
need
command -v jq > /dev/null || die "needs jq"
command -v strace > /dev/null || die "needs strace (apt-get install strace)"

[ -n "${SCION_PG_URL:-}" ] || die "no SCION_PG_URL: the profile needs Postgres"
pg_db="${SCION_PG_URL##*/}"
pg_db="${pg_db%%\?*}"
case $pg_db in
  scion_capacity_*) ;;
  *) die "SCION_PG_URL names database '${pg_db:-none}': this harness drops and creates the database it runs on, so it runs only on a scion_capacity_* database" ;;
esac
admin_url="${SCION_PG_URL%/*}/postgres"

make_work land-profile
# Whatever the run produced rides out even on a failure: the breakdown
# is the run's product, and a broken cell is data too.
dest="${RUNNER_TEMP:-/tmp}/land-profile-artifacts"
mkdir -p "$dest"
trap 'cp -r "$WORK"/. "$dest/" 2> /dev/null || true' EXIT
if [ -n "${SCION_BIN:-}" ]; then
  BIN=$SCION_BIN
else
  build
fi
fetch_dataset

# The cache and the daemon authenticate their upstream fetches with
# SCION_UPSTREAM_TOKEN, which common.sh set to its own random value:
# both tokens move together, or the readers' object fetches die as
# unauthenticated (the first reader run's 39466 failures).
export SCION_TOKEN=smoke SCION_UPSTREAM_TOKEN=smoke
# The Postgres URL reaches the server through the env (common.sh's
# start_scion reads it), as the capacity harness sets it.
export SCION_POSTGRES_URL=$SCION_PG_URL
# shellcheck disable=SC2034 # common.sh's import_dataset and start read it
PG_URL=$SCION_PG_URL

# reset_cell NAME: a fresh store (drop and create, the capacity
# harness's rule), the extension, and a server wrapped in $WRAP. The
# cell before's server log moves aside, so each cell keeps its own:
# the profile reads the land's per-phase lines from it.
reset_cell() {
  # The cell before's stack stops first: its cache and daemon still
  # hold the ports the next cell's stack needs.
  stop server
  stop cache
  stop daemon
  psql "$admin_url" -X -q -c "DROP DATABASE IF EXISTS $pg_db" \
    -c "CREATE DATABASE $pg_db" || die "the database reset failed"
  rm -rf "$WORK/server"
  install -d -m 0700 "$WORK/server"
  import_dataset
  psql "$SCION_PG_URL" -X -q -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" \
    || die "pg_stat_statements did not load (the service needs shared_preload_libraries=pg_stat_statements)"
  start_scion
}

stop_server() {
  local pid
  pid=$(lsof -t -iTCP:"$SERVER_PORT" -sTCP:LISTEN 2> /dev/null | head -n 1)
  if [ -n "$pid" ]; then kill "$pid" 2> /dev/null || true; fi
  sleep 1
}

# cpu_sample: one /proc/stat delta sample every 5 s into the cell's
# cpu log, until the cell's teardown kills this pid.
cpu_sample() {
  prev=""
  while :; do
    cur=$(awk '/^cpu /{print $2+$3+$4+$7+$8, $5, $6}' /proc/stat)
    if [ -n "$prev" ]; then
      busy=$(( $(echo "$cur" | cut -d' ' -f1) - $(echo "$prev" | cut -d' ' -f1) ))
      idle=$(( $(echo "$cur" | cut -d' ' -f2) - $(echo "$prev" | cut -d' ' -f2) ))
      wait=$(( $(echo "$cur" | cut -d' ' -f3) - $(echo "$prev" | cut -d' ' -f3) ))
      total=$((busy + idle + wait))
      [ "$total" -gt 0 ] && echo "busy_pct $((100 * busy / total)) iowait_pct $((100 * wait / total)) load $(cut -d' ' -f1 /proc/loadavg)" >> "$WORK/cpu-$CELL.log"
    fi
    prev="$cur"
    sleep 5
  done
}

# metrics_land SUM|COUNT: the Land rpc's histogram number of a
# snapshot.
metrics_land() {
  local what=$1 file=$2
  case $what in
    SUM) awk '/^scion_request_seconds_sum\{.*rpc="Scion\/Land"/ { print $NF }' "$file" ;;
    COUNT) awk '/^scion_request_seconds_count\{.*rpc="Scion\/Land"/ { print $NF }' "$file" ;;
  esac
}

# reader NAME: one reader loop (the capacity harness's shape, carried
# whole): a checkout attempt of main through the cache and the daemon,
# into a destination of its own, until the sentinel file goes away.
# A killed reader must never share a path with the attempts after it.
reader() {
  local n=0 rc t0 t1 dest
  while [ -e "$WORK/reads.on" ]; do
    n=$((n + 1))
    echo "attempt $n start $(date -u +%T.%3N)" >> "$WORK/reads-$CELL.log"
    t0=$(date +%s%N)
    dest=$(mktemp -d "$WORK/reads.XXXXXX")
    if "$TIMEOUT" 60 "$BIN/scn" --server "$SERVER" --objects "$DAEMON" checkout main \
      --sparse "$SPARSE_DIR" --into "$dest" > /dev/null 2>> "$WORK/reads-$CELL.log"; then
      t1=$(date +%s%N)
      echo $(( (t1 - t0) / 1000000 )) >> "$WORK/read-times-$CELL.csv"
    else
      rc=$?
      echo "attempt $n failed rc=$rc at $(date -u +%T.%3N)" >> "$WORK/reads-$CELL.log"
    fi
    rm -rf "$dest"
  done
}

# One cell: name, writers, readers. The cell's load is its own reps of
# the bench; the readers (if any) run beside every rep.
cell() {
  local name=$1 writers=$2 readers=$3 rep i
  note "cell $name: $writers writers, $readers readers"
  # The cell's name reaches the reader loops: an env-prefix here would
  # scope it to reset_cell alone, and the loops would die on the unset
  # name (the first clean run's readers never checked out).
  CELL=$name
  reset_cell "$name"
  curl -fsS "http://127.0.0.1:$SERVER_METRICS_PORT/metrics" \
    > "$WORK/metrics-$name-before.txt" || die "no metrics before the bench"
  psql "$SCION_PG_URL" -X -A -t -c "SELECT wal_sync, wal_sync_time" \
    > "$WORK/wal-$name-before.txt" || die "no wal before the bench"
  window_start=$(date +%s)
  psql "$SCION_PG_URL" -X -q -c "SELECT pg_stat_statements_reset()" > /dev/null \
    || die "the statements reset failed"
  if [ "$readers" -gt 0 ]; then
    : > "$WORK/reads-$name.log"
    : > "$WORK/read-times-$name.csv"
    : > "$WORK/readers.pids"
    touch "$WORK/reads.on"
    i=0
    while [ "$i" -lt "$readers" ]; do
      i=$((i + 1))
      loop_bg "readers-$i" reader
      sed -n 's/^pid=//p' "$WORK/owned/readers-$i" >> "$WORK/readers.pids"
    done
  fi
  # The runner's own load, sampled through the cell: "the runner, not
  # the lane" needs a number, not the vCPU count alone. busy_pct is the
  # non-idle share of /proc/stat between samples; load is loadavg's
  # first minute figure.
  : > "$WORK/cpu-$name.log"
  cpu_sample &
  cpu_pid=$!
  rep=1
  while [ "$rep" -le "$PROFILE_REPS" ]; do
    if ! "$TIMEOUT" 900 "$BIN/scn" --server "$SERVER" bench --agents "$writers" \
      --lands "$PROFILE_LANDS" --json > "$WORK/bench-$name-rep$rep.json" \
      2> "$WORK/bench-$name-rep$rep.log"; then
      tail -n 20 "$WORK/bench-$name-rep$rep.log" >&2
      die "the bench of cell $name rep $rep failed"
    fi
    rep=$((rep + 1))
  done
  if [ "$readers" -gt 0 ]; then
    rm -f "$WORK/reads.on"
    # The teardown is the capacity harness's dance: freeze the loops,
    # kill their in-flight children, then the loops, then CONT delivers
    # the queued TERMs and the waits reap them.
    while read -r pid; do kill -STOP "$pid" 2> /dev/null || true; done < "$WORK/readers.pids"
    while read -r pid; do pkill -TERM -P "$pid" 2> /dev/null || true; done < "$WORK/readers.pids"
    while read -r pid; do kill -TERM "$pid" 2> /dev/null || true; done < "$WORK/readers.pids"
    while read -r pid; do kill -CONT "$pid" 2> /dev/null || true; done < "$WORK/readers.pids"
    while read -r pid; do wait "$pid" 2> /dev/null || true; done < "$WORK/readers.pids"
  fi
  kill "$cpu_pid" 2> /dev/null || true
  wait "$cpu_pid" 2> /dev/null || true
  curl -fsS "http://127.0.0.1:$SERVER_METRICS_PORT/metrics" \
    > "$WORK/metrics-$name-after.txt" || die "no metrics after the bench"
  window_end=$(date +%s)
  psql "$SCION_PG_URL" -X -A -t -c "SELECT wal_sync, wal_sync_time" \
    > "$WORK/wal-$name-after.txt" || die "no wal after the bench"
  psql "$SCION_PG_URL" -X -A -t \
    -c "SELECT calls, round(total_exec_time::numeric, 3), left(query, 160)
        FROM pg_stat_statements ORDER BY total_exec_time DESC" \
    > "$WORK/pg-statements-$name.csv" || die "the statements dump failed"
  stop_server
  mv -f "$WORK/server.log" "$WORK/server-$name.log"
}

note "land-profile: building"
sum="$WORK/summary.txt"
: > "$sum"

# One ticket key for every cell: the key must not change under the
# tiers, and ticket-key refuses to overwrite (its write is create_new).
install -d -m 0700 "$WORK/server"
if ! "$BIN/scion-server" --data "$WORK/server" ticket-key --out "$WORK/ticket.key" \
  > "$WORK/ticket.pub" 2> "$WORK/ticket.err"; then
  cat "$WORK/ticket.err" >&2
  die "ticket-key failed"
fi

# The grid: every writer count of the capacity envelope, with no
# readers and with the envelope's heavy reader count beside it.
for w in $PROFILE_CELLS; do
  cell "land-w$w-r0" "$w" 0
  cell "land-w$w-r$PROFILE_READERS" "$w" "$PROFILE_READERS"
done
{
  echo "one land, taken apart (runner: ${CAPACITY_MACHINE:-unspecified})"
  echo "branch: $(git rev-parse --short HEAD); the main it carries: $(git log --format=%P -1 HEAD | awk '{print $2}' | cut -c1-7)"
  echo "runner cpus: $(nproc) vCPU"
  psql "$SCION_PG_URL" -X -A -t -c \
    "SELECT name || '=' || setting FROM pg_settings
     WHERE name IN ('fsync', 'synchronous_commit', 'wal_sync_method',
                    'full_page_writes', 'track_wal_io_timing')
     ORDER BY name" | paste -sd' ' - | sed 's/^/postgres: /' || true
  if docker ps --format '{{.Names}}' 2> /dev/null | grep -q profile-pg; then
    echo "postgres data dir: $(docker exec profile-pg df -T /var/lib/postgresql/data 2> /dev/null | tail -1 | awk '{print $2, $5}')"
  fi
  names=""
  for w in $PROFILE_CELLS; do
    names="$names land-w$w-r0 land-w$w-r$PROFILE_READERS"
  done
  for name in $names; do
    echo ""
    echo "## cell $name"
    jq -r '"client (rep 2): landed \(.landed) in \(.seconds)s, land p50 \(.land_ms.p50) ms, p99 \(.land_ms.p99) ms, end-to-end p50 \(.end_to_end_ms.p50) ms"' \
      "$WORK/bench-$name-rep$PROFILE_REPS.json"
    lands=$(metrics_land COUNT "$WORK/metrics-$name-after.txt")
    lands=$((lands - $(metrics_land COUNT "$WORK/metrics-$name-before.txt")))
    sums=$(metrics_land SUM "$WORK/metrics-$name-after.txt")
    sumb=$(metrics_land SUM "$WORK/metrics-$name-before.txt")
    echo "server: $lands lands served"
    awk -v s="$sums" -v b="$sumb" -v l="$lands" \
      'BEGIN { if (l > 0) printf "server rpc: mean %.1f ms per land\n", (s - b) * 1000 / l }'
    echo "statements (calls, total ms, the head of the text):"
    head -n 15 "$WORK/pg-statements-$name.csv"
    calls=$(awk -F'|' '{ s += $1 } END { print s + 0 }' "$WORK/pg-statements-$name.csv")
    total_ms=$(awk -F'|' '{ s += $2 } END { printf "%.1f", s }' "$WORK/pg-statements-$name.csv")
    awk -v c="$calls" -v t="$total_ms" -v l="$lands" \
      'BEGIN { if (l > 0) printf "statements per land: %.1f calls, %.1f ms in Postgres\n", c / l, t / l }'
  done
  echo ""
  echo "readers (checkout ms over the cell's attempts):"
  p50=$(pct "$WORK/read-times-$name.csv" 0.50 2> /dev/null || true)
  p99=$(pct "$WORK/read-times-$name.csv" 0.99 2> /dev/null || true)
  tries=$(wc -l < "$WORK/read-times-$name.csv" 2> /dev/null | tr -d ' ')
  echo "completed $tries checkouts, p50 ${p50:--} ms, p99 ${p99:--} ms"
  if [ -s "$WORK/cpu-$name.log" ]; then
    awk '{ b += $2; w += $3; l += $5; if ($5 > m) m = $5; n++ } END { printf "runner: mean busy %.0f%%, iowait %.0f%%, loadavg mean %.1f, max %.1f\n", b / n, w / n, l / n, m }' "$WORK/cpu-$name.log"
  fi
  if [ -s "$WORK/wal-$name-after.txt" ]; then
    awk 'NR == FNR { s = $1; t = $2; next } { printf "wal in the window: %d syncs, %.1f s total, %.3f ms mean\n", $1 - s, ($2 - t) / 1000, ($2 - t) / ($1 - s) }' "$WORK/wal-$name-before.txt" "$WORK/wal-$name-after.txt"
  fi
} >> "$sum"

# The slice's phases, per cell: the land's own per-phase lines, averaged
# over the cell's lands. wall_ms is the phase's wall time per land,
# stmt_ms the round-trip time of its statements, stmts their count. The
# total row is the whole land: the phases nest inside it (settle holds
# build, cas and set_state), and slot_wait is the queue, not the work.
names=""
for w in $PROFILE_CELLS; do
  names="$names land-w$w-r0 land-w$w-r$PROFILE_READERS"
done
for name in $names; do
  log="$WORK/server-$name.log"
  [ -f "$log" ] || continue
  {
    echo ""
    echo "## the slice in $name (per land; wall and wait as mean/p50/p99 ms)"
    awk '
      / land_profile: / {
        phase = ""; wall = 0; sm = 0; st = 0; at = ""
        for (i = 1; i <= NF; i++) {
          if ($i ~ /^phase=/)         { phase = substr($i, 7); gsub(/"/, "", phase) }
          else if ($i ~ /^wall_ms=/)  { wall = substr($i, 9) + 0 }
          else if ($i ~ /^stmt_ms=/)  { sm = substr($i, 9) + 0 }
          else if ($i ~ /^stmts=/)    { st = substr($i, 7) + 0 }
          else if ($i ~ /^attempts=/) { at = substr($i, 10) + 0 }
        }
        if (phase == "") next
        n[phase]++; w[phase] += wall; s[phase] += sm; c[phase] += st
        # The wait is the wall time outside the statements: queueing,
        # the pool, and the work Postgres does between round trips.
        k = ++q[phase]; W[k, phase] = wall; wait[k, phase] = wall - sm
        if (at != "") a[phase] += at
      }
      function pct(vals, k, lo, hi,   i, j, v) {
        for (i = 2; i <= k; i++) { v = vals[i]; j = i - 1
          while (j > 0 && vals[j] > v) { vals[j + 1] = vals[j]; j-- }
          vals[j + 1] = v }
        lo = int((k + 1) / 2); hi = int((k * 99 + 99) / 100)
        if (hi < 1) hi = 1; if (hi > k) hi = k
        return vals[lo] SUBSEP vals[hi]
      }
      END {
        printf "%-10s %9s %9s %9s %12s %9s %9s %8s %9s\n", "phase", "wall50", "wall99", "wallavg", "wait50", "wait99", "waitavg", "stmts", "attempts"
        m = split("total pre_checks pre_intent pre_rest slot_wait record outcome plan fill_root lens settle build cas set_state post other", order, " ")
        for (i = 1; i <= m; i++) {
          p = order[i]
          if (!n[p]) continue
          for (j = 1; j <= q[p]; j++) { wv[j] = W[j, p]; xv[j] = wait[j, p] }
          split(pct(wv, q[p]), t, SUBSEP)
          split(pct(xv, q[p]), u, SUBSEP)
          printf "%-10s %9.2f %9.2f %9.2f %12.2f %9.2f %9.2f %8.1f %9.2f\n", p, t[1], t[2], w[p] / n[p], u[1], u[2], (w[p] - s[p]) / n[p], c[p] / n[p], (p == "total" ? a[p] / n[p] : 0)
        }
      }' "$log"
  } >> "$sum"
done

# The write lane's customers, per cell: the cumulative lane gauges of
# /metrics, diffed across the window and divided by the lands. held is
# the time one write transaction holds the connection; queued the time
# it waited for it; stmt_ms the round-trip time of its statements. The
# coverage row sums held over every customer and sets it against the
# window's own 1/rate: the lane's whole busy time is its holds, so the
# two agree unless an instrumented site is missing.
names=""
for w in $PROFILE_CELLS; do
  names="$names land-w$w-r0 land-w$w-r$PROFILE_READERS"
done
for name in $names; do
  before="$WORK/metrics-$name-before.txt"
  after="$WORK/metrics-$name-after.txt"
  [ -f "$before" ] && [ -f "$after" ] || continue
  {
    echo ""
    echo "## the write lane in $name (per land)"
    awk -v before="$before" -v after="$after" -v secs="$((window_end - window_start))" '
      function val(file, gauge,   line, kv, v) {
        v = 0
        while ((getline line < file) > 0)
          if (line ~ ("^" gauge)) { split(line, kv, " "); v += kv[2]; }
        close(file)
        return v
      }
      BEGIN {
        m = split("record settle gc_sweep derive_steps create_commit put_blob_row other", order, " ")
        total_held = 0
        for (i = 1; i <= m; i++) {
          p = order[i]
          calls[p] = val(after, "scion_lane_calls{phase=\"" p "\"}") - val(before, "scion_lane_calls{phase=\"" p "\"}")
          held[p] = val(after, "scion_lane_held_seconds{phase=\"" p "\"}") - val(before, "scion_lane_held_seconds{phase=\"" p "\"}")
          queued[p] = val(after, "scion_lane_queued_seconds{phase=\"" p "\"}") - val(before, "scion_lane_queued_seconds{phase=\"" p "\"}")
          stmts[p] = val(after, "scion_lane_stmts{phase=\"" p "\"}") - val(before, "scion_lane_stmts{phase=\"" p "\"}")
          stmtns[p] = val(after, "scion_lane_stmt_seconds{phase=\"" p "\"}") - val(before, "scion_lane_stmt_seconds{phase=\"" p "\"}")
          total_held += held[p]
        }
      }
      { lands = $1 - $2 }
      END {
        if (lands <= 0) { print "no lands"; exit }
        printf "%-14s %8s %10s %10s %10s %8s %9s\n", "customer", "calls", "queuedms", "heldms", "stmt_ms", "stmts", "commitms"
        for (i = 1; i <= m; i++) {
          p = order[i]
          if (calls[p] == 0) continue
          commit[p] = val(after, "scion_lane_commit_seconds{phase=\"" p "\"}") - val(before, "scion_lane_commit_seconds{phase=\"" p "\"}")
          printf "%-14s %8.1f %10.2f %10.2f %10.2f %8.1f %9.2f\n", p, calls[p] / lands, queued[p] * 1000 / lands, held[p] * 1000 / lands, stmtns[p] * 1000 / lands, stmts[p] / lands, commit[p] * 1000 / lands
        }
        printf "lane holds total %.1f ms/land; the window is %.1f s for %d lands, so 1/rate is %.1f ms; holds/rate %.1f%%\n", total_held * 1000 / lands, secs, lands, secs * 1000 / lands, 100 * total_held * 1000 / lands / (secs * 1000 / lands)
      }' <(printf '%s %s\n' \
        "$(awk 'index($1, "scion_lands_total") == 1 { s += $2 } END { print s + 0 }' "$after")" \
        "$(awk 'index($1, "scion_lands_total") == 1 { s += $2 } END { print s + 0 }' "$before")")
  } >> "$sum"
done

note "land-profile: the summary"
cat "$sum"
cp "$sum" "$dest/"
note "land-profile: done; the artifacts are in $dest"
