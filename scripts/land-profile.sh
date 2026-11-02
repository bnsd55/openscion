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
# - the blob root's flush: for its own short cell the server runs
#   under strace, whose log gives the fsync count and per-call time.
#   strace slows every syscall of the server it wraps, so that cell's
#   other numbers stand alone and are not compared to the untraced
#   cells.
#
# The database this harness drops and creates follows the capacity
# harness's naming rule, for the same reason: an operator URL that
# names a real database is refused before any DROP.
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"

PROFILE_LANDS=${PROFILE_LANDS:-200}
PROFILE_FLUSH_LANDS=${PROFILE_FLUSH_LANDS:-50}
PROFILE_WRITERS_CELL=${PROFILE_WRITERS_CELL:-"50 400"}
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

export SCION_TOKEN=smoke
# shellcheck disable=SC2034 # common.sh's import_dataset and start read it
PG_URL=$SCION_PG_URL

# reset_cell: a fresh store (drop and create, the capacity harness's
# rule), the extension, the ticket key, and a server wrapped in $WRAP.
reset_cell() {
  psql "$admin_url" -X -q -c "DROP DATABASE IF EXISTS $pg_db" \
    -c "CREATE DATABASE $pg_db" || die "the database reset failed"
  rm -rf "$WORK/server"
  install -d -m 0700 "$WORK/server"
  import_dataset
  psql "$SCION_PG_URL" -X -q -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" \
    || die "pg_stat_statements did not load (the service needs shared_preload_libraries=pg_stat_statements)"
  "$BIN/scion-server" --data "$WORK/server" ticket-key --out "$WORK/ticket.key" \
    > "$WORK/ticket.pub" 2> /dev/null
  start server serving $WRAP "$BIN/scion-server" --data "$WORK/server" --postgres-url "$PG_URL" serve \
    --listen "127.0.0.1:$SERVER_PORT" \
    --ticket-key "$WORK/ticket.key" \
    --metrics-listen "127.0.0.1:$SERVER_METRICS_PORT"
}

stop_server() {
  local pid
  pid=$(lsof -t -iTCP:"$SERVER_PORT" -sTCP:LISTEN 2> /dev/null | head -n 1)
  if [ -n "$pid" ]; then kill "$pid" 2> /dev/null || true; fi
  sleep 1
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

# One cell: name, writers, lands, the server wrapped in $WRAP.
cell() {
  local name=$1 writers=$2 lands=$3
  note "cell $name: $writers writers, $lands lands"
  reset_cell
  curl -fsS "http://127.0.0.1:$SERVER_METRICS_PORT/metrics" \
    > "$WORK/metrics-$name-before.txt" || die "no metrics before the bench"
  psql "$SCION_PG_URL" -X -q -c "SELECT pg_stat_statements_reset()" > /dev/null \
    || die "the statements reset failed"
  if ! "$TIMEOUT" 900 "$BIN/scn" --server "$SERVER" bench --agents "$writers" \
    --lands "$lands" --json > "$WORK/bench-$name.json" 2> "$WORK/bench-$name.log"; then
    tail -n 20 "$WORK/bench-$name.log" >&2
    die "the bench of cell $name failed"
  fi
  curl -fsS "http://127.0.0.1:$SERVER_METRICS_PORT/metrics" \
    > "$WORK/metrics-$name-after.txt" || die "no metrics after the bench"
  psql "$SCION_PG_URL" -X -A -t \
    -c "SELECT calls, round(total_exec_time::numeric, 3), left(query, 160)
        FROM pg_stat_statements ORDER BY total_exec_time DESC" \
    > "$WORK/pg-statements-$name.csv" || die "the statements dump failed"
  stop_server
}

note "land-profile: building"
sum="$WORK/summary.txt"
: > "$sum"

# The solo cell: one writer, so a land's cost carries no queueing.
WRAP=""
cell solo 1 "$PROFILE_LANDS"

# The writers cell: the same server at the writer count the measured
# curve flattens at, so the wait share has a number beside it.
cell writers $PROFILE_WRITERS_CELL

# The flush cell: the server under strace, only the fsync family.
WRAP="strace -f -tt -T -e trace=fsync,fdatasync -o $WORK/fsync.log -s 0"
cell flush 1 "$PROFILE_FLUSH_LANDS"
WRAP=""

{
  echo "one land, taken apart (runner: ${CAPACITY_MACHINE:-unspecified})"
  for name in solo writers flush; do
    echo ""
    echo "## cell $name"
    jq -r '"client: landed \(.landed) in \(.seconds)s, land p50 \(.land_ms.p50) ms, p99 \(.land_ms.p99) ms, end-to-end p50 \(.end_to_end_ms.p50) ms"' \
      "$WORK/bench-$name.json"
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
  echo "the flush (the strace cell only; its other numbers carry strace's overhead):"
  if [ -f "$WORK/fsync.log" ]; then
    n=$(grep -c 'fsync' "$WORK/fsync.log" || true)
    t=$(grep -o '<[0-9.]*>$' "$WORK/fsync.log" | tr -d '<>' | awk '{ s += $1 } END { printf "%.3f", s }')
    awk -v n="$n" -v t="$t" -v l="$PROFILE_FLUSH_LANDS" \
      'BEGIN { printf "fsync calls: %s (%.2f per land), total %s s (%.1f ms per land)\n", n, n / l, t, t * 1000 / l }'
  fi
} >> "$sum"

note "land-profile: the summary"
cat "$sum"
cp "$sum" "$dest/"
note "land-profile: done; the artifacts are in $dest"
