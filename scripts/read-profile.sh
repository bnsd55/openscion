#!/usr/bin/env bash
# The read path, taken apart: a checkout of a repository of many small
# files and of one of few large ones, straight from the server and
# through the cache, the cache cold and then warm. One checkout at a
# time, so the numbers are one call's read path, not a fleet's. Per
# run: the wall time, the CPU time of the stack, the bytes out of the
# server, and the GetObjects calls at each hop. Prints a table; its
# product is the breakdown the read-path change quotes.
#
# Settings, from the environment:
#   SMALL_FILES=20000 LARGE_FILES=40 REPS=2
#   SERVER_PORT=17878 CACHE_PORT=17879 SERVER_METRICS_PORT=17880
#   CACHE_METRICS_PORT=17881  loopback ports
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"
REPS=${REPS:-2}
SMALL_FILES=${SMALL_FILES:-20000}
LARGE_FILES=${LARGE_FILES:-40}

need
command -v jq > /dev/null || die "needs jq"
make_work read-profile
build
sum="$WORK/summary.txt"
: > "$sum"

# Whatever the run produced rides out even on a failure: the breakdown
# is the run's product, and a broken cell is data too.
dest="${RUNNER_TEMP:-/tmp}/read-profile-artifacts"
mkdir -p "$dest"
trap 'cp -r "$WORK"/. "$dest/" 2> /dev/null || true' EXIT

# The server's metadata lives in Postgres, in the schema pg_url_for
# names for this run's data directory.
PG_URL=$(pg_url_for "$WORK/server")

# make_dataset NAME COUNT DIRS SIZE_KIB: one commit of COUNT files of
# SIZE_KIB KiB over DIRS directories. The contents come from one block
# of urandom, made unique per file by the file's index written into the
# head, so every blob is its own object and the import hashes all of
# them.
make_dataset() {
  local name=$1 count=$2 dirs=$3 size_kib=$4
  local src="$WORK/src-$name"
  note "dataset $name: $count files of $size_kib KiB"
  git init -q --initial-branch=main "$src"
  local seed="$WORK/seed-$name"
  dd if=/dev/urandom of="$seed" bs=1024 count="$size_kib" status=none
  local i d file
  for ((i = 0; i < count; i++)); do
    d=$((i % dirs))
    file="$src/d$d/f$i"
    mkdir -p "$(dirname "$file")"
    { printf '%016x' "$i"; tail -c +17 "$seed"; } > "$file"
  done
  git -C "$src" add -A
  git -C "$src" -c user.name=read-profile -c user.email=read-profile@localhost \
    commit -q -m "dataset $name: $count files"
  note "dataset $name built"
}

# one_run LABEL RUN REPO OBJECTS: one checkout under /usr/bin/time,
# with the counters scraped before and after, one line into
# $WORK/results. OBJECTS empty reads from the server; the daemon's
# socket reads through the cache.
one_run() {
  local label=$1 run=$2 repo=$3 objects=$4
  export BIN DAEMON
  local sbefore safter sbytes gbefore gafter cgbefore cgafter
  sbefore=$(scrape_metrics "http://127.0.0.1:$SERVER_METRICS_PORT")
  gbefore=$(getobjects "http://127.0.0.1:$SERVER_METRICS_PORT")
  cgbefore=$(getobjects "http://127.0.0.1:$CACHE_METRICS_PORT")
  mkdir -p "$WORK/$run"
  if [ -n "$objects" ]; then
    export CHECKOUT_CMD='"$BIN/scn" --server "$SERVER" --objects "$DAEMON" --repo '"$repo"' checkout main --into "$1" > /dev/null'
  else
    export CHECKOUT_CMD='"$BIN/scn" --server "$SERVER" --repo '"$repo"' checkout main --into "$1" > /dev/null'
  fi
  if ! "$TIMEOUT" 600 /usr/bin/time -p sh -c "$CHECKOUT_CMD" sh "$WORK/$run/into" \
    > "$WORK/$run.out" 2> "$WORK/$run.log"; then
    tail -n 10 "$WORK/$run.log" >&2
    die "the checkout of run $run failed"
  fi
  safter=$(scrape_metrics "http://127.0.0.1:$SERVER_METRICS_PORT")
  gafter=$(getobjects "http://127.0.0.1:$SERVER_METRICS_PORT")
  cgafter=$(getobjects "http://127.0.0.1:$CACHE_METRICS_PORT")
  sbytes=$((safter - sbefore))
  printf '%s|%s|%s|%s|%s|%s|%s\n' "$label" "$run" "$(wall "$WORK/$run.log")" \
    "$(cpu "$WORK/$run.log" "$WORK/server.log" "$WORK/cache.log" "$WORK/daemon.log")" \
    "$sbytes" "$((gafter - gbefore))" "$((cgafter - cgbefore))" >> "$WORK/results"
  rm -rf "${WORK:?}/$run"
}

# The datasets: many small files, few large ones.
make_dataset many "$SMALL_FILES" 100 1
make_dataset large "$LARGE_FILES" 4 1024
# The datasets import into one server, one repository each.
install -d -m 0700 "$WORK/server"
"$BIN/scion-server" --data "$WORK/server" ticket-key --out "$WORK/ticket.key" \
  > "$WORK/ticket.pub"
"$BIN/scion-server" --data "$WORK/server" --postgres-url "$PG_URL" \
  import --repo many --rev main "$WORK/src-many" \
  > "$WORK/import-many.out" 2> "$WORK/import-many.log"
"$BIN/scion-server" --data "$WORK/server" --postgres-url "$PG_URL" \
  import --repo large --rev main "$WORK/src-large" \
  > "$WORK/import-large.out" 2> "$WORK/import-large.log"

mkdir -p "$WORK/daemon"
chmod 700 "$WORK/daemon"
start_scion

for dataset in many large; do
  # Straight from the server: two runs, the first with a cold page
  # cache of the store's files, the second with it warm.
  one_run "$dataset, server, first" "$dataset-server-1" "$dataset" ""
  one_run "$dataset, server, again" "$dataset-server-2" "$dataset" ""
  # Through the cache: the daemon's store cold, then warm.
  stop daemon
  rm -rf "${WORK:?}/daemon"
  mkdir -p "$WORK/daemon"
  chmod 700 "$WORK/daemon"
  start daemon serving "$BIN/scion-daemon" --data "$WORK/daemon" --socket "$SOCKET" \
    --upstream "http://127.0.0.1:$CACHE_PORT"
  one_run "$dataset, cache, cold" "$dataset-cache-cold" "$dataset" "$DAEMON"
  one_run "$dataset, cache, warm" "$dataset-cache-warm-1" "$dataset" "$DAEMON"
  one_run "$dataset, cache, warm" "$dataset-cache-warm-2" "$dataset" "$DAEMON"
done
stop_scion

# The table.
{
  files_many=$(find "$WORK/src-many" -type f -not -path '*/.git/*' | wc -l | tr -d ' ')
  files_large=$(find "$WORK/src-large" -type f -not -path '*/.git/*' | wc -l | tr -d ' ')
  echo "read path, taken apart (runner: ${CAPACITY_MACHINE:-unspecified})"
  echo "datasets: many = $files_many files of 1-4 KiB, large = $files_large files of 1 MiB"
  echo
  printf '%-26s %8s %9s %15s %13s %12s\n' \
    cell wall_s cpu_s server_kib_out server_calls cache_calls
  awk -F'|' '{
    printf "%-26s %8.2f %9.2f %15.1f %13d %12d\n", $1, $3, $4, $5 / 1024, $6, $7
  }' "$WORK/results"
} >> "$sum"
cat "$sum"
