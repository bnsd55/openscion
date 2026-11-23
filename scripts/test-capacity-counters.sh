#!/usr/bin/env bash
# Tests the capacity reader log's counters against a fixture: lines in
# the shapes the reader loop writes, counted by the functions
# scripts/capacity.sh uses. The bug this catches: a grep anchored to the
# line's start counted zero of every event, because every line starts
# with its attempt number (a review of #461, 2026-10-21). Run from the
# repository root; CI runs it in the lint job.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$here/common.sh"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cat >"$work/reader.log" <<'EOF'
attempt 1 start 10:00:00.000
attempt 1 end ms=107 10:00:05.321
attempt 2 start 10:00:05.400
attempt 2 timeout after 60s at 10:01:05.410
attempt 3 start 10:01:05.400
attempt 3 checkout failed rc=1 at 10:01:05.900
attempt 4 start 10:01:06.000
EOF

count=$(count_reader_timeouts "$work/reader.log")
[ "$count" -eq 1 ] || { echo "timeouts: wanted 1, got $count" >&2; exit 1; }
count=$(count_reader_failures "$work/reader.log")
[ "$count" -eq 1 ] || { echo "failures: wanted 1, got $count" >&2; exit 1; }

: >"$work/empty.log"
count=$(count_reader_timeouts "$work/empty.log")
[ "$count" -eq 0 ] || { echo "an empty log must count 0, got $count" >&2; exit 1; }
count=$(count_reader_failures "$work/empty.log")
[ "$count" -eq 0 ] || { echo "an empty log must count 0, got $count" >&2; exit 1; }

# A missing log counts 0 too: the functions never fail the cell.
count=$(count_reader_timeouts "$work/absent.log")
[ "$count" -eq 0 ] || { echo "an absent log must count 0, got $count" >&2; exit 1; }

# The throughput's arithmetic, pinned on a two-rep fixture: two reps,
# 1000 lands each over 10 s and 20 s, want 2000 / 30 = 66.7 lands/s.
# The bug this catches: summing the reports' FIRST field (the lands) as
# the seconds read 1.0 lands/s in every cell of run 40206407822.
jsons=$(mktemp -d)
trap 'rm -rf "$work" "$jsons"' EXIT
jq -n '{landed: 1000, seconds: 10.0}' > "$jsons/rep1.json"
jq -n '{landed: 1000, seconds: 20.0}' > "$jsons/rep2.json"
got=$(cell_throughput "$jsons"/rep1.json "$jsons"/rep2.json)
[ "$got" = "66.7" ] || { echo "throughput: wanted 66.7, got $got" >&2; exit 1; }
# A rep whose bench died leaves no report; the survivors still divide.
got=$(cell_throughput "$jsons"/rep1.json "$jsons/absent.json")
[ "$got" = "100.0" ] || { echo "throughput (one live rep): wanted 100.0, got $got" >&2; exit 1; }
# No live report: the dash, not a division by zero.
got=$(cell_throughput "$jsons/absent.json")
[ "$got" = "-" ] || { echo "throughput (no reports): wanted '-', got $got" >&2; exit 1; }

# cpu_ms reads utime + stime from /proc/PID/stat. proc(5): everything
# after the last ") " is state onward, and there utime and stime are the
# 12th and 13th fields. The check pulls the awk program out of
# capacity.sh itself, so it fails if the line drifts back. The fixture
# makes both old bugs visible: the comm holds a space and a ")", so a
# strip at any earlier ")" (or none) keeps the pid in the fields, and
# cmajflt (7), utime (40) and stime (5) all differ, so the old
# $11 + $12 line cannot pass by accident (it printed 0 here, or 47 with
# the strip fixed but the fields not).
prog=$(sed -n "s/^.*awk '\(.*\)' \"\\/proc\\/\$pid\\/stat\".*\$/\1/p" "$here/capacity.sh")
[ -n "$prog" ] || { echo "cpu_ms: no awk program found in capacity.sh" >&2; exit 1; }
printf '1234 (a )b c) R 0 0 0 0 0 0 0 0 0 7 40 5 0 0\n' > "$work/stat"
got=$(awk "$prog" "$work/stat")
[ "$got" -eq 45 ] || { echo "cpu_ms: wanted utime+stime = 45, got $got" >&2; exit 1; }

# The damage guard's markers, pinned to the Rust source (#1362 N3): a
# reword fails here instead of silently turning the guard off. Each
# line is "text:file"; the marker list in capacity.sh's scan_damage
# must stay in step with this list.
root=$(cd "$here/.." && pwd)
while IFS='|' read -r text file; do
  grep -qF -- "$text" "$root/$file" || {
    echo "marker '$text' is gone from $file - reword the scan_damage marker with it" >&2
    exit 1
  }
done <<'EOF'
appears twice on main|crates/scn/src/cmd/bench.rs
lands reported the same result|crates/scn/src/cmd/bench.rs
reported lands differ from main:|crates/scn/src/cmd/bench.rs
server sent a bad object for|crates/scn/src/cmd/checkout.rs
server sent tree |crates/scn/src/cmd/checkout.rs
server sent blob |crates/scn/src/cmd/checkout.rs
server sent a bad tree for|crates/scn/src/cmd/checkout.rs
server sent an object that was not asked for|crates/scn/src/cmd/checkout.rs
server sent none of the requested objects|crates/scn/src/cmd/checkout.rs
bytes, the tree says|crates/scn/src/cmd/checkout.rs
the reply ended in the middle of an object|crates/scn/src/cmd/checkout.rs
server sent a bad ID|crates/scn/src/client.rs
damaged data|crates/scion-server/src/error.rs
EOF
# The ticket refusal names no damage marker: it must not match (N2).
ticket=$(printf 'cannot mint a fresh ticket: the server sent no ticket\n')
for m in "server sent a bad object for" "server sent tree " "server sent blob " \
  "server sent a bad tree for" "server sent a bad ID" \
  "server sent an object that was not asked for" "server sent none of the requested objects" \
  "bytes, the tree says" "code: 'DataLoss'"; do
  printf '%s\n' "$ticket" | grep -qF -- "$m" && {
    echo "marker '$m' matches the ticket refusal - it is not lost data" >&2
    exit 1
  }
done

# N4: one bench log with a real verify message fails the scan and
# names the cell. scan_damage lives in capacity.sh; the test evals the
# function out of it, so the check tests the real code, not a copy.
eval "$(awk '/^scan_damage\(\)/, /^}/' "$here/capacity.sh")"
[ "$(type -t scan_damage)" = function ] || {
  echo "scan_damage not found in capacity.sh" >&2
  exit 1
}
dmg=$(mktemp -d)
printf 'scn: reported lands differ from main: 3 reported, 4 on main, 2 in common\n' \
  > "$dmg/bench-w1-r2-rep1.log"
if out=$(scan_damage "$dmg"); then
  echo "a verify failure must fail the scan, got: $out" >&2
  exit 1
fi
printf '%s\n' "$out" | grep -q "cell w1-r2" || {
  echo "the scan must name the cell, got: $out" >&2
  exit 1
}

# Only overload: a bench that died on a status, timeouts, an
# Unavailable reply - recorded, never fatal. The scan passes silently.
clean=$(mktemp -d)
printf "scn: cannot fetch objects: code: 'ResourceExhausted', message: \"the server is out of disk space; retry later\"\n" \
  > "$clean/bench-w3-r0-rep1.log"
printf "attempt 1 timeout after 60s at 10:00:05.410\nattempt 2 checkout failed rc=2\nattempt 3 cannot fetch objects: code: 'Unavailable', message: \"the database is busy; retry\"\n" \
  > "$clean/reads-w1-r1.log"
if out=$(scan_damage "$clean"); then
  [ -z "$out" ] || { echo "overload must stay silent, got: $out" >&2; exit 1; }
else
  echo "overload must not fail the scan, got: $out" >&2
  exit 1
fi

# The server's own data-loss status fails and names the cell.
dmg2=$(mktemp -d)
printf "scn: cannot fetch objects: code: 'DataLoss', message: \"damaged data\"\n" \
  > "$dmg2/reads-w2-r5.log"
if out=$(scan_damage "$dmg2"); then
  echo "DataLoss must fail the scan, got: $out" >&2
  exit 1
fi
printf '%s\n' "$out" | grep -q "cell w2-r5" || {
  echo "the scan must name the cell, got: $out" >&2
  exit 1
}

echo "counter test: ok"
