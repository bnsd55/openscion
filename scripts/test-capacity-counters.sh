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

echo "counter test: ok"
