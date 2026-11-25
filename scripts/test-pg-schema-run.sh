#!/bin/bash
# The parent half of the schema-cleanup test (#1422): a script run - a
# normal exit, an error exit, an INT, a TERM - leaves the run's own
# schema gone; a run with SCION_KEEP_LOGS keeps its schema and writes
# its name next to the kept logs; a schema made for a caller's own
# directory, outside $WORK, outlives the run.
#
# The test-database rule: the parent checks and drops only the name the
# run's own record names - never a listing of the database, never a
# pattern. Another session's schemas are invisible here: on a shared
# database, a whole-list comparison would make this test answer for
# runs it did not make. Needs psql and SCION_TEST_POSTGRES_URL; starts
# no server, builds nothing.
set -u
here=$(cd "$(dirname "$0")" && pwd)

fail() {
  echo "test-pg-schema: $*" >&2
  exit 1
}

: "${SCION_TEST_POSTGRES_URL:?SCION_TEST_POSTGRES_URL must name the test database}"
command -v psql > /dev/null || fail "needs psql"

# present NAME: the query ran, and the schema named NAME is in the
# test database. A psql failure fails the test - absent must mean "the
# query ran and found 0", never "the database did not answer" (F4).
present() {
  local count
  count=$(psql "$SCION_TEST_POSTGRES_URL" -v ON_ERROR_STOP=1 -tAc \
    "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = '$1'") \
    || fail "cannot ask the database about $1"
  [ "$count" -ge 1 ]
}

drop_one() {
  psql "$SCION_TEST_POSTGRES_URL" -v ON_ERROR_STOP=1 \
    -qc "DROP SCHEMA IF EXISTS \"$1\" CASCADE" > /dev/null || fail "cannot drop $1 for the test's own cleanup"
}

out=$(mktemp -d "${TMPDIR:-/tmp}/test-pg-schema-run.XXXXXX")
trap 'rm -rf "$out"' EXIT

# run_case MODE LOG: one child run; its exit code stands in $?. The
# child's record names the schema its own run made.
run_case() {
  local mode=$1 log=$2
  # The record goes before every run: a child that dies before it
  # writes must not leave the previous case's name behind to be read
  # (F5).
  rm -f "$out/record" "$out/record.ready"
  bash "$here/test-pg-schema.sh" "$mode" "$out/record" > "$log" 2>&1
}

# assert_gone RC WANT_RC MODE: the child ended with WANT_RC, recorded
# the schema its own run made, and that schema is out of the database
# after the run.
assert_no_schema() {
  local rc=$1 want_rc=$2 mode=$3 name
  [ "$rc" -eq "$want_rc" ] || fail "$mode: the run ended $rc, not $want_rc: $(cat "$out/child.$mode.log")"
  name=$(sed -n '1p' "$out/record" 2> /dev/null)
  [ -n "$name" ] || fail "$mode: the run made no schema"
  present "$name" && fail "$mode: the run's own schema $name stayed behind"
  return 0
}

# Normal exit.
run_case normal "$out/child.normal.log"
rc=$?
assert_no_schema "$rc" 0 normal

# Error exit: the way die ends every script's failure path.
run_case error "$out/child.error.log"
rc=$?
assert_no_schema "$rc" 1 error

# INT and TERM: make_work's traps turn the signal into an exit, whose
# EXIT trap runs the cleanup. The child signals readiness after its
# schema exists, so the kill never lands before the work it must clean.
# The signal goes to the child's process group, the way a runner ends a
# step: a backgrounded bash starts with INT and TERM ignored (the async
# rule), so a plain kill to the pid would be swallowed and the trap
# would never run. Job control gives the child its own group; the kill
# reaches it the way the runner's signal reaches a step.
set -m
for sig in INT TERM; do
  case $sig in
    INT) mode=int want_rc=130 ;;
    TERM) mode=term want_rc=143 ;;
  esac
  bash "$here/test-pg-schema.sh" "$mode" "$out/record" > "$out/child.$sig.log" 2>&1 &
  pid=$!
  rm -f "$out/record"
  for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    [ -e "$out/record.ready" ] && break
    kill -0 "$pid" 2> /dev/null || break
    sleep 0.5
  done
  rm -f "$out/record.ready"
  kill -0 "$pid" 2> /dev/null \
    || fail "$sig: the run ended before its schema was made: $(cat "$out/child.$sig.log")"
  kill -"$sig" -- -"$pid" 2> /dev/null || kill -"$sig" "$pid"
  wait "$pid"
  rc=$?
  assert_no_schema "$rc" "$want_rc" "$sig"
done
set +m

# SCION_KEEP_LOGS: the run's schema stays, and its name is written next
# to the kept logs. The parent drops the name its own run recorded, so
# the test leaves nothing behind.
keep=$out/kept-logs
SCION_KEEP_LOGS=$keep run_case keep "$out/child.keep.log"
rc=$?
[ "$rc" -eq 0 ] || fail "keep: the run failed: $(cat "$out/child.keep.log")"
name=$(sed -n '1p' "$out/record")
[ -n "$name" ] || fail "keep: the run made no schema"
present "$name" || fail "keep: the run's schema $name did not stay"
[ -d "$keep" ] || fail "keep: no logs directory was kept"
kept=
for f in "$keep"/*.schemas; do
  [ -e "$f" ] || continue
  kept=${f##*/}
  break
done
[ -n "$kept" ] || fail "keep: no schema list was written next to the kept logs"
grep -qxF "$name" "$keep/$kept" || fail "keep: the kept list does not name $name"
drop_one "$name"
present "$name" && fail "keep: the test left the schema $name behind"

# A caller's own directory, outside $WORK (the mount-nfs shape): the
# schema the run made there outlives it, under the name the child
# recorded. The parent drops that name only; the caller's directory
# went with the child's run.
run_case outside "$out/child.outside.log"
rc=$?
[ "$rc" -eq 0 ] || fail "outside: the run failed: $(cat "$out/child.outside.log")"
name=$(sed -n '1p' "$out/record")
[ -n "$name" ] || fail "outside: the child recorded no schema name"
present "$name" || fail "outside: the caller's schema $name did not outlive the run"
drop_one "$name"
present "$name" && fail "outside: the test left the schema $name behind"

echo "test-pg-schema: a run that exits normally, by error, by INT or by TERM leaves no schema; SCION_KEEP_LOGS keeps it with its name; a caller's own directory keeps its schema"
