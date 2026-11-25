#!/bin/bash
# The parent half of the schema-cleanup test (#1422): a script run - a
# normal exit, an error exit, an INT, a TERM - leaves no
# scion_script_ schema of its own behind; a run with SCION_KEEP_LOGS
# keeps its schema and writes its name next to the kept logs; a schema
# made for a caller's own directory, outside $WORK, outlives the run.
# The schema list is taken before and after every run, and the child's
# record says a schema was really made, so a run that made none fails
# here instead of passing vacuously. Needs psql and
# SCION_TEST_POSTGRES_URL; starts no server, builds nothing.
set -u
here=$(cd "$(dirname "$0")" && pwd)

fail() {
  echo "test-pg-schema: $*" >&2
  exit 1
}

: "${SCION_TEST_POSTGRES_URL:?SCION_TEST_POSTGRES_URL must name the test database}"
command -v psql > /dev/null || fail "needs psql"

# The scripts' schemas, sorted: the list the issue's rule is checked
# against. The escape keeps _ a literal, so the test suite's own
# scion_test_ schemas never enter this test's view.
schemas() {
  psql "$SCION_TEST_POSTGRES_URL" -tAc \
    "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname LIKE 'scion\\_script\\_%'" | sort
}

drop_one() {
  psql "$SCION_TEST_POSTGRES_URL" -v ON_ERROR_STOP=1 \
    -qc "DROP SCHEMA IF EXISTS \"$1\" CASCADE" > /dev/null || fail "cannot drop $1 for the test's own cleanup"
}

out=$(mktemp -d "${TMPDIR:-/tmp}/test-pg-schema-run.XXXXXX")
trap 'rm -rf "$out"' EXIT
before=$(schemas) || fail "cannot list the schemas before the runs"

# run_case MODE LOG: one child run; its exit code stands in $?. The
# child's record names the schema it made.
run_case() {
  local mode=$1 log=$2
  bash "$here/test-pg-schema.sh" "$mode" "$out/record" > "$log" 2>&1
}

# assert_no_schema RC WANT_RC MODE: the child ended with WANT_RC, made
# a schema, and the list after the run equals the list before it.
assert_no_schema() {
  local rc=$1 want_rc=$2 mode=$3 after
  [ "$rc" -eq "$want_rc" ] || fail "$mode: the run ended $rc, not $want_rc: $(cat "$out/child.$mode.log")"
  [ -s "$out/record" ] || fail "$mode: the run made no schema"
  after=$(schemas) || fail "$mode: cannot list the schemas"
  [ "$after" = "$before" ] || {
    diff <(printf '%s\n' "$before") <(printf '%s\n' "$after") >&2
    fail "$mode: the run left a schema behind"
  }
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

# SCION_KEEP_LOGS: the schema stays, and its name is written next to
# the kept logs. The parent drops it after, so the test leaves nothing.
keep=$out/kept-logs
SCION_KEEP_LOGS=$keep bash "$here/test-pg-schema.sh" keep "$out/record" > "$out/child.keep.log" 2>&1
rc=$?
[ "$rc" -eq 0 ] || fail "keep: the run failed: $(cat "$out/child.keep.log")"
name=$(sed -n '1p' "$out/record")
[ -n "$name" ] || fail "keep: the run made no schema"
after=$(schemas) || fail "keep: cannot list the schemas"
grep -qxF "$name" <<< "$after" || fail "keep: the run's schema $name did not stay"
kept=
for f in "$keep"/*.schemas; do
  [ -e "$f" ] || continue
  kept=${f##*/}
  break
done
[ -n "$kept" ] || fail "keep: no schema list was written next to the kept logs"
grep -qxF "$name" "$keep/$kept" || fail "keep: the kept list does not name $name"
drop_one "$name"
after=$(schemas) || fail "keep: cannot list the schemas"
[ "$after" = "$before" ] || fail "keep: the test left a schema behind"

# A caller's own directory, outside $WORK (the mount-nfs shape): its
# schema outlives the run, and no schema was recorded. The parent takes
# the leak from the list delta and drops it after, as the caller
# would; the child's own directory outside $WORK goes too.
run_case outside "$out/child.outside.log"
rc=$?
[ "$rc" -eq 0 ] || fail "outside: the run failed: $(cat "$out/child.outside.log")"
after=$(schemas) || fail "outside: cannot list the schemas"
name=$(comm -13 <(printf '%s\n' "$before" | sort) <(printf '%s\n' "$after" | sort) | head -n 1)
[ -n "$name" ] || fail "outside: the caller's own schema never appeared"
grep -qxF "$name" <<< "$after" || fail "outside: the caller's schema $name did not outlive the run"
drop_one "$name"
after=$(schemas) || fail "outside: cannot list the schemas"
[ "$after" = "$before" ] || fail "outside: the test left a schema behind"

echo "test-pg-schema: a run that exits normally, by error, by INT or by TERM leaves no schema; SCION_KEEP_LOGS keeps it with its name; a caller's own directory keeps its schema"
