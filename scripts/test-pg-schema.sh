#!/bin/bash
# The child half of the schema-cleanup test: a script run shaped like the
# real ones - it sources common.sh, makes its $WORK, and calls
# pg_url_for, which makes a real scion_script_ schema in the database
# SCION_TEST_POSTGRES_URL names. It records the name of the schema its
# own run made, and ends the way the parent asked: a normal exit, an
# error exit, or parked for the INT or TERM the parent sends. The
# parent holds the record. Needs psql and a test database; starts no
# server, builds nothing.
set -u
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$here/common.sh"

fail() {
  echo "test-pg-schema: $*" >&2
  exit 1
}

case ${1:-} in
  normal | error | int | term | keep | outside) mode=$1 ;;
  *) echo "test-pg-schema: need a mode: normal, error, int, term, keep, outside" >&2; exit 64 ;;
esac
record=${2:-}
[ -n "$record" ] || fail "the parent must name the record file"
[ -n "${SCION_TEST_POSTGRES_URL:-}" ] || fail "SCION_TEST_POSTGRES_URL must name the test database"
command -v psql > /dev/null || fail "needs psql"

make_work test-pg-schema
case $mode in
  outside)
    # The mount-nfs shape: the caller's own directory, outside $WORK.
    # The schema it makes is a caller's, and outlives the run.
    target=$(mktemp -d "${TMPDIR:-/tmp}/scion-pg-schema-outside.XXXXXX")
    ;;
  *)
    mkdir -p "$WORK/server"
    target=$WORK/server
    ;;
esac
url=$(pg_url_for "$target") || fail "pg_url_for failed"
# The schema's name, read back from the URL pg_url_for printed: the
# run's own record, never a listing of the database (the test-database
# rule: drop only what your own run recorded).
schema=$(printf '%s\n' "$url" | sed -n 's/.*search_path%3D\([^&%]*\).*/\1/p')
[ -n "$schema" ] || fail "pg_url_for named no schema"
psql "$SCION_TEST_POSTGRES_URL" -tAc \
  "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '$schema'" | grep -q 1 \
  || fail "the schema $schema is not in the database"
case $mode in
  outside)
    # The rule this case pins: a caller's own directory records
    # nothing, so no cleanup ever drops its schema. The directory is
    # the caller's too: the run removes it after the schema is made.
    [ -s "$WORK/.pg.schemas" ] && fail "a dir outside WORK was recorded"
    rm -rf "$target"
    ;;
  *)
    [ -s "$WORK/.pg.schemas" ] || fail "the run recorded no schema"
    grep -qxF "$schema" "$WORK/.pg.schemas" || fail "the run's own schema $schema is not in its record"
    ;;
esac
printf '%s\n' "$schema" > "$record"
case $mode in
  int | term)
    touch "$record.ready"
    # The parent ends this run with INT or TERM: the traps make_work
    # set turn it into an exit, whose EXIT trap runs the cleanup.
    while :; do
      sleep 1
    done
    ;;
  error)
    # The way die ends every script's failure path.
    exit 1
    ;;
  *)
    exit 0
    ;;
esac
