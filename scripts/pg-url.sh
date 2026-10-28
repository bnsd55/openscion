# shellcheck shell=bash
# pg_url_for, shared by the scripts that start a real server. Sourced by
# scripts/common.sh (so its consumers get it) and by scripts that do not
# source common.sh (mount-nfs.sh).
#
# pg_url_for DATA_DIR: the Postgres URL whose search_path names this
# data dir's schema, created here if missing. The base URL comes from
# SCION_TEST_POSTGRES_URL; the schema is `scion_script_<hash of the
# dir's path>`, the same rule the test suite's own schema follows (the
# hash of the deepest existing ancestor's canonical form, with the
# missing tail spelled as the caller did), so the name is the same
# before and after the directory exists, one directory's metadata never
# answers another directory's reads, and a rerun finds the schema it
# left. psql makes the schema: the server refuses a URL whose
# search_path names no existing schema, and the release binary creates
# none. The URL rides `options=--search_path=<schema>,public`, the
# spelling the server's own tests use.
pg_url_for() {
  local data=$1
  local cur=$data
  local tail=""
  local base=${SCION_TEST_POSTGRES_URL:?SCION_TEST_POSTGRES_URL must name the test database}
  base=${base%%\?*}
  while [ ! -e "$cur" ]; do
    tail="/$(basename "$cur")$tail"
    cur=$(dirname "$cur")
  done
  local canon
  canon=$(cd "$cur" && pwd -P)
  local hash
  hash=$(printf '%s' "$canon$tail" | cksum | cut -d' ' -f1)
  local schema="scion_script_$hash"
  command -v psql > /dev/null || {
    echo "FAIL: psql is needed to make the schema $schema" >&2
    return 1
  }
  psql "$SCION_TEST_POSTGRES_URL" -v ON_ERROR_STOP=1 \
    -qc "CREATE SCHEMA IF NOT EXISTS \"$schema\"" > /dev/null || {
    echo "FAIL: cannot create schema $schema in the test database" >&2
    return 1
  }
  printf '%s?options=--search_path%%3D%s%%2Cpublic' "$base" "$schema"
}
