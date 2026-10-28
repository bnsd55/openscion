# shellcheck shell=bash
# pg_url_for, shared by the scripts that start a real server. Sourced by
# scripts/common.sh (so its consumers get it) and by scripts that do not
# source common.sh (mount-nfs.sh).
#
# pg_url_for DATA_DIR: the Postgres URL whose search_path names this
# data dir's schema, created here if missing. The base URL comes from
# SCION_TEST_POSTGRES_URL; the schema is `scion_script_<hash of the
# dir's path>`, the test suite's own naming rule (test_support's
# backend.rs): the canonical form of the deepest existing ancestor,
# with the missing tail spelled as the caller did, so the name is the
# same before and after the directory exists and one directory's
# metadata never answers another directory's reads. The hash is the
# first 16 hex digits of a SHA-256 - a 64-bit name a CRC would let two
# paths collide on. A rerun finds the schema it left. psql makes the
# schema: the server refuses a URL whose search_path names no existing
# schema, and the release binary creates none. The URL rides
# `options=--search_path=<schema>,public`, the spelling the server's
# own tests use.
pg_url_for() {
  local data=$1
  local cur=$data
  local tail=""
  local base=${SCION_TEST_POSTGRES_URL:?SCION_TEST_POSTGRES_URL must name the test database}
  while [ ! -e "$cur" ]; do
    tail="/$(basename "$cur")$tail"
    cur=$(dirname "$cur")
  done
  local canon
  canon=$(cd "$cur" && pwd -P)
  local hash
  hash=$(printf '%s' "$canon$tail" | shasum -a 256 | cut -c1-16)
  local schema="scion_script_$hash"
  if [ "${SCION_PG_URL_TRACE:-}" = 1 ]; then
    echo "pg_url_for: data=$data canon=$canon tail=$tail schema=$schema" >&2
  fi
  command -v psql > /dev/null || {
    echo "FAIL: psql is needed to make the schema $schema" >&2
    return 1
  }
  psql "$SCION_TEST_POSTGRES_URL" -v ON_ERROR_STOP=1 \
    -qc "CREATE SCHEMA IF NOT EXISTS \"$schema\"" > /dev/null || {
    echo "FAIL: cannot create schema $schema in the test database" >&2
    return 1
  }
  # The base's own parameters stay (sslmode on a remote test database);
  # options is appended with the separator the base leaves (the same
  # rule tests/common/mod.rs's postgres_url follows).
  local sep='?'
  case $base in *\?*) sep='&' ;; esac
  printf '%s%soptions=--search_path%%3D%s%%2Cpublic' "$base" "$sep" "$schema"
}
