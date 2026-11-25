# shellcheck shell=bash
# pg_url_for, shared by the scripts that start a real server. Sourced by
# scripts/common.sh (so its consumers get it) and by scripts that do not
# source common.sh (mount-nfs.sh).
#
# pg_url_for DATA_DIR: the Postgres URL whose search_path names this
# data dir's schema, created here if missing. The base URL comes from
# SCION_TEST_POSTGRES_URL; the schema is `scion_script_<hash of the
# dir's path>`: the shape is the test suite's own naming rule
# (test_support's backend.rs) - the canonical form of the deepest
# existing ancestor, with the missing tail spelled as the caller did,
# so the name is the same before and after the directory exists and one
# directory's metadata never answers another directory's reads. The
# prefix and the hash are this script's own (scion_script_ and a
# SHA-256, not the suite's scion_test_ and its hasher), so the scripts'
# schemas stay out of the test suite's sweep. The hash is the
# first 16 hex digits of a SHA-256 - a 64-bit name a CRC would let two
# paths collide on. A rerun finds the schema it left. psql makes the
# schema: the server refuses a URL whose search_path names no existing
# schema, and the release binary creates none. The URL rides
# `options=--search_path=<schema>,public`, the spelling the server's
# own tests use.
#
# The schema's life is the data dir's life (#1422): a schema whose data
# dir lives under $WORK dies with $WORK, in the same cleanup that
# removes it. pg_url_for records every schema it made for a directory
# under $WORK in $WORK/.pg.schemas, and finish_script_schemas - the
# call both cleanups run before they remove $WORK - drops that list.
# A data dir outside $WORK (a caller's own directory, as mount-nfs.sh
# may pass) holds real metadata the run did not make: its schema is
# never dropped. With SCION_KEEP_LOGS set, the run's schema stays and
# its name lands next to the kept logs, so a failed run's database
# state stays readable with its logs. A run killed by SIGKILL leaves
# the schema behind: the trap cannot run, and nothing sweeps a
# scion_script_ schema whose directory is gone - drop it by hand, by
# its name in the run's kept logs or by its scion_script_ prefix.
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
  # The schema's life is its data dir's (#1422): a dir under $WORK gets
  # its schema recorded, so the cleanup that removes $WORK drops the
  # schema too. A dir outside $WORK records nothing - its schema is a
  # caller's, and outlives the run.
  if [ -n "${WORK:-}" ]; then
    case $canon$tail in
      "$WORK" | "$WORK"/*)
        printf '%s\n' "$schema" >> "$WORK/.pg.schemas"
        ;;
    esac
  fi
  # The base's own parameters stay (sslmode on a remote test database);
  # options is appended with the separator the base leaves (the same
  # rule tests/common/mod.rs's postgres_url follows).
  local sep='?'
  case $base in *\?*) sep='&' ;; esac
  printf '%s%soptions=--search_path%%3D%s%%2Cpublic' "$base" "$sep" "$schema"
}

# finish_script_schemas: the schema side of the work-directory cleanup
# (#1422). The rule: a schema goes away exactly when its data dir goes
# away. cleanup() removes $WORK, so the schemas recorded for
# directories under $WORK are dropped here, and no others. A run with
# SCION_KEEP_LOGS set keeps its schema - a failed run's metadata - and
# writes the names next to the kept logs. A drop that fails warns and
# answers success: the run's own exit code must stand. Safe to call
# with no WORK, or with no registry: a script that never called
# pg_url_for (test-owned.sh) touches no database.
finish_script_schemas() {
  local reg schema err
  [ -n "${WORK:-}" ] || return 0
  reg=$WORK/.pg.schemas
  [ -s "$reg" ] || return 0
  if [ -n "${SCION_KEEP_LOGS:-}" ]; then
    mkdir -p "$SCION_KEEP_LOGS"
    cp "$reg" "$SCION_KEEP_LOGS/$(basename "$WORK").schemas"
    return 0
  fi
  err=$WORK/.pg.drop.err
  while IFS= read -r schema; do
    [ -n "$schema" ] || continue
    if [ -z "${SCION_TEST_POSTGRES_URL:-}" ] || ! command -v psql > /dev/null; then
      echo "cleanup: the schema $schema is left in the database: psql or SCION_TEST_POSTGRES_URL is gone" >&2
      return 0
    fi
    if ! psql "$SCION_TEST_POSTGRES_URL" -v ON_ERROR_STOP=1 \
        -qc "DROP SCHEMA IF EXISTS \"$schema\" CASCADE" < /dev/null > /dev/null 2> "$err"; then
      echo "cleanup: the schema $schema is left in the database: $(tail -n 1 "$err")" >&2
    fi
  done < "$reg"
}
