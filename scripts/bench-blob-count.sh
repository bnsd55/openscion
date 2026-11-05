#!/usr/bin/env bash
# The blob-count measurement (issue #823): fills a store with small
# blobs at the counts the caller names, and for each records the
# server's open time, the fill time, `fsck`'s and `gc --dry-run`'s
# times, the bytes on disk against the blob bytes, and the inodes used
# and left (`df -i`, printed before and after each count). Runs in
# release mode, on the fixed runner the workflow dispatches: a run of
# millions of blobs takes tens of minutes. The default count is small
# so that a check run stays fast; the CI job passes the large counts.
#
# SCION_BENCH_COUNTS names the counts, space-separated (default
# "100000"). SCION_BENCH_WORK names a scratch directory for the stores
# (default a directory under /tmp, wiped per run on the runner).
set -euo pipefail

COUNTS="${SCION_BENCH_COUNTS:-100000}"
WORK="${SCION_BENCH_WORK:-$(mktemp -d /tmp/scion-blob-bench.XXXXXX)}"

mkdir -p "$WORK"
cd "$(dirname "$0")/.."

step() {
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
      echo "### $*"
    } >> "$GITHUB_STEP_SUMMARY"
  else
    echo "== $*"
  fi
}

summary_line() {
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    printf '%s\n' "$1" >> "$GITHUB_STEP_SUMMARY"
  else
    printf '%s\n' "$1"
  fi
}

step "blob-count costs (issue #823)"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  summary_line ""
  summary_line "| blobs | open (s) | reopen (s) | fill (s) | blob rows (s) | count query (ms) | fsck (s) | gc --dry-run (s) | blob bytes | objects bytes | inodes used | inodes free |"
  summary_line "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
fi

# The inode table row of the filesystem that holds the store, as one
# line of names the caller can read. Printed to stdout, so it reaches
# the run's output whatever drives the script.
inode_row() {
  df -i "$WORK"
}

for count in $COUNTS; do
  dir="$WORK/store-$count"
  rm -rf "$dir"
  mkdir -p "$dir"

  echo "count $count: the inodes before"
  inode_row | tee "/tmp/blob-bench-df-before-$count.txt"

  SCION_BENCH_BLOB_COUNT="$count" \
    SCION_BENCH_DIR="$dir" \
    SCION_TEST_POSTGRES_URL="${SCION_TEST_POSTGRES_URL:?the test database URL}" \
    cargo test --release -p open-scion-server --test bench_blob_count \
    -- --ignored --nocapture 2>&1 | tee "/tmp/blob-bench-run-$count.txt"

  # The walk's shape on the store just built, in a fresh process: an
  # fsck-only pass with the CPU split and a second walk of the same
  # store. One pass, on the largest store, where the growth shows.
  largest=""
  for c in $COUNTS; do largest="$c"; done
  if [ "$count" = "$largest" ]; then
    echo "fsck-only pass on store-$count"
    SCION_BENCH_FSCK_ONLY=1 \
      SCION_BENCH_DIR="$dir" \
      SCION_TEST_POSTGRES_URL="${SCION_TEST_POSTGRES_URL:?the test database URL}" \
      cargo test --release -p open-scion-server --test bench_blob_count \
      -- --ignored --nocapture 2>&1 | tee "/tmp/blob-bench-fsckonly-$count.txt" | grep '^BENCH' || true
  fi

  # The facts of the run, lifted from the test's BENCH lines. grep and
  # awk only: the runner has the image's own tools.
  facts() {
    grep '^BENCH ' "/tmp/blob-bench-run-$count.txt" | awk -v key="$1" '$2 == key { print $3 }'
  }
  open_s="$(facts open_seconds)"
  reopen_s="$(facts reopen_seconds)"
  fill_s="$(facts fill_seconds)"
  fsck_s="$(facts fsck_seconds)"
  gc_s="$(facts gc_dry_run_seconds)"
  blob_bytes="$(facts blob_bytes)"
  blob_rows_s="$(facts blob_rows_seconds)"
  count_query_ms="$(grep '^BENCH blob_count_query_ms ' "/tmp/blob-bench-run-$count.txt" | awk '{ print $4 }' | sort -n | sed -n '3p')"
  # empty when the store has no rows: the column stays empty, not 0

  # The bytes on disk against the blob bytes: the per-file overhead of
  # one file per blob (block rounding, the directory tree, the tmp
  # folders), measured on the objects folder the blobs live in.
  objects_bytes="$(du -sk "$dir/objects" | cut -f1)"
  objects_bytes=$((objects_bytes * 1024))
  echo "count $count: the objects folder holds $objects_bytes bytes for $blob_bytes blob bytes"

  echo "count $count: the inodes after"
  inode_row | tee "/tmp/blob-bench-df-after-$count.txt"

  # inodes: the used and free counts of the filesystem that holds the
  # store, before and after this run's fill.
  inodes_used_before="$(awk 'NR==2 { print $3 }' "/tmp/blob-bench-df-before-$count.txt")"
  inodes_used_after="$(awk 'NR==2 { print $3 }' "/tmp/blob-bench-df-after-$count.txt")"
  inodes_free_after="$(awk 'NR==2 { print $4 }' "/tmp/blob-bench-df-after-$count.txt")"

  summary_line "| $count | $open_s | $reopen_s | $fill_s | $blob_rows_s | $count_query_ms | $fsck_s | $gc_s | $blob_bytes | $objects_bytes | $((inodes_used_after - inodes_used_before)) | $inodes_free_after |"

  # The store is the measurement's subject; the next count starts
  # clean. On the runner the whole scratch directory goes with the job.
  if [ -z "${CI:-}" ]; then
    echo "keeping $dir for inspection; delete it when done"
  fi
done

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  summary_line ""
  summary_line "commit: $(git rev-parse HEAD); runner: $(uname -m), $(nproc) cores"
fi
