#!/usr/bin/env bash
# The per-PR daemon check (#592's rule needs a job that exercises it):
# start the server, cache and daemon exactly as the dispatch scripts do
# (common.sh's start_scion), make one call through the daemon, stop.
# A few seconds; it catches a startup rule that only the dispatch
# scripts hit.
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"

note "smoke: building the debug binaries"
"$TIMEOUT" 600 cargo build --locked --quiet \
  -p open-scion-server -p open-scion-tier -p open-scn

make_work smoke
start_scion
trap 'stop_scion 2> /dev/null || true; rm -rf "$WORK"' EXIT
"$TIMEOUT" 60 "$BIN/scn" --server "$SERVER" --objects "$DAEMON" ls > /dev/null
note "smoke: one call through the daemon answered; the stack started and stopped clean"
