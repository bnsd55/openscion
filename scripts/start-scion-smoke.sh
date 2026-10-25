#!/usr/bin/env bash
# The per-PR daemon check (#592's rule needs a job that exercises it):
# start the server, cache and daemon exactly as the dispatch scripts do
# (common.sh's start_scion), make one call through the daemon, stop.
# A few seconds; it catches a startup rule that only the dispatch
# scripts hit.
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"
need

# The scripts run the release binaries by default; the smoke builds
# them (no SCION_BIN), so a PR that breaks the build fails here.
note "smoke: building the release binaries"
"$TIMEOUT" 600 cargo build --locked --release --quiet \
  -p open-scion-server -p open-scion-tier -p open-scion

make_work smoke
# The server refuses a missing data directory (#578), and the daemon
# refuses one open to other users: the same prep every script does.
mkdir -p "$WORK/server"
chmod 700 "$WORK/server"

# One tiny repo, so the call through the daemon has something to
# resolve: a one-file git repository, as the other scripts seed.
git init -q --initial-branch=main "$WORK/src"
printf 'smoke\n' > "$WORK/src/README.md"
git -C "$WORK/src" add README.md
git -C "$WORK/src" -c user.name=smoke -c user.email=smoke@localhost \
  commit -qm "seed"
git init -q --bare --initial-branch=main "$WORK/src.git"
git -C "$WORK/src.git" fetch -q "$WORK/src" main
SEED_HEX=$(git -C "$WORK/src" rev-parse main)
git -C "$WORK/src.git" update-ref refs/heads/main "$SEED_HEX"
"$BIN/scion-server" --data "$WORK/server" import --rev main "$WORK/src.git" \
  > /dev/null 2> "$WORK/import.log" || {
  tail -n 20 "$WORK/import.log" >&2
  die "the import failed"
}

start_scion
trap 'stop_scion 2> /dev/null || true; rm -rf "$WORK"' EXIT
"$TIMEOUT" 60 "$BIN/scn" --server "$SERVER" --objects "$DAEMON" ls main > /dev/null
note "smoke: one call through the daemon answered; the stack started and stopped clean"
