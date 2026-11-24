#!/usr/bin/env bash
# Tests the soaks' damage guard (#1376): a line that reports lost or
# damaged data never passes either classifier, however broad the
# expected-failure list is, and the logs no classifier reads are
# scanned for the same texts. The check evals the classifiers the runs
# use - classifies out of soak.sh and soak-fault.sh, and the damage
# helpers out of common.sh - not copies. Run from the repository root;
# CI runs it in the lint job.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)

# shellcheck source=common.sh source-path=SCRIPTDIR
. "$here/common.sh"

# The classifiers, eval'd out of the scripts: the check tests the
# functions the runs use.
eval "$(awk '/^classifies\(\)/, /^}/' "$here/soak.sh")"
plain_def=$(declare -f classifies)
eval "$(awk '/^classifies\(\)/, /^}/' "$here/soak-fault.sh")"
fault_def=$(declare -f classifies)

plain_c() { eval "$plain_def"; classifies "$@"; }
fault_c() { eval "$fault_def"; classifies "$@"; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# The damage lines, as the clients print them: the bench's exactly-once
# verify, the checkout's size check (the truncated reply's shape), and
# the server's data-loss status. Each must be refused by both
# classifiers, before any accepted pattern is tried.
printf '%s\n' \
  "scn: reported lands differ from main: 3 reported, 4 on main, 2 in common" \
  "scn: server sent 7: 100 bytes, the tree says 120" \
  "scn: cannot fetch objects: code: 'DataLoss', message: \"damaged data\"" \
  > "$work/damage.log"
for c in plain_c fault_c; do
  if "$c" "$work/damage.log" 2> /dev/null; then
    echo "damage lines must fail the $c classifier" >&2
    exit 1
  fi
done

# The same refusal, line by line: the shape a tail -n 3 of readers.log
# carries into a read-fail log.
if is_damage "scn: server sent an object that was not asked for"; then
  :
else
  echo "a checkout damage line must be damage" >&2
  exit 1
fi

# The ticket refusal is not damage (#1365 N2): a restart can refuse a
# ticket, and the guard keys on the exact texts.
if is_damage "cannot mint a fresh ticket: the server sent no ticket"; then
  echo "the ticket refusal must not count as damage" >&2
  exit 1
fi

# The expected-failure lines keep passing: a fault soak's marker line
# and the cancel loop's kill are accepted there; the plain soak accepts
# its own expected lines and still refuses a checkout failure.
printf '%s\n' "checkout 7 failed" > "$work/marker.log"
if fault_c "$work/marker.log"; then
  :
else
  echo "the fault soak must accept its checkout marker" >&2
  exit 1
fi
if plain_c "$work/marker.log" 2> /dev/null; then
  echo "the plain soak must refuse a checkout failure" >&2
  exit 1
fi
printf '%s\n' "exit 137" > "$work/kill.log"
for c in plain_c fault_c; do
  if ! "$c" "$work/kill.log" 2> /dev/null; then
    echo "the cancel loop's kill must classify in the $c soak" >&2
    exit 1
  fi
done

# The damage line riding the marker's tail is refused even though the
# marker alone is accepted (the read-fail log's shape, #1376 F2).
printf '%s\n' "checkout 7 failed" \
  "Error: server sent an object that was not asked for" > "$work/tail.log"
if fault_c "$work/tail.log" 2> /dev/null; then
  echo "a damage line in the tail must fail the fault soak" >&2
  exit 1
fi

# The logs no classifier reads (import-soak.log, gc.log, a writer log):
# damage_in finds the evidence the classified file's marker swallowed
# (#1376 F3).
printf '%s\n' \
  "scn: cannot fetch objects: code: 'DataLoss', message: \"damaged data\"" \
  > "$work/import-soak.log"
[ -n "$(damage_in "$work/import-soak.log")" ] || {
  echo "damage_in must find the DataLoss line in the import log" >&2
  exit 1
}
printf '%s\n' "gc 9 done in 2.1s" > "$work/gc.log"
[ -z "$(damage_in "$work/gc.log")" ] || {
  echo "damage_in must stay quiet on a clean gc log" >&2
  exit 1
}
[ -z "$(damage_in "$work/missing.log")" ] || {
  echo "damage_in must stay quiet on a missing file" >&2
  exit 1
}

echo "soak damage guard test: ok"
