#!/usr/bin/env bash
# Tests the soak-fault log check's classifier against the run that
# caught the gap (run 45160834460, job soak-fault-postgres, #1374): an
# import whose Postgres connect landed inside the first pg-arm restart
# window. With the pg arm on the block classifies - the fault itself
# caused the refusal; with it off the refused connect fails the log
# check, because nothing in the plain fault soak restarts Postgres. Run
# from the repository root; CI runs it in the lint job.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)

# shellcheck source=common.sh source-path=SCRIPTDIR
. "$here/common.sh"

# The classifier, eval'd out of soak-fault.sh: the check tests the
# function the run uses, not a copy.
eval "$(awk '/^classifies\(\)/, /^}/' "$here/soak-fault.sh")"
[ "$(type -t classifies)" = function ] || {
  echo "classifies not found in soak-fault.sh" >&2
  exit 1
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# The run's own block, as soak-fault.sh captures it: the failed
# import's line plus the last three lines of the import's output.
printf 'import 21 failed\n    1: the database is busy\n    2: error connecting to server\n    3: Connection refused (os error 111)\n' \
  > "$work/import-fail.log"

# The pg arm on: every line classifies.
if SCION_FAULT_PG=1 classifies "$work/import-fail.log"; then
  :
else
  echo "the pg arm's restart window must classify the refused import" >&2
  exit 1
fi

# The pg arm off: the refused Postgres connect is a real failure.
if SCION_FAULT_PG=0 classifies "$work/import-fail.log"; then
  echo "a refused Postgres connect without the pg arm must fail the check" >&2
  exit 1
fi

# The busy line alone classifies either way: the chain's head is ours,
# and a busy database under load is not a refused one.
printf '%s\n' "    1: the database is busy" > "$work/busy.log"
for arm in 0 1; do
  if ! SCION_FAULT_PG=$arm classifies "$work/busy.log"; then
    echo "the busy line alone must classify with the arm at $arm" >&2
    exit 1
  fi
done

echo "soak-fault classifier test: ok"
