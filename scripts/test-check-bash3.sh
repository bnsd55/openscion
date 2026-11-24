#!/usr/bin/env bash
# Tests the bash 3.2 gate (scripts/check-bash3.sh, #1395): a script with
# a bash 4 form fails it with the file and line named, every form the
# issue lists is refused, today's scripts pass, and a name that only
# contains one of the words does not trip it. Run from anywhere; CI
# runs it in the lint job, next to the gate itself.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
gate=$here/check-bash3.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Each form the issue lists fails the gate. The fixtures are single-
# quoted on purpose: nothing in them may expand.
# shellcheck disable=SC2016
forms=(
  'mapfile -t rows < file'
  'readarray -t rows < file'
  'declare -A seen'
  'local -A seen'
  'lower=${x,,}'
  'upper=${x^^}'
  'one=${x,}'
  'one_up=${x^}'
  'cmd1 |& cmd2'
  'cmd &>> log'
  'coproc tail -f log'
  'declare -n ref=other'
)
for form in "${forms[@]}"; do
  printf '#!/usr/bin/env bash\n%s\n' "$form" > "$work/f.sh"
  if "$gate" "$work/f.sh" >/dev/null 2>&1; then
    echo "the gate passed a bash 4 form: $form"
    exit 1
  fi
done

# One form fails with the file and the line in the message.
printf '#!/usr/bin/env bash\nrows=()\nmapfile -t rows < file\n' > "$work/bad.sh"
out=$("$gate" "$work/bad.sh" 2>&1) && { echo "the gate passed: $out"; exit 1; }
printf '%s\n' "$out" | grep -q "$work/bad.sh:3" ||
  { echo "no file and line in: $out"; exit 1; }
printf '%s\n' "$out" | grep -q mapfile ||
  { echo "no form named in: $out"; exit 1; }

# A name that contains one of the words is not a bash 4 form.
printf '#!/usr/bin/env bash\nmy_mapfile_count=0\nreadarray_size=3\n' > "$work/words.sh"
"$gate" "$work/words.sh" >/dev/null ||
  { echo "the gate tripped on a whole-word miss"; exit 1; }

# A legal form stays legal: a ${x:-a,b} default is not a case change.
# shellcheck disable=SC2016
printf '#!/usr/bin/env bash\nv=${x:-a,b}\n' > "$work/legal.sh"
"$gate" "$work/legal.sh" >/dev/null ||
  { echo "the gate tripped on a default"; exit 1; }

# Today's scripts pass, minus the ones that sit out of the lint's scan:
# the gate itself names the forms it refuses, its test's fixtures are
# the forms themselves, and capacity.sh sits out of the lint's scan.
scripts=()
for f in "$here"/*.sh; do
  case "$(basename "$f")" in
    check-bash3.sh | test-check-bash3.sh) continue ;;
  esac
  scripts+=("$f")
done
"$gate" "${scripts[@]}" >/dev/null ||
  { echo "the gate refused today's scripts"; exit 1; }

echo "check-bash3: ${#forms[@]} forms refused, whole words and defaults legal, today's scripts pass"
