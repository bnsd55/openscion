#!/bin/sh
# The bash 3.2 gate for scripts (#1395): macOS runs the scripts under
# /bin/bash 3.2, and a bash 4 form costs a macOS job's wait to find
# (#1388 put mapfile in a test script; Linux passed and macOS failed 20
# minutes in). The gate refuses the forms bash 3.2 cannot run, with the
# file and line of each, so the lint job catches one in seconds:
#
#   mapfile, readarray     read into an array
#   declare -A, local -A   associative arrays
#   ${x,,}, ${x^^}         case changes
#   |&, &>>                both streams at once
#   coproc                 coprocesses
#   declare -n             name references
#
# The match is whole-word, so a name like my_mapfile_count does not
# trip it. No script in this repository may need bash 4; if one ever
# must, it is named here with its reason.
set -eu

note() { printf '%s %s\n' "check-bash3:" "$*"; }
die() { printf '%s %s\n' "check-bash3:" "$*" >&2; exit 1; }

[ "$#" -ge 1 ] || die "usage: check-bash3.sh FILE..."

# The forms. Whole-word bounds on the command words keep a name that
# contains one of them out; the case-change operators need only the
# "${name" prefix, so a ${x:-a,b} default stays legal.
pattern='(^|[^A-Za-z0-9_])(mapfile|readarray|coproc)([^A-Za-z0-9_]|$)|(^|[^A-Za-z0-9_])(declare|local)[[:space:]]+-(A|n)([^A-Za-z0-9_]|$)|\$\{[A-Za-z_][A-Za-z0-9_]*(\^\^|,,)|\|\&|&>>'

hits=0
for f in "$@"; do
  # -H names the file even when one file is given: the message must
  # carry the file and the line.
  if grep -nHE "$pattern" -- "$f"; then
    hits=1
  fi
done

[ "$hits" = 0 ] || die "bash 4 forms above; every script must run under /bin/bash 3.2, which macOS ships"
note "no bash 4 forms in $# script(s)"
