#!/usr/bin/env bash
# Tests scripts/merge-changelog.sh against a fixture: a CHANGELOG.md and
# three fragments, two of them in one section, the output compared with
# the expected text. Two in one section is what catches a check on the
# fragment list that reads its own newlines as whitespace (2026-10-19:
# that bug refused every section that held more than one fragment). Run
# from the repository root; CI runs it in the lint job.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/changes"

cat >"$work/CHANGELOG.md" <<'EOF'
# Changelog

## [Unreleased]

## [v0.1.0] - unreleased

The first release.

### Added

- An existing row.

### Fixed

- An existing fix.
EOF

cat >"$work/changes/11.added.md" <<'EOF'
An earlier thing, whose file sorts before the other added fragment, so
the merged rows come out in the fragment names' order.
EOF

cat >"$work/changes/12.added.md" <<'EOF'
A new thing that the changelog will name first, in a fragment of three
lines of plain text, no row marker and no indent on any line, wrapped
at 76 columns so the merged row fits in 79.
EOF

cat >"$work/changes/13.security.md" <<'EOF'
A report whose subsection does not exist yet, so the merge creates it
at the end of the release block.
EOF

(cd "$work" && "$here/merge-changelog.sh" v0.1.0) >"$work/out.txt"

if [ ! -f "$work/CHANGELOG.md" ]; then
    echo "the merge lost CHANGELOG.md" >&2
    exit 1
fi
if [ -d "$work/changes" ] && [ -n "$(ls -A "$work/changes")" ]; then
    echo "the merge left fragments behind:" >&2
    ls "$work/changes" >&2
    exit 1
fi

cat >"$work/expected.md" <<'EOF'
# Changelog

## [Unreleased]

## [v0.1.0] - unreleased

The first release.

### Added

- An earlier thing, whose file sorts before the other added fragment, so
  the merged rows come out in the fragment names' order.
- A new thing that the changelog will name first, in a fragment of three
  lines of plain text, no row marker and no indent on any line, wrapped
  at 76 columns so the merged row fits in 79.
- An existing row.

### Fixed

- An existing fix.

### Security

- A report whose subsection does not exist yet, so the merge creates it
  at the end of the release block.
EOF

if ! diff -u "$work/expected.md" "$work/CHANGELOG.md"; then
    echo "the merged changelog does not match the expected text" >&2
    exit 1
fi
echo "merge-changelog: fixture ok"

# A fragment that starts with a front-matter block (a stray "---" first
# line, the ms: preamble one author added on 2026-10-22) is refused, and
# the refusal names the file.
cat >"$work/CHANGELOG.md" <<'EOF'
# Changelog

## [v0.1.0] - unreleased

### Added

- An existing row.
EOF
cat >"$work/changes/14.added.md" <<'EOF'
---
ms: "a one-liner that is not a format"
---

A body that must never merge behind that block.
EOF

if (cd "$work" && "$here/merge-changelog.sh" v0.1.0) >"$work/out.txt" 2>"$work/err.txt"; then
    echo "the merge accepted a fragment that starts with ---" >&2
    exit 1
fi
grep -q "changes/14.added.md" "$work/err.txt" || {
    echo "the refusal does not name the file; it said:" >&2
    cat "$work/err.txt" >&2
    exit 1
}

echo "merge-changelog: fixture ok"


# --check: the same rules, nothing merged. A fragment that fails the
# merge's own contract - front matter, a section the script would
# silently drop, an empty file, a row marker on the first line - must
# fail with the fragment named, and a clean fixture must pass.
check=$(mktemp -d)
trap 'rm -rf "$work" "$check"' EXIT
mkdir "$check/changes"
cat >"$check/CHANGELOG.md" <<'EOF'
## [v0.1.0] - unreleased
EOF

for bad in 'front-matter' 'bad-section' 'empty' 'row-marker' \
    'no-final-newline' 'blank-last-line'; do
    # Each case alone in changes/: a bad fragment left by the case before
    # would fail the check for its own reason, and pass this one.
    rm -f "$check"/changes/*
    case $bad in
        front-matter)
            printf -- '---\nms: "x"\n---\n\nA thing.\n' >"$check/changes/1.fixed.md"
            ;;
        bad-section)
            printf 'A thing.\n' >"$check/changes/1.bad.md"
            ;;
        empty)
            : >"$check/changes/1.added.md"
            ;;
        row-marker)
            printf -- '- A thing.\n' >"$check/changes/1.fixed.md"
            ;;
        no-final-newline)
            printf 'A thing.' >"$check/changes/1.fixed.md"
            ;;
        blank-last-line)
            printf 'A thing.\n\n' >"$check/changes/1.fixed.md"
            ;;
    esac
    if (cd "$check" && "$here/merge-changelog.sh" --check) >/dev/null 2>&1; then
        echo "--check accepted a $bad fragment" >&2
        exit 1
    fi
done

rm -rf "$check/changes"
mkdir "$check/changes"
printf 'A thing that passes every rule, no front matter, no row\nmarker on the first line, no indent on any line.\n' >"$check/changes/1.fixed.md"
# --check reports the work it did: one fragment checked, not "no
# fragments in changes/" (a silence that hides a full pass).
(cd "$check" && "$here/merge-changelog.sh" --check) >"$check/out.txt"
grep -q '^1 fragments pass the checks$' "$check/out.txt" || {
    echo "--check does not name the fragments it checked; it said:" >&2
    cat "$check/out.txt" >&2
    exit 1
}

# A missing changes/ passes too: the release fold deletes every
# fragment, git keeps no empty directory, so the tree after the fold
# has none - and --check ran there on the release branch (2026-10-25:
# 'find: changes: No such file or directory', set -e's pipefail, 9 red
# CI jobs). An empty changes/ passed already; the missing one died.
rm -rf "$check/changes"
(cd "$check" && "$here/merge-changelog.sh" --check) >"$check/out.txt"
grep -q '^0 fragments pass the checks$' "$check/out.txt" || {
    echo "an empty changes/ does not say so; it said:" >&2
    cat "$check/out.txt" >&2
    exit 1
}
echo "merge-changelog --check: fixture ok"

# Two fragments of one section, the first without a final newline: the
# merge must refuse it, not glue the second's row onto its last line
# ("- One (#1)- Two (#2)").
glue=$(mktemp -d)
trap 'rm -rf "$work" "$check" "$glue"' EXIT
mkdir "$glue/changes"
printf '## [v0.1.0] - unreleased\n' >"$glue/CHANGELOG.md"
printf 'One (#1)' >"$glue/changes/1.fixed.md"
printf 'Two (#2)\n' >"$glue/changes/2.fixed.md"
if (cd "$glue" && "$here/merge-changelog.sh" v0.1.0) >/dev/null 2>"$glue/err.txt"; then
    echo "the merge took a fragment with no final newline:" >&2
    cat "$glue/CHANGELOG.md" >&2
    exit 1
fi
grep -q 'does not end in a newline: changes/1.fixed.md' "$glue/err.txt" || {
    echo "the refusal does not name the rule and the file; it said:" >&2
    cat "$glue/err.txt" >&2
    exit 1
}

# A release with no heading in the changelog: the rows would land
# nowhere, so the merge must refuse before it changes anything - every
# fragment and CHANGELOG.md keep the checksum they had.
nohead=$(mktemp -d)
trap 'rm -rf "$work" "$check" "$glue" "$nohead"' EXIT
mkdir "$nohead/changes"
printf '## [v0.1.0] - unreleased\n' >"$nohead/CHANGELOG.md"
printf 'A fragment that must stay on the books.\n' >"$nohead/changes/1.fixed.md"
before=$(find "$nohead" -type f | sort | xargs cat | cksum)
if (cd "$nohead" && "$here/merge-changelog.sh" v0.2.0) >/dev/null 2>"$nohead/err.txt"; then
    echo "the merge ran for a release the changelog has no heading for" >&2
    exit 1
fi
rm "$nohead/err.txt"
after=$(find "$nohead" -type f | sort | xargs cat | cksum)
if [ "$before" != "$after" ]; then
    echo "the merge for a missing heading changed files:" >&2
    ls -R "$nohead" >&2
    exit 1
fi
echo "merge-changelog refusals: fixture ok"

# The argument is either --check or a release name: a flag nobody reads
# ("--dry-run" merged every fragment under "## [--dry-run]" and deleted
# them, 2026-10-24), a word like v1, or an empty string must be refused
# with exit 2 and must touch nothing - every fragment and CHANGELOG.md
# keep the checksum they had.
args=$(mktemp -d)
trap 'rm -rf "$work" "$check" "$args"' EXIT
mkdir "$args/changes"
printf 'A real fragment, still on the books.\n' >"$args/changes/1.fixed.md"
printf '## [v0.1.0] - unreleased\n' >"$args/CHANGELOG.md"
for bad in '--dry-run' 'v1' ''; do
    before=$(find "$args" -type f | sort | xargs cat | cksum)
    st=0
    (cd "$args" && "$here/merge-changelog.sh" "$bad") >/dev/null 2>&1 || st=$?
    if [ "$st" -eq 0 ]; then
        echo "the argument [$bad] was accepted; only --check or a release may be" >&2
        exit 1
    fi
    if [ "$st" -ne 2 ]; then
        echo "the argument [$bad] failed with status $st, not the usage exit 2" >&2
        exit 1
    fi
    after=$(find "$args" -type f | sort | xargs cat | cksum)
    if [ "$before" != "$after" ]; then
        echo "the argument [$bad] touched files before its refusal" >&2
        exit 1
    fi
done
echo "merge-changelog arguments: fixture ok"
