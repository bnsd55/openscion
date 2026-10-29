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

for bad in 'front-matter' 'bad-section' 'empty' 'row-marker'; do
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

# A fragment whose row the section already holds is skipped, not
# folded a second time: a merge that brings a folded fragment back
# must not double the row (2026-10-29: the release branch folded the
# admin-lines fragments twice, and the v0.1.0 refresh carried the
# doubles). The skip is said on stderr, the fragment is deleted, and
# a different fragment in the same fold still merges.
dup=$(mktemp -d)
trap 'rm -rf "$work" "$check" "$args" "$dup"' EXIT
mkdir "$dup/changes"
cat >"$dup/CHANGELOG.md" <<'EOF'
# Changelog

## [v0.1.0] - unreleased

The first release.

### Added

- A thing the changelog already holds, from a fragment that a
  merge brought back after an earlier fold.
EOF
printf 'A thing the changelog already holds, from a fragment that a\nmerge brought back after an earlier fold.\n' >"$dup/changes/1.added.md"
printf 'A different thing that must still merge beside the skipped\nfragment, so the skip is not a refusal of the whole fold.\n' >"$dup/changes/2.added.md"

(cd "$dup" && "$here/merge-changelog.sh" v0.1.0) >"$dup/out.txt" 2>"$dup/err.txt"
grep -q "skipped.*changes/1.added.md" "$dup/err.txt" || {
    echo "the fold does not say on stderr that it skipped the duplicate; it said:" >&2
    cat "$dup/err.txt" >&2
    exit 1
}
[ "$(grep -c '^- A thing the changelog already holds' "$dup/CHANGELOG.md")" -eq 1 ] || {
    echo "the duplicate fragment was folded a second time; the changelog holds:" >&2
    grep -n 'A thing the changelog' "$dup/CHANGELOG.md" >&2
    exit 1
}
[ "$(grep -c '^- A different thing' "$dup/CHANGELOG.md")" -eq 1 ] || {
    echo "the distinct fragment did not merge beside the skipped one" >&2
    exit 1
}
if [ -d "$dup/changes" ] && [ -n "$(ls -A "$dup/changes")" ]; then
    echo "the fold left fragments behind after the skip:" >&2
    ls "$dup/changes" >&2
    exit 1
fi
echo "merge-changelog skip: fixture ok"

# Exact text, after the merge's own normalisation, is the rule: a
# fragment whose first line matches an existing row but whose body
# differs is a new row, not a duplicate.
rm -rf "$dup/changes"
mkdir "$dup/changes"
cp "$dup/CHANGELOG.md" "$dup/CHANGELOG.kept"
printf 'A thing the changelog already holds, from a fragment that a\nmerge brought back with a different body than the row holds.\n' >"$dup/changes/3.added.md"
(cd "$dup" && "$here/merge-changelog.sh" v0.1.0) >"$dup/out.txt" 2>"$dup/err.txt"
if grep -q 'skipped' "$dup/err.txt"; then
    echo "a fragment whose body differs was skipped as a duplicate; stderr said:" >&2
    cat "$dup/err.txt" >&2
    exit 1
fi
[ "$(grep -c '^- A thing the changelog already holds' "$dup/CHANGELOG.md")" -eq 2 ] || {
    echo "the fragment with a different body did not fold; the changelog holds:" >&2
    grep -n 'A thing the changelog already holds' "$dup/CHANGELOG.md" >&2
    exit 1
}
echo "merge-changelog exact-text rule: fixture ok"
