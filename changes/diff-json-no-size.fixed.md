`scn diff --json` no longer prints `"size": 0` for the old and new side
of every entry. A diff's file states name a blob, not a length, so the
reply carries no size and the zero was never true; the field is gone,
as it went from `scn show --json` in #194. A script that read it gets
no field instead of a wrong zero (docs/stability.md: a 0.x minor may
remove a `--json` field).
