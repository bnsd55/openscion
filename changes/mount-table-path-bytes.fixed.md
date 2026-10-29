`scn mount` records a directory or overlay path that is not UTF-8
with its exact bytes, in a `_b64` sibling as scn's JSON output does,
so `scn unmount` and `scn commit --from-mount` find the mount. The
session file kept such a path with its bad bytes replaced, and no
later command matched it. A UTF-8 path is written as before, and an
older session file reads as before.
