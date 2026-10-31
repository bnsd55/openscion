`scion-daemon`'s "seeded the store from a snapshot" line counts only the
objects the store still holds once the seed ends. An object that a later
object of the same snapshot evicted to stay under `--max-bytes` was
counted as stored, so a snapshot larger than the store reported that it
all fit; it now counts as skipped.
