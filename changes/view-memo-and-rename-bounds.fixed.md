A view derivation no longer holds its whole memo in RAM until its one
write: past 4 MiB it stores the pending trees and rows mid-walk in
their own short writes, trees before rows and the root's row last, and
past its 60 MiB dedupe-map share it clears the map (the store's rows
dedupe what the map did). A store runs at most two derivations at once
and a derivation beyond that waits under its call's deadline with no
store lock and no write lane held. A diff's rename pass is bounded by
bytes, not entries: past 64 MiB of walked entries it finds no renames
and says so, where the old entry bound let a long-path diff hold far
more than the entries' count suggested. The memory floor counts both
shares.
