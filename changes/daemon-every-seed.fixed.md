`scion-daemon` with more than one `--seed` reads every file into a cold
store, as the flag's help ("Repeat for more") and docs/architecture.md
say. It checked for an empty store before each file, so the first
file's objects made it skip the rest ("the store holds objects; the
snapshot is not read"); it now checks once, before the first file.
