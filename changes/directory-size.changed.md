docs/operations.md gains a "Directory size" section: a land into a
directory stores and re-encodes the directory's whole tree, so the
land's time, the server's memory and the stored tree bytes grow with
the directory's entries, at about 41 stored tree bytes per entry per
land. Operators keep a directory that gets frequent lands under about
10k entries and split large drop points into subdirectories (#1428).
