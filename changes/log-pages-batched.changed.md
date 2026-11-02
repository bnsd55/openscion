On Postgres, a page of the log read one changeset per entry: a page of
64 entries paid 64 round trips, and a page for one path paid, per
entry, a read of the path history's value, of one node per part of the
path, and of the entry's changeset. The page without a path now rides
one statement, and the page with a path reads the chain of path
histories in batches and fetches the entries' bodies in batches, so a
page's round trips no longer grow with its entry count. The walk reads
the same nodes as before, and every budget, deadline check, and
corruption check stays where it was.
