On Postgres, a graph walk over a merge-heavy history could answer a
false `commit graph: no node for X` on a healthy store, and a repeated
subtree read could answer as missing: two of the backend's batch reads
emptied their reply map as they answered it, so when one batch named
the same ID twice (two merges sharing a side parent, or one tree under
two paths), the second ask found nothing. Both reads now answer one
entry per asked ID, in the order asked, as the metadata contract says.
No rows were lost; the reads lost them.
