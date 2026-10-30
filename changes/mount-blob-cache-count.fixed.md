`scn mount` counts a file's bytes once in its in-memory cache when
two reads fetch the file at the same time, as NFS clients do on a
file's first read. Each racing read added the bytes again, so the
count drifted above what the cache held, the cache evicted too early,
and a drift past the cache's 128 MiB bound could leave its eviction
loop spinning on an empty cache with every read of the mount waiting.
