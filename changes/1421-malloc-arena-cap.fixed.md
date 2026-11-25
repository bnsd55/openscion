A long-lived serving process (`serve`, `scn mount`, `scion-cache`,
`scion-daemon`) caps its glibc malloc arenas at 2 at startup, before
any worker thread exists. `MALLOC_ARENA_MAX` in the environment wins
over the default; on a platform whose allocator is not glibc, the
process logs "not capped: the allocator is not glibc" - nothing is
capped there. Under thread contention glibc opens more arenas, each
keeping its own freed memory, while the live heap stays near 11 MB:
at 24k lands, 73.5 MB uncapped, 35.8 MB capped, p99 unchanged.
