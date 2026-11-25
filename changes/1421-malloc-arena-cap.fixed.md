A long-lived serving process caps its glibc malloc arenas at 2 at
startup, before any worker thread exists (`MALLOC_ARENA_MAX` in the
environment wins over the default; non-glibc targets compile to
nothing). The 60-minute soak that stepped its RSS to 152 MB sits at
35.8 MB with the cap, and the p99 latencies do not move.
