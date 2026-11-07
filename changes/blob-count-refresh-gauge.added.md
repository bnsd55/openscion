The blob-count refresh exports scion_blob_count_refresh_seconds, the
seconds its latest scan took, success or failure. The refresh spaces
itself by that duration (the next wait is at least 100x the scan), so
the scan stays near 1% of one read connection at any store size and
the gauge says how stale the count may be.
