The metrics surface gains one counter: `scion_accept_errors_total`, the
accepts the listener failed (most often the process out of file
descriptors). After a failed accept the accept task waits 10 ms before
it accepts again, doubling to 1 s and reset by a success, so a failure
that persists cannot spin a core on accepts that all fail at once; and
its warning line is rate-limited to one per 10 seconds, with the count
the line covers.
