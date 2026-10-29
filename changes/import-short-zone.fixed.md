`scion-server import` refuses a git commit whose time zone is shorter
than `+hhmm` (`+1`, or a bare `+`) instead of panicking: such a zone is
a date git cannot write, and the import now reports it like any other
identity it cannot keep.
