`scion-server import` aborted with an index panic on a git commit
whose author or committer time zone is shorter than `+hhmm` (`+1`, or
a bare `+`): the zone splitter assumed two digits after the sign. gix
accepts such a zone (the offset becomes zero), so a repo git itself
wrote could refuse the import with a raw `end byte index 2 is out of
bounds`. The import now refuses a malformed zone with the zone named,
and imports the rest as before.
