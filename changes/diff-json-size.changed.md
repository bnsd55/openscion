`scn diff --json` printed `"size": 0` for the old and new side of
every file and symlink, whatever the file's length: the diff reply
carries kind, ID and the executable flag, not a length, and the JSON
printed a default. The field is gone from entries whose side carries
no size, so a script that read it no longer reads a number the server
never sent.
