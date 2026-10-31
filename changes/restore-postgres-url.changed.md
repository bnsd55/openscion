`scion-server restore` resolved the Postgres URL from the command
line alone: a URL given by `--postgres-url-file` or
`SCION_POSTGRES_URL` was ignored, and the restore silently built a
SQLite store beside the Postgres one the operator meant. Restore now
resolves the URL the way serve does, from every source, and
`--postgres-ca` without a URL from any source is refused with the
three sources named, instead of a clap "requires" error that cannot
fire for the file and environment forms.
