The metadata schema moves only when you ask it to. `scion-server
migrate` runs the pending steps of the store's schema, one transaction
each, under the schema's lock; `migrate --check` prints the database's
version, this build's version, and the steps a migration would run,
and exits 3 when steps are pending (0 clean, 1 unreadable). A serve
refuses a database that is behind the build until it is started with
`--migrate`, and so does every other command that opens the store:
import, gc, fsck and derive name the migrate that moves it. A fresh,
empty database is still created by `serve` and `import`.
