A fresh store's first open logs what happened, and the temp-folder
sweep claims only what it knows. A creation logged nine fake migration
lines ("migrating the metadata schema 0->9", one per step) for a
database that moved nothing; it now logs one line, "the metadata schema
was created to_version=9". The sweep's "removed temp folders that a
crash left behind" fired for folders whose process is gone whatever the
way it went - a crash's leftovers or a one-shot import's clean exit;
the line now says that. A store with data keeps its whole per-step
migration story unchanged (`crates/scion-server/tests/migrate_log.rs`).
