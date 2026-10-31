A backup and restore runbook for a server that keeps serving:
the metadata snapshot first (`pg_dump`), the
blob files second, the store's UUID marker with the metadata; what the
backup promises, why gc must stay out of it, and a test that restores
a backup taken while writes land.
