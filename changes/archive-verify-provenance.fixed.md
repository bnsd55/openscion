The archive reader names a provenance row whose changeset the archive
does not hold. The format's provenance rule (each row's changeset is
one the archive's `changesets.bin` MUST hold) was checked for refs and
pending lands but not for provenance: such an archive verified clean,
and its restore died later on the store's foreign key - after the
store was open, with an error that named the key, not the archive. The
reader now names the row the way it names a ref or a pending land that
points past; the restore refuses before it writes a row. A restore of
such an archive already left the target empty on main (the staging
never published), so the fix changes the reader's verdict, not the
restore's choreography.
