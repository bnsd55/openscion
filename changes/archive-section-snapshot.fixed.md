# Archive: a section's reads hold one snapshot

An export's metadata sections claim one read snapshot, and each
section reads inside one explicit read transaction
(`Backend::read_snapshot`): REPEATABLE READ for its statements, so a
land that commits between a section's changesets query and its refs
query leaves the archive holding exactly the ref the snapshot named.
The cost is the one the section's snapshot always names: the log
cannot restart while the section reads.
