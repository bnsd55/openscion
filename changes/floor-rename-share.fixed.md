The memory floor counts the diffs' rename memory: the four rename
passes' 64 MiB bounds at once (256 MiB), and the paged diffs' shared
rename maps' 512 MiB cache, both missing from the constant. #856's
merge message said the floor gained the rename passes' share along
with the view memo's, and the floor's doc comment and
`MAX_RENAME_BYTES`'s repeated it, but `FLOOR` grew by the view share
only - a host at the floor could run four concurrent rename passes
and their cached maps into memory the start-up check never reserved.
The floor is 768 MiB higher, the start-up line names both shares with
the rest, and the stored rename maps' doc comments no longer carry
arithmetic from the entry-count bound the byte bound replaced.
