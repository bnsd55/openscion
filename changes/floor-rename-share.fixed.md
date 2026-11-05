The memory floor counts the diffs' rename share: four diffs' 64 MiB
rename bounds at once, 256 MiB the constant never added. #856's merge
message said the floor gained the share along with the view memo's, and
the floor's doc comment and `MAX_RENAME_BYTES`'s repeated it, but
`FLOOR` grew by the view share only - a host at the floor could run
four concurrent rename passes into memory the start-up check never
reserved. The floor is 256 MiB higher, and the start-up line names the
share with the rest.
