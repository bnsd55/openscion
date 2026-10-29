Every promise in stability.md and compatibility.md now names what
enforces it, and the naming is checked: the ticket prefix's literal
bytes (`scion\0ticket\0v1\0`) are pinned by a unit test (the version
byte was, the prefix string was not - a rewrite would have passed
every test while every held ticket stopped verifying), and the
schema rows name the migration tests. The drills that named the
N-1 surfaces (the tagged-store ladder and the N-1 restore drill)
stand down for v0.1.0 and return with the first release tag, when
there is a release pair to prove.
