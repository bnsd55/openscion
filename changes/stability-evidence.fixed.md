Every promise in stability.md and compatibility.md now names what
enforces it, and the naming is checked: the ticket prefix's literal
bytes (`scion\0ticket\0v1\0`) are pinned by a unit test (the version
byte was, the prefix string was not - a rewrite would have passed
every test while every held ticket stopped verifying), and the
schema rows name the drills that run today instead of the issues
that promised them: the tagged-store ladder is store-upgrade.yml
(pinned stand-in commits until a tag exists) and the N-1 restore
drill is backup-n1.yml (C117).
