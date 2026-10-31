The server's global flags (`--data`, `--postgres-url`,
`--postgres-url-file`, `--postgres-ca`) parse after the subcommand too:
`scion-server fsck --data D` is the same call as
`scion-server --data D fsck`. Before, the flags parsed only before the
subcommand, and the after-position gave a bare usage error with no
hint. The rules the flags already carry (the URL file and URL conflict,
the CA requiring a URL) hold in both positions; the token flags of `scn`
are unchanged and still parse before the subcommand only.
