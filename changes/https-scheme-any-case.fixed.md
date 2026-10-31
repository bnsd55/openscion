`scn`, `scion-cache` and `scion-daemon` treat an `HTTPS://` address (a
scheme in any case) as https: they refuse it without `--tls-ca`, and
with one they connect over TLS. The check was case-sensitive, so an
uppercase scheme skipped the CA rule and the TLS configuration, and the
connection failed with a misleading "is a scion server running" hint.
