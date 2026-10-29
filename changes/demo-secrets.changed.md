The docker demo's database password and clients' token were committed
in `docker/compose.yml` (`postgres:dev`, token `dev`), though the
header said secrets are never committed. `docker/demo.sh` now makes
both with openssl at every run and exports them next to the ticket
public key; compose reads them with required interpolation and refuses
to start without the script. A second run of the demo carries
different secrets than the first, and the repository carries none.
