`scn mount --help` states the mount's single-user rule, as
docs/threat-model.md says it does; before, only the start of a mount
said it. docker/README.md names `docker/certs`, where `docker/demo.sh`
writes the demo certificates, not `docker/demo/certs`, and
docs/cli-vs-git.md's sparse checkout row carries the `--into` that
`scn checkout` requires.
